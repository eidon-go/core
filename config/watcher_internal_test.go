package config

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fastBackoff() Option {
	return WithWatchBackoff(time.Millisecond, 5*time.Millisecond)
}

func quiet() Option { return WithLogger(slog.New(slog.DiscardHandler)) }

func noEnv() Option { return WithEnvLookup(nil) }

// startRun runs cfg in the background and registers a cleanup that shuts it
// down and checks that Run returned.
func startRun(t *testing.T, cfg *Config) {
	t.Helper()

	done := make(chan error, 1)

	go func() { done <- cfg.Run(context.Background()) }()

	t.Cleanup(func() {
		require.NoError(t, cfg.Shutdown(context.Background()))

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Error("Run did not return after Shutdown")
		}
	})
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	require.Eventually(t, cond, 2*time.Second, 5*time.Millisecond)
}

func TestKV_StaticKeys(t *testing.T) {
	t.Parallel()

	kv := newFakeKV()
	kv.SetData("/a.json", []byte(`{"x": 1, "y": "a"}`))
	kv.SetData("/b.json", []byte(`{"x": 2}`))

	cfg, err := New(t.Context(), WithKV(kv), WithKeys("/a.json", "/b.json"), noEnv())
	require.NoError(t, err)

	assert.Equal(t, 2, Get[int](cfg, "x"))
	assert.Equal(t, "a", Get[string](cfg, "y"))
	assert.Equal(t, 0, kv.CloseCount(), "Config owns the KV until Shutdown")

	require.NoError(t, cfg.Shutdown(t.Context()))
	require.NoError(t, cfg.Shutdown(t.Context()))
	assert.Equal(t, 1, kv.CloseCount())
}

func TestKV_NewFailureClosesKV(t *testing.T) {
	t.Parallel()

	kv := newFakeKV()

	_, err := New(t.Context(), WithKV(kv), WithKeys("/missing.json"), noEnv())
	require.ErrorIs(t, err, ErrKVKeyNotFound)
	assert.Equal(t, 1, kv.CloseCount())
}

func TestKV_DuplicateKeys(t *testing.T) {
	t.Parallel()

	for _, opts := range [][]Option{
		{WithKeys("/a.json"), WithWatchedKeys("/a.json")},
		{WithWatchedKeys("/a.json", "/a.json")},
		{WithKeys("/a.json", "/a.json")},
	} {
		kv := newFakeKV()
		kv.SetData("/a.json", []byte(`{}`))

		_, err := New(t.Context(), append(opts, WithKV(kv), noEnv())...)
		require.ErrorIs(t, err, ErrDuplicateKey)
		assert.Equal(t, 1, kv.CloseCount())
	}
}

func TestKV_Timeout(t *testing.T) {
	t.Parallel()

	_, err := New(t.Context(), WithKV(blockingKV{}), WithKeys("/a.json"), WithKVTimeout(10*time.Millisecond), noEnv())
	require.ErrorIs(t, err, context.DeadlineExceeded)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	_, err = New(ctx, WithKV(blockingKV{}), WithKeys("/a.json"), WithKVTimeout(0), noEnv())
	require.ErrorIs(t, err, context.DeadlineExceeded, "no own timeout: bounded by the caller's ctx")
}

type blockingKV struct{}

func (blockingKV) Get(ctx context.Context, _ string) ([]byte, error) {
	<-ctx.Done()

	return nil, ctx.Err()
}

func (blockingKV) Watch(context.Context, string) <-chan KVEvent { return nil }

func (blockingKV) Close() error { return nil }

func TestKV_WatchedKeys(t *testing.T) {
	t.Parallel()

	kv := newFakeKV()
	kv.SetData("/static.json", []byte(`{"limit": 1, "name": "static", "level": "debug"}`))
	kv.SetData("/flags.json", []byte(`{"limit": 5, "old": true}`))
	kv.SetData("/more.json", []byte(`{"extra": 1}`))

	logs := &logRecorder{}

	cfg, err := New(t.Context(),
		WithKV(kv), WithKeys("/static.json"), WithWatchedKeys("/flags.json", "/more.json"),
		WithLogger(slog.New(logs)), fastBackoff(),
		WithEnvLookup(func(name string) (string, bool) { return "info", name == "LEVEL" }),
	)
	require.NoError(t, err)

	limit := Live[int](cfg, "limit")
	assert.Equal(t, 5, limit(), "watched keys override static keys")
	assert.True(t, Get[bool](cfg, "old"))

	startRun(t, cfg)
	require.True(t, kv.WaitForWatcher("/flags.json", 1, time.Second))
	require.True(t, kv.WaitForWatcher("/more.json", 1, time.Second))

	kv.Set("/flags.json", []byte(`{"limit": 9, "level": "error"}`))
	eventually(t, func() bool { return limit() == 9 })
	assert.False(t, Get[bool](cfg, "old"), "a key dropped from the document disappears")
	assert.Equal(t, "static", Get[string](cfg, "name"))
	assert.Equal(t, "info", Get[string](cfg, "level"), "an env override pins the key")

	kv.Set("/flags.json", []byte(`{"limit": null}`))
	eventually(t, func() bool { return limit() == 1 })

	kv.Set("/flags.json", []byte(`{"limit": 4}`))
	eventually(t, func() bool { return limit() == 4 })

	kv.Set("/flags.json", []byte(`{"limit": [`))
	logs.waitFor(t, "config: invalid watched document, keeping previous values")
	assert.Equal(t, 4, limit(), "an invalid document keeps the previous values")

	kv.Set("/flags.json", []byte(`{"limit": 7}`))
	eventually(t, func() bool { return limit() == 7 })

	kv.Delete("/flags.json")
	eventually(t, func() bool { return limit() == 1 })
	assert.Equal(t, 1, Get[int](cfg, "extra"))
}

// N3: a change made between New and Run (while other components start) must
// not be lost: the watch's initial state carries it.
func TestKV_ChangeBetweenNewAndRun(t *testing.T) {
	t.Parallel()

	kv := newFakeKV()
	kv.SetData("/flags.json", []byte(`{"v": 1}`))

	cfg, err := New(t.Context(), WithKV(kv), WithWatchedKeys("/flags.json"), noEnv(), quiet())
	require.NoError(t, err)

	kv.SetData("/flags.json", []byte(`{"v": 2}`)) // no watcher yet: no event

	startRun(t, cfg)
	eventually(t, func() bool { return Get[int](cfg, "v") == 2 })
}

func TestKV_WatchReconnectsAndCatchesUp(t *testing.T) {
	t.Parallel()

	kv := newFakeKV()
	kv.SetData("/flags.json", []byte(`{"v": 1}`))

	cfg, err := New(t.Context(), WithKV(kv), WithWatchedKeys("/flags.json"), noEnv(), quiet(), fastBackoff())
	require.NoError(t, err)

	startRun(t, cfg)
	require.True(t, kv.WaitForWatcher("/flags.json", 1, time.Second))

	// The value changes without an event, then the stream fails: the new
	// watch starts with the changed value.
	kv.SetData("/flags.json", []byte(`{"v": 2}`))
	kv.SetError("/flags.json", errors.New("connection lost"))

	eventually(t, func() bool { return Get[int](cfg, "v") == 2 })
	require.True(t, kv.WaitForWatcher("/flags.json", 1, time.Second), "watch is re-established")

	kv.Set("/flags.json", []byte(`{"v": 3}`))
	eventually(t, func() bool { return Get[int](cfg, "v") == 3 })

	// Deleted while disconnected: the new watch starts with a delete.
	kv.DeleteData("/flags.json")
	kv.SetError("/flags.json", errors.New("connection lost"))
	eventually(t, func() bool { return Get[int](cfg, "v") == 0 })
}

// The watcher never reads a key itself after New: the initial state of each
// watch brings the values up to date, so no read can race the watch and put
// an older value back.
func TestKV_WatcherDoesNotReadAfterNew(t *testing.T) {
	t.Parallel()

	kv := newFakeKV()
	kv.SetData("/flags.json", []byte(`{"v": 1}`))

	cfg, err := New(t.Context(), WithKV(kv), WithWatchedKeys("/flags.json"), noEnv(), quiet(), fastBackoff())
	require.NoError(t, err)

	reads := kv.GetCalls()

	startRun(t, cfg)
	require.True(t, kv.WaitForWatcher("/flags.json", 1, time.Second))

	kv.Set("/flags.json", []byte(`{"v": 2}`))
	eventually(t, func() bool { return Get[int](cfg, "v") == 2 })

	kv.SetData("/flags.json", []byte(`{"v": 3}`))
	kv.SetError("/flags.json", errors.New("connection lost"))
	eventually(t, func() bool { return Get[int](cfg, "v") == 3 })

	assert.Equal(t, reads, kv.GetCalls())
}

// Watches that fail right after their initial state are not healthy: the
// reconnect delay keeps growing instead of starting over every time.
func TestKV_BackoffGrowsWhenWatchesOnlyReportState(t *testing.T) {
	t.Parallel()

	kv := &scriptedKV{fakeKV: newFakeKV(), script: func(state KVEvent) []KVEvent {
		return []KVEvent{state, {Err: errors.New("cancelled by the server")}}
	}}
	kv.SetData("/flags.json", []byte(`{"v": 1}`))

	logs := &logRecorder{}

	cfg, err := New(t.Context(), WithKV(kv), WithWatchedKeys("/flags.json"), noEnv(),
		WithLogger(slog.New(logs)), WithWatchBackoff(time.Millisecond, 64*time.Millisecond))
	require.NoError(t, err)

	startRun(t, cfg)

	const reconnects = 5

	eventually(t, func() bool { return len(logs.records("config: watch ended, reconnecting")) >= reconnects })

	for i, rec := range logs.records("config: watch ended, reconnecting")[:reconnects] {
		want := min(time.Millisecond<<i, 64*time.Millisecond)
		got := recordDuration(t, rec, "delay")
		assert.GreaterOrEqual(t, got, time.Duration(float64(want)*(1-watchJitter)), "reconnect %d", i+1)
		assert.LessOrEqual(t, got, want, "reconnect %d", i+1)
	}
}

// A first event that is not a state breaks the KV.Watch contract: the watch
// is reopened instead of being trusted.
func TestKV_WatchWithoutStateAsFirstEventIsReopened(t *testing.T) {
	t.Parallel()

	kv := &scriptedKV{fakeKV: newFakeKV(), script: func(KVEvent) []KVEvent {
		return []KVEvent{{}} // no Type, no Err
	}}
	kv.SetData("/flags.json", []byte(`{"v": 1}`))

	logs := &logRecorder{}

	cfg, err := New(t.Context(), WithKV(kv), WithWatchedKeys("/flags.json"), noEnv(),
		WithLogger(slog.New(logs)), fastBackoff())
	require.NoError(t, err)

	startRun(t, cfg)
	logs.waitFor(t, "config: watch did not start with the key's state")
	eventually(t, func() bool { return kv.WatchCalls() >= 3 })
}

// scriptedKV answers every Watch with the events of script (given the key's
// state as an initial event) and then closes the channel.
type scriptedKV struct {
	*fakeKV

	script func(state KVEvent) []KVEvent
	calls  atomic.Int32
}

func (s *scriptedKV) Watch(_ context.Context, key string) <-chan KVEvent {
	s.calls.Add(1)

	s.mu.Lock()

	state := KVEvent{Type: KVEventDelete}
	if v, ok := s.data[key]; ok {
		state = KVEvent{Type: KVEventPut, Value: bytes.Clone(v)}
	}
	s.mu.Unlock()

	events := s.script(state)
	out := make(chan KVEvent, len(events))

	for _, ev := range events {
		out <- ev
	}

	close(out)

	return out
}

func (s *scriptedKV) WatchCalls() int { return int(s.calls.Load()) }

// logRecorder is a slog.Handler that keeps every record, so tests can wait
// for the watcher to log something instead of sleeping.
type logRecorder struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (*logRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (l *logRecorder) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.recs = append(l.recs, r.Clone())

	return nil
}

func (l *logRecorder) WithAttrs([]slog.Attr) slog.Handler { return l }

func (l *logRecorder) WithGroup(string) slog.Handler { return l }

func (l *logRecorder) records(msg string) []slog.Record {
	l.mu.Lock()
	defer l.mu.Unlock()

	var out []slog.Record

	for _, r := range l.recs {
		if r.Message == msg {
			out = append(out, r)
		}
	}

	return out
}

func (l *logRecorder) waitFor(t *testing.T, msg string) {
	t.Helper()
	eventually(t, func() bool { return len(l.records(msg)) > 0 })
}

func recordDuration(t *testing.T, r slog.Record, key string) time.Duration {
	t.Helper()

	var (
		d     time.Duration
		found bool
	)

	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			d, found = a.Value.Duration(), true

			return false
		}

		return true
	})

	require.True(t, found, "no %q attribute", key)

	return d
}

func TestKV_NilWatchChannel(t *testing.T) {
	t.Parallel()

	kv := &nilWatchKV{fakeKV: newFakeKV()}
	kv.SetData("/flags.json", []byte(`{"v": 1}`))

	logs := &logRecorder{}

	cfg, err := New(t.Context(), WithKV(kv), WithWatchedKeys("/flags.json"), noEnv(),
		WithLogger(slog.New(logs)), fastBackoff())
	require.NoError(t, err)

	done := make(chan error, 1)

	go func() { done <- cfg.Run(t.Context()) }()

	logs.waitFor(t, "config: KV returned no watch channel")

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	require.NoError(t, cfg.Shutdown(ctx), "a nil watch channel must not hang the watcher")
	require.NoError(t, <-done)
	assert.Equal(t, 1, kv.CloseCount())
}

type nilWatchKV struct{ *fakeKV }

func (*nilWatchKV) Watch(context.Context, string) <-chan KVEvent { return nil }

// A watch that does not report the key's state within the KV timeout is
// reopened: the values may be stale.
func TestKV_WatchWithoutInitialStateIsReopened(t *testing.T) {
	t.Parallel()

	kv := newFakeKV()
	kv.SetData("/flags.json", []byte(`{"v": 1}`))
	kv.silent = true

	cfg, err := New(t.Context(), WithKV(kv), WithWatchedKeys("/flags.json"),
		WithKVTimeout(5*time.Millisecond), noEnv(), quiet(), fastBackoff())
	require.NoError(t, err)

	startRun(t, cfg)
	eventually(t, func() bool { return kv.WatchCalls() >= 3 })
	assert.Equal(t, 1, Get[int](cfg, "v"), "values from New stay")
}

// An initial state that is an error (the backend could not read the key)
// ends the stream; the next watch catches up.
func TestKV_WatchInitialErrorIsRetried(t *testing.T) {
	t.Parallel()

	kv := &initialErrorKV{fakeKV: newFakeKV(), failures: 2}
	kv.SetData("/flags.json", []byte(`{"v": 1}`))

	cfg, err := New(t.Context(), WithKV(kv), WithWatchedKeys("/flags.json"), noEnv(), quiet(), fastBackoff())
	require.NoError(t, err)

	kv.SetData("/flags.json", []byte(`{"v": 2}`))

	startRun(t, cfg)
	eventually(t, func() bool { return Get[int](cfg, "v") == 2 })
	assert.GreaterOrEqual(t, kv.WatchCalls(), 3)
}

// initialErrorKV fails the first watches with an error as their first event.
type initialErrorKV struct {
	*fakeKV

	mu       sync.Mutex
	failures int
}

func (f *initialErrorKV) Watch(ctx context.Context, key string) <-chan KVEvent {
	f.mu.Lock()
	fail := f.failures > 0
	f.failures--
	f.mu.Unlock()

	ch := f.fakeKV.Watch(ctx, key)
	if !fail {
		return ch
	}

	out := make(chan KVEvent, 1)
	out <- KVEvent{Err: errors.New("read failed")}

	close(out)

	return out
}

func TestBackoff_Resets(t *testing.T) {
	t.Parallel()

	b := backoff{initial: time.Second, max: 30 * time.Second}

	tests := []struct {
		name    string
		changed bool
		lasted  time.Duration
		want    bool
	}{
		{name: "initial state only, short stream", lasted: time.Second, want: false},
		{name: "a change after the initial state", changed: true, lasted: time.Millisecond, want: true},
		{name: "open for the maximum delay", lasted: 30 * time.Second, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, b.resets(tt.changed, tt.lasted))
		})
	}
}

func TestKV_ShutdownLifecycle(t *testing.T) {
	t.Parallel()

	kv := newFakeKV()
	kv.SetData("/flags.json", []byte(`{"v": 1}`))

	cfg, err := New(t.Context(), WithKV(kv), WithWatchedKeys("/flags.json"), noEnv(), quiet())
	require.NoError(t, err)

	done := make(chan error, 1)

	go func() { done <- cfg.Run(t.Context()) }()

	require.True(t, kv.WaitForWatcher("/flags.json", 1, time.Second))
	require.ErrorIs(t, cfg.Run(t.Context()), ErrAlreadyRunning)
	require.NoError(t, cfg.Shutdown(t.Context()))
	require.NoError(t, <-done)
	assert.Equal(t, 1, kv.CloseCount())
	assert.Equal(t, 1, Get[int](cfg, "v"), "values stay readable after Shutdown")
	require.NoError(t, cfg.Run(t.Context()), "Run after Shutdown returns at once")
}

func TestKV_ShutdownBeforeRun(t *testing.T) {
	t.Parallel()

	kv := newFakeKV()
	kv.SetData("/flags.json", []byte(`{"v": 1}`))

	cfg, err := New(t.Context(), WithKV(kv), WithWatchedKeys("/flags.json"), noEnv(), quiet())
	require.NoError(t, err)

	require.NoError(t, cfg.Shutdown(t.Context()))
	assert.Equal(t, 1, kv.CloseCount())
	require.NoError(t, cfg.Run(t.Context()))
	assert.Equal(t, 0, kv.WatcherCount("/flags.json"), "no watch is opened after Shutdown")
}

// Run and Shutdown racing must never close the KV while the watcher uses it.
func TestKV_RunShutdownRace(t *testing.T) {
	t.Parallel()

	for range 50 {
		kv := newFakeKV()
		kv.SetData("/flags.json", []byte(`{"v": 1}`))

		cfg, err := New(t.Context(), WithKV(kv), WithWatchedKeys("/flags.json"), noEnv(), quiet())
		require.NoError(t, err)

		var wg sync.WaitGroup

		wg.Go(func() { _ = cfg.Run(t.Context()) })
		wg.Go(func() { _ = cfg.Shutdown(t.Context()) })
		wg.Wait()

		require.NoError(t, cfg.Shutdown(t.Context()))
		assert.Equal(t, 1, kv.CloseCount())
		assert.Equal(t, 0, kv.WatcherCount("/flags.json"))
	}
}

// Readers see consistent snapshots while the watcher publishes.
func TestKV_ConcurrentReadsDuringUpdates(t *testing.T) {
	t.Parallel()

	kv := newFakeKV()
	kv.SetData("/flags.json", []byte(`{"a": 0, "b": 0}`))

	cfg, err := New(t.Context(), WithKV(kv), WithWatchedKeys("/flags.json"), noEnv(), quiet())
	require.NoError(t, err)

	startRun(t, cfg)
	require.True(t, kv.WaitForWatcher("/flags.json", 1, time.Second))

	var wg sync.WaitGroup

	wg.Go(func() {
		for i := range 200 {
			kv.Set("/flags.json", []byte(`{"a": `+strconv.Itoa(i)+`, "b": `+strconv.Itoa(i)+`}`))
		}
	})

	for range 4 {
		wg.Go(func() {
			for range 1000 {
				_ = Get[int](cfg, "a")
				_ = Live[int](cfg, "b")()
			}
		})
	}

	wg.Wait()
	eventually(t, func() bool { return Get[int](cfg, "a") == 199 && Get[int](cfg, "b") == 199 })
}

func TestBackoff(t *testing.T) {
	t.Parallel()

	b := backoff{initial: 100 * time.Millisecond, max: time.Second}

	for failures, want := range map[int]time.Duration{
		1:    100 * time.Millisecond,
		2:    200 * time.Millisecond,
		3:    400 * time.Millisecond,
		5:    time.Second,
		1000: time.Second,
	} {
		d := b.delay(failures)
		assert.LessOrEqual(t, d, want, "failures=%d", failures)
		assert.GreaterOrEqual(t, d, time.Duration(float64(want)*(1-watchJitter)), "failures=%d", failures)
	}
}
