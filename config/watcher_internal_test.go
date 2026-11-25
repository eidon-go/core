package config

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
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

	cfg, err := New(t.Context(),
		WithKV(kv), WithKeys("/static.json"), WithWatchedKeys("/flags.json", "/more.json"),
		quiet(), fastBackoff(),
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

	kv.Set("/flags.json", []byte(`{"limit": [`))
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, 1, limit(), "an invalid document keeps the previous values")

	kv.Set("/flags.json", []byte(`{"limit": 7}`))
	eventually(t, func() bool { return limit() == 7 })

	kv.Delete("/flags.json")
	eventually(t, func() bool { return limit() == 1 })
	assert.Equal(t, 1, Get[int](cfg, "extra"))
}

// N3: a change made between New and Run (while other components start) must
// not be lost, even with a backend that only delivers changes made after
// Watch.
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

func TestKV_WatchReconnectsAndResyncs(t *testing.T) {
	t.Parallel()

	kv := newFakeKV()
	kv.SetData("/flags.json", []byte(`{"v": 1}`))

	cfg, err := New(t.Context(), WithKV(kv), WithWatchedKeys("/flags.json"), noEnv(), quiet(), fastBackoff())
	require.NoError(t, err)

	startRun(t, cfg)
	require.True(t, kv.WaitForWatcher("/flags.json", 1, time.Second))

	// The stream fails; meanwhile the value changes without an event.
	kv.SetError("/flags.json", errors.New("connection lost"))
	kv.SetData("/flags.json", []byte(`{"v": 2}`))

	eventually(t, func() bool { return Get[int](cfg, "v") == 2 })
	require.True(t, kv.WaitForWatcher("/flags.json", 1, time.Second), "watch is re-established")

	kv.Set("/flags.json", []byte(`{"v": 3}`))
	eventually(t, func() bool { return Get[int](cfg, "v") == 3 })

	// Deleted while disconnected: the resync sees "not found", like a delete.
	kv.SetError("/flags.json", errors.New("connection lost"))
	kv.Delete("/flags.json")
	eventually(t, func() bool { return Get[int](cfg, "v") == 0 })
}

func TestKV_NilWatchChannel(t *testing.T) {
	t.Parallel()

	kv := &nilWatchKV{fakeKV: newFakeKV()}
	kv.SetData("/flags.json", []byte(`{"v": 1}`))

	cfg, err := New(t.Context(), WithKV(kv), WithWatchedKeys("/flags.json"), noEnv(), quiet(), fastBackoff())
	require.NoError(t, err)

	done := make(chan error, 1)

	go func() { done <- cfg.Run(t.Context()) }()

	time.Sleep(20 * time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	require.NoError(t, cfg.Shutdown(ctx), "a nil watch channel must not hang the watcher")
	require.NoError(t, <-done)
	assert.Equal(t, 1, kv.CloseCount())
}

type nilWatchKV struct{ *fakeKV }

func (*nilWatchKV) Watch(context.Context, string) <-chan KVEvent { return nil }

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
