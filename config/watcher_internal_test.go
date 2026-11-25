package config

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/retry"
)

func fastBackoff() Option {
	return WithWatchBackoff(
		retry.Policy{InitialDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond},
	)
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
	kv.SetData("/a.yaml", []byte("x: 1\ny: a\n"))
	kv.SetData("/b.json", []byte(`{"x": 2}`))

	cfg, err := New(t.Context(), WithKV(kv), WithKeys("/a.yaml", "/b.json"), noEnv())
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

	_, err := New(t.Context(), WithKV(kv), WithKeys("/missing.yaml"), noEnv())
	require.ErrorIs(t, err, ErrKVKeyNotFound)
	assert.Equal(t, 1, kv.CloseCount())
}

func TestKV_Timeout(t *testing.T) {
	t.Parallel()

	_, err := New(t.Context(), WithKV(blockingKV{}), WithKeys("/a.yaml"), WithKVTimeout(10*time.Millisecond), noEnv())
	require.ErrorIs(t, err, context.DeadlineExceeded)
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
	kv.SetData("/static.yaml", []byte("limit: 1\nname: static\n"))
	kv.SetData("/flags.yaml", []byte("limit: 5\nold: yes\n"))
	kv.SetData("/more.yaml", []byte("extra: 1\n"))

	cfg, err := New(t.Context(),
		WithKV(kv), WithKeys("/static.yaml"), WithWatchedKeys("/flags.yaml", "/more.yaml"),
		noEnv(), quiet(), fastBackoff())
	require.NoError(t, err)

	limit := Live[int](cfg, "limit")
	assert.Equal(t, 5, limit(), "watched keys override static keys")
	assert.True(t, Get[bool](cfg, "old"))

	startRun(t, cfg)
	require.True(t, kv.WaitForWatcher("/flags.yaml", 1, time.Second))
	require.True(t, kv.WaitForWatcher("/more.yaml", 1, time.Second))

	kv.Set("/flags.yaml", []byte("limit: 9\n"))
	eventually(t, func() bool { return limit() == 9 })
	assert.False(t, Get[bool](cfg, "old"), "a key dropped from the document disappears")
	assert.Equal(t, "static", Get[string](cfg, "name"))

	kv.Set("/flags.yaml", []byte("limit: [broken\n"))
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, 9, limit(), "an invalid document keeps the previous values")

	kv.Delete("/flags.yaml")
	eventually(t, func() bool { return limit() == 1 })
	assert.Equal(t, 1, Get[int](cfg, "extra"))
}

func TestKV_WatchReconnectsAndResyncs(t *testing.T) {
	t.Parallel()

	kv := newFakeKV()
	kv.SetData("/flags.yaml", []byte("v: 1\n"))

	cfg, err := New(t.Context(), WithKV(kv), WithWatchedKeys("/flags.yaml"), noEnv(), quiet(), fastBackoff())
	require.NoError(t, err)

	startRun(t, cfg)
	require.True(t, kv.WaitForWatcher("/flags.yaml", 1, time.Second))

	// The stream fails; meanwhile the value changes without an event.
	kv.SetError("/flags.yaml", errors.New("connection lost"))
	kv.SetData("/flags.yaml", []byte("v: 2\n"))

	eventually(t, func() bool { return Get[int](cfg, "v") == 2 })
	require.True(t, kv.WaitForWatcher("/flags.yaml", 1, time.Second), "watch is re-established")

	kv.Set("/flags.yaml", []byte("v: 3\n"))
	eventually(t, func() bool { return Get[int](cfg, "v") == 3 })
}

func TestKV_ShutdownClosesKVAfterWatchers(t *testing.T) {
	t.Parallel()

	kv := newFakeKV()
	kv.SetData("/flags.yaml", []byte("v: 1\n"))

	cfg, err := New(t.Context(), WithKV(kv), WithWatchedKeys("/flags.yaml"), noEnv(), quiet())
	require.NoError(t, err)

	done := make(chan error, 1)

	go func() { done <- cfg.Run(t.Context()) }()

	require.True(t, kv.WaitForWatcher("/flags.yaml", 1, time.Second))
	require.NoError(t, cfg.Shutdown(t.Context()))
	require.NoError(t, <-done)
	assert.Equal(t, 1, kv.CloseCount())
	assert.Equal(t, 1, Get[int](cfg, "v"), "values stay readable after Shutdown")
	require.Error(t, cfg.Run(t.Context()), "Run may be called once")
}
