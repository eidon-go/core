package config

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runConfig runs cfg until the test ends and waits for the watch of key.
func runConfig(t *testing.T, cfg *Config, kv *fakeKV, key string) {
	t.Helper()

	done := make(chan error, 1)

	go func() { done <- cfg.Run(t.Context()) }()

	t.Cleanup(func() {
		require.NoError(t, cfg.Shutdown(context.Background()))
		require.NoError(t, <-done)
	})

	require.True(t, kv.WaitForWatcher(key, 1, time.Second))
}

func receive(t *testing.T, ch <-chan string) string {
	t.Helper()

	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("no OnChange call")

		return ""
	}
}

func TestConfig_OnChange(t *testing.T) {
	t.Parallel()

	kv := newFakeKV()
	kv.SetData("/flags.json", []byte(`{"limits": {"rps": 10}, "banner": "a"}`))

	cfg, err := New(t.Context(), WithKV(kv), WithWatchedKeys("/flags.json"), noEnv(), quiet())
	require.NoError(t, err)

	calls := make(chan string, 16)

	cfg.OnChange("", func() { panic("boom") }) // recovered: the others still run
	cfg.OnChange("limits", func() { calls <- "limits:" + Get[string](cfg, "limits.rps") })
	stopView := cfg.Sub("limits").OnChange("rps", func() { calls <- "view" })
	cfg.OnChange("", func() { calls <- "any" }) // registered last: runs last

	runConfig(t, cfg, kv, "/flags.json")

	kv.Set("/flags.json", []byte(`{"limits": {"rps": 10}, "banner": "b"}`))
	assert.Equal(t, "any", receive(t, calls), "a change outside limits calls only the catch-all")

	kv.Set("/flags.json", []byte(`{"limits": {"rps": 20}, "banner": "b"}`))
	assert.Equal(t, "limits:20", receive(t, calls), "the callback sees the new values")
	assert.Equal(t, "view", receive(t, calls), "the prefix of a view is relative to it")
	assert.Equal(t, "any", receive(t, calls))

	kv.Set("/flags.json", []byte(`{"limits": {"rps": 20}, "banner": "b"}`))
	stopView()
	kv.Set("/flags.json", []byte(`{"limits": {"rps": 30}, "banner": "b"}`))
	assert.Equal(t, "limits:30", receive(t, calls), "an update that changes nothing calls nothing")
	assert.Equal(t, "any", receive(t, calls), "a stopped callback is not called")

	kv.Delete("/flags.json")
	assert.Equal(t, "limits:", receive(t, calls), "a deleted key is a change")
	assert.Equal(t, "any", receive(t, calls))
}

func TestConfig_OptionalWatchedKeys(t *testing.T) {
	t.Parallel()

	kv := newFakeKV()

	_, err := New(t.Context(), WithKV(kv), WithWatchedKeys("/flags.json"), noEnv(), quiet())
	require.ErrorIs(t, err, ErrKVKeyNotFound, "a watched key is required by default")

	cfg, err := New(t.Context(), WithKV(kv), WithOptionalWatchedKeys("/flags.json"), noEnv(), quiet())
	require.NoError(t, err)
	assert.Equal(t, 1, GetOrDefault(cfg, "v", 1), "a missing optional key reads as an empty document")

	changed := make(chan string, 1)

	cfg.OnChange("v", func() { changed <- "v" })

	runConfig(t, cfg, kv, "/flags.json")

	kv.Set("/flags.json", []byte(`{"v": 2}`))
	receive(t, changed)
	assert.Equal(t, 2, Get[int](cfg, "v"), "the watch picks the key up once it is created")
}

func TestConfig_OptionalWatchedKeysStillFailOnOtherErrors(t *testing.T) {
	t.Parallel()

	kv := newFakeKV()
	kv.SetData("/flags.json", []byte(`{not json`))

	_, err := New(t.Context(), WithKV(kv), WithOptionalWatchedKeys("/flags.json"), noEnv(), quiet())
	require.Error(t, err)
	assert.Equal(t, 1, kv.CloseCount(), "New closes the KV on failure")
}
