package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/eidon-go/core/config"
	"github.com/eidon-go/core/contrib/config/etcd"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
)

const etcdWait = 10 * time.Second

// etcdKey returns a key unique to the test.
func etcdKey(t *testing.T, name string) string {
	t.Helper()

	return "/" + strings.ReplaceAll(t.Name(), "/", "_") + "/" + name
}

// newEtcdKV returns a KV against the shared container; the test closes it
// unless ownership passes to a config.Config.
func newEtcdKV(t *testing.T) *etcd.KV {
	t.Helper()

	kv, err := etcd.New(etcd.Config{Endpoints: []string{etcdEndpoint}})
	require.NoError(t, err)

	return kv
}

// etcdWriter is a plain client the tests use to change keys.
func etcdWriter(t *testing.T) *clientv3.Client {
	t.Helper()

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdEndpoint}, DialTimeout: etcdWait})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })

	return cli
}

func receiveKVEvent(t *testing.T, ch <-chan config.KVEvent) config.KVEvent {
	t.Helper()

	select {
	case ev, ok := <-ch:
		require.True(t, ok, "watch channel closed")
		require.NoError(t, ev.Err)

		return ev
	case <-time.After(etcdWait):
		require.FailNow(t, "no watch event")

		return config.KVEvent{}
	}
}

func TestEtcd_Get(t *testing.T) {
	t.Parallel()
	requireDocker(t)

	kv := newEtcdKV(t)
	t.Cleanup(func() { assert.NoError(t, kv.Close()) })

	key := etcdKey(t, "doc.json")
	_, err := etcdWriter(t).Put(t.Context(), key, `{"a":1}`)
	require.NoError(t, err)

	got, err := kv.Get(t.Context(), key)
	require.NoError(t, err)
	assert.JSONEq(t, `{"a":1}`, string(got))

	_, err = kv.Get(t.Context(), etcdKey(t, "missing.json"))
	require.ErrorIs(t, err, config.ErrKVKeyNotFound)
}

func TestEtcd_Watch(t *testing.T) {
	t.Parallel()
	requireDocker(t)

	kv := newEtcdKV(t)
	t.Cleanup(func() { assert.NoError(t, kv.Close()) })

	w := etcdWriter(t)
	key := etcdKey(t, "doc.json")
	_, err := w.Put(t.Context(), key, "v1")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	ch := kv.Watch(ctx, key)

	assert.Equal(t, config.KVEvent{Type: config.KVEventPut, Value: []byte("v1")}, receiveKVEvent(t, ch),
		"the first event is the current state")

	_, err = w.Put(t.Context(), key, "v2")
	require.NoError(t, err)
	_, err = w.Delete(t.Context(), key)
	require.NoError(t, err)
	_, err = w.Put(t.Context(), key, "v3")
	require.NoError(t, err)

	assert.Equal(t, config.KVEvent{Type: config.KVEventPut, Value: []byte("v2")}, receiveKVEvent(t, ch))
	assert.Equal(t, config.KVEvent{Type: config.KVEventDelete}, receiveKVEvent(t, ch))
	assert.Equal(t, config.KVEvent{Type: config.KVEventPut, Value: []byte("v3")}, receiveKVEvent(t, ch))

	cancel()

	select {
	case _, ok := <-ch:
		for ok {
			_, ok = <-ch
		}
	case <-time.After(etcdWait):
		require.FailNow(t, "watch channel not closed after cancel")
	}
}

func TestEtcd_WatchMissingKey(t *testing.T) {
	t.Parallel()
	requireDocker(t)

	kv := newEtcdKV(t)
	t.Cleanup(func() { assert.NoError(t, kv.Close()) })

	key := etcdKey(t, "doc.json")
	ch := kv.Watch(t.Context(), key)

	assert.Equal(t, config.KVEvent{Type: config.KVEventDelete}, receiveKVEvent(t, ch))

	_, err := etcdWriter(t).Put(t.Context(), key, "v1")
	require.NoError(t, err)

	assert.Equal(t, config.KVEvent{Type: config.KVEventPut, Value: []byte("v1")}, receiveKVEvent(t, ch))
}

// A change made while the watch is being opened is never lost: the last
// event seen is always the last value written.
func TestEtcd_WatchHasNoGapAtStart(t *testing.T) {
	t.Parallel()
	requireDocker(t)

	kv := newEtcdKV(t)
	t.Cleanup(func() { assert.NoError(t, kv.Close()) })

	w := etcdWriter(t)

	for i := range 20 {
		key := etcdKey(t, "doc.json")
		_, err := w.Put(t.Context(), key, "old")
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())
		ch := kv.Watch(ctx, key)

		_, err = w.Put(t.Context(), key, "new")
		require.NoError(t, err)

		for {
			ev := receiveKVEvent(t, ch)
			if string(ev.Value) == "new" {
				break
			}

			require.Equal(t, "old", string(ev.Value), "iteration %d", i)
		}

		cancel()
	}
}

func TestEtcd_ConfigHotReload(t *testing.T) {
	t.Parallel()
	requireDocker(t)

	w := etcdWriter(t)
	base := etcdKey(t, "base.json")
	flags := etcdKey(t, "flags.json")

	_, err := w.Put(t.Context(), base, `{"service":{"name":"api"},"rate":{"limit":10}}`)
	require.NoError(t, err)
	_, err = w.Put(t.Context(), flags, `{"rate":{"limit":20}}`)
	require.NoError(t, err)

	cfg, err := config.New(t.Context(),
		config.WithKV(newEtcdKV(t)),
		config.WithKeys(base),
		config.WithWatchedKeys(flags),
	)
	require.NoError(t, err)

	limit := config.Live[int](cfg, "rate.limit")
	assert.Equal(t, "api", config.Get[string](cfg, "service.name"))
	assert.Equal(t, 20, limit(), "watched keys win over static keys")

	runErr := make(chan error, 1)

	go func() { runErr <- cfg.Run(t.Context()) }()

	_, err = w.Put(t.Context(), flags, `{"rate":{"limit":30}}`)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return limit() == 30 }, etcdWait, 10*time.Millisecond)

	_, err = w.Delete(t.Context(), flags)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return limit() == 10 }, etcdWait, 10*time.Millisecond,
		"a deleted key removes its values")

	ctx, cancel := context.WithTimeout(t.Context(), etcdWait)
	defer cancel()

	require.NoError(t, cfg.Shutdown(ctx))

	select {
	case err := <-runErr:
		require.NoError(t, err)
	case <-time.After(etcdWait):
		require.FailNow(t, "Run did not return after Shutdown")
	}
}

func TestEtcd_ConfigNewFailsOnMissingKey(t *testing.T) {
	t.Parallel()
	requireDocker(t)

	_, err := config.New(t.Context(),
		config.WithKV(newEtcdKV(t)), // closed by New on failure
		config.WithKeys(etcdKey(t, "missing.json")),
	)
	require.ErrorIs(t, err, config.ErrKVKeyNotFound)
}

// config.Config over a real etcd still follows the Runner contract
// (watcher stopped and KV closed exactly once).
func TestEtcd_ConfigContract(t *testing.T) {
	t.Parallel()
	requireDocker(t)

	lifecycletest.Runner(t, func(t *testing.T) lifecycle.Runner {
		t.Helper()

		key := etcdKey(t, "flags.json")
		_, err := etcdWriter(t).Put(t.Context(), key, `{"a":1}`)
		require.NoError(t, err)

		cfg, err := config.New(t.Context(), config.WithKV(newEtcdKV(t)), config.WithWatchedKeys(key))
		require.NoError(t, err)

		return cfg
	}, lifecycletest.WithTimeout(30*time.Second))
}
