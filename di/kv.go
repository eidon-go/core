package di

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Sentinel errors of the KV backend seam.
var (
	// ErrKVKeyNotFound is returned (wrapped) by KV.Get when the key is
	// absent. Backends must wrap it so the loader can tell a missing key
	// from a transport failure.
	ErrKVKeyNotFound = errors.New("kv key not found")

	// ErrKVBackendRequired is returned by NewConfig when etcd paths are
	// configured (app.config.etcd.{static,dynamic}.paths or the legacy
	// app.config.etcd.path) but no backend was passed via WithKV.
	// Configured paths are never silently ignored.
	ErrKVBackendRequired = errors.New("kv backend is required when etcd paths are configured: pass di.WithKV")

	// ErrInvalidEtcdRequestTimeout is returned by NewConfig at startup
	// when APP_CONFIG_ETCD_REQUEST_TIMEOUT does not parse as a positive
	// Go duration (e.g. "5s", "500ms").
	ErrInvalidEtcdRequestTimeout = errors.New("invalid etcd request timeout")
)

// defaultEtcdRequestTimeout bounds every KV.Get call when
// APP_CONFIG_ETCD_REQUEST_TIMEOUT is not set.
const defaultEtcdRequestTimeout = 5 * time.Second

// KVEventType distinguishes PUT and DELETE events delivered by KV.Watch.
type KVEventType int

const (
	// KVEventPut indicates a key was created or updated. Value holds the
	// new payload.
	KVEventPut KVEventType = iota
	// KVEventDelete indicates a key was removed. Value is nil.
	KVEventDelete
)

// KVEvent is one event delivered on the channel returned by KV.Watch.
// Either Type+Value is set (a normal event) OR Err is set (a terminal
// error — the channel closes right after delivering it).
type KVEvent struct {
	Type  KVEventType
	Value []byte
	Err   error
}

// KV is the key-value backend the remote config loader reads from. The
// core module ships no implementation; github.com/eidon-go/core/di/etcd
// provides one for etcd v3. The interface is deliberately small so a
// backend (or a test fake) is easy to write.
type KV interface {
	// Get fetches the latest value for key. Returns ErrKVKeyNotFound
	// (wrapped) when the key is absent.
	Get(ctx context.Context, key string) ([]byte, error)

	// Watch returns a channel that emits events for key. The channel
	// closes when ctx is cancelled OR a terminal error occurs (in the
	// latter case the last delivered event has Err != nil). The loader
	// re-Gets the key after every channel close to catch up on events it
	// may have missed, so an implementation does not have to guarantee
	// gap-free delivery across reconnects.
	Watch(ctx context.Context, key string) <-chan KVEvent

	// Close releases the underlying transport. The loader calls it
	// exactly once, after every Watch started on this KV has returned
	// (its ctx cancelled and its channel closed).
	Close() error
}

// KVFactory opens a KV bound to endpoint (app.config.etcd.endpoint).
// requestTimeout (APP_CONFIG_ETCD_REQUEST_TIMEOUT, 5s by default) must
// bound every Get call. NewConfig opens one short-lived KV for the static
// load and one long-lived KV for the dynamic watcher.
type KVFactory func(endpoint string, requestTimeout time.Duration) (KV, error)

// ConfigOption customises NewConfig.
type ConfigOption func(*configDeps)

// WithKV sets the backend used for app.config.etcd.* paths. Without it,
// configuring any etcd path fails NewConfig with ErrKVBackendRequired.
//
//	cfg, err := di.NewConfig(ctx, di.WithKV(etcd.Factory))
func WithKV(factory KVFactory) ConfigOption {
	return func(d *configDeps) {
		d.kvFactory = factory
	}
}

// openKV resolves the endpoint and opens a KV via the configured factory.
func openKV(deps configDeps, serviceName string, requestTimeout time.Duration) (KV, error) {
	if deps.kvFactory == nil {
		return nil, ErrKVBackendRequired
	}

	endpoint, err := resolveEtcdAccess(deps, serviceName)
	if err != nil {
		return nil, err
	}

	kv, err := deps.kvFactory(endpoint, requestTimeout)
	if err != nil {
		return nil, fmt.Errorf("open kv backend: %w", err)
	}

	return kv, nil
}

// resolveEtcdRequestTimeout reads APP_CONFIG_ETCD_REQUEST_TIMEOUT via
// envLookup, parses it as a Go duration ("5s", "500ms", "1m") and
// returns the value. Unset or empty falls back to defaultEtcdRequestTimeout.
// A non-empty value that fails to parse yields ErrInvalidEtcdRequestTimeout.
func resolveEtcdRequestTimeout(envLookup func(string) (string, bool)) (time.Duration, error) {
	raw, ok := lookupConfigEnv(envLookup, "app.config.etcd.request.timeout")
	if !ok || raw == "" {
		return defaultEtcdRequestTimeout, nil
	}

	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%w: %q: %w", ErrInvalidEtcdRequestTimeout, raw, err)
	}

	if d <= 0 {
		return 0, fmt.Errorf("%w: %q: must be positive", ErrInvalidEtcdRequestTimeout, raw)
	}

	return d, nil
}
