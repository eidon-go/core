// Package config loads service configuration from layered sources and reads
// it through a small generic API.
//
// Sources, from lowest to highest priority:
//
//	defaults  <  files  <  KV keys  <  watched KV keys  <  environment
//
// Every source is chosen with an Option: WithDefaults, WithFiles, WithKV with
// WithKeys and WithWatchedKeys. WithEnvBootstrap additionally lets the
// deployment pick files and KV keys through environment variables.
//
// Keys are dotted paths ("http.public.port"). Nested maps from yaml or json
// are flattened to such keys. Every key can be overridden by an environment
// variable: by default "http.public.port" maps to HTTP_PUBLIC_PORT, and
// WithEnvPrefix("APP") maps it to APP_HTTP_PUBLIC_PORT. An environment
// variable set to "" and a null value (`key:` in yaml) count as not set, so
// a lower-priority source is used instead.
//
// Values are read with the generic functions Get, GetOrDefault, Lookup, Live
// and LiveOrDefault; required keys are collected with a Reader and Required.
// Config.Sub returns a view of the keys under a prefix, Config.Keys lists the
// names below one (the entries of a map), and Config.Items reads a list of
// objects. A map key that itself contains a dot cannot be told from nesting
// once flattened; keep such names (host names) in a list of objects.
//
// # Remote configuration
//
// A KV backend (e.g. github.com/eidon-go/core/contrib/config/etcd) is passed with
// WithKV. Keys passed with WithWatchedKeys are watched for changes; the
// watcher runs in Config.Run, so a Config with watched keys must be run,
// typically by registering it with app.App:
//
//	cfg, err := config.New(ctx, config.WithKV(kv), config.WithWatchedKeys("/svc/flags.yaml"))
//	...
//	a.Add(cfg)
//
// Config.OnChange calls a function after an update has changed the keys under
// a prefix, to rebuild what was built from earlier values.
//
// Config implements lifecycle.Runner (Run, Shutdown) and lifecycle.Namer.
// Reads are safe for concurrent use with updates: every update publishes a
// new immutable snapshot.
package config

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/eidon-go/core/lifecycle"
)

var (
	// ErrKVRequired is returned by New when KV keys are configured but no
	// KV backend was passed with WithKV.
	ErrKVRequired = errors.New("config: KV keys are configured but no KV backend was set (use WithKV)")
	// ErrAlreadyRunning is returned by Config.Run when it is called more
	// than once.
	ErrAlreadyRunning = errors.New("config: Run called more than once")
	// ErrDuplicateKey is returned by New when a KV key is passed more than
	// once, e.g. to both WithKeys and WithWatchedKeys.
	ErrDuplicateKey = errors.New("config: KV key configured more than once")
)

// ValueType is the set of types the generic readers support. New types may
// be added in later versions (which does not affect existing callers); do
// not rely on the set being closed, e.g. in an exhaustive type switch.
type ValueType interface {
	string | int | int64 | uint | uint64 | bool | float64 | time.Duration | []string | []int
}

// Config is a loaded configuration. Create it with New (or FromMap in tests).
type Config struct {
	store  *store
	logger *slog.Logger
	kv     KV
	watch  *watcher
	prefix string // set on a Sub view: prepended to every key read

	worker   *lifecycle.Worker   // runs the watcher
	loopDone chan struct{}       // closed when the watcher loop has returned
	releaser *lifecycle.Releaser // closes kv
}

// New loads the configuration from the sources selected by opts and returns
// it. ctx bounds the loading only; watching (if any) happens in Run.
//
// When a KV backend is passed with WithKV, Config takes ownership of it: New
// closes it on failure, and Shutdown closes it otherwise.
func New(ctx context.Context, opts ...Option) (*Config, error) {
	o := newOptions(opts)

	cfg, err := load(ctx, o)
	if err != nil {
		if o.kv != nil {
			err = errors.Join(err, o.kv.Close())
		}

		return nil, err
	}

	return cfg, nil
}

func load(ctx context.Context, o *options) (*Config, error) {
	if err := o.resolve(); err != nil {
		return nil, err
	}

	files, err := loadFiles(o.fsys, o.parsers, o.files)
	if err != nil {
		return nil, err
	}

	static, err := loadKeys(ctx, o.kv, o.parsers, o.kvTimeout, o.keys)
	if err != nil {
		return nil, err
	}

	cfg := newConfig(newStore(o.envLookup, o.envName, o.defaults), o.logger)
	cfg.kv = o.kv
	base := merge(files, static)

	if len(o.watchedKeys) == 0 {
		cfg.store.setSnapshot(base)

		return cfg, nil
	}

	watch, err := loadWatched(ctx, o, base, cfg.store.publish)
	if err != nil {
		return nil, err
	}

	cfg.watch = watch

	return cfg, nil
}

// resolve applies the environment bootstrap and validates the sources.
func (o *options) resolve() error {
	if o.err != nil {
		return o.err
	}

	if o.envBootstrap {
		if err := o.bootstrapFromEnv(); err != nil {
			return err
		}
	}

	if o.kv == nil && (len(o.keys) > 0 || len(o.watchedKeys) > 0) {
		return ErrKVRequired
	}

	return checkDuplicateKeys(o.keys, o.watchedKeys)
}

// loadWatched reads the watched keys and builds the watcher, which publishes
// the initial snapshot.
func loadWatched(ctx context.Context, o *options, base map[string]any, publish func(map[string]any)) (*watcher, error) {
	initial := make(map[string]map[string]any, len(o.watchedKeys))

	for _, key := range o.watchedKeys {
		parsed, err := loadKey(ctx, o.kv, o.parsers, o.kvTimeout, key)

		switch {
		case err == nil:
		case o.optionalWatched[key] && errors.Is(err, ErrKVKeyNotFound):
			parsed = map[string]any{} // the watch picks the key up once it is created
		default:
			return nil, err
		}

		initial[key] = parsed
	}

	return newWatcher(watcherConfig{
		kv:      o.kv,
		parsers: o.parsers,
		keys:    o.watchedKeys,
		base:    base,
		initial: initial,
		timeout: o.kvTimeout,
		backoff: backoff{initial: o.watchInitial, max: o.watchMax},
		logger:  o.logger,
		publish: publish,
	}), nil
}

// checkDuplicateKeys rejects a KV key listed more than once across keys and
// watched: its values could not be removed consistently on delete.
func checkDuplicateKeys(keys, watched []string) error {
	seen := make(map[string]bool, len(keys)+len(watched))

	for _, key := range append(append([]string{}, keys...), watched...) {
		if seen[key] {
			return fmt.Errorf("%w: %s", ErrDuplicateKey, key)
		}

		seen[key] = true
	}

	return nil
}

// FromMap returns a Config holding values, with no other sources and no
// environment layer — handy in tests. Nested maps are flattened to dotted
// keys; dotted keys can also be used directly. FromMap panics if two entries
// resolve to the same dotted key (ErrKeyCollision): values is a literal in
// code, so that is a programming error.
func FromMap(values map[string]any) *Config {
	flat, err := flatten(values)
	if err != nil {
		panic(err)
	}

	cfg := newConfig(newStore(nil, nil, nil), nil)
	cfg.store.setSnapshot(flat)

	return cfg
}

func newConfig(s *store, logger *slog.Logger) *Config {
	if logger == nil {
		logger = slog.Default()
	}

	cfg := &Config{ //nolint:exhaustruct_v5 // kv and watch are set by load; worker and releaser below
		store:    s,
		logger:   logger,
		loopDone: make(chan struct{}),
	}
	s.logger = logger
	cfg.worker = lifecycle.NewWorker(lifecycle.WorkerFuncs{Run: cfg.watchLoop, Logger: nil})
	cfg.releaser = lifecycle.CloseOnce(cfg.closeKV)

	return cfg
}

// Name implements lifecycle.Namer.
func (c *Config) Name() string { return "config" }

// Run watches the keys passed with WithWatchedKeys and publishes every change
// until ctx is cancelled or Shutdown is called; it then returns nil. Without
// watched keys it just waits. Run may be called once (ErrAlreadyRunning
// afterwards); called after Shutdown it returns nil at once.
func (c *Config) Run(ctx context.Context) error {
	err := c.worker.Run(ctx)
	if errors.Is(err, lifecycle.ErrAlreadyStarted) {
		return ErrAlreadyRunning
	}

	return err //nolint:wrapcheck // watchLoop returns no error of its own
}

// Shutdown stops Run (waiting for it to return, bounded by ctx) and closes
// the KV backend, if any. The last published values stay readable. If ctx
// ends before the watcher stops, Shutdown returns the ctx error and the KV is
// closed once the watcher has stopped.
func (c *Config) Shutdown(ctx context.Context) error {
	if err := c.worker.Shutdown(ctx); err != nil {
		// The watcher was abandoned at ctx's deadline: KV.Close must wait
		// until it has stopped reading.
		go func() {
			<-c.loopDone

			_ = c.releaser.Shutdown(context.WithoutCancel(ctx))
		}()

		return fmt.Errorf("config: waiting for the watcher: %w", err)
	}

	// The watcher has stopped or never ran.
	return c.releaser.Shutdown(ctx) //nolint:wrapcheck // closeKV wraps the KV error
}

// closeKV is the release of the Releaser: it closes the KV backend, if any.
func (c *Config) closeKV() error {
	if c.kv == nil {
		return nil
	}

	if err := c.kv.Close(); err != nil {
		return fmt.Errorf("config: close KV: %w", err)
	}

	return nil
}

// watchLoop is the loop of the Worker.
func (c *Config) watchLoop(stop, _ context.Context) error {
	defer close(c.loopDone)

	if c.watch != nil {
		c.watch.run(stop)
	} else {
		<-stop.Done()
	}

	return nil
}

// Get returns the value of key as T, or the zero value of T when the key is
// not set or its value cannot be converted to T. A value of the wrong type is
// logged once per key (see WithLogger); use Lookup or a Reader to get it as
// an error. A nil cfg reads as empty.
func Get[T ValueType](cfg *Config, key string) T {
	v, raw, res := lookupAs[T](cfg, key)
	if res == lookupMistyped {
		warnMistyped(cfg, key, v, raw)
	}

	return v
}

// GetOrDefault returns the value of key as T, or def when the key is not set
// or its value cannot be converted to T. A value of the wrong type is logged
// once per key, as in Get.
func GetOrDefault[T ValueType](cfg *Config, key string, def T) T {
	v, raw, res := lookupAs[T](cfg, key)

	switch res {
	case lookupFound:
		return v
	case lookupMistyped:
		warnMistyped(cfg, key, v, raw)
	case lookupMissing:
	}

	return def
}

// Lookup returns the value of key as T, or a *KeyError wrapping
// ErrKeyMissing when the key is not set, or ErrKeyMistyped when its value
// cannot be converted to T. Use it where a typo in a value must not pass
// silently and no Reader is at hand.
func Lookup[T ValueType](cfg *Config, key string) (T, error) {
	v, raw, res := lookupAs[T](cfg, key)
	if res == lookupFound {
		return v, nil
	}

	return v, keyError(cfg, key, v, raw, res)
}

// Live returns a function that reads key on every call, for values that may
// change at runtime through a watched KV key (rate limits, feature flags).
// An environment variable that overrides key pins it for the process
// lifetime.
//
// Live is a getter, not a subscription: it does not reconfigure anything
// built from an earlier value, and two calls during one operation may see
// different values if an update lands between them.
func Live[T ValueType](cfg *Config, key string) func() T {
	return func() T { return Get[T](cfg, key) }
}

// LiveOrDefault is Live with a fallback for an unset or mistyped key.
func LiveOrDefault[T ValueType](cfg *Config, key string, def T) func() T {
	return func() T { return GetOrDefault(cfg, key, def) }
}

type lookupResult int

const (
	lookupFound lookupResult = iota
	lookupMissing
	lookupMistyped
)

// lookupAs resolves key and converts it to T. raw is the stored value, used
// to describe a type mismatch.
func lookupAs[T ValueType](cfg *Config, key string) (T, any, lookupResult) {
	var zero T

	if cfg == nil || cfg.store == nil {
		return zero, nil, lookupMissing
	}

	raw, ok := cfg.store.lookup(viewKey(cfg.prefix, key))
	if !ok {
		return zero, nil, lookupMissing
	}

	v, ok := coerce[T](raw)
	if !ok {
		return zero, raw, lookupMistyped
	}

	return v, raw, lookupFound
}
