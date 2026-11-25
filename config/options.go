package config

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/eidon-go/core/retry"
)

// ErrInvalidEnvBootstrap is returned by New when an environment variable read
// by WithEnvBootstrap has an invalid value.
var ErrInvalidEnvBootstrap = errors.New("config: invalid bootstrap environment variable")

// DefaultKVTimeout bounds every KV read unless WithKVTimeout sets another
// value.
const DefaultKVTimeout = 5 * time.Second

// Default watch reconnect backoff: 1s growing ×2 up to 30s, with jitter.
const (
	defaultWatchInitialDelay = time.Second
	defaultWatchMultiplier   = 2
	defaultWatchMaxDelay     = 30 * time.Second
	defaultWatchJitter       = 0.3
)

// Option configures New.
type Option func(*options)

type options struct {
	defaults     map[string]any
	files        []string
	fsys         fs.FS
	kv           KV
	keys         []string
	watchedKeys  []string
	kvTimeout    time.Duration
	watchBackoff retry.Policy
	envLookup    func(string) (string, bool)
	envPrefix    string
	envBootstrap bool
	logger       *slog.Logger
}

func newOptions(opts []Option) *options {
	o := &options{ //nolint:exhaustruct_v5 // zero values mean "not set"
		kvTimeout: DefaultKVTimeout,
		watchBackoff: retry.Policy{ //nolint:exhaustruct_v5 // attempts are irrelevant: only Delay is used
			InitialDelay: defaultWatchInitialDelay,
			Multiplier:   defaultWatchMultiplier,
			MaxDelay:     defaultWatchMaxDelay,
			Jitter:       defaultWatchJitter,
		},
		envLookup: os.LookupEnv,
		logger:    slog.Default(),
	}

	for _, opt := range opts {
		opt(o)
	}

	return o
}

// WithDefaults sets the lowest-priority values. Nested maps are flattened to
// dotted keys; dotted keys can also be used directly.
func WithDefaults(values map[string]any) Option {
	return func(o *options) { o.defaults = values }
}

// WithFiles adds configuration files, merged in order (later files win). The
// format follows the extension: .yaml/.yml (also the default when there is
// none) or .json. Files are read from the OS file system, or from the one set
// with WithFS. Each file must be a regular file of at most 10 MiB.
func WithFiles(paths ...string) Option {
	return func(o *options) { o.files = append(o.files, paths...) }
}

// WithFS reads files from fsys instead of the OS file system (e.g. an
// embed.FS or fstest.MapFS). Paths must then be valid fs.FS paths: slash
// separated and relative.
func WithFS(fsys fs.FS) Option {
	return func(o *options) { o.fsys = fsys }
}

// WithKV sets the KV backend for WithKeys and WithWatchedKeys. Config takes
// ownership of kv and closes it (see New and Config.Shutdown).
func WithKV(kv KV) Option {
	return func(o *options) { o.kv = kv }
}

// WithKeys adds KV keys that are read once by New. Each value is a yaml or
// json document (by the key's extension, yaml by default); documents are
// merged in order.
func WithKeys(keys ...string) Option {
	return func(o *options) { o.keys = append(o.keys, keys...) }
}

// WithWatchedKeys adds KV keys that are read by New and then watched by
// Config.Run: every change publishes new values, a deleted key removes its
// values. Watched keys take priority over WithKeys and are merged in order.
func WithWatchedKeys(keys ...string) Option {
	return func(o *options) { o.watchedKeys = append(o.watchedKeys, keys...) }
}

// WithKVTimeout bounds every KV read (default DefaultKVTimeout). Values <= 0
// mean no limit other than the caller's context.
func WithKVTimeout(d time.Duration) Option {
	return func(o *options) { o.kvTimeout = d }
}

// WithWatchBackoff sets the delays between reconnects of a failed watch
// (default: 1s growing ×2 up to 30s, with jitter). Only the delay fields of p
// are used: a watch is retried until Shutdown.
func WithWatchBackoff(p retry.Policy) Option {
	return func(o *options) { o.watchBackoff = p }
}

// WithEnvLookup replaces os.LookupEnv for the environment layer and
// WithEnvBootstrap, e.g. with a map in tests. nil disables the environment
// layer.
func WithEnvLookup(lookup func(name string) (string, bool)) Option {
	return func(o *options) { o.envLookup = lookup }
}

// WithEnvPrefix makes environment variables override keys only when they
// carry prefix: with "APP", key "http.port" maps to APP_HTTP_PORT instead of
// HTTP_PORT. It protects keys from unrelated variables such as PATH, USER or
// HOSTNAME.
func WithEnvPrefix(prefix string) Option {
	return func(o *options) { o.envPrefix = strings.TrimSuffix(prefix, "_") }
}

// WithEnvBootstrap lets environment variables select sources, in addition to
// the options (names shown without a prefix; WithEnvPrefix applies):
//
//   - CONFIG_FILES: comma-separated files, appended to WithFiles
//   - CONFIG_KV_KEYS: comma-separated keys, appended to WithKeys
//   - CONFIG_KV_WATCHED_KEYS: comma-separated keys, appended to WithWatchedKeys
//   - CONFIG_KV_TIMEOUT: a Go duration overriding WithKVTimeout
//
// The KV backend itself still comes from WithKV.
func WithEnvBootstrap() Option {
	return func(o *options) { o.envBootstrap = true }
}

// WithLogger sets the logger for watch errors and reconnects. nil means
// slog.Default().
func WithLogger(logger *slog.Logger) Option {
	return func(o *options) {
		if logger == nil {
			logger = slog.Default()
		}

		o.logger = logger
	}
}

// envName maps a dotted key to its environment variable name.
func (o *options) envName(key string) string {
	name := strings.ToUpper(strings.NewReplacer(".", "_", "-", "_").Replace(key))
	if o.envPrefix != "" {
		name = strings.ToUpper(o.envPrefix) + "_" + name
	}

	return name
}

func (o *options) bootstrapFromEnv() error {
	if o.envLookup == nil {
		return nil
	}

	get := func(key string) string {
		v, _ := o.envLookup(o.envName(key))

		return strings.TrimSpace(v)
	}

	o.files = append(o.files, splitList(get("config.files"))...)
	o.keys = append(o.keys, splitList(get("config.kv.keys"))...)
	o.watchedKeys = append(o.watchedKeys, splitList(get("config.kv.watched.keys"))...)

	if raw := get("config.kv.timeout"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return fmt.Errorf("%w: %s=%q: want a positive duration", ErrInvalidEnvBootstrap,
				o.envName("config.kv.timeout"), raw)
		}

		o.kvTimeout = d
	}

	return nil
}

// splitList splits a comma-separated list, trimming items and dropping empty
// ones.
func splitList(s string) []string {
	var out []string

	for item := range strings.SplitSeq(s, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}

	return out
}
