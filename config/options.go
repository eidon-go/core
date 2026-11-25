package config

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"strings"
	"time"
)

// ErrInvalidEnvBootstrap is returned by New when an environment variable read
// by WithEnvBootstrap has an invalid value.
var ErrInvalidEnvBootstrap = errors.New("config: invalid bootstrap environment variable")

// DefaultKVTimeout bounds every KV read, and the wait for the initial state
// of a watch, unless WithKVTimeout sets another value.
const DefaultKVTimeout = 5 * time.Second

// Default delays between reconnects of a failed watch.
const (
	DefaultWatchInitialDelay = time.Second
	DefaultWatchMaxDelay     = 30 * time.Second
)

// Option configures New.
type Option func(*options)

type options struct {
	defaults     map[string]any
	files        []fileSource
	fsys         fs.FS
	parsers      parsers
	kv           KV
	keys         []string
	watchedKeys  []string
	kvTimeout    time.Duration
	watchInitial time.Duration
	watchMax     time.Duration
	envLookup    func(string) (string, bool)
	envPrefix    string
	envBootstrap bool
	logger       *slog.Logger
	// err is an error found while applying options (e.g. a key collision in
	// WithDefaults); New returns it.
	err error
}

func newOptions(opts []Option) *options {
	o := &options{ //nolint:exhaustruct_v5 // zero values mean "not set"
		kvTimeout:    DefaultKVTimeout,
		parsers:      parsers{".json": ParseJSON},
		watchInitial: DefaultWatchInitialDelay,
		watchMax:     DefaultWatchMaxDelay,
		envLookup:    os.LookupEnv,
		logger:       slog.Default(),
	}

	for _, opt := range opts {
		opt(o)
	}

	return o
}

// WithDefaults adds lowest-priority values. Nested maps are flattened to
// dotted keys; dotted keys can also be used directly. Repeated calls merge,
// later values winning. Two entries of one map that resolve to the same
// dotted key make New fail with ErrKeyCollision.
func WithDefaults(values map[string]any) Option {
	return func(o *options) {
		flat, err := flatten(values)
		if err != nil {
			o.err = errors.Join(o.err, fmt.Errorf("defaults: %w", err))

			return
		}

		o.defaults = merge(o.defaults, flat)
	}
}

// WithFiles adds configuration files, merged in order (later files win). The
// format follows the extension: .json is built in, other formats need a
// parser registered with WithParser (YAML: github.com/eidon-go/core/contrib/config/yaml).
// Files are read from the OS file system, or from the one set with WithFS.
// Each file must be a regular file of at most MaxFileSize bytes.
func WithFiles(paths ...string) Option {
	return func(o *options) { o.addFiles(paths, false) }
}

// WithOptionalFiles is WithFiles for files that may be missing (e.g. a local
// override such as config.local.yaml): a file that does not exist is
// skipped. Any other error still fails New. Optional and required files are
// merged in the order they were added.
func WithOptionalFiles(paths ...string) Option {
	return func(o *options) { o.addFiles(paths, true) }
}

// WithFS reads files from fsys instead of the OS file system (e.g. an
// embed.FS or fstest.MapFS). Paths must then be valid fs.FS paths: slash
// separated and relative.
func WithFS(fsys fs.FS) Option {
	return func(o *options) { o.fsys = fsys }
}

// WithParser registers p for files and KV keys with the given extensions
// (case-insensitive; "yaml" and ".yaml" are the same; "" means no
// extension), replacing a parser registered earlier. ".json" is built in
// (ParseJSON). WithParser panics if p is nil.
//
//	config.WithParser(yaml.Parse, yaml.Extensions()...)
func WithParser(p Parser, exts ...string) Option {
	if p == nil {
		panic("config: WithParser: nil parser")
	}

	return func(o *options) {
		for _, ext := range exts {
			ext = strings.ToLower(ext)
			if ext != "" && !strings.HasPrefix(ext, ".") {
				ext = "." + ext
			}

			o.parsers[ext] = p
		}
	}
}

// WithKV sets the KV backend for WithKeys and WithWatchedKeys. Config takes
// ownership of kv and closes it (see New and Config.Shutdown).
func WithKV(kv KV) Option {
	return func(o *options) { o.kv = kv }
}

// WithKeys adds KV keys that are read once by New. Each value is a document
// parsed by the parser registered for the key's extension (see WithParser);
// documents are merged in order.
func WithKeys(keys ...string) Option {
	return func(o *options) { o.keys = append(o.keys, keys...) }
}

// WithWatchedKeys adds KV keys that are read by New and then watched by
// Config.Run: every change publishes new values, a deleted key removes its
// values. Watched keys take priority over WithKeys and are merged in order.
func WithWatchedKeys(keys ...string) Option {
	return func(o *options) { o.watchedKeys = append(o.watchedKeys, keys...) }
}

// WithKVTimeout bounds every KV read and the wait for the initial state of a
// watch (default DefaultKVTimeout). Values <= 0 mean no limit other than the
// caller's context.
func WithKVTimeout(d time.Duration) Option {
	return func(o *options) { o.kvTimeout = d }
}

// WithWatchBackoff sets the delays between reconnects of a failed watch: the
// first delay is initial, then it doubles up to maxDelay, each reduced by up
// to 30% of random jitter. A watch is retried until Shutdown. Values <= 0
// mean the defaults, DefaultWatchInitialDelay and DefaultWatchMaxDelay.
func WithWatchBackoff(initial, maxDelay time.Duration) Option {
	return func(o *options) {
		if initial > 0 {
			o.watchInitial = initial
		}

		if maxDelay > 0 {
			o.watchMax = maxDelay
		}
	}
}

// WithEnvLookup replaces os.LookupEnv for the environment layer and
// WithEnvBootstrap, e.g. with a map in tests. nil disables the environment
// layer. lookup is called on every read, so it must be safe for concurrent
// use.
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

// fileSource is a configuration file and whether it may be missing.
type fileSource struct {
	path     string
	optional bool
}

func (o *options) addFiles(paths []string, optional bool) {
	for _, p := range paths {
		o.files = append(o.files, fileSource{path: p, optional: optional})
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

	o.addFiles(splitList(get("config.files")), false)
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
