package config_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/config"
)

func env(m map[string]string) config.Option {
	return config.WithEnvLookup(func(name string) (string, bool) {
		v, ok := m[name]

		return v, ok
	})
}

// parseLines is a tiny test Parser: one "key=value" per line.
func parseLines(data []byte) (map[string]any, error) {
	out := map[string]any{}

	for line := range strings.Lines(string(data)) {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		k, v, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("bad line %q", line)
		}

		out[k] = v
	}

	return out, nil
}

func TestNew_LayersAndPriority(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"base.json": {
			Data: []byte(`{"http": {"port": 8080, "host": "base"}, "log": {"level": "info"}, "empty": null}`),
		},
		"prod.json": {Data: []byte(`{"http": {"host": "prod"}, "workers": 4}`)},
	}

	cfg, err := config.New(t.Context(),
		config.WithFS(fsys),
		config.WithFiles("base.json", "prod.json"),
		config.WithDefaults(map[string]any{"http": map[string]any{"timeout": "5s"}, "log.level": "debug"}),
		config.WithDefaults(map[string]any{"empty": "def"}), // repeated calls merge
		env(map[string]string{"LOG_LEVEL": "warn", "HTTP_PORT": "", "WORKERS": "   "}),
	)
	require.NoError(t, err)

	assert.Equal(t, 8080, config.Get[int](cfg, "http.port"), "an empty env var does not shadow the file")
	assert.Equal(t, 4, config.Get[int](cfg, "workers"), "a whitespace-only env var does not shadow the file")
	assert.Equal(t, "prod", config.Get[string](cfg, "http.host"), "later file wins")
	assert.Equal(t, 5*time.Second, config.Get[time.Duration](cfg, "http.timeout"), "defaults are the lowest layer")
	assert.Equal(t, "warn", config.Get[string](cfg, "log.level"), "env wins over every source")
	assert.Equal(t, "def", config.Get[string](cfg, "empty"), "a null value does not shadow defaults")
}

func TestNew_EnvPrefix(t *testing.T) {
	t.Parallel()

	cfg, err := config.New(t.Context(),
		config.WithDefaults(map[string]any{"path": "/srv", "rate-limit": 1}),
		config.WithEnvPrefix("app_"),
		env(map[string]string{"PATH": "/usr/bin", "APP_RATE_LIMIT": "7"}),
	)
	require.NoError(t, err)

	assert.Equal(t, "/srv", config.Get[string](cfg, "path"), "unprefixed variables are ignored")
	assert.Equal(t, 7, config.Get[int](cfg, "rate-limit"), "dashes map to underscores, prefix is upper-cased")
}

func TestNew_WithoutEnv(t *testing.T) {
	t.Parallel()

	cfg, err := config.New(t.Context(),
		config.WithDefaults(map[string]any{"home": "default"}),
		config.WithEnvLookup(nil),
	)
	require.NoError(t, err)
	assert.Equal(t, "default", config.Get[string](cfg, "home"))
}

func TestNew_EnvBootstrap(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"a.json": {Data: []byte(`{"x": 1}`)},
		"b.json": {Data: []byte(`{"x": 2}`)},
	}

	cfg, err := config.New(t.Context(),
		config.WithFS(fsys),
		config.WithFiles("a.json"),
		config.WithEnvBootstrap(),
		config.WithEnvPrefix("SVC"),
		env(map[string]string{"SVC_CONFIG_FILES": " b.json , ", "SVC_CONFIG_KV_TIMEOUT": "2s"}),
	)
	require.NoError(t, err)
	assert.Equal(t, 2, config.Get[int](cfg, "x"), "bootstrap files are appended")

	_, err = config.New(t.Context(),
		config.WithEnvBootstrap(),
		env(map[string]string{"CONFIG_KV_TIMEOUT": "soon"}),
	)
	require.ErrorIs(t, err, config.ErrInvalidEnvBootstrap)

	for _, name := range []string{"CONFIG_KV_KEYS", "CONFIG_KV_WATCHED_KEYS"} {
		_, err = config.New(t.Context(),
			config.WithEnvBootstrap(),
			env(map[string]string{name: "/svc/flags.json"}),
		)
		require.ErrorIs(t, err, config.ErrKVRequired, name)
	}

	cfg, err = config.New(t.Context(),
		config.WithDefaults(map[string]any{"x": 1}),
		env(map[string]string{"CONFIG_FILES": "missing.json"}), // not bootstrapped
	)
	require.NoError(t, err, "bootstrap variables are ignored without WithEnvBootstrap")
	assert.Equal(t, 1, config.Get[int](cfg, "x"))
}

func TestNew_Files(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"upper.JSON":    {Data: []byte(`{"a": 1}`)},
		"empty.json":    {Data: []byte("  \n")},
		"null.json":     {Data: []byte("null")},
		"big-int.json":  {Data: []byte(`{"id": 9007199254740993}`)},
		"settings.conf": {Data: []byte("b=2\n")},
		"plain":         {Data: []byte("c=3\n")},
	}

	cfg, err := config.New(t.Context(),
		config.WithFS(fsys),
		config.WithParser(parseLines, ".CONF", ""),
		config.WithFiles("upper.JSON", "empty.json", "null.json", "big-int.json", "settings.conf", "plain"),
		config.WithOptionalFiles("local.json"),
		config.WithEnvLookup(nil),
	)
	require.NoError(t, err)

	assert.Equal(t, 1, config.Get[int](cfg, "a"), "extensions are case-insensitive")
	assert.Equal(t, int64(9007199254740993), config.Get[int64](cfg, "id"), "json integers keep their precision")
	assert.Equal(t, uint64(9007199254740993), config.Get[uint64](cfg, "id"))
	assert.Equal(t, 2, config.Get[int](cfg, "b"), "custom parser by extension")
	assert.Equal(t, 3, config.Get[int](cfg, "c"), "custom parser for files without extension")
}

func TestNew_OptionalFiles(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"base.json":  {Data: []byte(`{"x": 1, "y": 1}`)},
		"local.json": {Data: []byte(`{"y": 2}`)},
		"bad.json":   {Data: []byte(`{`)},
	}

	cfg, err := config.New(t.Context(),
		config.WithFS(fsys),
		config.WithFiles("base.json"),
		config.WithOptionalFiles("missing.json", "local.json"),
		config.WithEnvLookup(nil),
	)
	require.NoError(t, err)
	assert.Equal(t, 2, config.Get[int](cfg, "y"))

	_, err = config.New(t.Context(), config.WithFS(fsys), config.WithOptionalFiles("bad.json"))
	require.ErrorContains(t, err, "parse bad.json", "an optional file that exists must be valid")
}

func TestNew_SourceErrors(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"conf.toml":   {Data: []byte("a = 1")},
		"conf.yaml":   {Data: []byte("a: 1")},
		"broken.json": {Data: []byte(`{"a": [1, 2`)},
		"list.json":   {Data: []byte(`[1, 2]`)},
		"dir/x.json":  {Data: []byte(`{"a": 1}`)},
		"big.json":    {Data: make([]byte, config.MaxFileSize+1)},
	}

	tests := []struct {
		name string
		opts []config.Option
		want error
	}{
		{"unknown format", []config.Option{config.WithFiles("conf.toml")}, config.ErrUnknownFormat},
		{"yaml without parser", []config.Option{config.WithFiles("conf.yaml")}, config.ErrUnknownFormat},
		{"not regular", []config.Option{config.WithFiles("dir")}, config.ErrFileNotRegular},
		{"too large", []config.Option{config.WithFiles("big.json")}, config.ErrFileTooLarge},
		{"missing", []config.Option{config.WithFiles("nope.json")}, fs.ErrNotExist},
		{"kv keys without kv", []config.Option{config.WithKeys("/a.json")}, config.ErrKVRequired},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := config.New(t.Context(), append(tt.opts, config.WithFS(fsys))...)
			require.ErrorIs(t, err, tt.want)
		})
	}

	_, err := config.New(t.Context(), config.WithFS(fsys), config.WithFiles("conf.yaml"))
	require.ErrorContains(t, err, "config/yaml", "the error points to the YAML module")

	_, err = config.New(t.Context(), config.WithFS(fsys), config.WithFiles("broken.json"))
	require.ErrorContains(t, err, "parse broken.json")

	_, err = config.New(t.Context(), config.WithFS(fsys), config.WithFiles("list.json"))
	require.ErrorContains(t, err, "parse list.json", "a top-level array is not a config document")
}

func TestNew_OSFiles(t *testing.T) {
	t.Parallel()

	cfg, err := config.New(t.Context(), config.WithFiles("testdata/config.json"), config.WithEnvLookup(nil))
	require.NoError(t, err)
	assert.Equal(t, "test-service", config.Get[string](cfg, "app.service.name"))
	assert.Equal(t, 30*time.Second, config.Get[time.Duration](cfg, "limits.timeout"))

	_, err = config.New(t.Context(), config.WithFiles("testdata"))
	require.ErrorIs(t, err, config.ErrFileNotRegular)
}

func TestParseJSON(t *testing.T) {
	t.Parallel()

	doc, err := config.ParseJSON([]byte(`{"a": {"b": 1.5}}`))
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"a": map[string]any{"b": json.Number("1.5")}}, doc)

	doc, err = config.ParseJSON(nil)
	require.NoError(t, err)
	assert.Empty(t, doc)

	_, err = config.ParseJSON([]byte(`{`))
	require.ErrorContains(t, err, "decode json")
}

func TestReaders(t *testing.T) {
	t.Parallel()

	cfg := config.FromMap(map[string]any{
		"name":  "svc",
		"port":  "8080",
		"debug": "yes",
		"hosts": []any{"a", "b"},
		"ids":   "1, 2,3",
		"ratio": 0.5,
		"codes": map[any]any{200: "ok"},
	})

	assert.Equal(t, "svc", config.Get[string](cfg, "name"))
	assert.Equal(t, int64(8080), config.Get[int64](cfg, "port"))
	assert.Equal(t, uint(8080), config.Get[uint](cfg, "port"))
	assert.True(t, config.Get[bool](cfg, "debug"))
	assert.Equal(t, []string{"a", "b"}, config.Get[[]string](cfg, "hosts"))
	assert.Equal(t, []int{1, 2, 3}, config.Get[[]int](cfg, "ids"))
	assert.InDelta(t, 0.5, config.Get[float64](cfg, "ratio"), 0)
	assert.Equal(t, "ok", config.Get[string](cfg, "codes.200"), "non-string map keys are stringified")

	assert.Equal(t, 0, config.Get[int](cfg, "missing"))
	assert.Equal(t, 3, config.GetOrDefault(cfg, "missing", 3))
	assert.Equal(t, 3, config.GetOrDefault(cfg, "name", 3), "mistyped value falls back to the default")
	assert.Equal(t, 8080, config.Live[int](cfg, "port")())
	assert.Equal(t, 1, config.LiveOrDefault(cfg, "missing", 1)())

	var nilCfg *config.Config
	assert.Equal(t, "x", config.GetOrDefault(nilCfg, "name", "x"), "a nil config reads as empty")
}

func TestReader_Required(t *testing.T) {
	t.Parallel()

	cfg := config.FromMap(map[string]any{"smtp": map[string]any{"host": "mail", "port": "not-a-number"}})
	r := config.NewReader(cfg)

	host := config.Required[string](r, "smtp.host")
	port := config.Required[int](r, "smtp.port")
	user := config.Required[string](r, "smtp.user")

	assert.Equal(t, "mail", host)
	assert.Zero(t, port)
	assert.Empty(t, user)

	err := r.Err()
	require.ErrorIs(t, err, config.ErrKeyMistyped)
	require.ErrorIs(t, err, config.ErrKeyMissing)

	var keyErr *config.KeyError
	require.ErrorAs(t, err, &keyErr)
	assert.Equal(t, "smtp.port", keyErr.Key)
	require.EqualError(t, err,
		"config: value has the wrong type: want int, got string: smtp.port\n"+
			"config: required key is missing: smtp.user")

	ok := config.NewReader(cfg)
	_ = config.Required[string](ok, "smtp.host")
	require.NoError(t, ok.Err())

	nilReader := config.NewReader(nil)
	_ = config.Required[string](nilReader, "a")
	require.ErrorIs(t, nilReader.Err(), config.ErrKeyMissing)
}

func TestReader_Optional(t *testing.T) {
	t.Parallel()

	cfg := config.FromMap(map[string]any{"timeout": 30, "retries": "3"})
	r := config.NewReader(cfg)

	assert.Equal(t, 10*time.Second, config.Optional(r, "timeout", 10*time.Second), "mistyped: default + error")
	assert.Equal(t, 3, config.Optional(r, "retries", 1))
	assert.Equal(t, "x", config.Optional(r, "missing", "x"), "missing: default, no error")

	err := r.Err()
	require.ErrorIs(t, err, config.ErrKeyMistyped)
	require.NotErrorIs(t, err, config.ErrKeyMissing)
}

func TestReader_Concurrent(t *testing.T) {
	t.Parallel()

	r := config.NewReader(config.FromMap(nil))

	var wg sync.WaitGroup

	for range 10 {
		wg.Go(func() {
			for range 100 {
				_ = config.Required[int](r, "missing")
			}
		})
	}

	wg.Wait()

	var keyErr *config.KeyError
	require.ErrorAs(t, r.Err(), &keyErr)
	assert.Len(t, unwrapAll(r.Err()), 1000)
}

func unwrapAll(err error) []error {
	var joined interface{ Unwrap() []error }
	if errors.As(err, &joined) {
		return joined.Unwrap()
	}

	return []error{err}
}

func TestConfig_RunAndShutdownWithoutKV(t *testing.T) {
	t.Parallel()

	cfg := config.FromMap(nil)
	assert.Equal(t, "config", cfg.Name())

	done := make(chan error, 1)

	go func() { done <- cfg.Run(t.Context()) }()

	time.Sleep(10 * time.Millisecond)
	require.NoError(t, cfg.Shutdown(t.Context()))
	require.NoError(t, <-done)
	require.NoError(t, cfg.Shutdown(t.Context()), "Shutdown is idempotent")
	require.NoError(t, cfg.Run(t.Context()), "Run after Shutdown returns nil at once")
}
