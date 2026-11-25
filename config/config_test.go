package config_test

import (
	"io/fs"
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

func TestNew_LayersAndPriority(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"base.yaml": {Data: []byte("http:\n  port: 8080\n  host: base\nlog:\n  level: info\nempty:\n")},
		"prod.json": {Data: []byte(`{"http": {"host": "prod"}, "workers": 4}`)},
	}

	cfg, err := config.New(
		t.Context(),
		config.WithFS(fsys),
		config.WithFiles("base.yaml", "prod.json"),
		config.WithDefaults(
			map[string]any{"http": map[string]any{"timeout": "5s"}, "log.level": "debug", "empty": "def"},
		),
		env(map[string]string{"LOG_LEVEL": "warn", "HTTP_PORT": ""}),
	)
	require.NoError(t, err)

	assert.Equal(t, 8080, config.Get[int](cfg, "http.port"), "empty env var does not shadow the file")
	assert.Equal(t, "prod", config.Get[string](cfg, "http.host"), "later file wins")
	assert.Equal(t, 4, config.Get[int](cfg, "workers"), "json numbers read as int")
	assert.Equal(t, 5*time.Second, config.Get[time.Duration](cfg, "http.timeout"), "defaults are the lowest layer")
	assert.Equal(t, "warn", config.Get[string](cfg, "log.level"), "env wins over every source")
	assert.Equal(t, "def", config.Get[string](cfg, "empty"), "a null value does not shadow defaults")
}

func TestNew_EnvPrefix(t *testing.T) {
	t.Parallel()

	cfg, err := config.New(t.Context(),
		config.WithDefaults(map[string]any{"path": "/srv", "rate-limit": 1}),
		config.WithEnvPrefix("APP_"),
		env(map[string]string{"PATH": "/usr/bin", "APP_RATE_LIMIT": "7"}),
	)
	require.NoError(t, err)

	assert.Equal(t, "/srv", config.Get[string](cfg, "path"), "unprefixed variables are ignored")
	assert.Equal(t, 7, config.Get[int](cfg, "rate-limit"), "dashes map to underscores")
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
		"a.yaml": {Data: []byte("x: 1\n")},
		"b.yaml": {Data: []byte("x: 2\n")},
	}

	cfg, err := config.New(t.Context(),
		config.WithFS(fsys),
		config.WithFiles("a.yaml"),
		config.WithEnvBootstrap(),
		env(map[string]string{"CONFIG_FILES": " b.yaml , "}),
	)
	require.NoError(t, err)
	assert.Equal(t, 2, config.Get[int](cfg, "x"), "bootstrap files are appended")

	_, err = config.New(t.Context(),
		config.WithEnvBootstrap(),
		env(map[string]string{"CONFIG_KV_TIMEOUT": "soon"}),
	)
	require.ErrorIs(t, err, config.ErrInvalidEnvBootstrap)

	_, err = config.New(t.Context(),
		config.WithEnvBootstrap(),
		env(map[string]string{"CONFIG_KV_WATCHED_KEYS": "/svc/flags.yaml"}),
	)
	require.ErrorIs(t, err, config.ErrKVRequired)
}

func TestNew_SourceErrors(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"conf.toml":   {Data: []byte("a = 1")},
		"broken.yaml": {Data: []byte("a: [1, 2\n")},
		"dir/x.yaml":  {Data: []byte("a: 1\n")},
		"big.yaml":    {Data: make([]byte, config.MaxFileSize+1)},
	}

	tests := []struct {
		name string
		opts []config.Option
		want error
	}{
		{"unknown format", []config.Option{config.WithFiles("conf.toml")}, config.ErrUnknownFormat},
		{"not regular", []config.Option{config.WithFiles("dir")}, config.ErrFileNotRegular},
		{"too large", []config.Option{config.WithFiles("big.yaml")}, config.ErrFileTooLarge},
		{"missing", []config.Option{config.WithFiles("nope.yaml")}, fs.ErrNotExist},
		{"kv keys without kv", []config.Option{config.WithKeys("/a.yaml")}, config.ErrKVRequired},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := config.New(t.Context(), append(tt.opts, config.WithFS(fsys))...)
			require.ErrorIs(t, err, tt.want)
		})
	}

	_, err := config.New(t.Context(), config.WithFS(fsys), config.WithFiles("broken.yaml"))
	require.ErrorContains(t, err, "parse broken.yaml")
}

func TestNew_OSFiles(t *testing.T) {
	t.Parallel()

	cfg, err := config.New(t.Context(), config.WithFiles("testdata/config.yaml"), config.WithEnvLookup(nil))
	require.NoError(t, err)
	assert.NotEmpty(t, config.Get[string](cfg, "app.service.name"))

	_, err = config.New(t.Context(), config.WithFiles("testdata"))
	require.ErrorIs(t, err, config.ErrFileNotRegular)
}

func TestReaders(t *testing.T) {
	t.Parallel()

	cfg := config.FromMap(map[string]any{
		"name":  "svc",
		"port":  "8080",
		"debug": "yes",
		"hosts": []any{"a", "b"},
		"ratio": 0.5,
	})

	assert.Equal(t, "svc", config.Get[string](cfg, "name"))
	assert.Equal(t, int64(8080), config.Get[int64](cfg, "port"))
	assert.True(t, config.Get[bool](cfg, "debug"))
	assert.Equal(t, []string{"a", "b"}, config.Get[[]string](cfg, "hosts"))
	assert.InDelta(t, 0.5, config.Get[float64](cfg, "ratio"), 0)

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
}
