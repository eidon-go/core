package config_test

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/config"
)

func TestConfig_Sub(t *testing.T) {
	t.Parallel()

	env := map[string]string{"DB_PRIMARY_HOST": "from-env"}

	cfg, err := config.New(t.Context(),
		config.WithDefaults(map[string]any{
			"db": map[string]any{
				"primary": map[string]any{"host": "localhost", "port": 5432},
			},
		}),
		config.WithEnvLookup(func(name string) (string, bool) {
			v, ok := env[name]

			return v, ok
		}))
	require.NoError(t, err)

	db := cfg.Sub("db")
	primary := db.Sub("primary")

	assert.Equal(t, 5432, config.Get[int](primary, "port"))
	assert.Equal(t, 5432, config.Get[int](db, "primary.port"))
	assert.Equal(t, "from-env", config.Get[string](primary, "host"), "a view reads the environment layer")
	assert.Equal(t, []string{"primary"}, db.Keys(""))
	assert.Zero(t, config.Get[int](primary, "missing"))

	var nilCfg *config.Config
	assert.Nil(t, nilCfg.Sub("db"))
	assert.Zero(t, config.Get[int](nilCfg.Sub("db"), "port"), "a nil view reads as empty")
}

func TestConfig_Keys(t *testing.T) {
	t.Parallel()

	cfg, err := config.New(t.Context(),
		config.WithEnvLookup(nil),
		config.WithDefaults(map[string]any{
			"tenants": map[string]any{"c": map[string]any{"limit": 3}},
		}),
		config.WithFS(fstest.MapFS{"app.json": {Data: []byte(`{
			"tenants": {"a": {"limit": 1}, "b": {"limit": 2, "burst": 5}, "gone": null},
			"name": "svc"
		}`)}}),
		config.WithFiles("app.json"))
	require.NoError(t, err)

	assert.Equal(t, []string{"a", "b", "c"}, cfg.Keys("tenants"), "files and defaults, null values skipped")
	assert.Equal(t, []string{"name", "tenants"}, cfg.Keys(""))
	assert.Equal(t, []string{"burst", "limit"}, cfg.Keys("tenants.b"))
	assert.Equal(t, []string{"burst", "limit"}, cfg.Sub("tenants").Keys("b"))
	assert.Empty(t, cfg.Keys("tenants.a.limit"), "a leaf has no children")
	assert.Empty(t, cfg.Keys("missing"))

	for _, name := range cfg.Keys("tenants") {
		assert.Positive(t, config.Get[int](cfg.Sub("tenants."+name), "limit"))
	}
}

func TestConfig_Items(t *testing.T) {
	t.Parallel()

	cfg := config.FromMap(map[string]any{
		"routes": []any{
			map[string]any{"host": "api.example.com", "upstream": map[string]any{"port": 8080}},
			map[any]any{"host": "www.example.com", 1: "one"},
		},
		"mixed":  []any{map[string]any{"a": 1}, "scalar"},
		"scalar": "x",
	})

	items, err := cfg.Items("routes")
	require.NoError(t, err)
	require.Len(t, items, 2)
	assert.Equal(t, "api.example.com", config.Get[string](items[0], "host"), "dots inside values survive")
	assert.Equal(t, 8080, config.Get[int](items[0], "upstream.port"))
	assert.Equal(t, "www.example.com", config.Get[string](items[1], "host"))
	assert.Equal(t, "one", config.Get[string](items[1], "1"))

	items, err = cfg.Items("missing")
	require.NoError(t, err)
	assert.Empty(t, items)

	_, err = cfg.Items("scalar")
	require.ErrorIs(t, err, config.ErrKeyMistyped)

	var keyErr *config.KeyError

	_, err = cfg.Items("mixed")
	require.ErrorAs(t, err, &keyErr)
	assert.Equal(t, "mixed[1]", keyErr.Key)
	require.ErrorIs(t, err, config.ErrKeyMistyped)
}

func TestConfig_ItemsAreCopies(t *testing.T) {
	t.Parallel()

	cfg := config.FromMap(map[string]any{"list": []any{map[string]any{"tags": []any{"a"}}}})

	items, err := cfg.Items("list")
	require.NoError(t, err)

	again, err := cfg.Items("list")
	require.NoError(t, err)

	config.Get[[]string](items[0], "tags")[0] = "changed"
	assert.Equal(t, []string{"a"}, config.Get[[]string](again[0], "tags"))
}

func TestLookup(t *testing.T) {
	t.Parallel()

	cfg := config.FromMap(map[string]any{"http": map[string]any{"port": 8080, "bad": "80a"}})

	port, err := config.Lookup[int](cfg, "http.port")
	require.NoError(t, err)
	assert.Equal(t, 8080, port)

	var keyErr *config.KeyError

	_, err = config.Lookup[int](cfg, "http.missing")
	require.ErrorIs(t, err, config.ErrKeyMissing)
	require.ErrorAs(t, err, &keyErr)
	assert.Equal(t, "http.missing", keyErr.Key)

	_, err = config.Lookup[int](cfg.Sub("http"), "bad")
	require.ErrorIs(t, err, config.ErrKeyMistyped)
	require.ErrorAs(t, err, &keyErr)
	assert.Equal(t, "http.bad", keyErr.Key, "the key of a view's error is the full key")

	r := config.NewReader(cfg.Sub("http"))
	config.Required[int](r, "absent")
	require.ErrorAs(t, r.Err(), &keyErr)
	assert.Equal(t, "http.absent", keyErr.Key)
}

// syncBuffer is a bytes.Buffer safe for the concurrent writes of a logger.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

func TestGet_LogsMistypedValueOnce(t *testing.T) {
	t.Parallel()

	var logs syncBuffer

	cfg, err := config.New(t.Context(),
		config.WithEnvLookup(nil),
		config.WithLogger(slog.New(slog.NewTextHandler(&logs, nil))),
		config.WithDefaults(map[string]any{"port": "80a", "ok": 1}))
	require.NoError(t, err)

	assert.Zero(t, config.Get[int](cfg, "port"))
	assert.Equal(t, 7, config.GetOrDefault(cfg, "port", 7))
	assert.Zero(t, config.Live[int](cfg, "port")())
	assert.Equal(t, 1, config.Get[int](cfg, "ok"))

	out := logs.String()
	assert.Equal(t, 1, strings.Count(out, "wrong type"), out)
	assert.Contains(t, out, "key=port")
	assert.Contains(t, out, "want=int")
}
