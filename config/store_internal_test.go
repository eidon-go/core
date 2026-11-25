package config

import (
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlatten(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   map[string]any
		want map[string]any
		err  string // collision: the key named in the error
	}{
		{
			name: "nested and dotted",
			in:   map[string]any{"a": map[string]any{"b": 1, "c": map[string]any{}}, "a.d": 2, "e": nil},
			want: map[string]any{"a.b": 1, "a.d": 2, "e": nil},
		},
		{
			name: "a prefix of another key is not a collision",
			in:   map[string]any{"a": 1, "a.b": 2},
			want: map[string]any{"a": 1, "a.b": 2},
		},
		{
			name: "non-string keys",
			in:   map[string]any{"codes": map[any]any{200: "ok", 404: map[string]any{"text": "missing"}}},
			want: map[string]any{"codes.200": "ok", "codes.404.text": "missing"},
		},
		{
			name: "nested next to dotted",
			in:   map[string]any{"http": map[string]any{"port": 1}, "http.port": 2},
			err:  `"http.port"`,
		},
		{
			name: "deep nested next to partly dotted",
			in:   map[string]any{"a": map[string]any{"b": map[string]any{"c": 1}}, "a.b": map[string]any{"c": 2}},
			err:  `"a.b.c"`,
		},
		{
			name: "nil value still occupies its key",
			in:   map[string]any{"a": map[string]any{"b": nil}, "a.b": 1},
			err:  `"a.b"`,
		},
		{
			name: "non-string key next to its string form",
			in:   map[string]any{"codes": map[any]any{1: "int", "1": "string"}},
			err:  `"codes.1"`,
		},
		{
			name: "non-string keys next to a dotted key",
			in:   map[string]any{"codes": map[any]any{200: "ok"}, "codes.200": "dotted"},
			err:  `"codes.200"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Map iteration order varies between runs; a collision must be
			// reported whichever entry comes first.
			for range 20 {
				got, err := flatten(tt.in)
				if tt.err != "" {
					require.ErrorIs(t, err, ErrKeyCollision)
					require.ErrorContains(t, err, tt.err)
					assert.Nil(t, got)

					continue
				}

				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestKV_WatchedDocumentWithCollisionKeepsPreviousValues(t *testing.T) {
	t.Parallel()

	kv := newFakeKV()
	kv.SetData("/flags.json", []byte(`{"limit": {"max": 5}}`))

	logs := &logRecorder{}

	cfg, err := New(t.Context(), WithKV(kv), WithWatchedKeys("/flags.json"),
		WithLogger(slog.New(logs)), fastBackoff(), noEnv())
	require.NoError(t, err)

	limit := Live[int](cfg, "limit.max")
	assert.Equal(t, 5, limit())

	startRun(t, cfg)
	require.True(t, kv.WaitForWatcher("/flags.json", 1, time.Second))

	kv.Set("/flags.json", []byte(`{"limit": {"max": 9}, "limit.max": 10}`))
	logs.waitFor(t, "config: invalid watched document, keeping previous values")
	assert.Equal(t, 5, limit(), "a document with colliding keys keeps the previous values")

	kv.Set("/flags.json", []byte(`{"limit.max": 7}`))
	eventually(t, func() bool { return limit() == 7 })
}

// Lists in a WithDefaults or FromMap literal are copied: changing the
// caller's slice afterwards does not change the config.
func TestFlatten_CopiesLists(t *testing.T) {
	t.Parallel()

	hosts := []string{"a", "b"}
	ports := []int{1, 2}
	nested := map[string]any{"name": "x"}
	inner := []any{"y"}

	flat, err := flatten(map[string]any{
		"hosts": hosts,
		"srv":   map[string]any{"ports": ports},
		"items": []any{nested, inner},
	})
	require.NoError(t, err)

	hosts[0] = "changed"
	ports[0] = 99
	nested["name"] = "changed"
	inner[0] = "changed"

	assert.Equal(t, []string{"a", "b"}, flat["hosts"])
	assert.Equal(t, []int{1, 2}, flat["srv.ports"])
	assert.Equal(t, []any{map[string]any{"name": "x"}, []any{"y"}}, flat["items"])
}

func TestStore_EnvNameComputedOnce(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32

	name := func(key string) string {
		calls.Add(1)

		return "APP_" + key
	}

	s := newStore(func(string) (string, bool) { return "", false }, name, nil)

	for range 3 {
		_, _ = s.lookup("http.port")
	}

	assert.Equal(t, int32(1), calls.Load())
	assert.Equal(t, "APP_http.port", s.envVar("http.port"))
}
