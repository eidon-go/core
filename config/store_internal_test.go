package config

import (
	"log/slog"
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
