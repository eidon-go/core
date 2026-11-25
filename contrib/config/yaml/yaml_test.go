package yaml_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/config/yaml"
)

func TestParse(t *testing.T) {
	t.Parallel()

	doc, err := yaml.Parse([]byte("http:\n  port: 8080\n  timeout: 15s\ncodes:\n  200: ok\nlist:\n  - {1: a}\n"))
	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"http":  map[string]any{"port": 8080, "timeout": "15s"},
		"codes": map[string]any{"200": "ok"},
		"list":  []any{map[string]any{"1": "a"}},
	}, doc)
}

func TestParse_TimestampsStayStrings(t *testing.T) {
	t.Parallel()

	doc, err := yaml.Parse([]byte("date: 2024-01-01\nlist: [2024-01-02T10:00:00Z, x]\nexplicit: !!str 7\n"))
	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"date":     "2024-01-01",
		"list":     []any{"2024-01-02T10:00:00Z", "x"},
		"explicit": "7",
	}, doc)
}

func TestParse_Empty(t *testing.T) {
	t.Parallel()

	for _, empty := range []string{"", "\n", "# comment\n", "~\n"} {
		doc, err := yaml.Parse([]byte(empty))
		require.NoError(t, err, "%q", empty)
		assert.Empty(t, doc, "%q", empty)
	}
}

func TestParse_Errors(t *testing.T) {
	t.Parallel()

	_, err := yaml.Parse([]byte("a: [1, 2\n"))
	require.ErrorContains(t, err, "decode yaml")

	_, err = yaml.Parse([]byte("- a\n- b\n"))
	require.Error(t, err, "a top-level list is not a config document")
}

func TestExtensions(t *testing.T) {
	t.Parallel()

	assert.ElementsMatch(t, []string{".yaml", ".yml", ""}, yaml.Extensions())
}

func TestParse_KeyCollisions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		doc  string
		key  string
	}{
		{"int and float", "v:\n  1: a\n  1.0: b\n", `"v.1"`},
		{"null and its string form", "v:\n  ~: a\n  \"<nil>\": b\n", `"v.<nil>"`},
		{"inside a list", "list:\n  - {1: a, 1.0: b}\n", `"list[0].1"`},
		{"deeply nested", "a:\n  b:\n    2: x\n    2.0: y\n", `"a.b.2"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Map iteration order varies; the collision must be reported
			// whichever key comes first.
			for range 20 {
				doc, err := yaml.Parse([]byte(tt.doc))
				require.ErrorIs(t, err, yaml.ErrKeyCollision)
				require.ErrorContains(t, err, tt.key)
				assert.Nil(t, doc)
			}
		})
	}

	_, err := yaml.Parse([]byte("v:\n  1: a\n  \"1\": b\n"))
	require.ErrorContains(t, err, "decode yaml", "the decoder itself rejects 1 next to \"1\"")
}
