package yaml_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/config/yaml"
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
