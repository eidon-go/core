package config_test

import (
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/config"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
)

func TestUintFromWholeFloats(t *testing.T) {
	t.Parallel()

	cfg, err := config.New(t.Context(),
		config.WithFS(fstest.MapFS{"c.json": {Data: []byte(`{"a": 1e3, "b": 1.0, "c": -1, "d": 1.5}`)}}),
		config.WithFiles("c.json"),
		config.WithEnvLookup(nil),
	)
	require.NoError(t, err)

	assert.Equal(t, uint(1000), config.Get[uint](cfg, "a"))
	assert.Equal(t, uint64(1), config.Get[uint64](cfg, "b"))
	assert.Equal(t, uint(5), config.GetOrDefault[uint](cfg, "c", 5), "negative numbers are not uints")
	assert.Equal(t, uint(5), config.GetOrDefault[uint](cfg, "d", 5), "fractional numbers are not uints")
}

func TestParseJSON_Strict(t *testing.T) {
	t.Parallel()

	_, err := config.ParseJSON([]byte(`{"a": 1} garbage`))
	require.ErrorContains(t, err, "unexpected data")

	_, err = config.ParseJSON([]byte(`{"a": 1} {"b": 2}`))
	require.Error(t, err)

	doc, err := config.ParseJSON([]byte(" null \n"))
	require.NoError(t, err)
	assert.NotNil(t, doc)
	assert.Empty(t, doc)
}

func TestWithParser_Validation(t *testing.T) {
	t.Parallel()

	assert.PanicsWithValue(t, "config: WithParser: nil parser", func() { config.WithParser(nil, ".x") })

	cfg, err := config.New(t.Context(),
		config.WithFS(fstest.MapFS{"a.conf": {Data: []byte("k=v\n")}}),
		config.WithParser(parseLines, "CONF"), // no dot, upper case
		config.WithFiles("a.conf"),
		config.WithEnvLookup(nil),
	)
	require.NoError(t, err)
	assert.Equal(t, "v", config.Get[string](cfg, "k"))
}

// A path passed to WithFiles stays required even if the same path is also
// passed to WithOptionalFiles.
func TestOptionalFiles_ArePerEntry(t *testing.T) {
	t.Parallel()

	_, err := config.New(t.Context(),
		config.WithFS(fstest.MapFS{}),
		config.WithOptionalFiles("x.json"),
		config.WithFiles("x.json"),
	)
	require.Error(t, err)
}

func TestConfig_Contract(t *testing.T) {
	t.Parallel()

	lifecycletest.Runner(t, func(*testing.T) lifecycle.Runner { return config.FromMap(nil) })
}
