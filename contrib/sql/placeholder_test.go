package sql_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/sql"
)

func TestDollar(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in, want string
	}{
		{in: "SELECT 1", want: "SELECT 1"},
		{in: "SELECT * FROM t WHERE a = ? AND b = ?", want: "SELECT * FROM t WHERE a = $1 AND b = $2"},
		{in: "?,?,?,?,?,?,?,?,?,?,?", want: "$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11"},
		{in: "SELECT doc ?? 'key' FROM t WHERE id = ?", want: "SELECT doc ? 'key' FROM t WHERE id = $1"},
		{in: "doc ??| array[?]", want: "doc ?| array[$1]"},
		{in: "???", want: "?$1"},
		{in: "?", want: "$1"},
		{in: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()

			got, err := sql.Dollar.ReplacePlaceholders(tt.in)

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestQuestion(t *testing.T) {
	t.Parallel()

	got, err := sql.Question.ReplacePlaceholders("a = ? AND b ?? c")

	require.NoError(t, err)
	assert.Equal(t, "a = ? AND b ?? c", got)
}
