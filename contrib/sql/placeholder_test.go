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
		{in: "SELECT 'what?' || ? FROM t", want: "SELECT 'what?' || $1 FROM t"},
		{in: "SELECT 'it''s ?' , ?", want: "SELECT 'it''s ?' , $1"},
		{in: `SELECT E'it\'s ?', ?`, want: `SELECT E'it\'s ?', $1`},
		{in: `SELECT "odd?col" FROM t WHERE a = ?`, want: `SELECT "odd?col" FROM t WHERE a = $1`},
		{in: "SELECT $$ a ? b $$, $fn$ ? $fn$, ?", want: "SELECT $$ a ? b $$, $fn$ ? $fn$, $1"},
		{in: "SELECT ? -- why?\n, ?", want: "SELECT $1 -- why?\n, $2"},
		{in: "SELECT /* ? */ ?", want: "SELECT /* ? */ $1"},
		{in: "SELECT a$b, ?", want: "SELECT a$b, $1"},
		{in: "SELECT 'unterminated ?", want: "SELECT 'unterminated ?"},
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
