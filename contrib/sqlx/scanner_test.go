package sqlx_test

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/sql"
	coresqlx "github.com/eidon-go/core/contrib/sqlx"
)

func TestScanner_WithSQLManager(t *testing.T) {
	t.Parallel()

	db, mock := newDB(t, sql.WithScanner(coresqlx.NewScanner(nil)))

	m, err := sql.NewManager(db)
	require.NoError(t, err)

	q := m.GetQuerier(t.Context())

	mock.ExpectQuery("SELECT id, name FROM users").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "ann"))

	got, err := sql.Get[user](t.Context(), q, sql.Raw("SELECT id, name FROM users"))
	require.NoError(t, err)
	assert.Equal(t, user{ID: 1, Name: "ann"}, got)

	mock.ExpectQuery("SELECT id, name FROM users").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "ann").AddRow(2, "bob"))

	all, err := sql.Select[user](t.Context(), q, sql.Raw("SELECT id, name FROM users"))
	require.NoError(t, err)
	assert.Equal(t, []user{{ID: 1, Name: "ann"}, {ID: 2, Name: "bob"}}, all)
}

func TestScanner_Errors(t *testing.T) {
	t.Parallel()

	db, mock := newDB(t)
	q := newManager(t, db).GetQuerier(t.Context())

	mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))

	var n int
	require.ErrorContains(t, q.GetContext(t.Context(), n, "SELECT 1"), "non-nil pointer")

	mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
	require.ErrorContains(t, q.GetContext(t.Context(), (*int)(nil), "SELECT 1"), "non-nil pointer")

	rowErr := assert.AnError

	mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"n"}).RowError(0, rowErr).AddRow(1))
	require.ErrorIs(t, q.GetContext(t.Context(), &n, "SELECT 1"), rowErr, "an error before the first row")

	mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1).AddRow(2).RowError(1, rowErr))
	require.ErrorIs(t, q.GetContext(t.Context(), &n, "SELECT 1"), rowErr, "an error after the first row")

	mock.ExpectQuery("SELECT 'x'").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow("x"))
	require.Error(t, q.GetContext(t.Context(), &n, "SELECT 'x'"), "a value that does not convert")

	mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1).CloseError(rowErr))
	require.ErrorIs(t, q.GetContext(t.Context(), &n, "SELECT 1"), rowErr, "an error closing the rows")
}
