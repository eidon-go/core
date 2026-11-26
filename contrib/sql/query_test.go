package sql_test

import (
	stdsql "database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/sql"
)

type audit struct {
	CreatedAt time.Time `db:"created_at"`
}

type user struct {
	audit

	UserID   int64
	FullName string `db:"name"`
	Email    stdsql.Null[string]
}

const userQuery = "SELECT user_id, name, email, created_at FROM users WHERE user_id = $1"

func userRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"user_id", "name", "email", "created_at"})
}

func TestGet(t *testing.T) {
	t.Parallel()

	created := time.Date(2026, 11, 26, 0, 0, 0, 0, time.UTC)

	t.Run("struct: snake_case, db tag, embedded, Null", func(t *testing.T) {
		t.Parallel()

		q, mock := newQuerier(t)
		mock.ExpectQuery(userQuery).WithArgs(7).WillReturnRows(userRows().AddRow(7, "Ann", nil, created))

		got, err := sql.Get[user](t.Context(), q, sql.Raw(userQuery, 7))

		require.NoError(t, err)
		assert.Equal(t, user{audit: audit{CreatedAt: created}, UserID: 7, FullName: "Ann"}, got)
	})

	t.Run("scalar", func(t *testing.T) {
		t.Parallel()

		q, mock := newQuerier(t)
		mock.ExpectQuery("SELECT count(*) FROM users").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))

		got, err := sql.Get[int](t.Context(), q, sql.Raw("SELECT count(*) FROM users"))

		require.NoError(t, err)
		assert.Equal(t, 3, got)
	})

	t.Run("no rows", func(t *testing.T) {
		t.Parallel()

		q, mock := newQuerier(t)
		mock.ExpectQuery(userQuery).WithArgs(7).WillReturnRows(userRows())

		_, err := sql.Get[user](t.Context(), q, sql.Raw(userQuery, 7))

		require.ErrorIs(t, err, sql.ErrNoRows)
	})

	t.Run("more than one row", func(t *testing.T) {
		t.Parallel()

		q, mock := newQuerier(t)
		mock.ExpectQuery(userQuery).WithArgs(7).
			WillReturnRows(userRows().AddRow(7, "Ann", nil, created).AddRow(8, "Bob", nil, created))

		_, err := sql.Get[user](t.Context(), q, sql.Raw(userQuery, 7))

		require.ErrorIs(t, err, sql.ErrTooManyRows)
	})

	t.Run("unknown column", func(t *testing.T) {
		t.Parallel()

		q, mock := newQuerier(t)
		mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"user_id", "extra"}).AddRow(1, 2))

		_, err := sql.Get[user](t.Context(), q, sql.Raw("SELECT 1"))

		require.ErrorContains(t, err, "extra")
	})

	t.Run("query error as is", func(t *testing.T) {
		t.Parallel()

		errDriver := errors.New("driver")
		q, mock := newQuerier(t)
		mock.ExpectQuery("SELECT 1").WillReturnError(errDriver)

		_, err := sql.Get[int](t.Context(), q, sql.Raw("SELECT 1"))

		require.ErrorIs(t, err, errDriver)
	})

	t.Run("rows error", func(t *testing.T) {
		t.Parallel()

		errRows := errors.New("rows")
		q, mock := newQuerier(t)
		mock.ExpectQuery("SELECT 1").
			WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1).AddRow(2).RowError(1, errRows))

		_, err := sql.Get[int](t.Context(), q, sql.Raw("SELECT 1"))

		require.ErrorIs(t, err, errRows)
	})
}

func TestSelect(t *testing.T) {
	t.Parallel()

	t.Run("rows", func(t *testing.T) {
		t.Parallel()

		q, mock := newQuerier(t)
		mock.ExpectQuery("SELECT user_id, name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"user_id", "name"}).AddRow(1, "Ann").AddRow(2, "Bob"))

		got, err := sql.Select[user](t.Context(), q, sql.Raw("SELECT user_id, name FROM users"))

		require.NoError(t, err)
		assert.Equal(t, []user{{UserID: 1, FullName: "Ann"}, {UserID: 2, FullName: "Bob"}}, got)
	})

	t.Run("no rows is nil", func(t *testing.T) {
		t.Parallel()

		q, mock := newQuerier(t)
		mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"n"}))

		got, err := sql.Select[int](t.Context(), q, sql.Raw("SELECT 1"))

		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("query error", func(t *testing.T) {
		t.Parallel()

		errDriver := errors.New("driver")
		q, mock := newQuerier(t)
		mock.ExpectQuery("SELECT 1").WillReturnError(errDriver)

		_, err := sql.Select[int](t.Context(), q, sql.Raw("SELECT 1"))

		require.ErrorIs(t, err, errDriver)
	})
}

func TestExec(t *testing.T) {
	t.Parallel()

	t.Run("result", func(t *testing.T) {
		t.Parallel()

		q, mock := newQuerier(t)
		mock.ExpectExec("DELETE FROM users WHERE user_id = $1").WithArgs(7).WillReturnResult(result(1))

		res, err := sql.Exec(t.Context(), q, sql.Raw("DELETE FROM users WHERE user_id = $1", 7))
		require.NoError(t, err)

		n, err := res.RowsAffected()
		require.NoError(t, err)
		assert.EqualValues(t, 1, n)
	})

	t.Run("error", func(t *testing.T) {
		t.Parallel()

		errDriver := errors.New("driver")
		q, mock := newQuerier(t)
		mock.ExpectExec("DELETE FROM users").WillReturnError(errDriver)

		_, err := sql.Exec(t.Context(), q, sql.Raw("DELETE FROM users"))

		require.ErrorIs(t, err, errDriver)
	})
}

type failingBuilder struct{ err error }

func (b failingBuilder) ToSql() (string, []any, error) { return "", nil, b.err } //nolint:revive // Builder

func TestHelpers_BuilderError(t *testing.T) {
	t.Parallel()

	errBuild := errors.New("build")
	b := failingBuilder{err: errBuild}
	q, _ := newQuerier(t) // no query may reach the database

	_, err := sql.Get[int](t.Context(), q, b)
	require.ErrorIs(t, err, errBuild)

	_, err = sql.Select[int](t.Context(), q, b)
	require.ErrorIs(t, err, errBuild)

	_, err = sql.Exec(t.Context(), q, b)
	require.ErrorIs(t, err, errBuild)
}

// dollarBuilder renders a "?" query with Dollar, the way squirrel does.
type dollarBuilder struct {
	query string
	args  []any
}

func (b dollarBuilder) ToSql() (string, []any, error) { //nolint:revive // Builder
	query, err := sql.Dollar.ReplacePlaceholders(b.query)

	return query, b.args, err
}

func TestHelpers_BuilderOutputUsedAsIs(t *testing.T) {
	t.Parallel()

	q, mock := newQuerier(t)
	mock.ExpectQuery("SELECT id FROM docs WHERE body ? 'k' AND id = $1").WithArgs(5).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(5))

	got, err := sql.Get[int](
		t.Context(),
		q,
		dollarBuilder{query: "SELECT id FROM docs WHERE body ?? 'k' AND id = ?", args: []any{5}},
	)

	require.NoError(t, err)
	assert.Equal(t, 5, got)
}

// stubScanner fills every destination with the same int.
type stubScanner struct{ n int }

func (s stubScanner) ScanOne(rows *stdsql.Rows, dest any) error {
	*dest.(*int) = s.n //nolint:forcetypeassert // test stub

	return rows.Close()
}

func (s stubScanner) ScanAll(rows *stdsql.Rows, dest any) error {
	*dest.(*[]int) = []int{s.n} //nolint:forcetypeassert // test stub

	return rows.Close()
}

func TestWithScanner(t *testing.T) {
	t.Parallel()

	db, mock := newMock(t, sql.WithScanner(stubScanner{n: 42}))
	m := newManager(t, db)

	mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
	mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))

	one, err := sql.Get[int](t.Context(), m.GetQuerier(t.Context()), sql.Raw("SELECT 1"))
	require.NoError(t, err)
	assert.Equal(t, 42, one)

	all, err := sql.Select[int](t.Context(), m.GetQuerier(t.Context()), sql.Raw("SELECT 1"))
	require.NoError(t, err)
	assert.Equal(t, []int{42}, all)
}
