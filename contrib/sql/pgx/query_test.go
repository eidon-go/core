package pgx_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	pgxv5 "github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/sql"
	corepgx "github.com/eidon-go/core/contrib/sql/pgx"
)

type audit struct {
	CreatedAt time.Time `db:"created_at"`
}

type user struct {
	audit

	UserID   uuid.UUID
	FullName string `db:"name"`
	Tags     []string
}

const userQuery = "SELECT user_id, name, tags, created_at FROM users WHERE user_id = $1"

func userRows() *pgxmock.Rows {
	return pgxmock.NewRows([]string{"user_id", "name", "tags", "created_at"})
}

func newQuerier(t *testing.T) (corepgx.PgxQuerier, pgxmock.PgxPoolIface) {
	t.Helper()

	mock, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)

	t.Cleanup(func() { assert.NoError(t, mock.ExpectationsWereMet()) })

	return corepgx.NewQuerier(mock, nil), mock
}

func TestGet(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	created := time.Date(2026, 11, 26, 0, 0, 0, 0, time.UTC)

	t.Run("struct with the same mapping as package sql", func(t *testing.T) {
		t.Parallel()

		q, mock := newQuerier(t)
		mock.ExpectQuery(userQuery).WithArgs(id).
			WillReturnRows(userRows().AddRow(id, "Ann", []string{"a", "b"}, created))

		got, err := sql.Get[user](t.Context(), q, sql.Raw(userQuery, id))

		require.NoError(t, err)
		assert.Equal(
			t,
			user{audit: audit{CreatedAt: created}, UserID: id, FullName: "Ann", Tags: []string{"a", "b"}},
			got,
		)
	})

	t.Run("no rows is sql.ErrNoRows", func(t *testing.T) {
		t.Parallel()

		q, mock := newQuerier(t)
		mock.ExpectQuery(userQuery).WithArgs(id).WillReturnRows(userRows())

		_, err := sql.Get[user](t.Context(), q, sql.Raw(userQuery, id))

		require.ErrorIs(t, err, sql.ErrNoRows)
	})

	t.Run("more than one row", func(t *testing.T) {
		t.Parallel()

		q, mock := newQuerier(t)
		mock.ExpectQuery("SELECT n FROM t").WillReturnRows(pgxmock.NewRows([]string{"n"}).AddRow(1).AddRow(2))

		_, err := sql.Get[int](t.Context(), q, sql.Raw("SELECT n FROM t"))

		require.ErrorIs(t, err, sql.ErrTooManyRows)
	})

	t.Run("query error", func(t *testing.T) {
		t.Parallel()

		errDriver := errors.New("driver")
		q, mock := newQuerier(t)
		mock.ExpectQuery("SELECT n FROM t").WillReturnError(errDriver)

		_, err := sql.Get[int](t.Context(), q, sql.Raw("SELECT n FROM t"))

		require.ErrorIs(t, err, errDriver)
	})
}

func TestSelect(t *testing.T) {
	t.Parallel()

	q, mock := newQuerier(t)
	mock.ExpectQuery("SELECT n FROM t").WillReturnRows(pgxmock.NewRows([]string{"n"}).AddRow(1).AddRow(2))
	mock.ExpectQuery("SELECT n FROM t").WillReturnError(errDomain)

	got, err := sql.Select[int](t.Context(), q, sql.Raw("SELECT n FROM t"))
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2}, got)

	_, err = sql.Select[int](t.Context(), q, sql.Raw("SELECT n FROM t"))
	require.ErrorIs(t, err, errDomain)
}

func TestExecContext(t *testing.T) {
	t.Parallel()

	q, mock := newQuerier(t)
	mock.ExpectExec("DELETE FROM t").WillReturnResult(pgxmock.NewResult("DELETE", 3))
	mock.ExpectExec("DELETE FROM t").WillReturnError(errDomain)

	res, err := sql.Exec(t.Context(), q, sql.Raw("DELETE FROM t"))
	require.NoError(t, err)

	n, err := res.RowsAffected()
	require.NoError(t, err)
	assert.EqualValues(t, 3, n)

	_, err = res.LastInsertId()
	require.ErrorIs(t, err, corepgx.ErrLastInsertIDUnsupported)

	_, err = sql.Exec(t.Context(), q, sql.Raw("DELETE FROM t"))
	require.ErrorIs(t, err, errDomain)
}

// stubScanner fills every destination with the same int.
type stubScanner struct{ n int }

func (s stubScanner) ScanOne(rows pgxv5.Rows, dest any) error {
	rows.Close()

	*dest.(*int) = s.n //nolint:forcetypeassert // test stub

	return nil
}

func (s stubScanner) ScanAll(rows pgxv5.Rows, dest any) error {
	rows.Close()

	*dest.(*[]int) = []int{s.n} //nolint:forcetypeassert // test stub

	return nil
}

func TestWithScanner(t *testing.T) {
	t.Parallel()

	db, mock := newMock(t, corepgx.WithScanner(stubScanner{n: 42}))
	q := newManager(t, db).GetQuerier(t.Context())

	mock.ExpectQuery("SELECT 1").WillReturnRows(pgxmock.NewRows([]string{"n"}).AddRow(1))
	mock.ExpectQuery("SELECT 1").WillReturnRows(pgxmock.NewRows([]string{"n"}).AddRow(1))

	one, err := sql.Get[int](t.Context(), q, sql.Raw("SELECT 1"))
	require.NoError(t, err)
	assert.Equal(t, 42, one)

	all, err := sql.Select[int](t.Context(), q, sql.Raw("SELECT 1"))
	require.NoError(t, err)
	assert.Equal(t, []int{42}, all)
}

func TestUnstarted(t *testing.T) {
	t.Parallel()

	db, err := corepgx.New(unreachable())
	require.NoError(t, err)

	q := newManager(t, db).GetQuerier(t.Context())
	ctx := t.Context()

	_, err = q.Exec(ctx, "SELECT 1")
	require.ErrorIs(t, err, corepgx.ErrNotStarted)

	require.ErrorIs(t, q.QueryRow(ctx, "SELECT 1").Scan(), corepgx.ErrNotStarted)

	_, err = q.CopyFrom(ctx, pgxv5.Identifier{"t"}, nil, nil)
	require.ErrorIs(t, err, corepgx.ErrNotStarted)

	batch := q.SendBatch(ctx, &pgxv5.Batch{})

	_, err = batch.Exec()
	require.ErrorIs(t, err, corepgx.ErrNotStarted)

	_, err = batch.Query() //nolint:sqlclosecheck // no rows: the call fails
	require.ErrorIs(t, err, corepgx.ErrNotStarted)

	require.ErrorIs(t, batch.QueryRow().Scan(), corepgx.ErrNotStarted)
	withTx, ok := batch.(interface{ Tx() pgxv5.Tx }) // BatchResults.Tx exists from pgx v5.12
	require.True(t, ok)
	assert.Nil(t, withTx.Tx())
	require.ErrorIs(t, batch.Close(), corepgx.ErrNotStarted)
}
