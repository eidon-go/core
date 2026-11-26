package sql_test

import (
	stdsql "database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/sql"
)

// newMock returns a DB over a fresh sqlmock that matches queries exactly
// and expects pings; the expectations are checked at the end of the test.
func newMock(t *testing.T, opts ...sql.Option) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()

	sdb, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual),
		sqlmock.MonitorPingsOption(true),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, mock.ExpectationsWereMet())

		_ = sdb.Close()
	})

	db, err := sql.NewFromDB(sdb, opts...)
	require.NoError(t, err)

	return db, mock
}

// newQuerier returns a querier over a fresh sqlmock.
func newQuerier(t *testing.T) (sql.SQLQuerier, sqlmock.Sqlmock) {
	t.Helper()

	db, mock := newMock(t)

	return sql.NewQuerier(db.DB(), nil), mock
}

// result is the sqlmock result of a statement that changed n rows.
func result(n int64) stdsql.Result { return sqlmock.NewResult(0, n) }
