package pgx_test

import (
	"context"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corepgx "github.com/eidon-go/core/contrib/sql/pgx"
	"github.com/eidon-go/core/retry"
)

// newMock returns a DB over a fresh pgxmock pool that matches queries
// exactly; the expectations are checked at the end of the test.
func newMock(t *testing.T, opts ...corepgx.Option) (*corepgx.DB, pgxmock.PgxPoolIface) {
	t.Helper()

	mock, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)

	t.Cleanup(func() { assert.NoError(t, mock.ExpectationsWereMet()) })

	db, err := corepgx.NewFromDBTX(mock, opts...)
	require.NoError(t, err)

	return db, mock
}

// unreachable is a config of a server that refuses connections at once.
func unreachable() corepgx.Config {
	return corepgx.Config{Host: "127.0.0.1", Port: 1, SSLMode: "disable", ConnectTimeout: time.Second}
}

// fastRetry retries quickly; onRetry (optional) sees every retry.
func fastRetry(attempts int, onRetry func()) corepgx.Option {
	return corepgx.WithConnectRetry(retry.Policy{
		MaxAttempts:  attempts,
		InitialDelay: time.Millisecond,
		OnRetry: func(context.Context, retry.Attempt) {
			if onRetry != nil {
				onRetry()
			}
		},
	})
}
