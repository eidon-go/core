package pgx

import (
	"errors"

	"github.com/eidon-go/core/lifecycle"
)

// The transaction errors (sql.ErrRollbackOnly, sql.ErrNestedTx, …) and
// sql.ErrNoRows, sql.ErrTooManyRows, sql.ErrInvalidConfig are those of
// package sql: domain code checks one set in both worlds.
var (
	// ErrNotStarted is returned by queries, transactions, Healthcheck and
	// SQLDB of a DB from New before Start created its pool.
	ErrNotStarted = errors.New("sql/pgx: not started")

	// ErrClosed is returned by Start, Healthcheck and SQLDB after Shutdown.
	// It is lifecycle.ErrClosed.
	ErrClosed = lifecycle.ErrClosed

	// ErrNoPool is returned by SQLDB of a DB from NewFromDBTX, which has no
	// *pgxpool.Pool.
	ErrNoPool = errors.New("sql/pgx: no *pgxpool.Pool behind this DB")

	// ErrUnsupportedIsolation is returned by WithTx for an isolation level
	// PostgreSQL does not have (sql.LevelWriteCommitted, LevelSnapshot,
	// LevelLinearizable).
	ErrUnsupportedIsolation = errors.New("sql/pgx: isolation level not supported")

	// ErrLastInsertIDUnsupported is returned by Result.LastInsertId of
	// ExecContext: PostgreSQL has none, use RETURNING with sql.Get.
	ErrLastInsertIDUnsupported = errors.New("sql/pgx: LastInsertId is not supported, use RETURNING")
)
