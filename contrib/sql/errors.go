package sql

import (
	stdsql "database/sql"
	"errors"

	"github.com/eidon-go/core/contrib/sql/internal/scan"
	"github.com/eidon-go/core/contrib/sql/internal/txcore"
	"github.com/eidon-go/core/lifecycle"
)

var (
	// ErrNoRows is returned by Get (and Querier.GetContext) when the query
	// returned no row. It is database/sql's ErrNoRows in both worlds; in the
	// pgx world check this one, not pgx.ErrNoRows.
	ErrNoRows = stdsql.ErrNoRows

	// ErrTooManyRows is returned by Get (and Querier.GetContext) when the
	// query returned more than one row: add a LIMIT 1 or a narrower WHERE.
	ErrTooManyRows = scan.ErrTooManyRows

	// ErrNestedTx is returned by WithTx with TxModeForbid inside a
	// transaction of the same database.
	ErrNestedTx = txcore.ErrNestedTx

	// ErrRollbackOnly is returned, joined with the first cause, by the WithTx
	// that rolled back a transaction (or savepoint) because a joined WithTx
	// in it failed, even when its own function returned nil; and by a joined
	// or nested WithTx that starts in such a transaction.
	ErrRollbackOnly = txcore.ErrRollbackOnly

	// ErrTxOptionsIncompatible is returned by a joined or nested WithTx that
	// asks for a stricter isolation level than the enclosing transaction
	// has, or that is not ReadOnly inside a read-only transaction.
	ErrTxOptionsIncompatible = txcore.ErrTxOptionsIncompatible

	// ErrInvalidTxMode is returned by WithTx for a TxMode that is none of
	// the declared ones.
	ErrInvalidTxMode = txcore.ErrInvalidTxMode

	// ErrTxAborted is the cause joined to ErrRollbackOnly when a joined
	// function panicked or exited its goroutine and the panic was recovered
	// before it reached the WithTx that began the transaction.
	ErrTxAborted = txcore.ErrAborted

	// ErrInvalidConfig is wrapped by every error from Config.Validate and by
	// the constructors for invalid arguments.
	ErrInvalidConfig = errors.New("sql: invalid config")

	// ErrClosed is returned by Start and Healthcheck after Shutdown. It is
	// lifecycle.ErrClosed.
	ErrClosed = lifecycle.ErrClosed
)
