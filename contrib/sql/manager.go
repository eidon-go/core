package sql

import (
	"context"
	stdsql "database/sql"
	"fmt"

	"github.com/eidon-go/core/contrib/sql/internal/txcore"
)

// Transactor runs functions in transactions. Domain code depends on it (and
// fakes it in tests) rather than on Manager. The pgx world implements it
// too.
type Transactor interface {
	// WithTx runs fn in a transaction: it commits when fn returns nil and
	// rolls back when fn returns an error, panics or exits its goroutine.
	// fn's error is returned as is. Inside a transaction of the same
	// database the TxMode decides (default TxModeJoin). Queries in fn go
	// through QuerierProvider.GetQuerier(ctx) with fn's ctx.
	WithTx(ctx context.Context, fn func(ctx context.Context) error, opts ...TxOption) error

	// OnCommit runs fn after the outermost commit of the transaction of ctx
	// on this database — to publish events or fill caches only once the
	// data is visible. It is dropped when the transaction (or the savepoint
	// it was registered in) rolls back. Outside a transaction, fn runs at
	// once. Callbacks run in registration order in the goroutine of the
	// outermost WithTx. A panicking callback does not stop the ones after
	// it: they all run, then the first panic propagates (the transaction is
	// committed).
	OnCommit(ctx context.Context, fn func())
}

// QuerierProvider gives repositories the querier of the current context.
type QuerierProvider interface {
	// GetQuerier returns the transaction of ctx on this database, or the
	// database itself outside one.
	GetQuerier(ctx context.Context) SQLQuerier
	// HasTx reports whether ctx carries a transaction of this database.
	HasTx(ctx context.Context) bool
}

// Manager is a Transactor and a QuerierProvider over one DB.
type Manager interface {
	Transactor
	QuerierProvider
}

// TxMode is the behavior of WithTx inside a transaction of the same
// database. Outside one, every mode begins a new transaction.
type TxMode int

// Transaction modes.
const (
	// TxModeJoin (the default) runs in the current transaction. An error or
	// panic marks it rollback-only: the WithTx that began it rolls back and
	// returns ErrRollbackOnly joined with the cause, even when its own
	// function handled the error and returned nil. Inside a TxModeNested
	// call, only that savepoint is marked.
	TxModeJoin = TxMode(txcore.ModeJoin)

	// TxModeNested runs in a savepoint (SAVEPOINT sp_<n>): an error rolls
	// back to it and the transaction goes on. MySQL commits implicitly on
	// DDL, savepoints included. Savepoints are a stack on one connection: do
	// not open them from several goroutines in one transaction.
	TxModeNested = TxMode(txcore.ModeNested)

	// TxModeRequiresNew begins a separate transaction on another
	// connection, committed on its own. It holds two connections at once:
	// beware of pool exhaustion and of deadlocks with the outer transaction.
	TxModeRequiresNew = TxMode(txcore.ModeRequiresNew)

	// TxModeForbid fails with ErrNestedTx inside a transaction.
	TxModeForbid = TxMode(txcore.ModeForbid)
)

// String returns the name of the mode.
func (m TxMode) String() string { return txcore.Mode(m).String() }

// TxOption configures one WithTx call; the default call needs none. The
// zero value changes nothing.
type TxOption = txcore.TxOption

// WithTxMode sets the mode of a nested WithTx (default TxModeJoin, or the
// Manager's WithDefaultTxMode).
func WithTxMode(mode TxMode) TxOption {
	return txcore.NewTxOption(func(o *txcore.Options) { o.Mode = txcore.Mode(mode) })
}

// WithIsolation sets the isolation level of a new transaction. A joined or
// nested call may ask for the same or a weaker level than the transaction
// has — a transaction begun with sql.LevelDefault counts as Read Committed,
// the weakest default of PostgreSQL, MySQL and SQL Server — and a stricter
// one fails with ErrTxOptionsIncompatible. Levels are compared by their order in
// database/sql, which fits PostgreSQL and MySQL; levels outside one scale
// (SQL Server's Snapshot against RepeatableRead) are compared the same way.
func WithIsolation(level stdsql.IsolationLevel) TxOption {
	return txcore.NewTxOption(func(o *txcore.Options) { o.Isolation = level })
}

// ReadOnly begins a read-only transaction. Inside a read-only transaction,
// a joined or nested call must be ReadOnly too (ErrTxOptionsIncompatible
// otherwise) — a repository method that wraps its reads in a plain WithTx
// fails there; let it use GetQuerier, or pass ReadOnly. Inside a read-write
// transaction it may be either.
func ReadOnly() TxOption {
	return txcore.NewTxOption(func(o *txcore.Options) { o.ReadOnly = true })
}

// ManagerOption configures NewManager (of this package and of sql/pgx). The
// zero value changes nothing.
type ManagerOption = txcore.ManagerOption

// WithDefaultTxMode sets the mode of a nested WithTx without WithTxMode
// (default TxModeJoin).
func WithDefaultTxMode(mode TxMode) ManagerOption {
	return txcore.NewManagerOption(func(o *txcore.ManagerOptions) { o.DefaultMode = txcore.Mode(mode) })
}

// NewManager returns a Manager over db. Managers over the same DB share its
// transactions; transactions of different DBs never mix, even in one ctx.
// A nil db is an error wrapping ErrInvalidConfig.
func NewManager(db *DB, opts ...ManagerOption) (Manager, error) {
	if db == nil {
		return nil, fmt.Errorf("sql: %w: nil *DB", ErrInvalidConfig)
	}

	o := txcore.ManagerOptions{DefaultMode: txcore.ModeJoin}
	txcore.ApplyManager(&o, opts)

	return &manager{db: db, defaultMode: o.DefaultMode}, nil
}

type manager struct {
	db          *DB
	defaultMode txcore.Mode
}

func (m *manager) WithTx(ctx context.Context, body func(ctx context.Context) error, opts ...TxOption) error {
	o := txcore.Options{Mode: m.defaultMode} //nolint:exhaustruct_v5 // defaults; options set the rest
	txcore.ApplyTx(&o, opts)

	return m.db.core.WithTx(ctx, o, body) //nolint:wrapcheck // fn's error as is
}

func (m *manager) OnCommit(ctx context.Context, fn func()) { m.db.core.OnCommit(ctx, fn) }

func (m *manager) GetQuerier(ctx context.Context) SQLQuerier {
	if tx, ok := m.db.core.Tx(ctx); ok {
		return querier{DBTX: tx, scanner: m.db.scanner}
	}

	return querier{DBTX: m.db.db, scanner: m.db.scanner}
}

func (m *manager) HasTx(ctx context.Context) bool {
	_, ok := m.db.core.Tx(ctx)

	return ok
}
