package pgx

import (
	"context"
	"fmt"

	"github.com/eidon-go/core/contrib/sql"
	"github.com/eidon-go/core/contrib/sql/internal/txcore"
)

// QuerierProvider gives repositories the querier of the current context;
// the pgx-world twin of sql.QuerierProvider.
type QuerierProvider interface {
	// GetQuerier returns the transaction of ctx on this database, or the
	// pool outside one.
	GetQuerier(ctx context.Context) PgxQuerier
	// HasTx reports whether ctx carries a transaction of this database.
	HasTx(ctx context.Context) bool
}

// Manager is an sql.Transactor and a QuerierProvider over one DB. It is not
// an sql.Manager: GetQuerier returns a PgxQuerier. Domain code that depends
// on sql.Transactor works with both worlds.
type Manager interface {
	sql.Transactor
	QuerierProvider
}

// NewManager returns a Manager over db; the options and transaction
// semantics are those of sql.NewManager. A nil db is an error wrapping
// sql.ErrInvalidConfig.
func NewManager(db *DB, opts ...sql.ManagerOption) (Manager, error) {
	if db == nil {
		return nil, fmt.Errorf("sql/pgx: %w: nil *DB", sql.ErrInvalidConfig)
	}

	o := txcore.ManagerOptions{DefaultMode: txcore.ModeJoin}

	for _, opt := range opts {
		txcore.ApplyManagerOption(opt, &o)
	}

	return &manager{db: db, defaultMode: o.DefaultMode}, nil
}

type manager struct {
	db          *DB
	defaultMode txcore.Mode
}

func (m *manager) WithTx(ctx context.Context, body func(ctx context.Context) error, opts ...sql.TxOption) error {
	o := txcore.Options{Mode: m.defaultMode} //nolint:exhaustruct_v5 // defaults; options set the rest

	for _, opt := range opts {
		txcore.ApplyTxOption(opt, &o)
	}

	return m.db.core.WithTx(ctx, o, body) //nolint:wrapcheck // fn's error as is
}

func (m *manager) OnCommit(ctx context.Context, fn func()) { m.db.core.OnCommit(ctx, fn) }

func (m *manager) GetQuerier(ctx context.Context) PgxQuerier {
	if tx, ok := m.db.core.Tx(ctx); ok {
		return querier{DBTX: tx, scanner: m.db.scanner}
	}

	return querier{DBTX: m.db.dbtx(), scanner: m.db.scanner}
}

func (m *manager) HasTx(ctx context.Context) bool {
	_, ok := m.db.core.Tx(ctx)

	return ok
}
