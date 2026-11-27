package sqlx

import (
	"context"
	stdsql "database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"

	"github.com/eidon-go/core/contrib/sql"
)

// Querier is the transaction of the context, or the database outside one,
// for code written for sqlx and for the helpers of package sql alike:
//
//   - an sqlx.ExtContext: sqlx.GetContext(ctx, q, …), sqlx.SelectContext,
//     q.Rebind (after sqlx.In), q.QueryxContext, q.QueryRowxContext;
//   - an sql.SQLQuerier: sql.Get[T](ctx, q, …), sqlc-generated code;
//   - named parameters with the DB's bind type and the Manager's mapper.
//
// Its GetContext keeps the contract of sql.Querier (sql.ErrTooManyRows when
// the query returns more than one row); the function sqlx.GetContext(ctx,
// q, …) keeps sqlx's (the first row).
type Querier interface {
	sql.SQLQuerier
	sqlx.ExtContext

	// NamedExecContext runs a statement with :name parameters bound from
	// arg (a struct, a map, or a slice of them for a batch insert).
	NamedExecContext(ctx context.Context, query string, arg any) (stdsql.Result, error)
	// NamedQueryContext runs a query with :name parameters bound from arg.
	NamedQueryContext(ctx context.Context, query string, arg any) (*sqlx.Rows, error)
}

// Manager is an sql.Transactor whose GetQuerier returns a Querier. It
// shares the transactions of the DB with every sql.Manager over it.
type Manager interface {
	sql.Transactor

	// GetQuerier returns the transaction of ctx on the DB, or the DB itself
	// outside one.
	GetQuerier(ctx context.Context) Querier
	// HasTx reports whether ctx carries a transaction of the DB.
	HasTx(ctx context.Context) bool
}

// Option configures NewManager.
type Option func(*options)

type options struct {
	mapper     *reflectx.Mapper
	managerOps []sql.ManagerOption
}

// WithMapper sets the mapper of struct scans and named parameters (default
// DefaultMapper()), e.g. reflectx.NewMapperFunc("db", strcase.ToSnake).
func WithMapper(mapper *reflectx.Mapper) Option { return func(o *options) { o.mapper = mapper } }

// WithDefaultTxMode sets the mode of a nested WithTx without sql.WithTxMode
// (default sql.TxModeJoin), as sql.WithDefaultTxMode does.
func WithDefaultTxMode(mode sql.TxMode) Option {
	return func(o *options) { o.managerOps = append(o.managerOps, sql.WithDefaultTxMode(mode)) }
}

// NewManager returns a Manager over db. The bind type comes from
// db.DriverName(): ErrUnknownDriver when sqlx does not know it (a DB from
// sql.NewFromDB needs sql.WithDriverName).
func NewManager(db *sql.DB, opts ...Option) (Manager, error) {
	if db == nil {
		return nil, ErrNilDB
	}

	if sqlx.BindType(db.DriverName()) == sqlx.UNKNOWN {
		return nil, fmt.Errorf("%w: %q (use sql.WithDriverName or sqlx.BindDriver)", ErrUnknownDriver, db.DriverName())
	}

	var o options

	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}

	if o.mapper == nil {
		o.mapper = DefaultMapper()
	}

	inner, err := sql.NewManager(db, o.managerOps...)
	if err != nil {
		return nil, err //nolint:wrapcheck // sql's error, as is
	}

	x := sqlx.NewDb(db.DB(), db.DriverName())
	x.Mapper = o.mapper

	return &manager{Transactor: inner, db: db, x: x, scanner: scanner{mapper: o.mapper}}, nil
}

type manager struct {
	sql.Transactor

	db      *sql.DB
	x       *sqlx.DB
	scanner scanner
}

func (m *manager) GetQuerier(ctx context.Context) Querier {
	if tx, ok := sql.Tx(ctx, m.db); ok {
		// A Tx built outside sqlx has no driver name: it only runs queries;
		// the dialect (Rebind, named parameters) comes from x.
		return querier{conn: &sqlx.Tx{Tx: tx, Mapper: m.x.Mapper}, x: m.x, scanner: m.scanner}
	}

	return querier{conn: m.x, x: m.x, scanner: m.scanner}
}

func (m *manager) HasTx(ctx context.Context) bool {
	_, ok := sql.Tx(ctx, m.db)

	return ok
}

// conn is what *sqlx.DB and *sqlx.Tx have in common for running queries.
type conn interface {
	sql.DBTX

	QueryxContext(ctx context.Context, query string, args ...any) (*sqlx.Rows, error)
	QueryRowxContext(ctx context.Context, query string, args ...any) *sqlx.Row
}

type querier struct {
	conn

	x       *sqlx.DB
	scanner scanner
}

func (q querier) DriverName() string { return q.x.DriverName() }

func (q querier) Rebind(query string) string { return q.x.Rebind(query) }

func (q querier) BindNamed(query string, arg any) (string, []any, error) {
	return q.x.BindNamed(query, arg) //nolint:wrapcheck // sqlx's error, as is
}

func (q querier) GetContext(ctx context.Context, dest any, query string, args ...any) error {
	rows, err := q.QueryContext(ctx, query, args...) // the scanner checks and closes rows
	if err != nil {
		return err //nolint:wrapcheck // the driver's error, as is
	}

	return q.scanner.ScanOne(rows, dest)
}

func (q querier) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	rows, err := q.QueryContext(ctx, query, args...) // the scanner checks and closes rows
	if err != nil {
		return err //nolint:wrapcheck // the driver's error, as is
	}

	return q.scanner.ScanAll(rows, dest) //nolint:contextcheck // scanning does no I/O of its own
}

func (q querier) NamedExecContext(ctx context.Context, query string, arg any) (stdsql.Result, error) {
	bound, args, err := q.BindNamed(query, arg)
	if err != nil {
		return nil, err
	}

	return q.ExecContext(ctx, bound, args...) //nolint:wrapcheck // the driver's error, as is
}

func (q querier) NamedQueryContext(ctx context.Context, query string, arg any) (*sqlx.Rows, error) {
	bound, args, err := q.BindNamed(query, arg)
	if err != nil {
		return nil, err
	}

	return q.QueryxContext(ctx, bound, args...)
}
