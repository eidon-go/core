package sql

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"slices"

	"github.com/XSAM/otelsql"
	"go.opentelemetry.io/otel/metric"

	"github.com/eidon-go/core/contrib/sql/internal/txcore"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/retry"
)

// DB is a database/sql pool with the lifecycle of a component and the
// transactions of its Managers: New opens the pool (database/sql connects
// lazily, so no I/O), Start pings it with retry, Shutdown closes it,
// Healthcheck pings it.
//
// It is a Starter, Resource, Healthchecker and Namer. Register components
// that use the pool (job queues, consumers writing to it) after the DB, so
// that they shut down first: closing the pool waits for the queries in
// flight.
type DB struct {
	name     string
	db       *stdsql.DB
	scanner  Scanner
	connect  retry.Policy
	core     *txcore.Core[*stdsql.Tx]
	releaser *lifecycle.Releaser
}

// New validates cfg and opens the pool, instrumented with otelsql (traces
// and metrics, pool stats included). It does no I/O: database/sql connects
// on first use. The driver must be registered (imported) beforehand.
func New(cfg Config, opts ...Option) (*DB, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	if !slices.Contains(stdsql.Drivers(), cfg.DriverName) {
		return nil, fmt.Errorf("%w: driver %q is not registered (forgotten import?)", ErrInvalidConfig, cfg.DriverName)
	}

	o := newOptions(opts)

	db, err := otelsql.Open(cfg.DriverName, cfg.DataSource, o.otelSQL()...)
	if err != nil {
		return nil, fmt.Errorf("%w: open: %w", ErrInvalidConfig, err)
	}

	applyPool(db, cfg)

	stats, err := otelsql.RegisterDBStatsMetrics(db, o.otelSQL()...)
	if err != nil {
		_ = db.Close()

		return nil, fmt.Errorf("sql: register pool metrics: %w", err)
	}

	return newDB(db, o, stats), nil
}

// NewFromDB wraps an open *sql.DB (a custom connector, sqlmock) and takes
// it over: Shutdown closes it. It is not instrumented: wrap the connector
// with otelsql yourself if needed.
func NewFromDB(db *stdsql.DB, opts ...Option) (*DB, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: nil *sql.DB", ErrInvalidConfig)
	}

	return newDB(db, newOptions(opts), nil), nil
}

func newDB(db *stdsql.DB, o options, stats metric.Registration) *DB {
	d := &DB{ //nolint:exhaustruct_v5 // releaser is set below
		name:    o.name,
		db:      db,
		scanner: o.scanner,
		connect: o.connectPolicy(),
		core:    txcore.New[*stdsql.Tx](stdDriver{db: db}),
	}

	d.core.SetDefaultIsolation(o.defaultIso)

	d.releaser = lifecycle.CloseOnce(func() error {
		var errs []error

		if stats != nil {
			errs = append(errs, stats.Unregister())
		}

		errs = append(errs, db.Close())

		return errors.Join(errs...)
	})

	return d
}

func applyPool(db *stdsql.DB, cfg Config) {
	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}

	if cfg.MaxIdleConns != 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}

	if cfg.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}

	if cfg.ConnMaxIdleTime > 0 {
		db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	}
}

// DB returns the pool, for libraries that take a *sql.DB (migrations, job
// queues). It is owned by DB: do not close it, call Shutdown. Queries on it
// run outside the transactions of WithTx; use Manager.GetQuerier.
func (d *DB) DB() *stdsql.DB { return d.db }

// Name returns the component name (WithName). Implements lifecycle.Namer.
func (d *DB) Name() string { return d.name }

// Start pings the database until it answers, retried with the
// WithConnectRetry policy; Shutdown ends the retries. It returns ErrClosed
// after Shutdown. Implements lifecycle.Starter.
func (d *DB) Start(ctx context.Context) error {
	return wrap(d.releaser.Do(ctx, func(ctx context.Context) error {
		if err := d.connect.Do(ctx, d.db.PingContext); err != nil {
			return fmt.Errorf("ping: %w", err)
		}

		return nil
	}))
}

// Shutdown closes the pool: database/sql waits for the queries in flight.
// When ctx is done first it returns ctx.Err() and the close goes on in the
// background. Idempotent, safe without Start. Implements lifecycle.Resource.
func (d *DB) Shutdown(ctx context.Context) error {
	return wrap(d.releaser.Shutdown(ctx))
}

// Healthcheck pings the database within ctx; ErrClosed after Shutdown.
// Implements lifecycle.Healthchecker.
func (d *DB) Healthcheck(ctx context.Context) error {
	if d.releaser.Closing() {
		return wrap(ErrClosed)
	}

	if err := d.db.PingContext(ctx); err != nil {
		if d.releaser.Closing() { // Shutdown raced the ping: "database is closed"
			return wrap(ErrClosed)
		}

		return fmt.Errorf("sql: ping: %w", err)
	}

	return nil
}

// Tx returns the *sql.Tx of the WithTx in ctx on db, if any. It is the
// extension point for libraries that enlist in the caller's transaction
// through their own API (a job queue's transactional insert); prefer
// Manager.GetQuerier for queries.
func Tx(ctx context.Context, db *DB) (*stdsql.Tx, bool) {
	return db.core.Tx(ctx)
}

// wrap prefixes an error returned by a lifecycle method with the module
// name.
func wrap(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("sql: %w", err)
}

// stdDriver runs the transactions of txcore on database/sql.
type stdDriver struct{ db *stdsql.DB }

func (s stdDriver) Begin(ctx context.Context, opts txcore.Options) (*stdsql.Tx, error) {
	txOpts := &stdsql.TxOptions{Isolation: opts.Isolation, ReadOnly: opts.ReadOnly}

	return s.db.BeginTx(ctx, txOpts) //nolint:wrapcheck // txcore wraps
}

func (stdDriver) Commit(_ context.Context, tx *stdsql.Tx) error {
	return tx.Commit() //nolint:wrapcheck // txcore wraps
}

// Rollback treats a transaction database/sql already rolled back (its
// context was cancelled) as rolled back. *sql.Tx.Rollback takes no ctx, so
// txcore's rollback timeout does not bound it.
func (stdDriver) Rollback(_ context.Context, tx *stdsql.Tx) error {
	if err := tx.Rollback(); err != nil && !errors.Is(err, stdsql.ErrTxDone) {
		return err //nolint:wrapcheck // txcore wraps
	}

	return nil
}

func (stdDriver) Exec(ctx context.Context, tx *stdsql.Tx, query string) error {
	_, err := tx.ExecContext(ctx, query)

	return err //nolint:wrapcheck // txcore wraps
}

// Compile-time checks.
var (
	_ lifecycle.Starter       = (*DB)(nil)
	_ lifecycle.Resource      = (*DB)(nil)
	_ lifecycle.Healthchecker = (*DB)(nil)
	_ lifecycle.Namer         = (*DB)(nil)
)
