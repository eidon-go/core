package pgx

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync"

	"github.com/exaring/otelpgx"
	pgxv5 "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"

	"github.com/eidon-go/core/contrib/sql"
	"github.com/eidon-go/core/contrib/sql/internal/txcore"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/retry"
)

// PoolIface is what *pgxpool.Pool and pgxmock's PgxPoolIface have in
// common: the pool of a DB from NewFromDBTX.
type PoolIface interface {
	DBTX
	BeginTx(ctx context.Context, opts pgxv5.TxOptions) (pgxv5.Tx, error)
	Ping(ctx context.Context) error
	Close()
}

// DB is a pgx pool with the lifecycle of a component and the transactions
// of its Managers: New prepares the pool configuration (no I/O), Start
// creates the pool and pings the server with retry, Shutdown closes it,
// Healthcheck pings it.
//
// It is a Starter, Resource, Healthchecker and Namer. Closing the pool
// waits until every acquired connection is released: register the
// components that use the pool (job queues, consumers writing to it) after
// the DB, so that they shut down first.
type DB struct {
	name    string
	connect retry.Policy
	scanner Scanner
	core    *txcore.Core[pgxv5.Tx]

	poolCfg *pgxpool.Config // New only: the pool is created in Start
	stats   []otelpgx.StatsOption

	mu     sync.Mutex
	pool   PoolIface     // nil before Start
	real   *pgxpool.Pool // nil for NewFromDBTX
	sqlDB  *stdsql.DB    // created by SQLDB
	closed bool

	closing       context.Context //nolint:containedctx // a close signal for a Start in progress
	cancelClosing context.CancelFunc
	releaser      *lifecycle.Releaser
}

// New validates cfg and builds the pool configuration with the otelpgx
// tracer; it does no I/O: the pool is created in Start.
func New(cfg Config, opts ...Option) (*DB, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.DataSource())
	if err != nil {
		return nil, parseError(err)
	}

	applyPool(poolCfg, cfg)

	o := newOptions(opts)
	poolCfg.ConnConfig.Tracer = o.tracer()

	for _, fn := range o.poolConfig {
		fn(poolCfg)
	}

	// pgxpool starts a time.Ticker with it in Start: a zero or negative
	// period (a DSN's pool_health_check_period=0s, a WithPoolConfig) would
	// panic in a background goroutine.
	if poolCfg.HealthCheckPeriod <= 0 {
		return nil, invalid(fmt.Sprintf("health check period %s must be positive", poolCfg.HealthCheckPeriod))
	}

	d := newDB(o, nil)
	d.poolCfg = poolCfg
	d.stats = []otelpgx.StatsOption{otelpgx.WithStatsMeterProvider(o.meterProvider)}

	if o.name != DefaultName { // otherwise otelpgx names the pool host:port/database
		d.stats = append(d.stats, otelpgx.WithStatsAttributes(semconv.DBClientConnectionPoolName(o.name)))
	}

	return d, nil
}

// parseError reports a connection string pgx could not parse without the
// string itself: pgx redacts the password in its message, but not one that
// contains a quote.
func parseError(err error) error {
	var parseErr *pgconn.ParseConfigError
	if !errors.As(err, &parseErr) {
		return fmt.Errorf("sql/pgx: %w: %w", sql.ErrInvalidConfig, err)
	}

	if inner := parseErr.Unwrap(); inner != nil {
		return fmt.Errorf("sql/pgx: %w: cannot parse the connection string: %w", sql.ErrInvalidConfig, inner)
	}

	return fmt.Errorf("sql/pgx: %w: cannot parse the connection string", sql.ErrInvalidConfig)
}

// NewFromPool wraps a pool created elsewhere and takes it over: Shutdown
// closes it. Its tracer is left as it is.
func NewFromPool(pool *pgxpool.Pool, opts ...Option) (*DB, error) {
	if pool == nil {
		return nil, fmt.Errorf("sql/pgx: %w: nil *pgxpool.Pool", sql.ErrInvalidConfig)
	}

	d := newDB(newOptions(opts), pool)
	d.real = pool

	return d, nil
}

// NewFromDBTX wraps a pool-like value — pgxmock's PgxPoolIface in tests —
// and takes it over: Shutdown closes it. Pool returns nil and SQLDB
// ErrNoPool.
func NewFromDBTX(pool PoolIface, opts ...Option) (*DB, error) {
	if pool == nil || isNilPointer(pool) {
		return nil, fmt.Errorf("sql/pgx: %w: nil pool", sql.ErrInvalidConfig)
	}

	return newDB(newOptions(opts), pool), nil
}

// isNilPointer reports a typed nil pointer in an interface.
func isNilPointer(v any) bool {
	rv := reflect.ValueOf(v)

	return rv.Kind() == reflect.Pointer && rv.IsNil()
}

func newDB(o options, pool PoolIface) *DB {
	closing, cancel := context.WithCancel(context.Background())

	d := &DB{ //nolint:exhaustruct_v5 // the rest is set by the constructors or starts at zero
		name:          o.name,
		connect:       o.connectPolicy(),
		scanner:       o.scanner,
		pool:          pool,
		closing:       closing,
		cancelClosing: cancel,
	}

	d.core = txcore.New[pgxv5.Tx](pgxDriver{db: d})
	d.releaser = lifecycle.CloseOnce(d.close)

	return d
}

func applyPool(poolCfg *pgxpool.Config, cfg Config) {
	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}

	if cfg.MinConns > 0 {
		poolCfg.MinConns = cfg.MinConns
	}

	if cfg.MaxConnLifetime > 0 {
		poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	}

	if cfg.MaxConnIdleTime > 0 {
		poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	}

	if cfg.HealthCheckPeriod > 0 {
		poolCfg.HealthCheckPeriod = cfg.HealthCheckPeriod
	}

	if cfg.ConnectTimeout > 0 {
		// The dial timeout too: a DSN's connect_timeout set both, and the
		// dialer would otherwise keep the DSN's.
		poolCfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
		dialer := &net.Dialer{Timeout: cfg.ConnectTimeout} //nolint:exhaustruct_v5 // Go's defaults, as pgconn's
		poolCfg.ConnConfig.DialFunc = dialer.DialContext
	}
}

// Pool returns the pool, for libraries that take a *pgxpool.Pool (River's
// riverpgxv5). It is nil before Start and for a DB from NewFromDBTX. It is
// owned by DB: do not close it, call Shutdown. Queries on it run outside
// the transactions of WithTx; use Manager.GetQuerier, or Tx for the
// transaction itself.
func (d *DB) Pool() *pgxpool.Pool {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.real
}

// SQLDB returns a *sql.DB over the same pool, created on the first call,
// for libraries that only speak database/sql (migrations, riverdatabasesql).
// It shares the pool and its tracer but not the transactions of WithTx.
// Shutdown closes it. It fails with ErrNotStarted before Start, ErrNoPool
// for a DB from NewFromDBTX and ErrClosed after Shutdown.
func (d *DB) SQLDB() (*stdsql.DB, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	switch {
	case d.closed:
		return nil, ErrClosed
	case d.real == nil && d.pool == nil:
		return nil, ErrNotStarted
	case d.real == nil:
		return nil, ErrNoPool
	}

	if d.sqlDB == nil {
		d.sqlDB = stdlib.OpenDBFromPool(d.real)
	}

	return d.sqlDB, nil
}

// Name returns the component name (WithName). Implements lifecycle.Namer.
func (d *DB) Name() string { return d.name }

// Start creates the pool (New) and pings the server until it answers,
// retried with the WithConnectRetry policy; Shutdown ends the retries. It
// returns ErrClosed after Shutdown. Implements lifecycle.Starter.
func (d *DB) Start(ctx context.Context) error {
	pool, err := d.create(ctx)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stop := context.AfterFunc(d.closing, cancel) //nolint:contextcheck // the close signal, not a parent context
	defer stop()

	if err := d.connect.Do(ctx, pool.Ping); err != nil {
		if d.closing.Err() != nil {
			return wrap(ErrClosed)
		}

		return fmt.Errorf("sql/pgx: ping: %w", err)
	}

	return nil
}

// Shutdown closes the *sql.DB of SQLDB, if any, then the pool, which waits
// until every acquired connection is released. When ctx is done first it
// returns ctx.Err() and the close goes on in the background. Idempotent,
// safe without Start. Implements lifecycle.Resource.
func (d *DB) Shutdown(ctx context.Context) error {
	return wrap(d.releaser.Shutdown(ctx))
}

// Healthcheck pings the server within ctx; ErrNotStarted before Start,
// ErrClosed after Shutdown. Implements lifecycle.Healthchecker.
func (d *DB) Healthcheck(ctx context.Context) error {
	d.mu.Lock()
	closed, pool := d.closed, d.pool
	d.mu.Unlock()

	switch {
	case closed:
		return wrap(ErrClosed)
	case pool == nil:
		return ErrNotStarted
	}

	if err := pool.Ping(ctx); err != nil {
		if d.releaser.Closing() { // Shutdown raced the ping: "closed pool"
			return wrap(ErrClosed)
		}

		return fmt.Errorf("sql/pgx: ping: %w", err)
	}

	return nil
}

// create creates the pool of a DB from New on the first Start.
func (d *DB) create(ctx context.Context) (PoolIface, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil, wrap(ErrClosed)
	}

	if d.pool != nil {
		return d.pool, nil
	}

	// pgxpool keeps this ctx for the background goroutine that opens
	// MinConns: a start ctx cancelled after startup must not cut it short.
	pool, err := pgxpool.NewWithConfig(context.WithoutCancel(ctx), d.poolCfg)
	if err != nil {
		return nil, fmt.Errorf("sql/pgx: create pool: %w", err)
	}

	if err := otelpgx.RecordStats(pool, d.stats...); err != nil {
		pool.Close()

		return nil, fmt.Errorf("sql/pgx: record pool stats: %w", err)
	}

	d.pool, d.real = pool, pool

	return pool, nil
}

// close is the release of Shutdown, run once.
func (d *DB) close() error {
	d.mu.Lock()
	d.closed = true
	pool, sqlDB := d.pool, d.sqlDB
	d.mu.Unlock()

	d.cancelClosing()

	var err error

	if sqlDB != nil {
		err = sqlDB.Close()
	}

	if pool != nil {
		pool.Close()
	}

	return err //nolint:wrapcheck // Shutdown wraps
}

// dbtx returns the pool, or a stand-in that fails with ErrNotStarted.
func (d *DB) dbtx() PoolIface {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.pool == nil {
		return unstarted{}
	}

	return d.pool
}

// Tx returns the pgx.Tx of the WithTx in ctx on db, if any. It is the
// extension point for libraries that enlist in the caller's transaction
// (River's riverpgxv5 InsertTx); prefer Manager.GetQuerier for queries.
func Tx(ctx context.Context, db *DB) (pgxv5.Tx, bool) {
	return db.core.Tx(ctx)
}

// wrap prefixes an error returned by a lifecycle method with the package
// name.
func wrap(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("sql/pgx: %w", err)
}

// pgxDriver runs the transactions of txcore on the pool of a DB.
type pgxDriver struct{ db *DB }

func (p pgxDriver) Begin(ctx context.Context, opts txcore.Options) (pgxv5.Tx, error) {
	iso, err := isoLevel(opts.Isolation)
	if err != nil {
		return nil, err
	}

	txOpts := pgxv5.TxOptions{IsoLevel: iso} //nolint:exhaustruct_v5 // server defaults for the rest
	if opts.ReadOnly {
		txOpts.AccessMode = pgxv5.ReadOnly
	}

	return p.db.dbtx().BeginTx(ctx, txOpts) //nolint:wrapcheck // txcore wraps
}

func (pgxDriver) Commit(ctx context.Context, tx pgxv5.Tx) error {
	return tx.Commit(ctx) //nolint:wrapcheck // txcore wraps
}

// Rollback treats a closed transaction as rolled back.
func (pgxDriver) Rollback(ctx context.Context, tx pgxv5.Tx) error {
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgxv5.ErrTxClosed) {
		return err //nolint:wrapcheck // txcore wraps
	}

	return nil
}

func (pgxDriver) Exec(ctx context.Context, tx pgxv5.Tx, query string) error {
	_, err := tx.Exec(ctx, query)

	return err //nolint:wrapcheck // txcore wraps
}

func isoLevel(level stdsql.IsolationLevel) (pgxv5.TxIsoLevel, error) {
	switch level { //nolint:exhaustive // the rest are unsupported
	case stdsql.LevelDefault:
		return "", nil
	case stdsql.LevelReadUncommitted:
		return pgxv5.ReadUncommitted, nil
	case stdsql.LevelReadCommitted:
		return pgxv5.ReadCommitted, nil
	case stdsql.LevelRepeatableRead:
		return pgxv5.RepeatableRead, nil
	case stdsql.LevelSerializable:
		return pgxv5.Serializable, nil
	default:
		return "", fmt.Errorf("%w: %s", ErrUnsupportedIsolation, level)
	}
}

// Compile-time checks.
var (
	_ lifecycle.Starter       = (*DB)(nil)
	_ lifecycle.Resource      = (*DB)(nil)
	_ lifecycle.Healthchecker = (*DB)(nil)
	_ lifecycle.Namer         = (*DB)(nil)
	_ PoolIface               = (*pgxpool.Pool)(nil)
	_ PoolIface               = unstarted{}
)
