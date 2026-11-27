package river

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	riverqueue "github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"

	corepgx "github.com/eidon-go/core/contrib/sql/pgx"
	"github.com/eidon-go/core/lifecycle"
)

// hardStopTimeout bounds the wait for the jobs River cancels once Shutdown's
// deadline has passed; Run has returned by then (lifecycle.Worker), so it
// only bounds a goroutine.
const hardStopTimeout = 5 * time.Second

// Inserter inserts jobs; domain code depends on it (and fakes it in tests)
// rather than on Client.
type Inserter interface {
	// Insert inserts a job in the transaction of ctx, or on the pool
	// outside one.
	Insert(
		ctx context.Context,
		args riverqueue.JobArgs,
		opts *riverqueue.InsertOpts,
	) (*rivertype.JobInsertResult, error)
	// InsertMany inserts jobs in the transaction of ctx, or on the pool
	// outside one.
	InsertMany(ctx context.Context, params []riverqueue.InsertManyParams) ([]*rivertype.JobInsertResult, error)
}

// engine is what Client uses of *riverqueue.Client[pgx.Tx]; tests replace it.
type engine interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	StopAndCancel(ctx context.Context) error
	Stopped() <-chan struct{}
	Insert(
		ctx context.Context,
		args riverqueue.JobArgs,
		opts *riverqueue.InsertOpts,
	) (*rivertype.JobInsertResult, error)
	InsertTx(ctx context.Context, tx pgx.Tx, args riverqueue.JobArgs,
		opts *riverqueue.InsertOpts) (*rivertype.JobInsertResult, error)
	InsertMany(ctx context.Context, params []riverqueue.InsertManyParams) ([]*rivertype.JobInsertResult, error)
	InsertManyTx(ctx context.Context, tx pgx.Tx,
		params []riverqueue.InsertManyParams) ([]*rivertype.JobInsertResult, error)
}

// Client is a River client as a lifecycle component: a Starter, Runner,
// Resource, Healthchecker and Namer, and an Inserter. Register it after the
// DB (and after the DB of WithTxFrom): it starts on the DB's pool, and it
// must stop before the pool closes.
//
// A client without river.Config.Queues only inserts: its Run waits for the
// stop and works no jobs.
type Client struct {
	name    string
	db      *corepgx.DB
	config  riverqueue.Config
	working bool
	worker  *lifecycle.Worker

	// newEngine builds the River client on the pool; txOf finds the
	// transaction of ctx. Tests replace both.
	newEngine func(pool *pgxpool.Pool) (engine, *riverqueue.Client[pgx.Tx], error)
	txOf      func(ctx context.Context) (pgx.Tx, bool)

	mu     sync.Mutex
	engine engine
	river  *riverqueue.Client[pgx.Tx]
	closed bool
}

// New validates cfg and builds the client; it does no I/O. The River client
// itself is created in Start, on db's pool, which exists only once db has
// started. cfg is River's configuration as is, with two additions: its
// Logger defaults to the component's logger (WithLogger), and the otelriver
// middleware is appended to its Plugins.
func New(db *corepgx.DB, cfg riverqueue.Config, opts ...Option) (*Client, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: nil *corepgx.DB", ErrInvalidConfig)
	}

	o := newOptions(opts)

	if cfg.Logger == nil {
		cfg.Logger = o.logger
	}

	cfg.Plugins = append(slices.Clone(cfg.Plugins), o.middleware())

	// River validates its configuration in NewClient, which does no I/O; a
	// pool that is never used stands in for the one Start will have.
	if _, err := riverqueue.NewClient(riverpgxv5.New(&pgxpool.Pool{}), &cfg); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}

	txFrom := o.txFrom
	if txFrom == nil {
		txFrom = db
	}

	c := &Client{ //nolint:exhaustruct_v5 // state starts at zero; worker is set below
		name:    o.name,
		db:      db,
		config:  cfg,
		working: len(cfg.Queues) > 0,
		txOf:    func(ctx context.Context) (pgx.Tx, bool) { return corepgx.Tx(ctx, txFrom) },
	}
	c.newEngine = c.newRiver
	c.worker = lifecycle.NewWorker(lifecycle.WorkerFuncs{Run: c.run, Logger: o.logger})

	return c, nil
}

// Name returns the component name (WithName). Implements lifecycle.Namer.
func (c *Client) Name() string { return c.name }

// River returns the River client, for what this package does not wrap
// (JobCancel, Subscribe, QueueAdd, …); nil before Start. Inserts through it
// do not join the transaction of the context: use Insert, or InsertTx with
// corepgx.Tx(ctx, db).
func (c *Client) River() *riverqueue.Client[pgx.Tx] {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.river
}

// Start creates the River client on the DB's pool; it does no I/O. It
// returns ErrNotStarted when the DB has not started (register the client
// after it), ErrClosed after Shutdown and ErrAlreadyStarted when called
// twice. Implements lifecycle.Starter.
func (c *Client) Start(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch {
	case c.closed:
		return ErrClosed
	case c.engine != nil:
		return ErrAlreadyStarted
	}

	pool := c.db.Pool()
	if pool == nil {
		return fmt.Errorf("river: %w: DB %q (register the river client after it)", ErrNotStarted, c.db.Name())
	}

	eng, client, err := c.newEngine(pool)
	if err != nil {
		return err
	}

	c.engine, c.river = eng, client

	return nil
}

// Run works jobs until the client is stopped — ctx cancelled or Shutdown
// called — and the jobs in flight have finished, then returns nil. A client
// without queues only waits for the stop. It returns ErrStopped when the
// River client stops on its own, ErrNotStarted without Start,
// ErrAlreadyStarted when called twice, and nil at once after Shutdown.
// Implements lifecycle.Runner.
func (c *Client) Run(ctx context.Context) error {
	c.mu.Lock()
	notStarted := c.engine == nil && !c.closed
	c.mu.Unlock()

	if notStarted {
		return fmt.Errorf("river: %w", ErrNotStarted)
	}

	return c.worker.Run(ctx) //nolint:wrapcheck // run's errors carry the prefix; the rest is lifecycle's
}

// Shutdown stops Run, if it runs: River takes no new jobs and waits for the
// jobs in flight until ctx is done; then it cancels their contexts, and
// Shutdown returns ctx.Err(). The pool is the DB's: Shutdown does not close
// it. Safe without Start or Run; idempotent. Implements lifecycle.Resource.
func (c *Client) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()

	return c.worker.Shutdown(ctx) //nolint:wrapcheck // ctx.Err() as is
}

// Healthcheck returns nil while Run runs, ErrNotRunning otherwise.
// Implements lifecycle.Healthchecker.
func (c *Client) Healthcheck(ctx context.Context) error {
	if err := c.worker.Healthcheck(ctx); err != nil {
		return fmt.Errorf("river: %w", err)
	}

	return nil
}

// Insert inserts a job in the transaction of ctx on the DB (or the DB of
// WithTxFrom) — it is then committed or rolled back with it — or on the pool
// outside one. It returns ErrNotStarted before Start. Implements Inserter.
func (c *Client) Insert(
	ctx context.Context, args riverqueue.JobArgs, opts *riverqueue.InsertOpts,
) (*rivertype.JobInsertResult, error) {
	eng, err := c.getEngine()
	if err != nil {
		return nil, err
	}

	var res *rivertype.JobInsertResult

	if tx, ok := c.txOf(ctx); ok {
		res, err = eng.InsertTx(ctx, tx, args, opts)
	} else {
		res, err = eng.Insert(ctx, args, opts)
	}

	if err != nil {
		return nil, fmt.Errorf("river: insert %s: %w", args.Kind(), err)
	}

	return res, nil
}

// InsertMany inserts jobs in the transaction of ctx, or on the pool outside
// one, as Insert does. Implements Inserter.
func (c *Client) InsertMany(
	ctx context.Context, params []riverqueue.InsertManyParams,
) ([]*rivertype.JobInsertResult, error) {
	eng, err := c.getEngine()
	if err != nil {
		return nil, err
	}

	var res []*rivertype.JobInsertResult

	if tx, ok := c.txOf(ctx); ok {
		res, err = eng.InsertManyTx(ctx, tx, params)
	} else {
		res, err = eng.InsertMany(ctx, params)
	}

	if err != nil {
		return nil, fmt.Errorf("river: insert many: %w", err)
	}

	return res, nil
}

func (c *Client) newRiver(pool *pgxpool.Pool) (engine, *riverqueue.Client[pgx.Tx], error) {
	cfg := c.config

	client, err := riverqueue.NewClient(riverpgxv5.New(pool), &cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("river: new client: %w", err)
	}

	return client, client, nil
}

func (c *Client) getEngine() (engine, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.engine == nil {
		return nil, fmt.Errorf("river: %w", ErrNotStarted)
	}

	return c.engine, nil
}

// run is the loop of the lifecycle.Worker: River fetches jobs on its own,
// so the loop starts it, waits for the stop and stops it — softly, then by
// cancelling the jobs in flight when work is cancelled (Shutdown's deadline).
func (c *Client) run(stop, work context.Context) error {
	if !c.working {
		<-stop.Done()

		return nil
	}

	// Run checked that Start built the engine; it is never reset.
	c.mu.Lock()
	eng := c.engine
	c.mu.Unlock()

	// River's own stop is Stop/StopAndCancel; its Start ctx must outlive the
	// stop's beginning.
	if err := eng.Start(context.WithoutCancel(work)); err != nil {
		return fmt.Errorf("river: start: %w", err)
	}

	select {
	case <-stop.Done():
	case <-eng.Stopped():
		return ErrStopped
	}

	err := eng.Stop(work)
	if err != nil && work.Err() != nil {
		// Shutdown's deadline: cancel the jobs in flight.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(work), hardStopTimeout)
		defer cancel()

		err = eng.StopAndCancel(ctx)
	}

	if err != nil {
		return fmt.Errorf("river: stop: %w", err)
	}

	return nil
}
