package river_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	riverqueue "github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"github.com/riverqueue/rivercontrib/otelriver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	coreriver "github.com/eidon-go/core/contrib/river"
	corepgx "github.com/eidon-go/core/contrib/sql/pgx"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
)

type sendArgs struct {
	To string `json:"to"`
}

func (sendArgs) Kind() string { return "send" }

type sendWorker struct {
	riverqueue.WorkerDefaults[sendArgs]
}

func (sendWorker) Work(context.Context, *riverqueue.Job[sendArgs]) error { return nil }

func workingConfig() riverqueue.Config {
	workers := riverqueue.NewWorkers()
	riverqueue.AddWorker(workers, sendWorker{})

	return riverqueue.Config{
		Queues:  map[string]riverqueue.QueueConfig{riverqueue.QueueDefault: {MaxWorkers: 2}},
		Workers: workers,
	}
}

// newDB returns a DB on a pool that has not connected (pgxpool connects on
// first use) to a port nothing listens on.
func newDB(t *testing.T) *corepgx.DB {
	t.Helper()

	pool, err := pgxpool.New(t.Context(), "postgres://app@127.0.0.1:1/app?connect_timeout=1")
	require.NoError(t, err)

	db, err := corepgx.NewFromPool(pool)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Shutdown(context.Background()) })

	return db
}

func quietLogger() coreriver.Option { return coreriver.WithLogger(slog.New(slog.DiscardHandler)) }

func newClient(t *testing.T, cfg riverqueue.Config, e *fakeEngine) *coreriver.Client {
	t.Helper()

	c, err := coreriver.New(newDB(t), cfg, quietLogger())
	require.NoError(t, err)

	if e != nil {
		coreriver.SetEngine(c, e)
	}

	return c
}

// fakeEngine records what Client does with the River client.
type fakeEngine struct {
	startErr  error
	insertErr error
	// blockStop makes Stop wait for its ctx (jobs that do not finish).
	blockStop bool

	mu       sync.Mutex
	calls    []string
	stopped  chan struct{}
	stopOnce sync.Once
}

func newFakeEngine() *fakeEngine { return &fakeEngine{stopped: make(chan struct{})} }

func (f *fakeEngine) Calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.calls...)
}

func (f *fakeEngine) Start(ctx context.Context) error {
	f.record("Start")

	if ctx.Err() != nil {
		return errors.New("start ctx must not be cancelled")
	}

	return f.startErr
}

func (f *fakeEngine) Stop(ctx context.Context) error {
	f.record("Stop")

	if f.blockStop {
		<-ctx.Done()

		return ctx.Err()
	}

	f.stop()

	return nil
}

func (f *fakeEngine) StopAndCancel(context.Context) error {
	f.record("StopAndCancel")
	f.stop()

	return nil
}

func (f *fakeEngine) Stopped() <-chan struct{} { return f.stopped }

func (f *fakeEngine) Insert(
	context.Context,
	riverqueue.JobArgs,
	*riverqueue.InsertOpts,
) (*rivertype.JobInsertResult, error) {
	f.record("Insert")

	return &rivertype.JobInsertResult{}, f.insertErr
}

func (f *fakeEngine) InsertTx(
	context.Context,
	pgx.Tx,
	riverqueue.JobArgs,
	*riverqueue.InsertOpts,
) (*rivertype.JobInsertResult, error) {
	f.record("InsertTx")

	return &rivertype.JobInsertResult{}, f.insertErr
}

func (f *fakeEngine) InsertMany(context.Context, []riverqueue.InsertManyParams) ([]*rivertype.JobInsertResult, error) {
	f.record("InsertMany")

	return []*rivertype.JobInsertResult{{}}, f.insertErr
}

func (f *fakeEngine) InsertManyTx(
	context.Context,
	pgx.Tx,
	[]riverqueue.InsertManyParams,
) ([]*rivertype.JobInsertResult, error) {
	f.record("InsertManyTx")

	return []*rivertype.JobInsertResult{{}}, f.insertErr
}

func (f *fakeEngine) record(call string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, call)
}

func (f *fakeEngine) stop() { f.stopOnce.Do(func() { close(f.stopped) }) }

func TestNew_Errors(t *testing.T) {
	t.Parallel()

	_, err := coreriver.New(nil, riverqueue.Config{})
	require.ErrorIs(t, err, coreriver.ErrInvalidConfig)

	cfg := workingConfig()
	cfg.Workers = nil

	_, err = coreriver.New(newDB(t), cfg, quietLogger())
	require.ErrorIs(t, err, coreriver.ErrInvalidConfig)
	require.ErrorContains(t, err, "Workers must be set if Queues is set", "River's own validation")
}

func TestNew_Config(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.DiscardHandler)
	tuned := false

	c, err := coreriver.New(newDB(t), riverqueue.Config{},
		nil, coreriver.WithName("jobs"), coreriver.WithLogger(logger),
		coreriver.WithTelemetry(nil), coreriver.WithTelemetry(telemetry{}),
		coreriver.WithTracerProvider(tracenoop.NewTracerProvider()),
		coreriver.WithMeterProvider(metricnoop.NewMeterProvider()),
		coreriver.WithOtelRiverConfig(nil),
		coreriver.WithOtelRiverConfig(func(cfg *otelriver.MiddlewareConfig) {
			tuned = cfg.EnableTracePropagation
			cfg.EnableSemanticMetrics = true
		}))
	require.NoError(t, err)

	assert.Equal(t, "jobs", c.Name())
	assert.True(t, tuned, "trace propagation is on by default")

	cfg := coreriver.Config(c)
	require.Len(t, cfg.Plugins, 1)
	assert.IsType(t, &otelriver.Middleware{}, cfg.Plugins[0])
	assert.NotNil(t, cfg.Logger, "the component's logger")

	own := slog.New(slog.DiscardHandler)
	c, err = coreriver.New(newDB(t), riverqueue.Config{Logger: own}, coreriver.WithTxFrom(newDB(t)))
	require.NoError(t, err)
	assert.Same(t, own, coreriver.Config(c).Logger, "River's Logger wins")
	assert.Equal(t, coreriver.DefaultName, c.Name())
}

type telemetry struct{}

// fakeTx is a pgx.Tx the fake engine never uses.
type fakeTx struct{ pgx.Tx }

func (telemetry) TracerProvider() trace.TracerProvider { return tracenoop.NewTracerProvider() }
func (telemetry) MeterProvider() metric.MeterProvider  { return metricnoop.NewMeterProvider() }

func TestClient_Start(t *testing.T) {
	t.Parallel()

	notStarted, err := corepgx.New(corepgx.Config{DSN: "postgres://app@127.0.0.1:1/app"})
	require.NoError(t, err)

	c, err := coreriver.New(notStarted, riverqueue.Config{}, quietLogger())
	require.NoError(t, err)
	require.ErrorIs(t, c.Start(t.Context()), coreriver.ErrNotStarted, "the DB must start first")
	assert.Nil(t, c.River())

	c = newClient(t, workingConfig(), nil)
	require.NoError(t, c.Start(t.Context()))
	assert.NotNil(t, c.River(), "the real River client on the DB's pool")
	require.ErrorIs(t, c.Start(t.Context()), coreriver.ErrAlreadyStarted)

	require.NoError(t, c.Shutdown(t.Context()))
	require.ErrorIs(t, c.Start(t.Context()), coreriver.ErrClosed)
}

func TestClient_Insert(t *testing.T) {
	t.Parallel()

	e := newFakeEngine()
	c := newClient(t, riverqueue.Config{}, e)

	_, err := c.Insert(t.Context(), sendArgs{To: "ann"}, nil)
	require.ErrorIs(t, err, coreriver.ErrNotStarted)

	_, err = c.InsertMany(t.Context(), nil)
	require.ErrorIs(t, err, coreriver.ErrNotStarted)

	require.NoError(t, c.Start(t.Context()))

	type txKey struct{}

	coreriver.SetTxOf(c, func(ctx context.Context) (pgx.Tx, bool) {
		tx, ok := ctx.Value(txKey{}).(pgx.Tx)

		return tx, ok
	})

	inTx := context.WithValue(t.Context(), txKey{}, pgx.Tx(fakeTx{}))

	_, err = c.Insert(t.Context(), sendArgs{To: "ann"}, nil)
	require.NoError(t, err)
	_, err = c.Insert(inTx, sendArgs{To: "ann"}, nil)
	require.NoError(t, err)

	many := []riverqueue.InsertManyParams{{Args: sendArgs{To: "bob"}}}

	_, err = c.InsertMany(t.Context(), many)
	require.NoError(t, err)
	_, err = c.InsertMany(inTx, many)
	require.NoError(t, err)

	assert.Equal(t, []string{"Insert", "InsertTx", "InsertMany", "InsertManyTx"}, e.Calls())

	e.insertErr = errors.New("boom")

	_, err = c.Insert(t.Context(), sendArgs{To: "ann"}, nil)
	require.ErrorIs(t, err, e.insertErr)
	require.ErrorContains(t, err, "river: insert send")

	_, err = c.InsertMany(t.Context(), many)
	require.ErrorIs(t, err, e.insertErr)
}

func TestClient_InsertOnPool(t *testing.T) {
	t.Parallel()

	c := newClient(t, riverqueue.Config{}, nil)
	require.NoError(t, c.Start(t.Context()))

	var inserter coreriver.Inserter = c

	_, err := inserter.Insert(t.Context(), sendArgs{To: "ann"}, nil)
	require.ErrorContains(t, err, "river: insert send", "outside a transaction the pool is used (it cannot connect)")
}

func TestClient_Run(t *testing.T) {
	t.Parallel()

	t.Run("not started", func(t *testing.T) {
		t.Parallel()

		c := newClient(t, workingConfig(), newFakeEngine())
		require.ErrorIs(t, c.Run(t.Context()), coreriver.ErrNotStarted)
	})

	t.Run("soft stop", func(t *testing.T) {
		t.Parallel()

		e := newFakeEngine()
		c := newClient(t, workingConfig(), e)
		require.NoError(t, c.Start(t.Context()))

		ctx, cancel := context.WithCancel(t.Context())
		done := runAsync(ctx, c)

		require.Eventually(t, func() bool { return c.Healthcheck(t.Context()) == nil }, 5*time.Second, time.Millisecond)
		cancel()
		require.NoError(t, <-done)
		require.NoError(t, c.Shutdown(t.Context()))

		assert.Equal(t, []string{"Start", "Stop"}, e.Calls())
		require.ErrorIs(t, c.Healthcheck(t.Context()), coreriver.ErrNotRunning)
	})

	t.Run("hard stop at Shutdown's deadline", func(t *testing.T) {
		t.Parallel()

		e := newFakeEngine()
		e.blockStop = true
		c := newClient(t, workingConfig(), e)
		require.NoError(t, c.Start(t.Context()))

		done := runAsync(t.Context(), c)
		require.Eventually(t, func() bool { return c.Healthcheck(t.Context()) == nil }, 5*time.Second, time.Millisecond)

		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()

		require.ErrorIs(t, c.Shutdown(ctx), context.DeadlineExceeded)

		select {
		case err := <-done:
			require.NoError(t, err, "the jobs were cancelled")
		case <-time.After(5 * time.Second):
			t.Fatal("Run did not return")
		}

		require.Eventually(t, func() bool { return len(e.Calls()) == 3 }, 5*time.Second, time.Millisecond)
		assert.Equal(t, []string{"Start", "Stop", "StopAndCancel"}, e.Calls())
	})

	t.Run("River stops on its own", func(t *testing.T) {
		t.Parallel()

		e := newFakeEngine()
		c := newClient(t, workingConfig(), e)
		require.NoError(t, c.Start(t.Context()))

		done := runAsync(t.Context(), c)
		require.Eventually(t, func() bool { return len(e.Calls()) == 1 }, 5*time.Second, time.Millisecond)
		e.stop()
		require.ErrorIs(t, <-done, coreriver.ErrStopped)
	})

	t.Run("River fails to start", func(t *testing.T) {
		t.Parallel()

		e := newFakeEngine()
		e.startErr = errors.New("boom")
		c := newClient(t, workingConfig(), e)
		require.NoError(t, c.Start(t.Context()))
		require.ErrorIs(t, c.Run(t.Context()), e.startErr)
	})

	t.Run("insert only", func(t *testing.T) {
		t.Parallel()

		e := newFakeEngine()
		c := newClient(t, riverqueue.Config{}, e)
		require.NoError(t, c.Start(t.Context()))

		ctx, cancel := context.WithCancel(t.Context())
		done := runAsync(ctx, c)

		require.Eventually(t, func() bool { return c.Healthcheck(t.Context()) == nil }, 5*time.Second, time.Millisecond)
		cancel()
		require.NoError(t, <-done)
		assert.Empty(t, e.Calls(), "no jobs are worked")
	})

	t.Run("after Shutdown", func(t *testing.T) {
		t.Parallel()

		c := newClient(t, workingConfig(), newFakeEngine())
		require.NoError(t, c.Shutdown(t.Context()))
		require.NoError(t, c.Run(t.Context()))
	})
}

func runAsync(ctx context.Context, c *coreriver.Client) <-chan error {
	done := make(chan error, 1)

	go func() { done <- c.Run(ctx) }()

	return done
}

func TestClient_Contract(t *testing.T) {
	t.Parallel()

	t.Run("working", func(t *testing.T) {
		t.Parallel()

		lifecycletest.Runner(t, func(t *testing.T) lifecycle.Runner {
			t.Helper()

			return newClient(t, workingConfig(), newFakeEngine())
		})
	})

	t.Run("insert only, real River client", func(t *testing.T) {
		t.Parallel()

		lifecycletest.Runner(t, func(t *testing.T) lifecycle.Runner {
			t.Helper()

			return newClient(t, riverqueue.Config{}, nil)
		})
	})
}
