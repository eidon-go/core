package integration_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	riverqueue "github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	coreriver "github.com/eidon-go/core/contrib/river"
	corepgx "github.com/eidon-go/core/contrib/sql/pgx"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
)

type receiptArgs struct {
	OrderID int64 `json:"orderId"`
}

func (receiptArgs) Kind() string { return "receipt" }

// receiptWorker sends every order id it works to done.
type receiptWorker struct {
	riverqueue.WorkerDefaults[receiptArgs]

	done chan int64
}

func (w receiptWorker) Work(ctx context.Context, job *riverqueue.Job[receiptArgs]) error {
	select {
	case w.done <- job.Args.OrderID:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

var riverSchemas atomic.Int64 //nolint:gochecknoglobals // unique schema names across parallel tests

// riverSchema creates a schema of its own for River's tables and migrates it
// up, as a service does with rivermigrate (or goose) before it starts.
func riverSchema(t *testing.T, db *corepgx.DB) string {
	t.Helper()

	schema := fmt.Sprintf("river_%d", riverSchemas.Add(1))

	_, err := db.Pool().Exec(t.Context(), "CREATE SCHEMA "+schema)
	require.NoError(t, err)

	migrator, err := rivermigrate.New(riverpgxv5.New(db.Pool()), &rivermigrate.Config{Schema: schema})
	require.NoError(t, err)

	_, err = migrator.Migrate(t.Context(), rivermigrate.DirectionUp, nil)
	require.NoError(t, err)

	return schema
}

func newPgxDB(t *testing.T, name string) *corepgx.DB {
	t.Helper()

	db, err := corepgx.New(corepgx.Config{DSN: postgresDSN, MaxConns: 10}, corepgx.WithName(name))
	require.NoError(t, err)
	require.NoError(t, db.Start(t.Context()))
	shutdownAtCleanup(t, db)

	return db
}

func receiptConfig(schema string, done chan int64) riverqueue.Config {
	workers := riverqueue.NewWorkers()
	riverqueue.AddWorker(workers, receiptWorker{done: done})

	return riverqueue.Config{
		Queues:  map[string]riverqueue.QueueConfig{riverqueue.QueueDefault: {MaxWorkers: 2}},
		Workers: workers,
		Schema:  schema,
	}
}

// startRiver starts and runs a River client until the test ends.
func startRiver(t *testing.T, c *coreriver.Client) {
	t.Helper()

	require.NoError(t, c.Start(t.Context()))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() { done <- c.Run(ctx) }()

	t.Cleanup(func() {
		cancel()

		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelShutdown()

		assert.NoError(t, c.Shutdown(shutdownCtx))
		assert.NoError(t, <-done)
	})

	require.Eventually(t, func() bool { return c.Healthcheck(t.Context()) == nil }, 10*time.Second, 10*time.Millisecond)
}

func countJobs(t *testing.T, db *corepgx.DB, schema string) int {
	t.Helper()

	var n int
	require.NoError(t, db.Pool().QueryRow(t.Context(), "SELECT count(*) FROM "+schema+".river_job").Scan(&n))

	return n
}

func awaitReceipt(t *testing.T, done <-chan int64) int64 {
	t.Helper()

	select {
	case id := <-done:
		return id
	case <-time.After(20 * time.Second):
		t.Fatal("the job was not worked")

		return 0
	}
}

func TestRiver_InsertInTx(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// separatePool runs River on a pool of its own, inserting in the
		// transactions of the service's DB (WithTxFrom).
		separatePool bool
	}{
		{name: "shared pool"},
		{name: "separate pool", separatePool: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := newPgxDB(t, "app")
			schema := riverSchema(t, db)

			m, err := corepgx.NewManager(db)
			require.NoError(t, err)

			riverDB, opts := db, []coreriver.Option{coreriver.WithLogger(slog.New(slog.DiscardHandler))}
			if tt.separatePool {
				riverDB = newPgxDB(t, "jobs")

				opts = append(opts, coreriver.WithTxFrom(db))
			}

			done := make(chan int64, 4)

			jobs, err := coreriver.New(riverDB, receiptConfig(schema, done), opts...)
			require.NoError(t, err)
			startRiver(t, jobs)

			rollback := errors.New("rollback")

			err = m.WithTx(t.Context(), func(ctx context.Context) error {
				if _, err := jobs.Insert(ctx, receiptArgs{OrderID: 1}, nil); err != nil {
					return err
				}

				return rollback
			})
			require.ErrorIs(t, err, rollback)
			assert.Zero(t, countJobs(t, db, schema), "the job is rolled back with the transaction")

			err = m.WithTx(t.Context(), func(ctx context.Context) error {
				_, err := jobs.InsertMany(ctx, []riverqueue.InsertManyParams{{Args: receiptArgs{OrderID: 2}}})
				if err != nil {
					return err
				}

				n := countJobs(t, db, schema) //nolint:contextcheck // on another connection, on purpose
				assert.Zero(t, n, "not visible before the commit")

				return nil
			})
			require.NoError(t, err)
			assert.EqualValues(t, 2, awaitReceipt(t, done))

			_, err = jobs.Insert(t.Context(), receiptArgs{OrderID: 3}, nil)
			require.NoError(t, err, "outside a transaction: on the pool")
			assert.EqualValues(t, 3, awaitReceipt(t, done))
		})
	}
}

func TestRiver_Contract(t *testing.T) {
	t.Parallel()

	db := newPgxDB(t, "app")
	schema := riverSchema(t, db)

	lifecycletest.Runner(t, func(t *testing.T) lifecycle.Runner {
		t.Helper()

		c, err := coreriver.New(db, receiptConfig(schema, make(chan int64)),
			coreriver.WithLogger(slog.New(slog.DiscardHandler)))
		require.NoError(t, err)

		return c
	}, lifecycletest.WithTimeout(30*time.Second))
}
