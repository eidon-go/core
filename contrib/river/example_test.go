package river_test

import (
	"context"
	"log"

	riverqueue "github.com/riverqueue/river"

	"github.com/eidon-go/core/app"
	coreriver "github.com/eidon-go/core/contrib/river"
	corepgx "github.com/eidon-go/core/contrib/sql/pgx"
)

type receiptArgs struct {
	OrderID int64 `json:"orderId"`
}

func (receiptArgs) Kind() string { return "receipt" }

type receiptWorker struct {
	riverqueue.WorkerDefaults[receiptArgs]
}

func (receiptWorker) Work(context.Context, *riverqueue.Job[receiptArgs]) error {
	return nil // send the receipt
}

// The service's DB, its transaction manager and a River client on the same
// pool: the job is inserted in the transaction of the order. River's tables
// are migrated beforehand (rivermigrate, goose).
func Example() { //nolint:testableexamples // needs PostgreSQL and runs until a signal arrives
	ctx := context.Background()

	db, err := corepgx.New(corepgx.Config{DSN: "postgres://app@localhost/app"})
	if err != nil {
		log.Fatal(err)
	}

	m, err := corepgx.NewManager(db)
	if err != nil {
		log.Fatal(err)
	}

	workers := riverqueue.NewWorkers()
	riverqueue.AddWorker(workers, receiptWorker{})

	jobs, err := coreriver.New(db, riverqueue.Config{
		Queues:  map[string]riverqueue.QueueConfig{riverqueue.QueueDefault: {MaxWorkers: 10}},
		Workers: workers,
	})
	if err != nil {
		log.Fatal(err)
	}

	a := app.New()
	a.Add(db, app.WithReadiness())
	a.Add(jobs) // after the DB: started later, stopped first

	pay := func(ctx context.Context, orderID int64) error {
		return m.WithTx(ctx, func(ctx context.Context) error {
			// … update the order through m.GetQuerier(ctx) …
			_, err := jobs.Insert(ctx, receiptArgs{OrderID: orderID}, nil) // in the same transaction

			return err
		})
	}

	_ = pay

	if err := a.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

// River on a pool of its own (sized for the workers), inserting jobs in the
// transactions of the service's DB; both connect to the same database.
func ExampleWithTxFrom() { //nolint:testableexamples // wiring only
	db, err := corepgx.New(corepgx.Config{DSN: "postgres://app@localhost/app"})
	if err != nil {
		log.Fatal(err)
	}

	jobsDB, err := corepgx.New(corepgx.Config{DSN: "postgres://app@localhost/app", MaxConns: 20},
		corepgx.WithName("jobs"))
	if err != nil {
		log.Fatal(err)
	}

	workers := riverqueue.NewWorkers()
	riverqueue.AddWorker(workers, receiptWorker{})

	jobs, err := coreriver.New(jobsDB, riverqueue.Config{
		Queues:  map[string]riverqueue.QueueConfig{riverqueue.QueueDefault: {MaxWorkers: 16}},
		Workers: workers,
	}, coreriver.WithTxFrom(db))
	if err != nil {
		log.Fatal(err)
	}

	a := app.New()
	a.Add(db, app.WithReadiness())
	a.Add(jobsDB)
	a.Add(jobs)
}
