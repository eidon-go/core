# river

[![Go Reference](https://pkg.go.dev/badge/github.com/eidon-go/core/contrib/river.svg)](https://pkg.go.dev/github.com/eidon-go/core/contrib/river)
[![Release](https://img.shields.io/github/v/tag/eidon-go/core?filter=contrib%2Friver%2Fv*&sort=semver&label=release)](https://github.com/eidon-go/core/releases)
[![Go Version](https://img.shields.io/github/go-mod/go-version/eidon-go/core?filename=contrib%2Friver%2Fgo.mod)](./go.mod)
[![codecov](https://codecov.io/gh/eidon-go/core/graph/badge.svg?flag=contrib-river)](https://codecov.io/gh/eidon-go/core?flags%5B0%5D=contrib-river)

A [River](https://riverqueue.com) job queue client as a [`lifecycle`](../../lifecycle/README.md)
component, on the pgx pool of [`sql/pgx`](../sql/pgx/README.md), inserting jobs in the transaction
of the context — a job exists only once the data it is about is committed.

It is a thin wrapper: River's own `river.Config` (queues, workers, periodic jobs, retry policy,
schema) as is; migrations stay outside (`rivermigrate`, goose, a CLI — the service decides). What it
adds:

- the client is built in `Start` on the DB's pool and works in `Run`; `Shutdown` stops it softly
  (no new jobs, the jobs in flight finish) and cancels the jobs in flight at its deadline;
- `Insert` / `InsertMany` join the transaction of the context (`corepgx.Manager.WithTx`), or insert
  on the pool outside one;
- OpenTelemetry traces and metrics ([otelriver](https://github.com/riverqueue/rivercontrib/tree/master/otelriver)),
  on by default, with the trace of the insert carried to the work.

```sh
go get github.com/eidon-go/core/contrib/river
```

The module requires River v0.47 — the last release on pgx v5.10, which [`sql`](../sql/README.md#testing)
holds for pgxmock; a service is free to require a newer River (and pgx).

```go
import (
    riverqueue "github.com/riverqueue/river"

    coreriver "github.com/eidon-go/core/contrib/river"
    corepgx "github.com/eidon-go/core/contrib/sql/pgx"
)

db, err := corepgx.New(corepgx.Config{DSN: dsn})
// …
m, err := corepgx.NewManager(db)
// …

workers := riverqueue.NewWorkers()
riverqueue.AddWorker(workers, &SendReceiptWorker{})

jobs, err := coreriver.New(db, riverqueue.Config{
    Queues:  map[string]riverqueue.QueueConfig{riverqueue.QueueDefault: {MaxWorkers: 10}},
    Workers: workers,
})
// …

a := app.New()
a.Add(db, app.WithReadiness())
a.Add(jobs) // after the DB: started later, stopped first

// in a service:
err = m.WithTx(ctx, func(ctx context.Context) error {
    if err := orders.MarkPaid(ctx, id); err != nil {
        return err
    }

    _, err := jobs.Insert(ctx, SendReceiptArgs{OrderID: id}, nil) // committed with the order

    return err
})
```

Domain code depends on `coreriver.Inserter` (`Insert`, `InsertMany`) and fakes it in tests.

## Pools and transactions

River's pool and the transactions it inserts in are independent, as long as both reach the same
database (where River's tables are):

| | River's pool | Inserts in the service's transactions | When |
|---|---|---|---|
| shared | the service's `corepgx.DB` | yes | the default; fewest connections |
| separate pool | a second `corepgx.DB` (`MaxConns` for the workers, its own name in the pool metrics) + `WithTxFrom(db)` | yes | workers and River's LISTEN connection must not take connections from the API |
| another database | a `corepgx.DB` on another DSN | no: `Insert` on the pool only | the queue lives elsewhere |

`Insert` looks the transaction up in the DB of `WithTxFrom` (default: River's own DB). Outside a
transaction it inserts on River's pool. In a worker, `riverqueue.JobCompleteTx` takes a
transaction of either pool, e.g. `corepgx.Tx(ctx, db)` inside `m.WithTx`.

## Configuration

`New(db *corepgx.DB, cfg riverqueue.Config, opts...)` takes River's configuration as is — River
validates it in `New` (no I/O) and the errors wrap `ErrInvalidConfig`. Two fields are filled in:

- `Logger`, when nil: the component's logger (`WithLogger`);
- `Plugins`: the otelriver middleware is appended.

A configuration without `Queues` makes an insert-only client: `Run` then waits for the stop and
works no jobs (a service that enqueues jobs another service works).

| Option | Default |
|---|---|
| `WithName(string)` | `"river"` |
| `WithLogger(*slog.Logger)` | `slog.Default()`; River's logger too, unless `Config.Logger` is set |
| `WithTracerProvider` / `WithMeterProvider` | the otel globals at `New` time |
| `WithTelemetry` (both from the `otel` module's `Provider` when it is not global) | — |
| `WithOtelRiverConfig(func(*otelriver.MiddlewareConfig))` | trace propagation on (W3C trace context in the job's metadata, the work span linked to the insert); semantic metrics, span names, duration unit off/default |
| `WithTxFrom(*corepgx.DB)` | River's own DB |

otelriver propagates with W3C Trace Context only (not the global propagator), so there is no
`WithPropagators`.

## Lifecycle

| | `New` | `Start` | `Run` | `Shutdown` | `Healthcheck` |
|---|---|---|---|---|---|
| `Client` | validates (River's `NewClient` without a pool), no I/O | builds the River client on `db.Pool()`; `ErrNotStarted` when the DB has not started | starts River and works jobs until the stop; `ErrStopped` when River stops on its own | soft stop (River's `Stop`) bounded by ctx, then `StopAndCancel` (the jobs' contexts are cancelled) and `ctx.Err()` | `nil` while `Run` runs, `ErrNotRunning` otherwise |

**Registration order:** after the DB (and the DB of `WithTxFrom`), so that the pool exists at
`Start` and the client stops before the pool closes (closing a pgx pool waits for every connection
in use). Register it with `app.WithReadiness()` only if the service cannot serve without working
jobs (usually not).

`Shutdown` does not close the pool: it is the DB's. Insert through `Insert` keeps working until the
DB shuts down, so a server draining its requests after `Run` has stopped can still enqueue.

## Escape hatch

`River()` returns the `*riverqueue.Client[pgx.Tx]` (nil before `Start`) for what is not wrapped:
`JobCancel`, `Subscribe`, `QueueAdd`, `PeriodicJobs()`, … Its inserts do not look up the transaction
of the context: use `Insert`, or `InsertTx(ctx, tx, …)` with `corepgx.Tx(ctx, db)`.

## Migrations

River's tables are the service's business: `rivermigrate` before the service starts, or River's
SQL in your goose migrations (`river migrate-get --all --up`). `db.Start` gives the pool early:

```go
if err := db.Start(ctx); err != nil {
    return err
}

migrator, err := rivermigrate.New(riverpgxv5.New(db.Pool()), nil)
if err != nil {
    return err
}

if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
    return err
}
```

## Testing

- Domain code: fake `coreriver.Inserter`.
- Workers: River's [`rivertest`](https://pkg.go.dev/github.com/riverqueue/river/rivertest)
  (`rivertest.NewWorker`, `RequireInserted`).
- The module's container tests (`internal/integration`) insert in committed and rolled-back
  transactions, on a shared and on a separate pool, and run the lifecycle contract against
  PostgreSQL.
