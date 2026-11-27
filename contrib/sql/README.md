# sql

[![Go Reference](https://pkg.go.dev/badge/github.com/eidon-go/core/contrib/sql.svg)](https://pkg.go.dev/github.com/eidon-go/core/contrib/sql)
[![Release](https://img.shields.io/github/v/tag/eidon-go/core?filter=contrib%2Fsql%2Fv*&sort=semver&label=release)](https://github.com/eidon-go/core/releases)
[![Go Version](https://img.shields.io/github/go-mod/go-version/eidon-go/core?filename=contrib%2Fsql%2Fgo.mod)](./go.mod)
[![codecov](https://codecov.io/gh/eidon-go/core/graph/badge.svg?flag=contrib-sql)](https://codecov.io/gh/eidon-go/core?flags%5B0%5D=contrib-sql)

SQL databases as [`lifecycle`](../../lifecycle/README.md) components, in two worlds that share one
transaction model:

- package `sql` — any `database/sql` driver (PostgreSQL through pgx's driver, MySQL, SQLite, …);
- package `sql/pgx` — PostgreSQL on a [pgx](https://github.com/jackc/pgx) pool, with pgx's native
  types and batches.

Both give you:

- a `DB` that opens, pings with retry, reports health and closes like every other component;
- a transaction `Manager`: `WithTx` with nested modes (join, savepoint, separate transaction,
  forbid), rollback-only scopes, isolation and read-only options, and `OnCommit` callbacks that run
  only once the data is committed;
- `Get[T]`, `Select[T]` and `Exec` helpers that scan rows into structs with
  [scany](https://github.com/georgysavva/scany), over hand-written SQL or any query builder
  (squirrel fits without being a dependency);
- a querier that also passes for sqlc's `DBTX`, so generated code joins the same transactions;
- OpenTelemetry traces and metrics, on by default ([otelsql](https://github.com/XSAM/otelsql) /
  [otelpgx](https://github.com/exaring/otelpgx)).

```sh
go get github.com/eidon-go/core/contrib/sql
```

```go
import (
    "github.com/eidon-go/core/contrib/sql"
    corepgx "github.com/eidon-go/core/contrib/sql/pgx"
)

db, err := corepgx.New(corepgx.Config{DSN: dsn})
if err != nil {
    return err
}

m, err := corepgx.NewManager(db) // or sql.NewManager over sql.New(...)
if err != nil {
    return err
}

orders := NewOrderRepo(m) // repositories take sql.Transactor / a QuerierProvider

a := app.New()
a.Add(db, app.WithReadiness()) // before the components that use the pool; gates readiness
a.Add(server)                  // …
return a.Run(ctx)
```

```go
func (s *Service) Pay(ctx context.Context, id int64) error {
    return s.tx.WithTx(ctx, func(ctx context.Context) error {
        q := s.m.GetQuerier(ctx) // the transaction of ctx

        order, err := sql.Get[Order](ctx, q, sql.Raw("SELECT id, total, paid FROM orders WHERE id = $1 FOR UPDATE", id))
        if err != nil {
            return err // sql.ErrNoRows when there is none
        }

        if _, err := sql.Exec(ctx, q, sql.Raw("UPDATE orders SET paid = true WHERE id = $1", order.ID)); err != nil {
            return err
        }

        s.tx.OnCommit(ctx, func() { s.events.Publish(OrderPaid{ID: order.ID}) })

        return nil
    })
}
```

## Configuration

`sql.Config` (package `sql`):

| Field | Meaning | Default |
|---|---|---|
| `DriverName` | a registered `database/sql` driver: `pgx` (`corepgx.DriverName`, registered by importing `sql/pgx`), `mysql`, … | required |
| `DataSource` | the driver's DSN; never logged | required |
| `MaxOpenConns` | open connections cap | unlimited |
| `MaxIdleConns` | idle connections cap; negative: none | `database/sql`'s (2) |
| `ConnMaxLifetime` / `ConnMaxIdleTime` | close connections older / idle longer than this | never |

`corepgx.Config` (package `sql/pgx`) takes either `DSN` (URL or keyword/value) or the connection
fields `Host`, `Port`, `User`, `Password`, `Database`, `SSLMode`, `ApplicationName` — not both.
`DataSource()` renders the fields as a keyword/value string with every value quoted, so a password
with spaces or quotes survives; `sql.Config{DriverName: corepgx.DriverName, DataSource:
cfg.DataSource()}` reaches the same server through `database/sql`. Fields left empty fall back to
libpq's `PG*` environment variables, as pgx always does.

| Field | Meaning | Default |
|---|---|---|
| `ConnectTimeout` | bound of one connection attempt | the context's |
| `MaxConns` / `MinConns` | pool size | max(4, CPUs) / 0 |
| `MaxConnLifetime` / `MaxConnIdleTime` | connection recycling | 1h / 30m |
| `HealthCheckPeriod` | idle connection checks | 1m |

`Validate` reports every problem at once, each wrapping `sql.ErrInvalidConfig`.

## Options

| Option | `sql` | `sql/pgx` | Default |
|---|---|---|---|
| `WithName(string)` | ✓ | ✓ | `"sql"` / `"pgx"`: name two databases apart; in the pgx world it also names the pool in its stats metrics (`db.client.connection.pool.name`, otherwise `host:port/database`) |
| `WithLogger(*slog.Logger)` | ✓ | ✓ | `slog.Default()` (logs connect retries) |
| `WithConnectRetry(retry.Policy)` | ✓ | ✓ | `retry.ConnectPolicy()`, logged |
| `WithTracerProvider` / `WithMeterProvider` | ✓ | ✓ | the otel globals at `New` time; `New` only |
| `WithTelemetry` (both from the `otel` module's `Provider` when it is not global) | ✓ | ✓ | `New` only |
| `WithOtelSQLOptions(...otelsql.Option)` | ✓ | | — |
| `WithOtelPgxOptions(...otelpgx.Option)` | | ✓ | — (`otelpgx.WithDisableAcquireTracer()` against noisy acquire spans) |
| `WithPoolConfig(func(*pgxpool.Config))` | | ✓ | — (AfterConnect hooks, type registration), applied last |
| `WithScanner(Scanner)` | ✓ | ✓ | `DefaultScanner()` |
| `WithDefaultIsolation(stdsql.IsolationLevel)` | ✓ | | Read Committed (`LevelRepeatableRead` for MySQL/MariaDB) |

`NewManager(db, opts...)` takes `WithDefaultTxMode(TxMode)` (both worlds).

## Lifecycle

| | `New` | `Start` | `Shutdown` | `Healthcheck` |
|---|---|---|---|---|
| `sql.DB` | validates, opens the pool (`database/sql` connects lazily: no I/O) | pings with retry | closes the pool: waits for the queries in flight | pings |
| `corepgx.DB` | validates, builds the pool config and tracer (no I/O) | creates the pool, pings with retry | closes the `*sql.DB` of `SQLDB`, then the pool: waits until every connection is released | pings; `ErrNotStarted` before `Start` (a `DB` from `New`) |

Register the `DB` with `app.WithReadiness()` when the service cannot answer requests without
the database (the usual case); without it the health check only appears in `HealthReport`.

A `Shutdown` during `Start` ends its ping retries (`Start` returns `ErrClosed`); `Start` after
`Shutdown` returns `ErrClosed`. `Shutdown` is idempotent and safe without `Start`; when its ctx is
done it returns `ctx.Err()` and the close goes on in the background. Before `Start`, a pgx `DB`
fails queries and transactions with `ErrNotStarted`.

Other constructors: `sql.NewFromDB(*sql.DB)` (a custom connector, sqlmock; not instrumented),
`corepgx.NewFromPool(*pgxpool.Pool)` and `corepgx.NewFromDBTX(PoolIface)` (pgxmock). Each takes the
value over: `Shutdown` closes it.

**Registration order:** components that use the pool — job queues, consumers that write to the
database — must stop before it: register them **after** the `DB`.

## Transactions

`Transactor` (`WithTx`, `OnCommit`) is what domain code depends on; `QuerierProvider`
(`GetQuerier(ctx)`, `HasTx(ctx)`) is what repositories use. A `Manager` is both. In the pgx world
`GetQuerier` returns a `PgxQuerier`, so `corepgx.Manager` is an `sql.Transactor` but not an
`sql.Manager`; the options, modes and errors are the ones of package `sql` in both worlds.

`WithTx(ctx, fn, opts...)` commits when `fn` returns `nil`, rolls back when it returns an error,
panics or exits its goroutine (`t.FailNow`), and returns `fn`'s error as is. Queries go through
`GetQuerier(ctx)` with `fn`'s ctx. The transaction belongs to the `DB`: managers over one `DB`
share it, transactions of two `DB`s never mix, even in one ctx.

Inside a transaction of the same `DB`, `WithTxMode` (or the manager's default) decides:

| Mode | Behavior |
|---|---|
| `TxModeJoin` (default) | runs in the current transaction. An error or panic marks it **rollback-only**: the outermost `WithTx` rolls back and returns `ErrRollbackOnly` joined with the cause, even if its own `fn` handled the error and returned `nil`; later joined or nested calls fail at once. Inside a `TxModeNested` call, only that savepoint is marked. |
| `TxModeNested` | runs in a savepoint (`SAVEPOINT sp_<n>`): an error rolls back to it and the transaction goes on. MySQL commits implicitly on DDL. |
| `TxModeRequiresNew` | a separate transaction on another connection, committed on its own; two connections at once — beware of pool exhaustion and deadlocks with the outer one. |
| `TxModeForbid` | fails with `ErrNestedTx`. |

Outside a transaction every mode begins one. `WithIsolation(level)` and `ReadOnly()` configure a
new transaction; a joined or nested call may ask for the same or a weaker isolation level
(a transaction begun with `LevelDefault` counts as the server's default level: Read Committed,
right for PostgreSQL and SQL Server, unless the `DB` is built with
`WithDefaultIsolation(stdsql.LevelRepeatableRead)`, as it should be for MySQL/MariaDB) and must be `ReadOnly` inside a read-only transaction — otherwise
`ErrTxOptionsIncompatible`. The pgx world rejects levels PostgreSQL does not have
(`ErrUnsupportedIsolation`).

`OnCommit(ctx, fn)` runs `fn` after the outermost commit, in registration order, in the goroutine
of the outermost `WithTx` — to publish events or fill caches only once the data is visible. It is
dropped when the transaction, or the savepoint it was registered in, rolls back; outside a
transaction it runs at once. A panicking callback does not stop the ones after it: they all run,
then the first panic propagates (the transaction is committed). A rollback runs on a context
detached from the caller's, so a cancelled request still rolls back cleanly (bounded by 5s in the
pgx world; `*sql.Tx.Rollback` takes no context).

Savepoints are a stack on one connection: don't run `TxModeNested` calls of one transaction from
several goroutines. And a sharp edge of `ReadOnly()`: inside a read-only transaction a repository
method that wraps its reads in a plain `WithTx(ctx, fn)` fails with `ErrTxOptionsIncompatible` —
let read methods use `GetQuerier(ctx)` directly, or pass `ReadOnly()`.

## Queries

`Get[T]`, `Select[T]` and `Exec` take a `Querier` (`GetQuerier(ctx)`) and a `Builder` — anything
with `ToSql() (string, []any, error)`: `sql.Raw(query, args...)` or a squirrel builder. The query is
run as the builder rendered it; nothing rewrites it.

- `Get` returns `ErrNoRows` when there is no row and **`ErrTooManyRows` when there are more** (sqlx
  took the first one): add `LIMIT 1` or a narrower `WHERE`.
- `Select` returns `nil` when there are no rows.
- `Exec` returns an `sql.Result`; in the pgx world `LastInsertId` is not supported — use
  `RETURNING` with `Get`.

Placeholders: `sql.Dollar` and `sql.Question` have squirrel's `PlaceholderFormat` method set:
`sq.StatementBuilder.PlaceholderFormat(sql.Dollar)`. With `Dollar`, `??` is an escaped `?` — write
jsonb's operators as `??`, `??|` and `??&`. A `?` inside a string literal (`'…'`, `E'…'`,
`$tag$…$tag$`), a quoted identifier or a comment is left as it is.

Mapping (`DefaultScanner`): a field takes the column named by its `db` tag, or its name in
snake_case (`UserID` → `user_id`); embedded structs are flattened; a column that maps to no field is
an error; scalars and `sql.Scanner` types (`sql.Null[T]`) scan directly. The mapping is the same in
both worlds, **the value decoding is not**: pgx decodes arrays, `jsonb` and `uuid` natively,
`database/sql` does not, so a struct with a `[]string` field for a `text[]` column scans in the pgx
world and fails in the `sql` world.

## Extension points

| | `sql` | `sql/pgx` | For |
|---|---|---|---|
| the transaction of ctx | `sql.Tx(ctx, db) (*sql.Tx, bool)` | `corepgx.Tx(ctx, db) (pgx.Tx, bool)` | libraries that enlist through their own API (River's `InsertTx`) |
| the pool | `db.DB() *sql.DB` (from `New` on) | `db.Pool() *pgxpool.Pool` (`nil` before `Start`) | migrations, River's `riverpgxv5`; queries on it run outside `WithTx` |
| `database/sql` over the pgx pool | — | `db.SQLDB() (*sql.DB, error)` (`ErrNotStarted` before `Start`) | goose, `riverdatabasesql`; same pool, **no** shared transactions |
| sqlc | `GetQuerier(ctx)` is a `DBTX` | `GetQuerier(ctx)` is a pgx/v5 `DBTX` | `queries := db.New(m.GetQuerier(ctx))` inside `WithTx` |
| scanning | `WithScanner(sql.Scanner)` | `WithScanner(corepgx.Scanner)` | another mapper (an sqlx adapter) |
| after commit | `OnCommit` | `OnCommit` | outbox, cache invalidation |

The pgx pool exists only once `Start` has run. Migrations that must finish before the service
starts call `Start` themselves, before `app.Run` (a second `Start` from the app only pings):

```go
if err := db.Start(ctx); err != nil {
    return err
}

sqlDB, err := db.SQLDB()
if err != nil {
    return err
}

if err := goose.UpContext(ctx, sqlDB, "migrations"); err != nil {
    return err
}
```

A component built on the pool (a River client) takes `db.Pool()` in its own `Start` and is
registered after the `DB`, so `app.App` starts it later and stops it first.

A job queue that must insert jobs in the caller's transaction:

```go
err := m.WithTx(ctx, func(ctx context.Context) error {
    if err := orders.Create(ctx, o); err != nil {
        return err
    }

    tx, _ := corepgx.Tx(ctx, db)
    _, err := riverClient.InsertTx(ctx, tx, SendReceiptArgs{OrderID: o.ID}, nil)

    return err
})
```

## Testing

- Domain code: fake `sql.Transactor` (`WithTx` just calls `fn(ctx)`) and your repository
  interfaces — no database needed.
- Repositories in package `sql`: [sqlmock](https://github.com/DATA-DOG/go-sqlmock) through
  `sql.NewFromDB(mockDB)`; `WithTx` issues `Begin`/`Commit`/`Rollback` and `SAVEPOINT sp_<n>`
  statements you can expect.
- Repositories in package `sql/pgx`: [pgxmock](https://github.com/pashagolub/pgxmock) through
  `corepgx.NewFromDBTX(mockPool)`. pgxmock v4.9 does not compile with pgx v5.11 or newer yet; this
  module requires pgx v5.10 (its own tests hold it there), so a test module of yours that uses
  pgxmock must too until pgxmock catches up.
- Real databases: the module's container tests (`internal/integration`) run every helper and mode
  against PostgreSQL in both worlds and MySQL.
