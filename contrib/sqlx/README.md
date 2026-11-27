# sqlx

[![Go Reference](https://pkg.go.dev/badge/github.com/eidon-go/core/contrib/sqlx.svg)](https://pkg.go.dev/github.com/eidon-go/core/contrib/sqlx)
[![Release](https://img.shields.io/github/v/tag/eidon-go/core?filter=contrib%2Fsqlx%2Fv*&sort=semver&label=release)](https://github.com/eidon-go/core/releases)
[![Go Version](https://img.shields.io/github/go-mod/go-version/eidon-go/core?filename=contrib%2Fsqlx%2Fgo.mod)](./go.mod)
[![codecov](https://codecov.io/gh/eidon-go/core/graph/badge.svg?flag=contrib-sqlx)](https://codecov.io/gh/eidon-go/core?flags%5B0%5D=contrib-sqlx)

[sqlx](https://github.com/jmoiron/sqlx) on the transactions of [`sql`](../sql/README.md): code
written for sqlx — `sqlx.GetContext`, `sqlx.In` + `Rebind`, named parameters, `QueryxContext` —
runs in the transaction of the context, joined with everything else that runs through the same
`sql.DB` (`sql.Manager`, sqlc-generated code, `OnCommit`).

```sh
go get github.com/eidon-go/core/contrib/sqlx
```

```go
import (
    "github.com/eidon-go/core/contrib/sql"
    coresqlx "github.com/eidon-go/core/contrib/sqlx"
    "github.com/jmoiron/sqlx"
)

db, err := sql.New(sql.Config{DriverName: "pgx", DataSource: dsn})
if err != nil {
    return err
}

m, err := coresqlx.NewManager(db) // the bind type comes from db.DriverName()
if err != nil {
    return err
}

err = m.WithTx(ctx, func(ctx context.Context) error {
    q := m.GetQuerier(ctx) // the transaction of ctx, as an sqlx.ExtContext

    query, args, err := sqlx.In("SELECT * FROM accounts WHERE id IN (?)", ids)
    if err != nil {
        return err
    }

    var accounts []Account
    if err := sqlx.SelectContext(ctx, q, &accounts, q.Rebind(query), args...); err != nil {
        return err
    }

    _, err = q.NamedExecContext(ctx, "UPDATE accounts SET blocked = true WHERE owner = :owner", owner)

    return err
})
```

The `sql.DB` stays the component (`app.Add(db, app.WithReadiness())`); this module has no
lifecycle. It covers the `database/sql` world only: sqlx does not run on a pgx pool.

## Manager

`NewManager(db *sql.DB, opts...)` returns a `Manager`: an `sql.Transactor` (`WithTx`, `OnCommit`,
with every mode and option of package `sql`) whose `GetQuerier(ctx)` returns a `Querier` — the
transaction of ctx, or the database outside one. The transaction belongs to the `DB`: a
`coresqlx.Manager` and an `sql.Manager` over the same `DB` share it.

A `Querier` is:

- an `sqlx.ExtContext`: pass it to `sqlx.GetContext`, `sqlx.SelectContext`,
  `sqlx.NamedExecContext`; call `Rebind`, `BindNamed`, `QueryxContext`, `QueryRowxContext`;
- an `sql.SQLQuerier`: `sql.Get[T](ctx, q, …)`, `sql.Select[T]`, `sql.Exec`, sqlc's `DBTX`;
- `NamedExecContext(ctx, query, arg)` / `NamedQueryContext` on the querier itself (a struct, a map,
  or a slice of them for a batch insert).

Two `Get`s, two contracts:

| Call | No rows | Several rows |
|---|---|---|
| `q.GetContext(ctx, &dest, …)`, `sql.Get[T](ctx, q, …)` | `sql.ErrNoRows` | `sql.ErrTooManyRows` |
| `sqlx.GetContext(ctx, q, &dest, …)` | `sql.ErrNoRows` | the first row (sqlx's behavior) |

The bind type (`$1`, `?`, `@p1`, …) comes from `db.DriverName()`: `NewManager` fails with
`ErrUnknownDriver` when sqlx does not know the name. A `DB` from `sql.NewFromDB` (sqlmock) has
no driver name: pass `sql.WithDriverName("postgres")`; a custom driver name is registered with
`sqlx.BindDriver(name, sqlx.DOLLAR)`.

| Option | Default |
|---|---|
| `WithMapper(*reflectx.Mapper)` | `DefaultMapper()`: the `db` tag, otherwise the field name in lower case (`UserID` → `userid`) |
| `WithDefaultTxMode(sql.TxMode)` | `sql.TxModeJoin` |

## Scanner

`NewScanner(mapper)` is an `sql.Scanner` with sqlx's mapping, for code that keeps sqlx's
untagged lower-case fields but uses the helpers of package `sql`:

```go
db, err := sql.New(cfg, sql.WithScanner(coresqlx.NewScanner(nil)))
```

It keeps the contract of `sql.Scanner` (`ErrTooManyRows` from `Get`); a column that maps to no
field is an error; embedded structs are flattened; scalars and `sql.Scanner` types scan directly.
The `Manager`'s querier scans its `GetContext`/`SelectContext` with the same mapping, whatever
scanner the `DB` has.

## Migrating from sqlx

| sqlx | here |
|---|---|
| `*sqlx.DB` / `*sqlx.Tx` passed around | `m.GetQuerier(ctx)` (`Querier`) |
| `db.BeginTxx` … `Commit` / `Rollback` | `m.WithTx(ctx, fn, sql.WithIsolation(…), sql.ReadOnly())` |
| `db.GetContext(ctx, &v, …)` (first row) | `sqlx.GetContext(ctx, q, &v, …)` keeps it; `q.GetContext` fails on several rows |
| `db.Rebind`, `sqlx.In`, `NamedExecContext` | the same, on `q` |
| `sqlx.Connect(driver, dsn)` | `sql.New(sql.Config{DriverName: driver, DataSource: dsn})` (otelsql, ping with retry, health check) |

## Testing

Repositories: [sqlmock](https://github.com/DATA-DOG/go-sqlmock) through
`sql.NewFromDB(mockDB, sql.WithDriverName("postgres"))` and `coresqlx.NewManager`; domain code
fakes `sql.Transactor`.
