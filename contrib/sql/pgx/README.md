# sql/pgx

[![Go Reference](https://pkg.go.dev/badge/github.com/eidon-go/core/contrib/sql/pgx.svg)](https://pkg.go.dev/github.com/eidon-go/core/contrib/sql/pgx)

PostgreSQL on a [pgx](https://github.com/jackc/pgx) pool, as part of the
[`contrib/sql`](../README.md) module: the same transaction `Manager`, modes, options, errors and
`Get`/`Select`/`Exec` helpers as package `sql`, with pgx's native types, batches and `COPY`.

The package name clashes with `github.com/jackc/pgx/v5`; import it as `corepgx`:

```go
import corepgx "github.com/eidon-go/core/contrib/sql/pgx"

db, err := corepgx.New(corepgx.Config{DSN: "postgres://app:secret@db:5432/app?sslmode=disable"})
// …
m, err := corepgx.NewManager(db)
```

The pool is created in `Start` (pgxpool starts background goroutines, so `New` only builds its
configuration); `Pool()` is `nil` and queries fail with `ErrNotStarted` before it. `SQLDB()` gives
a `*sql.DB` over the same pool for libraries that only speak `database/sql`; it does not share the
transactions of `WithTx`.

Configuration, options, lifecycle, transactions and extension points are described in the
[module README](../README.md).
