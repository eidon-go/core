// Package pgx is the PostgreSQL world of the sql module, on a pgx pool
// (github.com/jackc/pgx/v5/pgxpool): a DB with the lifecycle contract (the
// pool is created and pinged with retry in Start, closed in Shutdown),
// transactions through a Manager with the same modes, options, errors and
// sql.Transactor contract as package sql, and the same Get, Select and Exec
// helpers over a PgxQuerier.
//
// The package name clashes with github.com/jackc/pgx/v5; import this one as
// corepgx:
//
//	import corepgx "github.com/eidon-go/core/contrib/sql/pgx"
//
//	db, err := corepgx.New(corepgx.Config{DSN: dsn})
//	m := corepgx.NewManager(db)
//	a.Add(db) // app.App: before the components that use the pool
//
//	err = m.WithTx(ctx, func(ctx context.Context) error {
//	    _, err := sql.Exec(ctx, m.GetQuerier(ctx), sql.Raw("UPDATE …"))
//	    return err
//	})
//
// Importing this package registers pgx's database/sql driver ("pgx",
// DriverName), so package sql can reach PostgreSQL through it too.
package pgx
