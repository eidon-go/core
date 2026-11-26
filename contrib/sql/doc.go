// Package sql is a database/sql component for eidon-go services: a DB with
// the lifecycle contract (open in New, ping with retry in Start, close in
// Shutdown), transactions through a Manager (nested WithTx modes,
// savepoints, after-commit callbacks), and a small convenience layer — Get,
// Select and Exec over a Builder — that scans rows into structs with scany.
//
// The package name shadows database/sql; import that one as stdsql when
// both are needed.
//
// Typical wiring:
//
//	db, err := sql.New(sql.Config{DriverName: "pgx", DataSource: dsn})
//	m := sql.NewManager(db)
//	a.Add(db) // app.App: before the components that use it
//
//	err = m.WithTx(ctx, func(ctx context.Context) error {
//	    _, err := sql.Exec(ctx, m.GetQuerier(ctx), sql.Raw("UPDATE …"))
//	    return err
//	})
//
// PostgreSQL through pgxpool is in the sql/pgx package of this module; it
// shares the transaction options, errors and Transactor contract.
package sql
