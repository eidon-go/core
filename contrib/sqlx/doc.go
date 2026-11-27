// Package sqlx adapts github.com/jmoiron/sqlx to the transactions of
// github.com/eidon-go/core/contrib/sql: a Manager whose GetQuerier returns
// an sqlx querier (ExtContext: Rebind, named parameters, QueryxContext) over
// the transaction of the context, and a Scanner with sqlx's column mapping
// for the helpers of package sql.
//
// The package name clashes with jmoiron/sqlx: import it as coresqlx.
//
// It works in the database/sql world (package sql) only: sqlx does not run
// on a pgx pool. The DB owns the lifecycle; this package has none.
package sqlx
