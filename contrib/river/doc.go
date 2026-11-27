// Package river runs a River (github.com/riverqueue/river) job queue client
// as a lifecycle component on the pgx pool of github.com/eidon-go/core/contrib/sql/pgx,
// and inserts jobs in the transaction of the context — the one that
// corepgx.Manager.WithTx began — so that a job exists only once the data it
// is about is committed.
//
// It is a thin wrapper: the River configuration (queues, workers, periodic
// jobs, retry policy) is River's own river.Config, and migrations stay
// outside (rivermigrate, goose, a CLI — the service decides). What it adds:
//
//   - the client is built in Start on the pool of the DB (which must be
//     registered before it) and works in Run; Shutdown stops it softly
//     (no new jobs, the jobs in flight finish) and cancels the jobs in
//     flight when Shutdown's deadline passes;
//   - Insert and InsertMany join the transaction of the context, or insert
//     on the pool outside one;
//   - OpenTelemetry traces and metrics (otelriver), on by default, with W3C
//     trace context carried from the insert to the work.
//
// The package name clashes with River's: import it as coreriver.
package river
