// Package lifecycle defines the small interfaces every component managed by
// app.App implements in some combination:
//
//   - Resource — holds connections, pools or goroutines and releases them in
//     Shutdown. Every component is a Resource.
//   - Starter — optional: performs startup work (connect, ping, warm up)
//     that must finish before the application starts serving.
//   - Runner — a Resource whose primary contract is long-running work in Run
//     (servers, consumers).
//   - Healthchecker — optional: reports readiness.
//   - Namer — optional: a human-readable name for logs, errors and health
//     reports.
//
// The package depends only on the standard library, so every other module can
// depend on it without import cycles.
//
// # Contract
//
// app.App drives a component through these phases:
//
//  1. Start: Starter.Start is called once, in registration order, before any
//     Run. If a Start returns an error (or ctx is done), the startup stops
//     and every registered component is shut down as in phase 3 — the
//     started ones, the one that failed and the ones never started — since
//     a component may hold resources from construction on (a preset
//     listener, a client built in its constructor).
//  2. Run: Runner.Run is called at most once, concurrently for all runners.
//     It blocks until the runner is stopped (see below).
//  3. Shutdown: the ctx passed to Run is cancelled first; then Shutdown is
//     called at most once per component — on every runner concurrently, then,
//     once their Run has returned, on the other components in reverse
//     registration order.
//
// Shutdown must therefore be safe in every state: never started, after a
// failed Start, after Start, during or after Run, and when called again.
//
// For a Runner, the cancellation of Run's ctx and the Shutdown call that
// follows are one stop request:
//
//   - On cancellation Run stops taking new work at once. It may finish the
//     work in flight (a server drains its requests), but it does not bound
//     that drain itself: Shutdown does. When the ctx passed to Shutdown is
//     done, the runner force-closes and Run returns promptly.
//   - Shutdown may therefore run while Run is draining. The two share one
//     stop: side effects such as closing a listener or running shutdown
//     hooks happen once.
//   - Shutdown may also be called while Run is still running and must make
//     Run return; it must be safe when Run was never called (startup failed)
//     or has already returned. Run may even be called after Shutdown has
//     started (another runner failed at once); it must then return nil
//     promptly.
//
// Runners are stopped together, so only sources of work (servers, consumers,
// schedulers) should be runners. Whatever they use while draining — a
// producer with a flush loop, a client with a refresh loop — is a plain
// Resource, shut down after every Run has returned.
//
// Code that drives components without app.App follows the same order:
// cancelling Run's ctx alone need not release resources or bound a drain, so
// always call Shutdown, with a deadline — also when Start has failed.
//
// When the ctx passed to Shutdown is done, app.App waits only a short grace
// period (100ms) for it to force-close, and another one for Run to return,
// then reports the component as timed out; a Shutdown that ignores ctx is
// abandoned, together with its goroutine. Once its ctx is done, Shutdown must
// not block on I/O with peers (a graceful TLS close, a flush to a slow
// consumer).
//
// Healthcheck and Name may be called at any time, concurrently with each
// other and with Start, Run and Shutdown.
package lifecycle

import "context"

// Resource is a stateful holder of connections, pools or background
// goroutines. It participates in graceful shutdown via Shutdown.
type Resource interface {
	// Shutdown releases the resources held by the component. ctx bounds the
	// time allowed for a graceful release: when ctx is done, the
	// implementation must force-close (drop connections, abandon in-flight
	// work) and return promptly, typically with ctx.Err().
	//
	// app.App calls it at most once, but Shutdown must be idempotent: a
	// repeated call (from Group, tests or cleanup code) returns nil once the
	// first one has released everything.
	Shutdown(ctx context.Context) error
}

// Starter is an optional interface for components that need startup work
// which must complete before the application serves: connecting with retry,
// pinging, running migrations, warming caches.
//
// app.App calls Start once per component, in registration order, before any
// Runner.Run. If Start fails, App stops the startup, shuts down every
// registered component (including this one and those never started) and
// returns the error. A failed Start therefore need not release what it has
// acquired: Shutdown does, and must cope with that state.
type Starter interface {
	// Start performs startup work. It must honor ctx: ctx is cancelled on a
	// shutdown signal or when the start timeout of app.App expires.
	Start(ctx context.Context) error
}

// Runner is a Resource that performs long-running work in Run: HTTP or gRPC
// servers, message consumers, schedulers.
//
// Run and Shutdown are called from different goroutines. Cancelling Run's ctx
// or calling Shutdown — whichever comes first — stops the runner; see the
// package documentation for how the two combine. A server would typically
// map Run to Serve, start its graceful stop when ctx is cancelled, and let
// Shutdown bound that stop and force-close at its deadline.
type Runner interface {
	Resource

	// Run blocks until the runner is stopped — its ctx cancelled or Shutdown
	// called — and its work in flight has finished or been cut off by
	// Shutdown, then returns nil (context.Canceled is also accepted as a
	// clean stop). Returning before it is stopped — with an error or with
	// nil — makes app.App shut the whole application down; an error is
	// reported as the cause. If Shutdown has already been called, Run must
	// return nil promptly.
	Run(ctx context.Context) error
}

// Healthchecker is an optional interface a component implements to report
// readiness (e.g. for a /readyz endpoint). app.App.Healthcheck aggregates
// the Healthcheckers of the components added with app.WithReadiness;
// app.App.HealthReport lists them all.
//
// Healthcheck may be called frequently, concurrently with any other method,
// including during shutdown, so it should be cheap — a cached result or a
// lightweight ping — and must honor ctx deadlines.
//
// Healthchecker is about readiness only. Liveness ("the process is not
// stuck") is deliberately not aggregated from components: a failing
// dependency must take an instance out of load balancing, not get it
// restarted. A /livez endpoint should just respond.
type Healthchecker interface {
	// Healthcheck returns nil if the component is ready to serve, or an error
	// describing why it is not.
	Healthcheck(ctx context.Context) error
}

// Namer is an optional interface that gives a component a human-readable
// name. app.App uses it in logs, errors and health reports; without it the
// component's Go type is used, which cannot tell two instances apart.
type Namer interface {
	Name() string
}
