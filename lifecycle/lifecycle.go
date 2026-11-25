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
//     Run. If Start returns an error, the component must release whatever it
//     acquired itself: its Shutdown is not called.
//  2. Run: Runner.Run is called at most once, concurrently for all runners.
//     It blocks until ctx is cancelled or Shutdown is called.
//  3. Shutdown: Shutdown is called at most once per component, in reverse
//     registration order, runners first. For a Runner, Shutdown may be called
//     while Run is still running and must make Run return; Shutdown must also
//     be safe when Run was never called (startup failed) or has already
//     returned. Run may even be called after Shutdown has started (another
//     runner failed at once); it must then return nil promptly.
//
// When the ctx passed to Shutdown is done, app.App waits only a short grace
// period (100ms) for it to force-close, then reports the component as timed
// out; a Shutdown that ignores ctx is abandoned, together with its goroutine.
//
// Healthcheck and Name may be called at any time, concurrently with each
// other and with Start, Run and Shutdown.
package lifecycle

import "context"

// Resource is a stateful holder of connections, pools or background
// goroutines. It participates in graceful shutdown via Shutdown.
type Resource interface {
	// Shutdown releases the resources held by the component. app.App calls it
	// at most once. ctx bounds the time allowed for a graceful release: when
	// ctx is done, the implementation must force-close (drop connections,
	// abandon in-flight work) and return promptly, typically with ctx.Err().
	Shutdown(ctx context.Context) error
}

// Starter is an optional interface for components that need startup work
// which must complete before the application serves: connecting with retry,
// pinging, running migrations, warming caches.
//
// app.App calls Start once per component, in registration order, before any
// Runner.Run. If Start fails, App stops the startup, shuts down everything
// started so far in reverse order and returns the error. A Start that fails
// must clean up after itself: its Shutdown is not called.
type Starter interface {
	// Start performs startup work. It must honor ctx: ctx is cancelled on a
	// shutdown signal or when the start timeout of app.App expires.
	Start(ctx context.Context) error
}

// Runner is a Resource that performs long-running work in Run: HTTP or gRPC
// servers, message consumers, schedulers.
//
// Run and Shutdown are called from different goroutines. Run returns when
// either its ctx is cancelled or Shutdown is called — whichever comes first.
// A server would typically map Run to Serve/ListenAndServe and Shutdown to its
// graceful stop, which is what makes Serve return.
type Runner interface {
	Resource

	// Run blocks until ctx is cancelled or Shutdown is called, then returns
	// nil (context.Canceled is also accepted as a clean stop). Returning
	// before that — with an error or with nil — makes app.App shut the whole
	// application down; an error is reported as the cause. If Shutdown has
	// already been called, Run must return nil promptly.
	Run(ctx context.Context) error
}

// Healthchecker is an optional interface a component implements to report
// readiness (e.g. for a /readyz endpoint). app.App.Healthcheck aggregates
// every registered Healthchecker.
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
