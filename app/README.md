# app

Runs the components of a service — anything implementing
[`lifecycle.Resource`](../lifecycle/README.md) — through one managed lifecycle.

```go
func main() {
    a := app.New(
        app.WithShutdownTimeout(30*time.Second),
        app.WithShutdownDelay(5*time.Second), // let load balancers notice
        app.WithStartTimeout(time.Minute),
    )

    a.Add(telemetry)               // dependencies first: telemetry is flushed last
    a.Add(cfg)
    a.Add(db, app.WithReadiness()) // cannot serve without it: its health gates readiness
    a.Add(cache)                   // serves without it, degraded: health is diagnostic
    a.Add(grpcServer)
    a.Add(httpServer)

    if err := a.Run(context.Background()); err != nil {
        log.Fatal(err)
    }
}
```

## Lifecycle

1. **Start.** Every `lifecycle.Starter` is started in registration order. If one fails (or the
   start timeout or a signal hits), every component is shut down as in step 3 — the started ones,
   the one that failed and the ones never started, since any of them may hold resources from
   construction on. A failure is returned; a signal is a clean stop.
2. **Run.** Every `lifecycle.Runner` runs concurrently until the context passed to `Run` is
   cancelled, a shutdown signal arrives, or any runner returns. A runner returning `nil` early is
   reported as `ErrRunnerStopped`. Without runners the app runs until the context is cancelled or
   a signal arrives.
3. **Shutdown.** Readiness fails at once; after the optional shutdown delay (skipped when a
   runner failed), under one timeout: the context of every `Run` is cancelled, every runner is
   shut down concurrently (which makes its `Run` return), the app waits for every `Run` to
   return, then the other components are shut down in reverse order. Runners shut down
   concurrently so that one server's drain does not eat into another's budget; they may depend
   on resources, not on each other.

Every `Shutdown` call is bounded: once the timeout has expired a component still gets 100ms to
force-close, and is then abandoned (its goroutine leaks) and reported with `ErrShutdownTimeout`.
Runners whose `Run` has not returned 100ms after that are reported the same way. So `ctx`-less
closes such as a pool's `Close()` cannot hang the process — but the phase can overrun the timeout
by up to 200ms for the runners (force-close grace, then the grace for `Run` to return) plus 100ms
per other component still pending; size the timeout (and the Kubernetes termination grace period)
accordingly.

During the shutdown delay runners keep running with their context intact; only readiness fails.
A runner that returns during the delay ends it.

`Run` returns `nil` after a clean stop; otherwise the cause joined with every shutdown error. It
may be called once (`ErrAlreadyStarted` afterwards).

## Options

| Option | Default | |
|---|---|---|
| `WithLogger(*slog.Logger)` | `slog.Default()` | lifecycle logs; never changes the global logger |
| `WithShutdownTimeout(d)` | 30s | budget for the whole shutdown phase |
| `WithShutdownDelay(d)` | none | keep serving with readiness failing before shutting down |
| `WithStartTimeout(d)` | none | budget for the whole start phase (cooperative: a `Start` that ignores its ctx overruns it) |
| `WithSignals(sigs...)` | SIGINT, SIGTERM | `WithSignals()` disables signal handling |
| `WithHealthcheckTimeout(d)` | none | per-component bound in `Healthcheck` / `HealthReport` |
| `WithObserver(fn)` | none | called after every Start, Run and Shutdown with an `Event` |

After the first signal the default signal behavior is restored, so a second Ctrl+C terminates a
shutdown that hangs.

## Registration

`Add(component lifecycle.Resource, opts ...AddOption)` registers one component; call it in
dependency order. A component implementing `lifecycle.Runner` is run; every component is shut
down. The only option, `WithReadiness()`, makes the component's health check gate readiness (see
[Health](#health)). To register several parts of one integration as a unit, combine them with
`lifecycle.Group` / `GroupNamed`. `Add` panics on a nil
component (including a typed nil pointer), on a component added twice, and after `Run` — all
programming errors. Duplicates are detected by pointer identity; non-pointer values and pointers to
zero-size types are never treated as duplicates.

Ordering patterns: telemetry first (flushed last), configuration and clients next, then
producers, then consumers and servers (stopped first).

Only **sources of work** — servers, consumers, schedulers — are runners. Runners are stopped
together, so nothing may depend on a runner while it drains. Whatever the runners use while
draining — a producer with a background flush loop, a client with a refresh loop — is a plain
`lifecycle.Resource` (`Starter` for its goroutines): it is shut down only after every `Run` has
returned.

## Health

`Healthcheck(ctx)` reports **readiness**: `ErrNotReady` before `Run`, while starting and from the
moment shutdown is requested; otherwise it runs the `lifecycle.Healthchecker` of every component
added `WithReadiness()` concurrently and joins the failures, each prefixed with the component's
name (`lifecycle.NameOf`). Components added without the option are not checked at all, so a slow
or hanging check of one never delays `/readyz`. `App` implements `lifecycle.Healthchecker`, so it
can back `/readyz` directly.

Readiness is **opt-in**. A failing readiness check takes every instance out of the load balancer
at once, so only the dependencies without which the service cannot answer any request belong in
it — typically the primary database. A cache, a message producer or a consumer that is down
should degrade the service, not take its API offline: add those without the option.

```go
a.Add(db, app.WithReadiness()) // /readyz fails while the database is unreachable
a.Add(cache)                   // reported, never fails /readyz
a.Add(consumer)                // reported; consumers are not load-balanced
```

`HealthReport(ctx)` runs **every** checker, with or without the option, and returns one
`HealthResult{Name, Err, Readiness}` per checker for structured output or a debug endpoint;
`Readiness` tells whether `Err` counts toward `Healthcheck`. Liveness (`/livez`) should not depend
on components: just respond.

## Observing the lifecycle

```go
app.WithObserver(func(ctx context.Context, e app.Event) {
    // e.Phase (PhaseStart, PhaseRun, PhaseShutdown), e.Component, e.Duration, e.Err
    startDuration.Record(ctx, e.Duration.Seconds(), ...)
})
```

Observers are called synchronously and possibly concurrently (Run events come from each runner's
goroutine, and runners are shut down concurrently); keep them fast and safe for concurrent use. A Shutdown event is reported when App
stops waiting — for an abandoned Shutdown that is before the call returns — and the Run event of
an abandoned runner may arrive after `App.Run` has returned. Events do not come in pairs: after a
failed startup every component gets a Shutdown event, including those that never got a Start or
Run event.

## Testing

Pass a context you control and disable signals:

```go
a := app.New(app.WithSignals())
a.Add(component)

ctx, cancel := context.WithCancel(t.Context())
go func() { done <- a.Run(ctx) }()
// ... exercise the app ...
cancel()
```
