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

    a.Add(telemetry, cfg, db, cache) // dependencies first: telemetry is flushed last
    a.Add(grpcServer, httpServer)

    if err := a.Run(context.Background()); err != nil {
        log.Fatal(err)
    }
}
```

## Lifecycle

1. **Start.** Every `lifecycle.Starter` is started in registration order. If one fails (or the
   start timeout or a signal hits), what was started — plus every component without `Start` — is
   shut down in reverse order. A failure is returned; a signal is a clean stop.
2. **Run.** Every `lifecycle.Runner` runs concurrently until the context passed to `Run` is
   cancelled, a shutdown signal arrives, or any runner returns. A runner returning `nil` early is
   reported as `ErrRunnerStopped`. Without runners the app runs until the context is cancelled or
   a signal arrives.
3. **Shutdown.** Readiness fails at once; after the optional shutdown delay (skipped when a
   runner failed), under one timeout: runners are shut down in reverse order (which makes their
   `Run` return), the app waits for every `Run` to return, then the other components are shut
   down in reverse order.

Every `Shutdown` call is bounded: once the timeout has expired a component still gets
`ForceCloseGrace` (100ms) to force-close, and is then abandoned (its goroutine leaks) and
reported with `ErrShutdownTimeout`. Runners still running at the deadline are reported the same
way. So `ctx`-less closes such as a pool's `Close()` cannot hang the process.

`Run` returns `nil` after a clean stop; otherwise the cause joined with every shutdown error. It
may be called once (`ErrAlreadyStarted` afterwards).

## Options

| Option | Default | |
|---|---|---|
| `WithLogger(*slog.Logger)` | `slog.Default()` | lifecycle logs; never changes the global logger |
| `WithShutdownTimeout(d)` | 30s | budget for the whole shutdown phase |
| `WithShutdownDelay(d)` | none | keep serving with readiness failing before shutting down |
| `WithStartTimeout(d)` | none | budget for the whole start phase |
| `WithSignals(sigs...)` | SIGINT, SIGTERM | `WithSignals()` disables signal handling |
| `WithHealthcheckTimeout(d)` | none | per-component bound in `Healthcheck` / `HealthReport` |
| `WithObserver(fn)` | none | called after every Start, Run and Shutdown with an `Event` |

After the first signal the default signal behavior is restored, so a second Ctrl+C terminates a
shutdown that hangs.

## Registration

`Add(components ...lifecycle.Resource)` registers components in dependency order. A component
implementing `lifecycle.Runner` is run; every component is shut down. `Add` panics on a nil
component (including a typed nil pointer), on a component added twice, and after `Run` — all
programming errors.

Ordering patterns: telemetry first (flushed last), configuration and clients next, then
producers, then consumers and servers (stopped first).

## Health

`Healthcheck(ctx)` reports **readiness**: `ErrNotReady` before `Run`, while starting and from the
moment shutdown is requested; otherwise it runs every registered `lifecycle.Healthchecker`
concurrently and joins the failures, each prefixed with the component's name
(`lifecycle.NameOf`). `App` implements `lifecycle.Healthchecker`, so it can back `/readyz`
directly. `HealthReport(ctx)` returns one `HealthResult{Name, Err}` per checker for structured
output. Liveness (`/livez`) should not depend on components: just respond.

## Observing the lifecycle

```go
app.WithObserver(func(ctx context.Context, e app.Event) {
    // e.Phase (PhaseStart, PhaseRun, PhaseShutdown), e.Component, e.Duration, e.Err
    startDuration.Record(ctx, e.Duration.Seconds(), ...)
})
```

Observers are called synchronously from the goroutine that made the call; keep them fast.

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
