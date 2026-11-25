# app

Runs the components of a service — anything implementing
[`lifecycle.Resource`](../lifecycle/README.md) — through one managed lifecycle.

```go
func main() {
    a := app.New(
        app.WithShutdownTimeout(30*time.Second),
        app.WithStartTimeout(time.Minute),
    )

    a.Add(cfg, telemetry, db, cache) // dependencies first
    a.Add(grpcServer, httpServer)

    if err := a.Run(context.Background()); err != nil {
        log.Fatal(err)
    }
}
```

## Lifecycle

1. **Start.** Every `lifecycle.Starter` is started in registration order. If one fails, what was
   started (plus every component without `Start`) is shut down in reverse order and `Run`
   returns the error.
2. **Run.** Every `lifecycle.Runner` runs concurrently until the context passed to `Run` is
   cancelled, a shutdown signal arrives, or any runner returns. A runner returning `nil` early is
   reported as `ErrRunnerStopped`. Without runners the app runs until the context is cancelled or
   a signal arrives.
3. **Shutdown.** Under one timeout: runners are shut down in reverse order (which makes their
   `Run` return), the app waits for every `Run` to return, then the other components are shut
   down in reverse order. Runners still running when the timeout expires are reported with
   `ErrShutdownTimeout`.

`Run` returns `nil` after a clean stop; otherwise the cause joined with every shutdown error.
It may be called once (`ErrAlreadyStarted` afterwards).

## Options

| Option | Default | |
|---|---|---|
| `WithLogger(*slog.Logger)` | `slog.Default()` | lifecycle logs; never changes the global logger |
| `WithShutdownTimeout(d)` | 30s | budget for the whole shutdown phase |
| `WithStartTimeout(d)` | none | budget for the whole start phase |
| `WithSignals(sigs...)` | SIGINT, SIGTERM | `WithSignals()` disables signal handling |
| `WithHealthcheckTimeout(d)` | none | per-component bound in `Healthcheck` / `HealthReport` |

After the first signal the default signal behavior is restored, so a second Ctrl+C terminates a
shutdown that hangs.

## Registration

`Add(components ...lifecycle.Resource)` registers components in dependency order. A component
implementing `lifecycle.Runner` is run; every component is shut down. `Add` panics on a nil
component (including a typed nil pointer) and after `Run` — both are programming errors.

## Health

`Healthcheck(ctx)` runs every registered `lifecycle.Healthchecker` concurrently and joins the
failures, each prefixed with the component's name (`lifecycle.NameOf`). `App` itself implements
`lifecycle.Healthchecker`, so it can back a `/readyz` endpoint directly. `HealthReport(ctx)`
returns one `HealthResult{Name, Err}` per checker for structured output.

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
