# lifecycle

Small interfaces every component managed by [`app.App`](../app/README.md) implements in some
combination. Standard library only, so any module can depend on it.

```go
type Resource interface {            // every component
    Shutdown(ctx context.Context) error
}

type Starter interface {             // optional: startup work (connect, ping, migrate)
    Start(ctx context.Context) error
}

type Runner interface {              // long-running work: servers, consumers
    Resource
    Run(ctx context.Context) error
}

type Healthchecker interface {       // optional: readiness
    Healthcheck(ctx context.Context) error
}

type Namer interface {               // optional: name in logs, errors, health reports
    Name() string
}
```

## Contract

`app.App` drives a component through three phases:

1. **Start** — `Start` is called once, in registration order, before any `Run`. If it fails,
   the component cleans up after itself: its `Shutdown` is not called.
2. **Run** — `Run` is called once, concurrently for all runners. It returns `nil` when its
   context is cancelled **or** `Shutdown` is called, whichever comes first. Returning before
   that (with an error or `nil`) stops the whole application.
3. **Shutdown** — called at most once, in reverse registration order, runners first. For a
   runner it may be called while `Run` is still running and must make `Run` return. It must
   also be safe when `Run` was never called (startup failed). When its context is done it
   force-closes and returns promptly.

`Healthcheck` and `Name` may be called at any time, concurrently with everything else.

A server maps naturally onto this: `Run` → `Serve`, `Shutdown` → graceful stop, which is what
makes `Serve` return.

## Helpers

- `Group(resources...)` — one `Resource` over many: shuts members down in reverse order,
  skips nil members (including typed nil pointers) and aggregates their health checks.
- `ShutdownFunc`, `HealthcheckFunc` — adapt plain functions:
  `a.Add(lifecycle.ShutdownFunc(tracerProvider.Shutdown))`.
- `NameOf(v)` — `v.Name()` if `v` is a `Namer`, otherwise its Go type. `IsNil(v)` — nil check
  that also catches typed nil pointers.
