# lifecycle

Small interfaces every component managed by [`app.App`](../app/README.md) implements in some
combination. Standard library only, so any module can depend on it.

```go
type Resource interface {            // every component
    Shutdown(ctx context.Context) error
}

type Starter interface {             // optional: startup work (listen, connect, ping, migrate)
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
2. **Run** — `Run` is called at most once, concurrently for all runners. It returns `nil` when
   its context is cancelled **or** `Shutdown` is called, whichever comes first. Returning before
   that (with an error or `nil`) stops the whole application.
3. **Shutdown** — called at most once, in reverse registration order, runners first. For a
   runner it may be called while `Run` is still running and must make `Run` return. It must
   also be safe when `Run` was never called (startup failed) — and `Run` may even be called
   *after* `Shutdown` started (another runner failed at once), in which case it returns `nil`
   promptly. When its context is done it force-closes and returns promptly; a `Shutdown` that
   ignores its context is abandoned by `app.App`.

`Healthcheck` and `Name` may be called at any time, concurrently with everything else.
`Healthchecker` is about **readiness** only; liveness is not aggregated from components.

A server maps naturally onto this: `Start` → `net.Listen` (port conflicts fail startup), `Run` →
`Serve(listener)` (map `ErrServerClosed` to `nil`), `Shutdown` → graceful stop, which is what
makes `Serve` return.

## Helpers

- `Group(members...)` / `GroupNamed(name, members...)` — one component over many: shuts the
  members down in reverse order and skips nil members (including typed nil pointers). It is a
  `Starter` only if a member is (starting them in order; on failure every member holding
  resources — those before the failed one and later members without `Start` — is shut down,
  bounded by the start context), and a `Healthchecker` only if a member is. Runners cannot be
  grouped: register them with `app.App` directly.
- `ShutdownFunc` — adapt a cleanup function: `a.Add(lifecycle.ShutdownFunc(tp.Shutdown))`.
- `HealthcheckFunc` — adapt a check function; it is also a `Resource` (no-op `Shutdown`), so it
  can be registered on its own.
- `NameOf(v)` — `v.Name()` if `v` is a `Namer`, otherwise its Go type. `IsNil(v)` — nil check
  that also catches typed nil pointers.

## Contract tests

`lifecycletest` checks a component against the contract — use it in every integration module:

```go
func TestServerContract(t *testing.T) {
    lifecycletest.Runner(t, func(t *testing.T) lifecycle.Runner { return newTestServer(t) },
        lifecycletest.WithTimeout(30*time.Second)) // default 10s
}
```

`lifecycletest.Resource` checks Shutdown without Start, Shutdown with a cancelled context,
Start→Shutdown and that Healthcheck returns before Start and after Shutdown;
`lifecycletest.Runner` adds: Run blocks until Shutdown and then returns nil, Run blocks until its
context is cancelled, and Run after Shutdown returns nil promptly.
