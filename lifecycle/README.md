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
2. **Run** — `Run` is called at most once, concurrently for all runners. It blocks until the
   runner is stopped — its context cancelled **or** `Shutdown` called — and its work in flight
   has finished or been cut off, then returns `nil`. Returning before it is stopped (with an
   error or `nil`) stops the whole application.
3. **Shutdown** — the context of `Run` is cancelled first; then `Shutdown` is called at most once
   per component: on every runner concurrently, then — once their `Run` has returned — on the
   other components in reverse registration order. When its context is done `Shutdown`
   force-closes and returns promptly, without blocking on I/O with peers; a `Shutdown` that
   ignores its context is abandoned by `app.App` after a 100ms grace. `Shutdown` must be
   idempotent: a repeated call returns `nil`.

For a runner, the cancelled context and the `Shutdown` that follows are **one stop request**:

- On cancellation `Run` stops taking new work at once. It may finish the work in flight (drain),
  but does not bound the drain itself — `Shutdown` does, and force-closes at its deadline, after
  which `Run` returns promptly.
- `Shutdown` may run while `Run` is draining; the two share one stop, so side effects (closing a
  listener, shutdown hooks) happen once.
- `Shutdown` may be called while `Run` is still running and must make it return; it must be safe
  when `Run` was never called (startup failed) or already returned. `Run` may even be called
  *after* `Shutdown` started (another runner failed at once); it then returns `nil` promptly.
- Without `app.App`, follow the same order: cancelling the context of `Run` alone need not
  release anything or bound a drain — always call `Shutdown` with a deadline.

Runners are stopped together, so only **sources of work** (servers, consumers, schedulers) should
be runners. Whatever they use while draining — a producer with a flush loop, a client with a
refresh loop — is a plain `Resource` (a `Starter` for its goroutines), shut down after every `Run`
has returned.

`Healthcheck` and `Name` may be called at any time, concurrently with everything else.
`Healthchecker` is about **readiness** only; liveness is not aggregated from components.

A server maps naturally onto this: `Start` → `net.Listen` (port conflicts fail startup), `Run` →
`Serve(listener)` (map `ErrServerClosed` to `nil`) and a graceful stop when the context is
cancelled, `Shutdown` → the same graceful stop, bounded by its context, then a force-close.

## Helpers

- `Group(members...)` / `GroupNamed(name, members...)` — one component over many: shuts the
  members down in reverse order and skips nil members (including typed nil pointers). It is a
  `Starter` only if a member is (starting them in order; on failure every member holding
  resources — those before the failed one and later members without `Start` — is shut down,
  bounded by the start context), and a `Healthchecker` only if a member is. Runners cannot be
  grouped: register them with `app.App` directly.
- `ShutdownFunc` — adapt a cleanup function: `a.Add(lifecycle.ShutdownFunc(tp.Shutdown))`. It
  is idempotent only if the function is.
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
Start→Shutdown, a repeated Shutdown, and that Healthcheck returns before Start and after
Shutdown; `lifecycletest.Runner` adds: Run blocks until Shutdown and then returns nil, Run blocks
until its context is cancelled, Run after Shutdown returns nil promptly, the `app.App` stop order
(cancel, then Shutdown with an already expired context: Run must return within a second), and
cancellation and Shutdown at the same time. A panic in any method fails the check.
