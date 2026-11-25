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

1. **Start** — `Start` is called once, in registration order, before any `Run`. If one fails
   (or the context is done), the startup stops and **every** component is shut down as in
   phase 3: the started ones, the one that failed and the ones never started — any of them may
   hold resources from construction on. A failed `Start` need not clean up after itself;
   `Shutdown` must be safe in every state: never started, after a failed `Start`, after `Run`.
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

## Writing a component

Most integrations need none of this state machine: two adapters implement it once.

**A client** (database, cache, producer, gRPC connection) is a `Resource`, usually a `Starter`
that connects with retry. `CloseOnce` / `ShutdownOnce` give it an idempotent, ctx-bounded
`Shutdown` — no `sync.Once`, flag or channel in the component:

```go
type Client struct {
    pool     *pgxpool.Pool
    releaser *lifecycle.Releaser // lifecycle.CloseOnce(func() error { pool.Close(); return nil })
    retry    retry.Policy        // WithConnectRetry, default retry.ConnectPolicy()
}

func (c *Client) Start(ctx context.Context) error {
    return c.retry.Logged(c.logger).Do(ctx, c.pool.Ping)
}

func (c *Client) Shutdown(ctx context.Context) error { return c.releaser.Shutdown(ctx) }
```

- `CloseOnce(close)` — for a blocking `Close()` without a context: the first `Shutdown` runs it
  in the background; every `Shutdown` waits for it until its own context is done.
- `ShutdownOnce(fn)` — for a release that honors a context (an SDK's `Shutdown(ctx)`): the first
  `Shutdown` calls it with its context.
- Either way the release runs once, its error goes to the first `Shutdown`, later calls return
  `nil` once it has finished (or the context error if their own context ends first), a release
  that has finished wins over an expired context, and a panic becomes the error. `Closing()`
  reports that `Shutdown` was called — advisory, since it races with a concurrent `Shutdown`;
  `Done()` is closed when the release has finished.

**A server** describes itself with four functions of its library; `NewServer` turns them into a
`Starter`, `Runner` and `Healthchecker`:

```go
lc := lifecycle.NewServer(lifecycle.ServerFuncs{
    Listen: s.listen,  // Start: open the listener, so a busy port fails the startup
    Serve:  s.serve,   // Run: Serve(listener), with ErrServerClosed mapped to nil
    Drain:  s.drain,   // graceful stop until ctx is cancelled; also closes an unserved listener
    Close:  srv.Close, // force-close at the Shutdown deadline, or after a failed Drain
    Logger: logger,    // optional: "server draining", "server stopped" (forced)
})
```

The adapter keeps the whole stop: one `Drain` shared by `Run`'s cancellation and `Shutdown`,
`Run` returning only once the drain has finished or been cut off (also when `Serve` returns or
fails as soon as the stop begins, like net/http's), `Close` in its own goroutine so that
`Shutdown` never blocks past its deadline (grpc-go's `Stop` can wait on stuck handlers), `Close`
after a failed `Drain` so that `Serve` returns, `Run` after `Shutdown` returning `nil`,
`Healthcheck` (`ErrNotServing` before `Run` and from the moment the stop begins), `Draining()`
for long-lived handlers, and panics in the four functions turned into errors. `Listen` returns
before `Serve` or `Drain` is called (a `Shutdown` during `Start` waits for it), so they can share
the listener without locking. It returns `ErrNotStarted`, `ErrAlreadyStarted` and `ErrClosed`;
modules re-export them (`var ErrClosed = lifecycle.ErrClosed`) and wrap the errors they return
with their own prefix. `DrainBlocking(srv.GracefulStop)` adapts a graceful stop without a
context.

Keep the `*Server` in an unexported field and delegate `Start`, `Run`, `Shutdown` and
`Healthcheck` to it: embedding would make every method added to `Server` later part of the
component's API.

Implementing `Runner` by hand remains possible for what fits neither shape; such a component must
follow the contract above in full and pass `lifecycletest.Runner`.

## Helpers

- `Group(members...)` / `GroupNamed(name, members...)` — one component over many: shuts the
  members down in reverse order and skips nil members (including typed nil pointers). It is a
  `Starter` only if a member is (starting them in order and stopping at the first error; it
  does not shut members down itself — the caller's `Shutdown` releases every member), and a
  `Healthchecker` only if a member is. Runners cannot be grouped: register them with `app.App`
  directly.
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
Start→Shutdown, Shutdown after a Start with a cancelled context, a repeated Shutdown, and that
Healthcheck returns before Start and after Shutdown; `lifecycletest.Runner` adds: Run blocks
until Shutdown and then returns nil, Run blocks until its context is cancelled, Run after
Shutdown returns nil promptly, the `app.App` stop order
(cancel, then Shutdown with an already expired context: Run must return within a second), and
cancellation and Shutdown at the same time. A panic in any method fails the check.
