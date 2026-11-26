# Contrib module guidelines

Every integration module under `contrib/` (`config/etcd`, `otel`, `http2`, `grpc`, `jwt`, `kafka`,
`nats`, `rabbitmq`, `redis`, `sql`, …) follows these rules, so that all of them look and behave the same.
The value of the project is this consistency; a module that does not follow them needs a written
reason in its README.

## 1. Module and dependencies

- One Go module per integration (`contrib/<name>/go.mod`, module path
  `github.com/eidon-go/core/contrib/<name>`), tagged `contrib/<name>/vX.Y.Z`.
- Depend on `github.com/eidon-go/core` for `lifecycle` and `retry` only. Never import `core/app`
  (components must work without it) or `core/config` (configuration comes in as a struct).
  Exception: `contrib/config/<backend>` modules (e.g. `contrib/config/etcd`) implement `config.KV` and therefore
  import `config` — and nothing else from it; the rest of their rules are in
  [11](#11-configuration-backends).
- Format modules (e.g. `contrib/config/yaml`) expose a plain `func([]byte) (map[string]any, error)` and
  need not import core at all.
- `require` the **minimum** versions you need (core and third-party), and the minimum `go`
  directive your dependencies allow. Dependabot does not bump them; the weekly latest-deps job
  checks compatibility instead.
- No `replace` directives in published modules.

## 2. Constructor and options

```go
func New(cfg Config, opts ...Option) (*Client, error)

type Option func(*options) // unexported options struct
func WithLogger(*slog.Logger) Option
```

- `New` validates and builds; it does **no network I/O**. Connecting, listening and pinging go in
  `Start(ctx)`; long-running work goes in `Run(ctx)`.
- Option names start with `With`. Every constructor that may ever need tuning takes
  `opts ...Option`.
- Required collaborators (a `*sql.DB`, a handler) are parameters, not options. A nil required
  collaborator is a programming error: return an error (or panic in `Must*` helpers only).

## 3. Configuration structs

- An exported `Config` struct of plain Go fields — **no required struct tags**, no reflection
  binding. Consumers fill it however they like: with `config` getters and a `Reader`, an env
  library, a YAML unmarshal or a literal. `yaml`/`json` tags may be added for convenience.
- Durations are `time.Duration`, sizes and counts are integers; document the unit and the default
  on every field.
- Zero means the documented default. A negative value disables the feature only where disabling
  makes sense and the field documents it (`http2` server and client timeouts, `http2/client`
  `KeepAlive`, `grpc/client` `IdleTimeout`, `config/etcd` `DialKeepAliveTime`, `sql`
  `MaxIdleConns`); everywhere else `Validate` rejects it. A field whose default already is "no
  limit" (`sql` `ConnMaxLifetime`, `grpc` `MaxConnectionAge`) has nothing to disable, so a negative
  value is an error there. When the library treats zero as "disabled", map the negative value to it
  after applying the defaults (see `http2/client`'s `disabledAsZero`).
- `func (c Config) Validate() error` reports every problem at once (`errors.Join`).
- Defaults are applied in `New`, never by reading the environment.

## 4. Lifecycle

Implement the [`lifecycle`](../lifecycle/README.md) contract and prove it with
`lifecycletest.Resource` / `lifecycletest.Runner` in the module's tests (raise the timeout with
`lifecycletest.WithTimeout` for container-backed components).

Build on the adapters instead of writing the stop state machine yourself (see "Writing a
component" in the lifecycle README):

- **Clients** (databases, caches, producers, connections): `Shutdown` through
  `lifecycle.CloseOnce` (a blocking `Close()`) or `lifecycle.ShutdownOnce` (a `Shutdown(ctx)`).
- **Servers**: `lifecycle.NewServer` with `Listen`/`Serve`/`Drain`/`Close`, kept in an unexported
  field (not embedded) and delegated to; re-export its state errors
  (`var ErrClosed = lifecycle.ErrClosed`) instead of declaring your own, and wrap the errors
  `Start`/`Run`/`Shutdown` return with the module's prefix (`"http2: %w"`).
- **Workers** (consumers, schedulers, poll loops): `lifecycle.NewWorker` with a loop that takes
  new work with its `stop` context and handles the work in flight with its `work` context, kept
  in an unexported field and delegated to; `Shutdown` calls the worker's `Shutdown` first, then
  releases the client (`CloseOnce`/`ShutdownOnce`).
- A hand-written `Runner` is the exception, for what fits none of these shapes.

The rules below are what the adapters implement; a hand-written component follows them in full.

- Every component implements `lifecycle.Namer`. Default name: the module name (`"redis"`);
  `WithName(string)` distinguishes instances (two databases).
- `Start(ctx)`: connect/listen/ping, typically through `retry.Policy` (see 5). On failure,
  return the error: `app.App` then calls `Shutdown` on every component — this one included, and
  those never started — so `Shutdown` releases whatever a partial `Start` acquired.
- `Run(ctx)` and `Shutdown(ctx)` are **one stop**: `app.App` cancels the ctx of `Run` first, then
  calls `Shutdown`. On cancellation `Run` stops taking new work at once and may drain the work in
  flight, without a deadline of its own; `Shutdown` starts the same stop if `Run` has not, bounds
  the drain and force-closes when its `ctx` is done, after which `Run` returns promptly. Side
  effects (closing listeners, shutdown hooks) happen once — typically a `sync.Once` around the
  stop that both methods wait on. `Run` returns `nil` (map `ErrServerClosed`, `ErrClosed` and
  similar to `nil`), and promptly if `Shutdown` was called before it.
- `Shutdown(ctx)`: graceful when possible, force-close when `ctx` is done, without blocking on
  I/O with peers afterwards; safe without `Start` or `Run` and after a failed `Start`;
  idempotent (a repeated call returns `nil`).
- `Healthcheck(ctx)`: readiness only, cheap (cached state or a bounded ping), honors `ctx`.
  Readiness in `app.App` is opt-in (`app.WithReadiness()`), because one failing check takes every
  instance out of the load balancer. The README says when to register the component with it:
  a client of a dependency the service cannot answer any request without (the primary database)
  usually is; caches, producers and consumers usually are not — the service degrades instead.
- Only sources of work (servers, consumers, schedulers) are `Runner`s: `app.App` stops runners
  together. Anything they use while draining — a producer with a flush loop, a client with a
  refresh loop — is a `Resource` (a `Starter` for its goroutines), so it is shut down after every
  `Run` has returned. Several parts of one integration may be combined with
  `lifecycle.GroupNamed` (resources only).

## 5. Retry

- Connection retry: `WithConnectRetry(retry.Policy)`, defaulting to `retry.ConnectPolicy()`
  (say so in its doc comment); a module that needs another default documents it.
- Redelivery/backoff of messages: `WithRedeliveryPolicy(retry.Policy)` using `Policy.Delay`.
- Reconnect loops use `retry.Forever` bounded by `ctx`; never retry forever without a context.
- Log retries with the component's logger: `policy.Logged(logger)` (it keeps an `OnRetry` the
  caller set).

## 6. Logging

- `WithLogger(*slog.Logger)`; nil means `slog.Default()`, resolved in `New`.
- Always the `*Context` methods (`InfoContext`, …) so trace correlation works.
- Add `slog.String("component", name)`; never log secrets; never call `slog.SetDefault`.

## 7. OpenTelemetry

- `WithTracerProvider(trace.TracerProvider)`, `WithMeterProvider(metric.MeterProvider)`,
  `WithPropagators(propagation.TextMapPropagator)`; defaults are the otel globals at `New` time.
- `WithTelemetry(Telemetry)` sets all three at once. `Telemetry` is a small interface declared
  in the module (`TracerProvider()`, `MeterProvider()`, `Propagators()` — fewer methods when the
  module does not propagate) that the `otel` module's `Provider` satisfies structurally, so the
  module does not import `contrib/otel`. Options apply in order; a single provider option after
  it replaces one of them.
- Instrumentation is on by default (disable with a noop provider); use semantic-convention
  attribute names. Only the `otel` module sets globals, and only when asked.

## 8. Errors

- Sentinel errors are `Err<What>` with messages prefixed `"<module>: "`; wrap with `%w`.
- Typed errors (`*XxxError`) when callers need details; `errors.Is`/`errors.As` must work.
- No panics in library code except programming errors in constructors.

## 9. Middleware and extensions

- Transport-native shapes only: `func(http.Handler) http.Handler`, gRPC interceptors,
  `func(Handler) Handler` for consumers; exported from a `middleware` sub-package with the order
  documented. The core defines no generic middleware type.
- Lifecycle metrics/traces come from `app.WithObserver`, not from wrapping components (wrappers
  hide optional interfaces).

## 10. Testing and docs

- Unit tests with `t.Parallel()` and `t.Context()`; ship a fake or in-memory mode when practical.
- Container-based tests live in `internal/integration` (not published).
- README: install, minimal example, options table with defaults, lifecycle mapping (what
  Start/Run/Shutdown/Healthcheck do), registration-order notes. At least one `Example` test.

## 11. Configuration backends

`contrib/config/<backend>` modules (etcd, …) feed `config` and are used before anything else
starts, so some of the rules above take a specific form for them. Every backend follows these:

- **`config.KV`, not lifecycle.** A backend implements `config.KV` (`Get`, `Watch`, `Close`). It
  is not a component: `config.Config` owns it (it is the `lifecycle.Runner`, runs the watches and
  closes the backend) — so no `Namer`/`Starter`/`Runner` and no `lifecycletest` on the backend
  itself. Prove the contract with `lifecycletest.Runner` on a `config.Config` backed by the real
  store in `internal/integration`.
- **The `Watch` contract** (see `config.KV`): the first event is the key's current state, then its
  later states in order — intermediate ones may be skipped by a backend that only sees the latest
  value, but never an older state, and every change is followed by the latest state; a failure is
  an event with `Err`, after which the channel closes. A backend whose watches are registered
  asynchronously takes the state and the start of the change stream from the same point of the
  store's history.
- **`New` does no I/O** (rule 2) — this includes authentication: create the client lazily on the
  first `Get`/`Watch`, shared by concurrent callers, retried after a failure, closed by `Close`
  even while it is being created.
- **No `WithConnectRetry`** (rule 5): there is no `Start`. `config` reopens failed watches
  (`WithWatchBackoff`); a read is retried only by the store's client, within
  `config.WithKVTimeout`. `config.New` reads every key once and fails if the store cannot answer in
  time — document that in the backend's README.
- **OpenTelemetry** (rule 7) as for any client, defaulting to the globals at `New` time. The
  global providers forward to the ones set later, so instrumentation starts working once the
  `otel` setup runs — usually after the configuration it needs was read through this backend.
  Leave long-lived watch streams out of tracing (their span would end only with the stream).
- **Errors** for a missing key wrap `config.ErrKVKeyNotFound`; after `Close`, `Get` and `Watch` fail
  with the module's `ErrClosed`.
