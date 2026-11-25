# Contrib module guidelines

Every integration module (`config/etcd`, `otel`, `http2`, `grpc`, `jwt`, `kafka`, `nats`,
`rabbitmq`, `redis`, `sql`, …) follows these rules, so that all of them look and behave the same.
The value of the project is this consistency; a module that does not follow them needs a written
reason in its README.

## 1. Module and dependencies

- One Go module per integration (`<name>/go.mod`), tagged `<name>/vX.Y.Z`.
- Depend on `github.com/eidon-go/core` for `lifecycle` and `retry` only. Never import `core/app`
  (components must work without it) or `core/config` (configuration comes in as a struct).
  Exception: `config/<backend>` modules (e.g. `config/etcd`) implement `config.KV` and therefore
  import `config` — and nothing else from it.
- Format modules (e.g. `config/yaml`) expose a plain `func([]byte) (map[string]any, error)` and
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
- `func (c Config) Validate() error` reports every problem at once (`errors.Join`).
- Defaults are applied in `New`, never by reading the environment.

## 4. Lifecycle

Implement the [`lifecycle`](../lifecycle/README.md) contract and prove it with
`lifecycletest.Resource` / `lifecycletest.Runner` in the module's tests (raise the timeout with
`lifecycletest.WithTimeout` for container-backed components).

- Every component implements `lifecycle.Namer`. Default name: the module name (`"redis"`);
  `WithName(string)` distinguishes instances (two databases).
- `Start(ctx)`: connect/listen/ping, typically through `retry.Policy` (see 5). On failure, release
  everything acquired and return the error.
- `Run(ctx)` and `Shutdown(ctx)` are **one stop**: `app.App` cancels the ctx of `Run` first, then
  calls `Shutdown`. On cancellation `Run` stops taking new work at once and may drain the work in
  flight, without a deadline of its own; `Shutdown` starts the same stop if `Run` has not, bounds
  the drain and force-closes when its `ctx` is done, after which `Run` returns promptly. Side
  effects (closing listeners, shutdown hooks) happen once — typically a `sync.Once` around the
  stop that both methods wait on. `Run` returns `nil` (map `ErrServerClosed`, `ErrClosed` and
  similar to `nil`), and promptly if `Shutdown` was called before it.
- `Shutdown(ctx)`: graceful when possible, force-close when `ctx` is done, without blocking on
  I/O with peers afterwards; safe without `Start` or `Run`; idempotent (a repeated call returns
  `nil`).
- `Healthcheck(ctx)`: readiness only, cheap (cached state or a bounded ping), honors `ctx`.
- Only sources of work (servers, consumers, schedulers) are `Runner`s: `app.App` stops runners
  together. Anything they use while draining — a producer with a flush loop, a client with a
  refresh loop — is a `Resource` (a `Starter` for its goroutines), so it is shut down after every
  `Run` has returned. Several parts of one integration may be combined with
  `lifecycle.GroupNamed` (resources only).

## 5. Retry

- Connection retry: `WithConnectRetry(retry.Policy)`; document the default policy.
- Redelivery/backoff of messages: `WithRedeliveryPolicy(retry.Policy)` using `Policy.Delay`.
- Reconnect loops use `retry.Forever` bounded by `ctx`; never retry forever without a context.
- Log retries through `Policy.OnRetry` with the component's logger (`WarnContext`).

## 6. Logging

- `WithLogger(*slog.Logger)`; nil means `slog.Default()`, resolved in `New`.
- Always the `*Context` methods (`InfoContext`, …) so trace correlation works.
- Add `slog.String("component", name)`; never log secrets; never call `slog.SetDefault`.

## 7. OpenTelemetry

- `WithTracerProvider(trace.TracerProvider)`, `WithMeterProvider(metric.MeterProvider)`,
  `WithPropagators(propagation.TextMapPropagator)`; defaults are the otel globals at `New` time.
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
