# otel

[![Go Reference](https://pkg.go.dev/badge/github.com/eidon-go/core/contrib/otel.svg)](https://pkg.go.dev/github.com/eidon-go/core/contrib/otel)
[![Release](https://img.shields.io/github/v/tag/eidon-go/core?filter=contrib%2Fotel%2Fv*&sort=semver&label=release)](https://github.com/eidon-go/core/releases)
[![Go Version](https://img.shields.io/github/go-mod/go-version/eidon-go/core?filename=contrib%2Fotel%2Fgo.mod)](./go.mod)
[![codecov](https://codecov.io/gh/eidon-go/core/graph/badge.svg?flag=contrib-otel)](https://codecov.io/gh/eidon-go/core?flags%5B0%5D=contrib-otel)

Sets up the OpenTelemetry SDK — traces, metrics and logs exported over OTLP/HTTP — and owns it as
a [`lifecycle.Resource`](../../lifecycle/README.md), so `app.App` flushes and closes it at the
right moment. The other contrib modules (`http2`, `config/etcd`, …) are instrumented already; this
module gives their telemetry somewhere to go.

```sh
go get github.com/eidon-go/core/contrib/otel
```

```go
telemetry, err := otel.New(otel.Config{
    ServiceName:    "orders",
    ServiceVersion: version,
    Environment:    "production",
    Endpoint:       "http://otel-collector:4318",
    Traces:         true,
    Metrics:        true,
    Logs:           true,
})
if err != nil {
    return err
}

telemetry.SetGlobal() // the contrib modules default to the otel globals

a := app.New()
a.Add(telemetry)  // first: shut down last, after everything that emits telemetry
a.Add(httpServer) // their spans and metrics recorded while stopping are still exported
a.Add(worker)
```

Without `SetGlobal`, hand the provider over explicitly: every integration module takes
`WithTelemetry(telemetry)` (`http2.WithTelemetry`, `kafka.WithTelemetry`, …), which sets the
tracer provider, the meter provider and the propagators together. With only
`WithTracerProvider(telemetry.TracerProvider())` the modules propagate W3C Trace Context +
Baggage (their default until a global propagator is set), not the propagators of `Provider`.

## Configuration

`Config` is a plain struct.

| Field | Default | Meaning |
|---|---|---|
| `ServiceName` | — (required) | `service.name` resource attribute |
| `ServiceVersion` | none | `service.version` |
| `Environment` | none | `deployment.environment.name` (`production`, `staging`) |
| `ResourceAttributes` | none | Extra resource attributes (`k8s.pod.name`, `team`); the three fields above win over the same keys here. Empty keys are rejected |
| `Endpoint` | — (required with any of the signals below) | Base URL of the OTLP/HTTP receiver, `http(s)://host:port[/prefix]`. The signal path (`/v1/traces`, `/v1/metrics`, `/v1/logs`) is appended, as with `OTEL_EXPORTER_OTLP_ENDPOINT`. `https` uses TLS and `http` does not, whatever the `OTEL_EXPORTER_OTLP_*` variables say. Rejected: a bare `host:port`, a path that already ends in `/v1/<signal>`, escaped slashes (`%2F`) in the path, a query or user info |
| `Headers` | none | Sent with every export request (authentication tokens). When set, they replace `OTEL_EXPORTER_OTLP_HEADERS` instead of adding to it; when empty, that variable's headers are sent. `New` copies the map |
| `ExportTimeout` | 10s | Bounds **each HTTP attempt**. Failed attempts are retried with backoff; the whole export, retries included, is bounded by the SDK processor's export timeout (30s by default, `OTEL_BSP_EXPORT_TIMEOUT` / `OTEL_BLRP_EXPORT_TIMEOUT`) |
| `Traces`, `Metrics`, `Logs` | off | Turn on the OTLP exporter of each signal |
| `MetricInterval` | 60s (SDK default) | How often metrics are collected and pushed |

A signal that is off gets a no-op provider: instrumentation keeps working and records nothing.
With every signal off, `New` builds nothing and `Shutdown` has nothing to do — the setting for local
runs without a collector. `Validate` reports every invalid field at once; each error wraps
`ErrInvalidConfig`.

## Options

| Option | Default | Meaning |
|---|---|---|
| `WithName(string)` | `"otel"` | Component name in logs and errors |
| `WithLogger(*slog.Logger)` | `slog.Default()` | Logs errors the SDK reports (failed exports) once `SetGlobal` installed the error handler |
| `WithTLSConfig(*tls.Config)` | system roots (or `OTEL_EXPORTER_OTLP_CERTIFICATE`) | TLS of the exporters with an `https` endpoint (private CA, client certificates); `New` rejects it with an `http` endpoint |
| `WithPropagators(propagation.TextMapPropagator)` | W3C Trace Context + Baggage | Returned by `Propagators`, installed by `SetGlobal` |
| `WithResourceOptions(...resource.Option)` | none | More resource attributes and detectors (`resource.WithFromEnv()`, `resource.WithHost()`, `resource.WithContainer()`) |
| `WithSpanProcessor(sdktrace.SpanProcessor)` | none | Another span destination (a batch processor around any exporter); turns traces on, with or without OTLP |
| `WithMetricReader(sdkmetric.Reader)` | none | Another metric reader (a periodic reader around any exporter, a Prometheus pull exporter); turns metrics on |
| `WithLogProcessor(sdklog.Processor)` | none | Another log destination; turns logs on |
| `WithTracerProviderOptions(...sdktrace.TracerProviderOption)` | none | Sampler, span limits, ID generator |
| `WithMeterProviderOptions(...sdkmetric.Option)` | none | Views (histogram buckets, dropped attributes), exemplar filter, cardinality limit |
| `WithLoggerProviderOptions(...sdklog.LoggerProviderOption)` | none | Attribute limits and the like |

The `With*ProviderOptions` apply after the module's own, so they can override them —
`sdktrace.WithResource` there replaces the resource built from `Config` rather than merging with
it. They are ignored while their signal is off: a signal is turned on by `Config` or by
`WithSpanProcessor` / `WithMetricReader` / `WithLogProcessor`, not by `sdktrace.WithBatcher` or
`sdkmetric.WithReader` passed here.

Processors and readers passed as options belong to the `Provider`: `Shutdown` shuts them down, and
so does `New` when it fails — the caller never has to clean them up.

The built-in exporters speak OTLP over HTTP only, so there is one protocol to configure. For OTLP
over gRPC, pass an `otlptracegrpc` (`otlpmetricgrpc`, `otlploggrpc`) exporter through
`WithSpanProcessor(sdktrace.NewBatchSpanProcessor(exp))` and so on.

## Behavior

- `New` does no network I/O: the exporters connect on their first export. It does not touch the
  otel globals either.
- `SetGlobal` installs the providers of the signals that are on (signals that are off leave their
  globals alone), the propagators and an error handler that logs SDK errors at Warn with
  `component=otel`. Tracers and meters taken from the globals before the first `SetGlobal` of the
  process follow too: the otel globals delegate once, to the first providers set. A later
  `SetGlobal` of another `Provider` changes what the globals return from then on, not what those
  early tracers use. After `Shutdown`, `SetGlobal` does nothing.
- Resource detection problems — a detector failing, a detector built against another semantic
  conventions version (conflicting schema URL), a malformed `OTEL_RESOURCE_ATTRIBUTES` with
  `resource.WithFromEnv()` — are logged at Warn and `New` goes on with what was detected:
  telemetry setup does not keep a service from starting.
- Logs: bridge `log/slog` with
  [`otelslog`](https://pkg.go.dev/go.opentelemetry.io/contrib/bridges/otelslog)
  (`otelslog.NewHandler(name, otelslog.WithLoggerProvider(telemetry.LoggerProvider()))`), usually
  next to a console handler. Do not send that handler's records to `WithLogger`: a failing log
  export would then log about itself.
- `ForceFlush(ctx)` exports what is buffered — for jobs and CLIs that exit right after their work.

### Environment variables

The [guidelines](../../docs/contrib-guidelines.md#3-configuration-structs) say modules never read
the environment. This module cannot fully keep that rule: the OpenTelemetry SDK and exporters read
the standard `OTEL_*` variables themselves, and following the OpenTelemetry specification there is
what operators (the OpenTelemetry Operator, k8s manifests) rely on. What the module sets always
wins; the rest comes from the SDK:

| Set by the module, variables ignored | Still read by the SDK |
|---|---|
| Endpoint, URL paths and TLS on/off (from `Config.Endpoint`): `OTEL_EXPORTER_OTLP_ENDPOINT`, `…_INSECURE` | `OTEL_EXPORTER_OTLP_HEADERS` while `Config.Headers` is empty |
| Request timeout (`Config.ExportTimeout`): `OTEL_EXPORTER_OTLP_TIMEOUT` | `OTEL_EXPORTER_OTLP_CERTIFICATE`, `…_CLIENT_CERTIFICATE`, `…_CLIENT_KEY` with an `https` endpoint and no `WithTLSConfig` (ignored with `http`) |
| `Config.Headers` when set | `OTEL_EXPORTER_OTLP_COMPRESSION` |
| Metric push interval (`Config.MetricInterval`): `OTEL_METRIC_EXPORT_INTERVAL` | `OTEL_METRIC_EXPORT_TIMEOUT` |
| `service.name`, `service.version`, `deployment.environment.name`, `ResourceAttributes` | `OTEL_RESOURCE_ATTRIBUTES`, `OTEL_SERVICE_NAME` — always merged **underneath** the module's resource (the SDK does it in `WithResource`), so they add attributes but never override `Config` |
| | `OTEL_TRACES_SAMPLER(_ARG)` (a sampler in `WithTracerProviderOptions` overrides it), `OTEL_BSP_*`, `OTEL_BLRP_*` |

Each signal-specific variant (`OTEL_EXPORTER_OTLP_TRACES_*`, …) behaves like its general one. A
malformed `OTEL_RESOURCE_ATTRIBUTES` is reported by the SDK through `otel.Handle` while `New` runs —
before `SetGlobal` installs the module's error handler, so it goes to the SDK's default (stderr).

## Lifecycle

| Method | What it does |
|---|---|
| `Shutdown(ctx)` | Flushes and shuts down the signal providers concurrently, each bounded by `ctx`; later telemetry is dropped. The first call does it; a concurrent call waits for it until its own `ctx` is done; calls after it return `nil`. Errors read `otel: shutdown <signal>: …` |

The `Provider` is a `Resource` and a `Namer` — no `Start`, `Run` or `Healthcheck`: it has no
background work of its own besides the SDK's batchers, and a collector being down is not a reason
to take the service out of rotation. Register it **before** the components that emit telemetry, so
`app.App` shuts it down after them.

## Tests

Unit tests run against an in-process fake OTLP/HTTP receiver (`httptest`, plain and TLS) that
decodes the protobuf requests, plus in-memory processors; `lifecycletest.Resource` checks the
contract. The interplay with `OTEL_*` variables (TLS, certificates, headers, resource attributes)
is tested with `t.Setenv`. No containers needed.
