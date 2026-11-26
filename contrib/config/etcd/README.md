# config/etcd

[![Go Reference](https://pkg.go.dev/badge/github.com/eidon-go/core/contrib/config/etcd.svg)](https://pkg.go.dev/github.com/eidon-go/core/contrib/config/etcd)
[![Release](https://img.shields.io/github/v/tag/eidon-go/core?filter=contrib%2Fconfig%2Fetcd%2Fv*&sort=semver&label=release)](https://github.com/eidon-go/core/releases)
[![Go Version](https://img.shields.io/github/go-mod/go-version/eidon-go/core?filename=contrib%2Fconfig%2Fetcd%2Fgo.mod)](./go.mod)
[![codecov](https://codecov.io/gh/eidon-go/core/graph/badge.svg?flag=contrib-config-etcd)](https://codecov.io/gh/eidon-go/core?flags%5B0%5D=contrib-config-etcd)

An etcd v3 backend for [`config`](../../../config/README.md): it implements `config.KV`, so
configuration documents stored in etcd keys are read by `config.New` and hot-reloaded by
`Config.Run`.

```sh
go get github.com/eidon-go/core/contrib/config/etcd
```

```go
kv, err := etcd.New(etcd.Config{Endpoints: []string{"etcd-0:2379", "etcd-1:2379", "etcd-2:2379"}})
if err != nil {
    return err
}

cfg, err := config.New(ctx,
    config.WithKV(kv),                         // cfg owns kv from here on and closes it
    config.WithKeys("/svc/base.json"),         // read once
    config.WithWatchedKeys("/svc/flags.json"), // read, then watched while cfg runs
)
if err != nil {
    return err
}

limit := config.Live[int](cfg, "rate.limit") // follows /svc/flags.json

a := app.New()
a.Add(cfg) // the watcher runs in cfg.Run
```

Every key holds one whole document; its extension selects the parser (`.json` is built in, YAML
via [`config/yaml`](../yaml/README.md) and `config.WithParser`). Values are read through the
`config` getters as usual — this module only moves bytes.

## Configuration

`Config` is a plain struct. Fill it from the environment or a local file: it is needed before
remote configuration can be read.

| Field | Default | Meaning |
|---|---|---|
| `Endpoints` | — (required) | Cluster members, `host:port` or `scheme://host:port` |
| `Username`, `Password` | none | etcd authentication; set both or neither |
| `DialTimeout` | 5s | Bounds the authentication when the client is created (with credentials) |
| `DialKeepAliveTime` | 30s | gRPC keepalive interval; detects dead connections under a watch. Keep it ≥ the server's `--grpc-keepalive-min-time` (5s by default). Negative disables |
| `DialKeepAliveTimeout` | 10s | How long to wait for a keepalive response |
| `AutoSyncInterval` | 0 (off) | Refresh the member list from the cluster at this interval |

`Validate` reports every invalid field at once; each error wraps `ErrInvalidConfig`.

## Options

| Option | Default | Meaning |
|---|---|---|
| `WithLogger(*slog.Logger)` | `slog.Default()` | Receives the etcd client's log messages (it logs through zap; they are bridged to slog with `component=etcd`) |
| `WithClientLogLevel(slog.Leveler)` | `slog.LevelError` | Lowest level of the client's own messages passed on; it logs every retry at Warn while the cluster is unreachable, and `config` already reports failed reads and watches |
| `WithTLSConfig(*tls.Config)` | none (TLS with system roots if the first endpoint is `https://`) | TLS and client certificates for the cluster connections; `New` rejects `http://` endpoints with it (clientv3 would silently drop TLS) |
| `WithTracerProvider(trace.TracerProvider)` | global, at `New` | Spans of the etcd gRPC calls (see [OpenTelemetry](#opentelemetry)) |
| `WithMeterProvider(metric.MeterProvider)` | global, at `New` | Metrics of the etcd gRPC calls |
| `WithPropagators(propagation.TextMapPropagator)` | W3C Trace Context + Baggage until a global propagator is set (also after `New`) | Trace context injected into the calls |
| `WithTelemetry(Telemetry)` | — | All three from the `otel` module's `Provider` when it is not global |
| `WithClientConfig(func(*clientv3.Config))` | — | Last-chance changes to the `clientv3.Config` (message sizes, gRPC dial options, retry backoff). `DialOptions` already holds the OpenTelemetry stats handler: append to it. Keep `DialTimeout` positive with credentials — it is the only bound on the authentication |

`NewFromClient(*clientv3.Client)` uses a client the application already has (for leases or
elections). That client stays the caller's: `Close` leaves it open.

## Behavior

- `New` only validates and prepares: it does no network I/O. The etcd client is created by the
  first `Get` or `Watch` (with credentials, that is when it authenticates, bounded by
  `DialTimeout`); concurrent callers share one creation, their contexts bound only their wait, and
  a failed creation is retried by the next call.
- `Get` returns an error wrapping `config.ErrKVKeyNotFound` for a missing key. It is bounded only
  by its context; `config` bounds every read with `config.WithKVTimeout`.
- `Watch` follows the `config.KV` contract: the first event is the key's current state (a put, or a
  delete if the key does not exist), then every later change, in order, with none missed. It reads
  the key and watches from the revision right after that read: `clientv3` registers a watch
  asynchronously, so a watch "from now" could miss a change made while it is being set up.
- A watch ends with an error event (and `config` reopens it with backoff) when the initial read
  fails, the member loses its leader (`clientv3.WithRequireLeader`), or the revision is compacted.
  While the cluster is unreachable, the client reconnects on its own and keepalive detects dead
  connections.
- `Close` closes the client created by `New` (also one whose creation is still running);
  afterwards `Get` and `Watch` fail with `ErrClosed` (and with `ErrNoClient` on a zero `KV`). Repeated calls return the first result.
  `config` calls it after every watch has returned.

## Lifecycle

A `config.KV` is not a lifecycle component: it belongs to the `config.Config` it is passed to.
`config.Config` is the `lifecycle.Runner` — register it with `app.App`; its `Run` runs the watches
and its `Shutdown` stops them and closes the `KV`. `config.New` closes the `KV` itself if it fails.
The integration tests run `lifecycletest.Runner` against a `config.Config` backed by a real etcd.
There is no `Start`, so no `WithConnectRetry`: `clientv3` retries a failed read within the
`config.WithKVTimeout` bound, and `config` reopens failed watches (`WithWatchBackoff`). `config.New`
reads every key once — if the cluster cannot answer within the timeout, `New` fails.

This follows the rules for configuration backends in the
[contrib guidelines](../../../docs/contrib-guidelines.md#11-configuration-backends).

## OpenTelemetry

The etcd gRPC calls are instrumented with `otelgrpc`: a span and metrics per RPC (a read that
`clientv3` retries is one span per attempt). Watch streams are left out: `clientv3` multiplexes
every watch of a client on one stream that lives for hours, so its span would be exported only
when the stream ends; reconnects show up in the `config` log instead. The defaults are the global
providers at `New` time; they forward to providers set later with `otel.SetTracerProvider` /
`otel.SetMeterProvider`, so the `KV` may be built before telemetry is configured — typically from
the very configuration it reads. Only the calls made before that are not recorded. Pass noop
providers to disable it.

## Tests

Unit tests use fakes of `clientv3.KV` and `clientv3.Watcher`. Tests against a real etcd (in a
container) live in [`internal/integration`](../../../internal/integration) and run with
`make test-integration` (needs Docker).
