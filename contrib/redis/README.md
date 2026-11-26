# redis

[![Go Reference](https://pkg.go.dev/badge/github.com/eidon-go/core/contrib/redis.svg)](https://pkg.go.dev/github.com/eidon-go/core/contrib/redis)
[![Release](https://img.shields.io/github/v/tag/eidon-go/core?filter=contrib%2Fredis%2Fv*&sort=semver&label=release)](https://github.com/eidon-go/core/releases)
[![Go Version](https://img.shields.io/github/go-mod/go-version/eidon-go/core?filename=contrib%2Fredis%2Fgo.mod)](./go.mod)
[![codecov](https://codecov.io/gh/eidon-go/core/graph/badge.svg?flag=contrib-redis)](https://codecov.io/gh/eidon-go/core?flags%5B0%5D=contrib-redis)

A Redis client component on [go-redis v9](https://github.com/redis/go-redis): a single node, a
cluster or a Sentinel-managed master behind the [`lifecycle`](../../lifecycle/README.md) contract,
instrumented with OpenTelemetry (redisotel). Commands use the go-redis API.

```sh
go get github.com/eidon-go/core/contrib/redis
```

```go
import (
    goredis "github.com/redis/go-redis/v9"

    "github.com/eidon-go/core/contrib/redis"
)

client, err := redis.New(redis.Config{
    Addrs:    []string{"localhost:6379"},
    Password: password,
})
a.Add(client) // app.App: Start pings it, Shutdown closes it; health is diagnostic unless app.WithReadiness()

rdb := client.Redis()
err = rdb.Set(ctx, "key", "value", time.Hour).Err()
value, err := rdb.Get(ctx, "key").Result()
if errors.Is(err, goredis.Nil) { /* missing */ }
```

`Redis()` returns go-redis's `UniversalClient` (a `*goredis.Client` or a `*goredis.ClusterClient`)
for commands, pipelines, transactions, Pub/Sub and scripts. It is owned by the component: call
`Shutdown`, not `Close`.

## Configuration

`Addrs` chooses the kind of client: one address is a single node, several are a cluster, and with
`MasterName` they are the Sentinels of that master. `Cluster: true` forces cluster mode with one
address. Zero keeps go-redis's default; a negative value disables the feature where the table says so.

| Field | Default | Meaning |
|---|---|---|
| `Addrs` | required | `host:port` of the node, the cluster nodes or the Sentinels |
| `MasterName` | — | Sentinel master to follow |
| `Cluster` | `false` | cluster mode with one address (not with `MasterName`) |
| `Username`, `Password` | — | ACL user / `requirepass` password |
| `SentinelUsername`, `SentinelPassword` | — | authentication to the Sentinels |
| `DB` | `0` | logical database (must be 0 in a cluster) |
| `TLS` | `false` | TLS with the system roots, server name from each address |
| `PoolSize` | 10 × GOMAXPROCS | connections per node |
| `MinIdleConns` | `0` | idle connections kept open per node (dialed in the background from `New`) |
| `DialTimeout` | 5s | new connection |
| `ReadTimeout` | 5s; < 0: none | reading a reply |
| `WriteTimeout` | `ReadTimeout`; < 0: none | writing a command |
| `PoolTimeout` | `ReadTimeout` + 1s (30s without one) | waiting for a free connection |
| `ConnMaxIdleTime` | 30m; < 0: never | closes idle connections |
| `ConnMaxLifetime` | 30m (`DefaultConnMaxLifetime`); < 0: never | replaces old connections (load balancers, failover) |
| `MaxRetries` | 3; < 0: none | command retries after a network error |
| `MinRetryBackoff`, `MaxRetryBackoff` | 10ms, 1s | wait between command retries |

`Config.Validate` reports every problem at once (each wraps `ErrInvalidConfig`).

A deadline of a command's ctx bounds its reads and writes as well (go-redis's
`ContextTimeoutEnabled` is on; `WithUniversalOptions` can turn it off). go-redis interrupts a
blocked read only at a deadline, so without a read timeout the PING of `Start` and `Healthcheck`
is bounded by 5s: a cancelled `Start` (a signal during startup) returns within that. Turning
`ContextTimeoutEnabled` off with a negative `ReadTimeout`, or setting `ReadTimeout` to -2, through
`WithUniversalOptions` removes that bound: go-redis then reads without any deadline.

## Options

| Option | Default | |
|---|---|---|
| `WithName(string)` | `"redis"` | component name (logs, errors, health) |
| `WithLogger(*slog.Logger)` | `slog.Default()` | logs connect retries; go-redis's global logger is left alone |
| `WithConnectRetry(retry.Policy)` | `retry.ConnectPolicy()` | the ping in `Start` |
| `WithTracerProvider`, `WithMeterProvider` | otel globals at `New` | command spans; command and pool metrics |
| `WithTelemetry(Telemetry)` | — | both providers, e.g. from the `otel` module's `Provider` |
| `WithTracingOptions(...redisotel.TracingOption)` | — | e.g. `redisotel.WithDialFilter(true)` drops background dial spans, `WithDBStatement(false)` the arguments |
| `WithTLSConfig(*tls.Config)` | — | custom TLS (cloned); implies TLS |
| `WithUniversalOptions(func(*goredis.UniversalOptions))` | — | anything `Config` does not cover (credentials provider, dialer, `RouteByLatency`, protocol, …) |

## Lifecycle

| | |
|---|---|
| `New` | validates, builds and instruments the go-redis client; no I/O (except `MinIdleConns`) |
| `Start` | pings until the server answers, with `WithConnectRetry`; `Shutdown` ends the retries |
| `Shutdown` | closes the client and its pools, unregisters the pool metrics; bounded by ctx, idempotent, safe without `Start` |
| `Healthcheck` | `PING` within ctx; `ErrClosed` after `Shutdown` |
| `Name` | `WithName` |

The `Client` is a `Resource`, not a `Runner`: register the components that use it (servers,
consumers) after it, so they shut down first. Used as a cache, add it without `app.WithReadiness()`: the service degrades
while Redis is down instead of failing readiness on every instance; the check still appears in
`HealthReport`.

Tests run on [miniredis](https://github.com/alicebob/miniredis), which you can use for your own
unit tests too.
