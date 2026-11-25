# grpc

[![Go Reference](https://pkg.go.dev/badge/github.com/eidon-go/core/contrib/grpc.svg)](https://pkg.go.dev/github.com/eidon-go/core/contrib/grpc)
[![Release](https://img.shields.io/github/v/tag/eidon-go/core?filter=contrib%2Fgrpc%2Fv*&sort=semver&label=release)](https://github.com/eidon-go/core/releases)
[![Go Version](https://img.shields.io/github/go-mod/go-version/eidon-go/core?filename=contrib%2Fgrpc%2Fgo.mod)](./go.mod)
[![codecov](https://codecov.io/gh/eidon-go/core/graph/badge.svg?flag=contrib-grpc)](https://codecov.io/gh/eidon-go/core?flags%5B0%5D=contrib-grpc)

A gRPC server as a [`lifecycle`](../../lifecycle/README.md) component: the port is bound in `Start`,
RPCs are served in `Run`, and `Shutdown` drains in-flight RPCs, then force-closes what is left when
its deadline expires. Keepalive and connection-age settings in a plain `Config`, TLS, OpenTelemetry
instrumentation on by default, the standard health service driven by the application's readiness,
and panic-recovery interceptors.

```sh
go get github.com/eidon-go/core/contrib/grpc
```

```go
import grpcsrv "github.com/eidon-go/core/contrib/grpc"

a := app.New()

srv, err := grpcsrv.New(
    grpcsrv.Config{Addr: ":50051", MaxConnectionAge: 5 * time.Minute},
    grpcsrv.WithHealth(a), // grpc.health.v1 follows the app's readiness
    grpcsrv.WithUnaryInterceptors(middleware.RecoverUnary(logger)),
    grpcsrv.WithStreamInterceptors(middleware.RecoverStream(logger)),
)
if err != nil {
    return err
}

pb.RegisterGreeterServer(srv, &greeter{}) // the Server is a grpc.ServiceRegistrar
reflection.Register(srv)                  // optional: grpcurl, grpcui

a.Add(srv)

return a.Run(ctx)
```

The package is named `grpc`, like grpc-go's; import it under another name (`grpcsrv`) when a file
uses both.

## Configuration

`Config` is a plain struct; every field is optional. Zero means the default — grpc-go's own for
everything but `Addr`; negative values are rejected by `Validate`.

| Field | Default | Meaning |
|---|---|---|
| `Addr` | `:50051` | TCP listen address; port `0` picks a free port (see `Server.Addr`) |
| `MaxRecvMsgSize` | 4 MB | largest message the server accepts |
| `MaxSendMsgSize` | `math.MaxInt32` | largest message the server sends |
| `MaxConcurrentStreams` | no limit | concurrent RPCs per client connection |
| `ConnectionTimeout` | 120s | setup of a new connection, TLS handshake included |
| `MaxConnectionIdle` | no limit | close a connection without RPCs after this long |
| `MaxConnectionAge` | no limit | close a connection (GOAWAY, clients reconnect) at this age, ±10% jitter — set it behind layer-4 load balancers and in Kubernetes so clients spread over new instances |
| `MaxConnectionAgeGrace` | no limit | RPCs may still run this long on a connection closed by `MaxConnectionAge` |
| `KeepaliveTime` | 2h | ping a silent client after this long (minimum 1s) |
| `KeepaliveTimeout` | 20s | close the connection when the ping is not answered in time |
| `KeepaliveMinTime` | 5m | shortest client ping interval tolerated; faster clients get GOAWAY `too_many_pings`. Must not exceed the clients' keepalive time |
| `KeepalivePermitWithoutStream` | false | allow client pings while no RPC is active |

## Options

| Option | Default | |
|---|---|---|
| `WithName(string)` | `"grpc"` | component name in logs, errors and health reports |
| `WithLogger(*slog.Logger)` | `slog.Default()` | lifecycle messages; grpc-go's own logging stays with the global `grpclog` |
| `WithListener(net.Listener)` | listen on `Addr` | unix socket, socket activation, `bufconn` in tests; closed on Shutdown |
| `WithTLSConfig(*tls.Config)` | plaintext | serve over TLS; the config must have a certificate; set `ClientAuth`/`ClientCAs` for mTLS; cloned |
| `WithHealth(lifecycle.Healthchecker)` | off | register `grpc.health.v1.Health` (see [Health](#health)) |
| `WithUnaryInterceptors(...)`, `WithStreamInterceptors(...)` | — | interceptors; across calls, the first passed is the outermost |
| `WithTracerProvider`, `WithMeterProvider`, `WithPropagators` | otel globals at `New` | RPC spans and metrics via otelgrpc; pass noop providers to disable |
| `WithOtelGRPCOptions(...otelgrpc.Option)` | — | extra otelgrpc options, e.g. `WithMetricAttributesFn`; a `WithFilter` here replaces the one that leaves health RPCs out |
| `WithServerOptions(...grpc.ServerOption)` | — | escape hatch, applied last: a codec, other credentials, extra stats handlers, … |

Spans follow the RPC semantic conventions: `{package}.{Service}/{Method}`. Interceptors passed with
`grpc.ChainUnaryInterceptor` / `grpc.ChainStreamInterceptor` through `WithServerOptions` run inside
the ones from `WithUnaryInterceptors` / `WithStreamInterceptors` and also see health RPCs; a single
`grpc.UnaryInterceptor` / `grpc.StreamInterceptor` runs outermost, before all of them.

## Lifecycle

| Method | Does |
|---|---|
| `Start` | binds the listener; a busy port fails the application start |
| `Run` | serves until the server is stopped (ctx cancelled or `Shutdown`), then returns `nil` once the in-flight RPCs have finished or been cut off |
| `Shutdown` | stops accepting, sends GOAWAY, waits for in-flight RPCs (`GracefulStop`); when its ctx is done, closes the remaining connections (`Stop`), cancelling the RPCs still running, and returns an error wrapping `ctx.Err()`. Idempotent, safe without `Start`/`Run` |
| `Healthcheck` | `nil` while serving, `ErrNotServing` before `Run` and once the stop has begun |

Following the [lifecycle contract](../../lifecycle/README.md#contract), the cancelled ctx of `Run`
and `Shutdown` are one stop: whichever comes first starts `GracefulStop`, once; `Run` sets no
deadline of its own, `Shutdown` bounds the drain (the app's shutdown timeout) and force-closes at
its deadline, and `Run` returns at the same moment. RPC contexts are not derived from the ctx of
`Run`, so in-flight RPCs are not aborted by the drain. Without `app.App`, call `Shutdown` yourself
to bound the drain.

A streaming RPC that does not end by itself (a subscription, a long watch) holds the drain until
the `Shutdown` deadline; its handler then sees its context cancelled and the stop is reported as
forced. To end such streams cleanly, select on `Server.Draining()`, which is closed when the stop
begins, and return from the handler. A handler that ignores its context entirely does not delay
`Shutdown` past its deadline: the connections are closed and `Shutdown` returns, while the handler
goroutine keeps running until it returns by itself.

Register services before `Run`. `RegisterService` panics after `Run`, for a service registered
twice and for an implementation of the wrong type — where grpc-go itself would exit the process.
A `Server` is single-use: `Start` after `Shutdown` returns `ErrClosed`.

## Health

`WithHealth(readiness)` registers the standard
[`grpc.health.v1.Health`](https://github.com/grpc/grpc/blob/master/doc/health-checking.md) service
used by Kubernetes gRPC probes, Envoy and `grpc-health-probe`. Statuses are not set by hand: `Check`
answers `SERVING` while the server serves and `readiness.Healthcheck` returns nil, `NOT_SERVING`
otherwise. Pass the application, so that readiness fails as soon as shutdown begins; with
`app.WithShutdownDelay` the server keeps serving for the delay while load balancers move away.
A nil readiness follows the server alone.

- The status is the same for the empty service name (the whole server) and for every registered
  service; other names get `NOT_FOUND`. `List` reports all of them.
- `Watch` is not implemented (`Unimplemented`); probes use `Check`. grpc-go clients with
  client-side health checking (`healthCheckConfig`) treat that as "healthy" and so do not see
  `NOT_SERVING` during the shutdown delay; they notice the GOAWAY when the drain begins.
- Health RPCs of this service bypass the interceptors from `WithUnaryInterceptors` / `WithStreamInterceptors` —
  so authentication does not block probes — and are not traced.

## Middleware

`middleware.RecoverUnary(logger)` and `middleware.RecoverStream(logger)` recover a panic in the
handler, log it with the method and the stack trace, and answer `codes.Internal` with a generic
message (the panic value is not sent to the client). Messages a stream sent before the panic stay
sent. Pass them last, directly around the handler, inside logging and authentication.
