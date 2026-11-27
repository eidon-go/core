# http2

[![Go Reference](https://pkg.go.dev/badge/github.com/eidon-go/core/contrib/http2.svg)](https://pkg.go.dev/github.com/eidon-go/core/contrib/http2)
[![Release](https://img.shields.io/github/v/tag/eidon-go/core?filter=contrib%2Fhttp2%2Fv*&sort=semver&label=release)](https://github.com/eidon-go/core/releases)
[![Go Version](https://img.shields.io/github/go-mod/go-version/eidon-go/core?filename=contrib%2Fhttp2%2Fgo.mod)](./go.mod)
[![codecov](https://codecov.io/gh/eidon-go/core/graph/badge.svg?flag=contrib-http2)](https://codecov.io/gh/eidon-go/core?flags%5B0%5D=contrib-http2)

An HTTP server as a [`lifecycle`](../../lifecycle/README.md) component: the port is bound in `Start`,
requests are served in `Run`, and `Shutdown` drains in-flight requests, then force-closes what is
left when its deadline expires. HTTP/1.1 and HTTP/2 (over TLS, or cleartext h2c), OpenTelemetry
instrumentation on by default, opt-in Kubernetes-style probes, and a panic-recovery middleware.
The [`client`](#client) package builds the matching outbound `*http.Client`.

```sh
go get github.com/eidon-go/core/contrib/http2
```

```go
mux := http.NewServeMux()
mux.HandleFunc("GET /hello/{name}", hello)

a := app.New()

srv, err := http2.New(
    http2.Config{Addr: ":8080"},
    middleware.Recover(logger)(mux),
    http2.WithProbes(a), // /livez, /readyz follow the app's readiness
)
if err != nil {
    return err
}

a.Add(srv)

return a.Run(ctx)
```

## Configuration

`Config` is a plain struct; every field is optional. For the timeouts, zero means the default and a
negative value disables the timeout.

| Field | Default | Meaning |
|---|---|---|
| `Addr` | `:8080` | TCP listen address; port `0` picks a free port (see `Server.Addr`) |
| `ReadHeaderTimeout` | 10s | reading the request headers (Slowloris protection) |
| `ReadTimeout` | 30s | reading the whole request, body included |
| `WriteTimeout` | 30s | writing the response; streaming handlers extend it per request with `http.ResponseController` |
| `IdleTimeout` | 120s | keep-alive connection waiting for the next request |
| `MaxHeaderBytes` | 1 MB | request header size limit |
| `UnencryptedHTTP2` | false | serve HTTP/2 over cleartext TCP (h2c, prior knowledge) next to HTTP/1.1 |

## Options

| Option | Default | |
|---|---|---|
| `WithName(string)` | `"http2"` | component name in logs, errors, health reports; otelhttp operation name |
| `WithLogger(*slog.Logger)` | `slog.Default()` | lifecycle messages and the `http.Server` error log |
| `WithListener(net.Listener)` | listen on `Addr` | unix socket, socket activation, custom listener; closed on Shutdown |
| `WithTLSConfig(*tls.Config)` | plain HTTP | serve HTTPS; HTTP/2 via ALPN; the config must have a certificate |
| `WithProbes(lifecycle.Healthchecker)` | off | serve `/livez` and `/readyz` in front of the handler |
| `WithTracerProvider`, `WithMeterProvider` | otel globals at `New` | request spans and metrics via otelhttp; pass noop providers to disable |
| `WithPropagators` | W3C Trace Context + Baggage until a global propagator is set (also after `New`); an empty `propagation.NewCompositeTextMapPropagator()` switches it off | trace-context extraction |
| `WithTelemetry(Telemetry)` | — | all three from the `otel` module's `Provider` when it is not global |
| `WithOtelHTTPOptions(...otelhttp.Option)` | — | extra otelhttp options, e.g. `WithMetricAttributesFn` against cardinality |
| `WithHTTPServer(func(*http.Server))` | — | escape hatch for other `http.Server` fields (`ConnState`, `RegisterOnShutdown`, `TLSConfig`, …) |

The `http.Server` error log (TLS handshake failures, broken client connections) goes to the logger
at Info level: it reports client-side noise, not server faults.

Spans are named `{method} {route}` from the `http.ServeMux` pattern (`GET /users/{id}`), or just
`{method}` when no pattern matched.

## Lifecycle

| Method | Does |
|---|---|
| `Start` | binds the listener; a busy port fails the application start |
| `Run` | serves until the server is stopped (ctx cancelled or `Shutdown`), then returns `nil` once the in-flight requests have finished or been cut off |
| `Shutdown` | stops accepting, waits for in-flight requests; when its ctx is done, closes the remaining connections and returns an error wrapping `ctx.Err()`. Idempotent, safe without `Start`/`Run` |
| `Healthcheck` | `nil` while serving, `ErrNotServing` before `Run` and once the stop has begun |

Following the [lifecycle contract](../../lifecycle/README.md#contract), the cancelled ctx of `Run`
and `Shutdown` are one stop: whichever comes first closes the listener and starts the drain, once
(shutdown hooks run once); `Run` sets no deadline of its own, `Shutdown` bounds the drain (the
app's shutdown timeout) and force-closes at its deadline, and `Run` returns at the same moment.
Request contexts are not derived from the ctx of `Run`, so in-flight requests are not aborted by
the drain. Without `app.App`, call `Shutdown` yourself to bound the drain.

To keep serving while load balancers notice that the instance is going away, use
`app.WithShutdownDelay` together with `WithProbes(a)`: `/readyz` fails as soon as shutdown begins,
the server keeps serving for the delay, then drains.

A `Server` is single-use: `Start` after `Shutdown` returns `ErrClosed`.

Hijacked connections (WebSockets) are not tracked by `net/http`: close them from a hook registered
with `WithHTTPServer(func(s *http.Server) { s.RegisterOnShutdown(...) })`.

## Probes

`WithProbes(readiness)` serves, before the handler:

- `GET /livez` — always `200 ok`; checks no dependency on purpose (a failing database must take the
  instance out of load balancing, not get it restarted).
- `GET /readyz` — `200 ready` when `readiness.Healthcheck` returns nil, `503 not ready` otherwise.
  The error itself is not written to the response (it may name internal hosts); use
  `app.HealthReport` for details.

Probe requests bypass the handler and its middleware (authentication) and are not traced.

## Middleware

`middleware.Recover(logger)` recovers a panic, logs it with the stack trace and answers 500 if the
response has not started (an informational header such as 103 Early Hints does not count).
`http.ErrAbortHandler` is re-raised. The wrapped writer keeps `http.Flusher`, `http.Hijacker` and
`io.ReaderFrom` on a bare type assertion, so streaming (SSE, connect-go server streams), WebSocket
upgrades and the sendfile path of `http.ServeContent` work through it; write errors are returned
unwrapped. Put it directly around the application handler,
inside logging and authentication.

## Client

Package `http2/client` builds an `*http.Client` for service-to-service and third-party API calls:

```go
c, err := client.New(
    client.Config{ResponseHeaderTimeout: 60 * time.Second}, // a slow upstream
    client.WithRoundTripper(bearer(token)),
)

ctx, cancel := context.WithTimeout(ctx, 2*time.Minute) // the per-call deadline
defer cancel()
req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
resp, err := c.Do(req)
```

It returns a plain `*http.Client`, not a lifecycle component. The client has no client-wide
`Timeout` on purpose: bound every call with its context, so deadlines propagate through retries
and request chains.

| `Config` field | Default | Meaning |
|---|---|---|
| `MaxIdleConns` | 100 | idle connections across all hosts |
| `MaxIdleConnsPerHost` | 20 | idle connections per host (net/http's default of 2 forces reconnects) |
| `MaxConnsPerHost` | 0 (no limit) | all connections per host |
| `DialTimeout` | 5s | TCP connect |
| `KeepAlive` | 30s | TCP keep-alive interval |
| `TLSHandshakeTimeout` | 5s | TLS handshake |
| `ResponseHeaderTimeout` | 10s | wait for the response headers after the request was written |
| `IdleConnTimeout` | 90s | how long an idle connection stays pooled |
| `UnencryptedHTTP2` | false | speak h2c (prior knowledge) to `http://` URLs, e.g. to an `http2.Server` with `UnencryptedHTTP2`; the client then speaks HTTP/2 only |

Zero means the default; a negative duration disables the timeout. Over TLS, HTTP/2 is negotiated
by default.

| Option | Default | |
|---|---|---|
| `WithTLSConfig(*tls.Config)` | system roots | mTLS, private CAs; cloned |
| `WithRoundTripper(wrap)` | — | auth headers, signing, rate limiting, retries; the first passed is the outermost |
| `WithTransport(func(*http.Transport))` | — | escape hatch for other transport fields (proxy, compression, dialer) |
| `WithTracerProvider`, `WithMeterProvider` | otel globals at `New` | client spans (`{method}`), metrics and trace-context injection via otelhttp |
| `WithPropagators` | W3C Trace Context + Baggage until a global propagator is set (also after `New`); an empty `propagation.NewCompositeTextMapPropagator()` switches it off | trace-context injection |
| `WithTelemetry(Telemetry)` | — | all three from the `otel` module's `Provider` when it is not global |
| `WithOtelHTTPOptions(...otelhttp.Option)` | — | extra otelhttp options |

Tracing is innermost, directly around the transport: a span covers one network round trip, and
each retry made by a wrapper gets its own span.
