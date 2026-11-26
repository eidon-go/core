# rabbitmq

[![Go Reference](https://pkg.go.dev/badge/github.com/eidon-go/core/contrib/rabbitmq.svg)](https://pkg.go.dev/github.com/eidon-go/core/contrib/rabbitmq)
[![Release](https://img.shields.io/github/v/tag/eidon-go/core?filter=contrib%2Frabbitmq%2Fv*&sort=semver&label=release)](https://github.com/eidon-go/core/releases)
[![Go Version](https://img.shields.io/github/go-mod/go-version/eidon-go/core?filename=contrib%2Frabbitmq%2Fgo.mod)](./go.mod)
[![codecov](https://codecov.io/gh/eidon-go/core/graph/badge.svg?flag=contrib-rabbitmq)](https://codecov.io/gh/eidon-go/core?flags%5B0%5D=contrib-rabbitmq)

RabbitMQ (AMQP 0-9-1) on [amqp091-go](https://github.com/rabbitmq/amqp091-go) as
[`lifecycle`](../../lifecycle/README.md) components:

- `Connection` — one AMQP connection that keeps itself connected: `Start` dials with retry, a
  background loop redials whenever the connection is lost, `Shutdown` closes it. Publishers and
  consumers open their channels on it.
- `Publisher` — publishing with publisher confirms: `Publish` returns once the broker has taken
  every message. `Shutdown` waits for the confirms outstanding.
- `Consumer` — a queue consumer that hands every message to a `Handler`, with a configurable number
  of workers, and acks what was handled. On stop the broker stops delivering, the messages in
  flight finish and are acked, the prefetched ones are requeued. A failed message is retried in
  place, requeued, rejected to the dead-letter exchange or stops the consumer — never dropped
  silently by default. After a reconnect it declares its topology and subscribes again.

OpenTelemetry (publish and process spans with trace context in the message headers, messaging
metrics) is on by default.

```sh
go get github.com/eidon-go/core/contrib/rabbitmq
```

```go
cfg := rabbitmq.Config{Host: "rabbitmq", User: "billing", Password: secret}

// Separate connections for publishing and consuming: see "Connections" below.
publishConn, err := rabbitmq.NewConnection(cfg, rabbitmq.WithName("rabbitmq-publish"))
if err != nil {
    return err
}

consumeConn, err := rabbitmq.NewConnection(cfg, rabbitmq.WithName("rabbitmq-consume"))
if err != nil {
    return err
}

publisher, err := rabbitmq.NewPublisher(publishConn)
if err != nil {
    return err
}

consumer, err := rabbitmq.NewConsumer(consumeConn, rabbitmq.ConsumerConfig{
    Queue:       "billing.orders",
    Concurrency: 4,
    Topology: rabbitmq.Topology{
        Exchanges: []rabbitmq.Exchange{{Name: "orders", Kind: rabbitmq.ExchangeTopic, Durable: true}},
        Queues: []rabbitmq.Queue{{Name: "billing.orders", Durable: true, Args: map[string]any{
            "x-queue-type":           "quorum",
            "x-dead-letter-exchange": "orders.dlx",
        }}},
        Bindings: []rabbitmq.Binding{{Queue: "billing.orders", Exchange: "orders", RoutingKey: "order.created"}},
    },
}, rabbitmq.JSON(func(ctx context.Context, o Order, _ *rabbitmq.Message) error {
    return billing.Charge(ctx, o) // idempotent: delivery is at least once
}), rabbitmq.WithErrorHandler(rabbitmq.DeadLetter))
if err != nil {
    return err
}

a := app.New()
a.Add(publishConn, consumeConn) // Starters: dialed first, closed last
a.Add(publisher)                // a Resource: shut down after the consumer has drained
a.Add(consumer)                 // a Runner: stopped first
return a.Run(ctx)
```

## Configuration

`Config` is a plain struct (`json`/`yaml` tags for convenience); `Validate` reports every problem at
once, wrapping `ErrInvalidConfig`.

| Field | Default | Meaning |
|---|---|---|
| `Host` | required | broker host name or IP (IPv6 without brackets) |
| `Port` | 5672, 5671 with TLS | AMQP port |
| `User`, `Password` | guest account | PLAIN credentials, sent as they are (no URL, no escaping) |
| `VHost` | `/` | virtual host |
| `TLS` | false | amqps; `WithTLSConfig` for certificates (implies TLS) |
| `Heartbeat` | 10s | heartbeat interval that detects a dead connection |
| `DialTimeout` | 30s | bound of the TCP connect and the TLS and AMQP handshakes of one dial |
| `ConnectionName` | component name | shown in the management UI |

`ConsumerConfig`:

| Field | Default | Meaning |
|---|---|---|
| `Queue` | required | queue to consume |
| `Topology` | none | exchanges, queues and bindings declared before every subscription |
| `Concurrency` | 1 | messages handled at the same time; above 1 the queue order is lost |
| `Prefetch` | `Concurrency` | messages delivered ahead of the acks (`basic.qos`) |
| `Exclusive` | false | be the queue's only consumer |
| `Tag` | component name | consumer tag prefix; a random suffix makes it unique |
| `Args` | none | `basic.consume` arguments (`x-priority`, …) |

## Options

| Option | Default | Applies to |
|---|---|---|
| `WithName(string)` | `"rabbitmq"` | all; also the default `ConnectionName` |
| `WithLogger(*slog.Logger)` | `slog.Default()` | all |
| `WithConnectRetry(retry.Policy)` | `retry.ConnectPolicy()` | `Connection`: `Start`; its delays also space the redials, which go on until `Shutdown` |
| `WithTLSConfig(*tls.Config)` | none | `Connection` |
| `WithTracerProvider`, `WithMeterProvider`, `WithPropagators` | otel globals | `Publisher`, `Consumer` |
| `WithErrorHandler(ErrorHandler)` | `DefaultErrorHandler` | `Consumer` |
| `WithRedeliveryPolicy(retry.Policy)` | `DefaultRedeliveryPolicy()`: 5 attempts, 100ms doubling to 2s | `Consumer` |

## Lifecycle

| Component | Role | Start | Run | Shutdown | Healthcheck |
|---|---|---|---|---|---|
| `Connection` | Starter, Resource | dials with retry, starts the reconnect loop | — | stops the loop, closes the connection (and every channel on it); the close handshake is bounded by the deadline (`DialTimeout` without one), then the socket is closed | up / `ErrNotConnected` while redialing |
| `Publisher` | Resource | — (the channel opens on the first `Publish`) | — | refuses new messages, waits for outstanding confirms until its deadline (then those `Publish` calls fail with `ErrNotConfirmed`), closes the channel within the deadline | — |
| `Consumer` | Runner | — (its connection connects) | subscribes and consumes; resubscribes after a lost channel; drains on stop | bounds the drain; at the deadline cancels the handlers' context — the channel closes once they return, or with the connection, and the unacked messages are requeued | subscribed / `ErrNotConnected` |

**Registration order:** connections, then publishers, then consumers. `app.App` stops the
consumers first — their handlers may still publish while they drain — then the publishers, then
the connections. A consumer whose connection is shut down under it returns `ErrClosed`.

### Connections

Use **one connection for publishing and another for consuming**. RabbitMQ throttles a connection
that publishes faster than the broker can take (credit flow) and blocks publishing connections
entirely on a memory or disk alarm; on a shared connection the consumers' acks wait as well, so
deliveries stall while the publisher is held. Give the connections distinct names
(`WithName("rabbitmq-publish")`, `WithName("rabbitmq-consume")`): they appear in logs, health
reports and the management UI. Several publishers may share the publishing connection, several
consumers the consuming one — each has its own channel.

While the connection is down:

- `Publish` waits for it until its `ctx` is done; the error then wraps `ErrNotConnected` and the
  context error. Bound it with a context deadline to fail fast.
- Consumers wait, then declare their topology and subscribe again; their `Healthcheck` reports
  `ErrNotConnected` meanwhile. Messages that were delivered but not acked when the connection
  dropped are redelivered by the broker (`Redelivered` set).

While the broker **blocks** a connection (a memory or disk alarm: it stops reading from the socket
of a connection that publishes):

- `Publish` may wait **past its `ctx`** until the alarm clears. amqp091-go opens channels without
  a context (the first `Publish`, and the first after a reconnect, opens the publisher's channel)
  and writes to the socket without a deadline — once the broker stops reading and the socket buffer
  is full, the write blocks, and every other `Publish` on the connection waits behind it. Only the
  wait for a confirm honors `ctx`.
- A consumer subscribing on a blocked connection waits until the alarm clears too; its stop is
  still bounded by `Shutdown`'s deadline. On a connection of their own, consumers are never blocked:
  one more reason to [separate the connections](#connections).
- `Shutdown` of the publisher and of the connection stays bounded by its deadline.
- `Healthcheck` still reports the connection as up; the block is logged (`rabbitmq connection
  blocked by the broker`, then `unblocked`).

A `Connection` whose `Start` failed stays unusable (a second `Start` returns `ErrAlreadyStarted`);
`app.App` shuts the service down after a failed `Start`.

Reconnection is this module's own loop; amqp091-go's experimental automatic recovery is not used.

## Consuming

A `Handler` gets every message; `nil` acks it. Delivery is **at least once**: an ack lost with the
connection makes the broker deliver the message again, so handlers must be idempotent
(`MessageID` helps). The handler's `ctx` is the consumer's work context: it is not cancelled when
the consumer stops — the message in flight finishes — only at `Shutdown`'s deadline; an attempt cut
off there is not a failure: the `ErrorHandler` is not asked, and the message is requeued. A retry
in place passes the same `*Message` again, changes the handler made to it included. `JSON[T]`
decodes the body into a `T`; a body that does not decode is a `retry.Permanent` error.

A handler error goes to the `ErrorHandler`, which returns one of:

| Action | What happens |
|---|---|
| `ErrorActionRetry` | handled again after the `WithRedeliveryPolicy` delay, in the same worker (which holds the message meanwhile); once the attempts are exhausted the `ErrorHandler` is asked again with `ErrRetriesExhausted` joined, and `Retry` counts as `Stop` |
| `ErrorActionRequeue` | `basic.nack` with requeue: back to the queue, delivered again at once — bound it with a quorum queue's `x-delivery-limit` |
| `ErrorActionReject` | `basic.nack` without requeue: to the queue's dead-letter exchange (`x-dead-letter-exchange`), or dropped when it has none |
| `ErrorActionStop` | `Run` returns an error wrapping `ErrStopped`; the message is not acked and is requeued |

`DefaultErrorHandler` retries, then stops (no silent loss). `DeadLetter` retries, then rejects —
at once for `retry.Permanent` errors. A handler or `ErrorHandler` panic is recovered: the handler's
becomes an error, the `ErrorHandler`'s stops the consumer. Failures are logged with the queue,
exchange, routing key, message ID and delivery tag — never the body or the headers.

With `Concurrency` above 1, `Concurrency` workers handle messages at the same time and the queue
order is lost; a `Retry` holds its worker, not the others.

## Publishing

`Publish(ctx, msgs...)` sends the messages in order, on one channel in confirm mode, and returns
once the broker has confirmed each: nil, `ErrNacked` (refused), or `ErrNotConfirmed` (the channel
closed first — the message may or may not have been delivered, so a retry may duplicate it). If
the channel dies in the middle, the rest of the batch is not sent on another one, which would
break the order. It is safe for concurrent use; all calls share the channel. A channel error —
publishing to an exchange that does not exist, say — makes the broker close that channel: every
`Publish` waiting on it fails with `ErrNotConfirmed` joined with the broker's `*amqp.Error`
(`errors.As`), and the next `Publish` opens a new channel. Messages are persistent unless
`Transient` is set. The trace context is added to
the headers; the caller's map is not changed. Unroutable messages are confirmed and dropped by the
broker: bind the queues first, or give the exchange an `alternate-exchange`. `JSONMessage` builds a
message with a JSON body.

## Testing

Unit tests run the components on an in-memory fake of the amqp091 calls (no broker). The tests
against a real RabbitMQ (reconnect after a broker restart, drain, confirms, dead-lettering) live in
[`internal/integration`](../../internal/integration) and need Docker: `make test-integration`.
