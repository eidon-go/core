# nats

[![Go Reference](https://pkg.go.dev/badge/github.com/eidon-go/core/contrib/nats.svg)](https://pkg.go.dev/github.com/eidon-go/core/contrib/nats)
[![Release](https://img.shields.io/github/v/tag/eidon-go/core?filter=contrib%2Fnats%2Fv*&sort=semver&label=release)](https://github.com/eidon-go/core/releases)
[![Go Version](https://img.shields.io/github/go-mod/go-version/eidon-go/core?filename=contrib%2Fnats%2Fgo.mod)](./go.mod)
[![codecov](https://codecov.io/gh/eidon-go/core/graph/badge.svg?flag=contrib-nats)](https://codecov.io/gh/eidon-go/core?flags%5B0%5D=contrib-nats)

NATS on [nats.go](https://github.com/nats-io/nats.go) as [`lifecycle`](../../lifecycle/README.md)
components:

- `Connection` — one client connection: `Start` connects with retry, nats.go then reconnects by
  itself (and re-creates the subscriptions) until `Shutdown` drains and closes it.
- `Publisher` — `Publish` (core NATS, at most once), `PublishJetStream` (stored by a stream and
  acknowledged; message IDs for safe retries) and `Request` (request-reply). `Shutdown` flushes.
- `Subscriber` — a core NATS subscription, optionally in a queue group, that hands every message to
  a `Handler`; the handler answers a request with `Message.Respond`. On stop the subscription is
  drained: the messages the client already holds are handled.
- `Consumer` — a durable JetStream pull consumer, created or updated by `Run`, that hands every
  message to a `Handler` and acks what was handled. On stop the messages in flight finish and are
  acked, the prefetched ones are naked (delivered again at once, to another instance). A failed
  message is retried in place, redelivered later, terminated or stops the consumer — never dropped
  silently by default.

OpenTelemetry (publish, request, respond and process spans with trace context in the message
headers, messaging metrics) is on by default.

```sh
go get github.com/eidon-go/core/contrib/nats
```

The package is named `nats`, like nats.go's; import one of them under another name
(`natsgo "github.com/nats-io/nats.go"`).

```go
conn, err := nats.NewConnection(nats.Config{URL: "nats://nats:4222", CredsFile: "/etc/nats/billing.creds"})
if err != nil {
    return err
}

publisher, err := nats.NewPublisher(conn)
if err != nil {
    return err
}

consumer, err := nats.NewConsumer(conn, nats.ConsumerConfig{
    Stream:         "ORDERS",
    Durable:        "billing",
    FilterSubjects: []string{"orders.created"},
    Concurrency:    4,
}, nats.JSON(func(ctx context.Context, order Order, _ *nats.Message) error {
    msg, err := nats.JSONMessage("invoices.requested", Invoice{OrderID: order.ID})
    if err != nil {
        return err
    }

    // A stream must capture invoices.requested (created beforehand, like an INVOICES stream
    // on "invoices.>"); otherwise PublishJetStream fails with jetstream.ErrNoStreamResponse.
    _, err = publisher.PublishJetStream(ctx, msg, jetstream.WithMsgID("invoice-"+order.ID))

    return err
}), nats.Discard, // the error strategy is required: see Errors
    nats.WithStream(jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}}))
if err != nil {
    return err
}

status, err := nats.NewSubscriber(conn, nats.SubscriberConfig{Subject: "billing.status", Queue: "billing"},
    nats.HandlerFunc(func(ctx context.Context, msg *nats.Message) error {
        return msg.Respond(ctx, &nats.Message{Data: []byte("ok")})
    }), nats.Discard)
if err != nil {
    return err
}

a := app.New()
a.Add(conn)      // connects in Start; drained and closed last
a.Add(publisher) // shut down after the runners have drained
a.Add(consumer)  // runners: stopped first
a.Add(status)

return a.Run(ctx)
```

## Configuration

`Config` is a plain struct (`json`/`yaml` tags for convenience); `Validate` reports every problem at
once, wrapping `ErrInvalidConfig`.

| Field | Default | Meaning |
|---|---|---|
| `URL` | `nats://127.0.0.1:4222` | server URL, or several separated by commas; schemes `nats`, `tls`, `ws`, `wss` (`host:port` means `nats`); no credentials in it |
| `User`, `Password` | none | user and password |
| `Token` | none | token |
| `CredsFile` | none | `.creds` file (user JWT and NKey seed) |
| `NKeySeedFile` | none | NKey seed file, read by `Start` |
| `TLS` | false | TLS (also implied by `tls://`, `wss://`); `WithTLSConfig` for certificates (implies TLS) |
| `ConnectionName` | component name | shown in the server's monitoring |
| `DialTimeout` | 2s | bound of the connect to one server |
| `ReconnectWait` | 2s | pause between the reconnect attempts to one server; the client reconnects until `Shutdown` |
| `PingInterval`, `MaxPingsOutstanding` | 2m, 2 | pings that detect a dead connection |
| `ReconnectBufferSize` | 8 MiB | bytes published while reconnecting that are kept and sent once reconnected; negative disables the buffer |
| `DrainTimeout` | 30s | upper bound of `Connection.Shutdown`'s drain, whatever its deadline (an earlier deadline cuts it shorter); also bounds `Publisher.Shutdown`'s flush when its ctx has no deadline |

At most one of `User`, `Token`, `CredsFile` and `NKeySeedFile` may be set.

`SubscriberConfig`:

| Field | Default | Meaning |
|---|---|---|
| `Subject` | required | subject, wildcards allowed (`orders.*`, `orders.>`) |
| `Queue` | none | queue group: its subscribers share the messages |
| `Concurrency` | 1 | messages handled at the same time; above 1 the order is lost |
| `PendingMessages`, `PendingBytes` | 512Ki, 64 MiB | messages the client holds for the handlers; beyond them it drops (a slow consumer, logged); negative: no limit. The client also holds at most the connection's sync queue length (64Ki messages; raise it with `WithConnOptions(nats.SyncQueueLen(n))`) |

`ConsumerConfig`:

| Field | Default | Meaning |
|---|---|---|
| `Stream` | required | stream to consume; must exist unless `WithStream` creates it |
| `Durable` | required | durable consumer name; the instances that share it share the messages |
| `FilterSubjects` | all | subjects of the stream to consume |
| `AckWait` | server's, 30s | time before an unacked message is delivered again; kept from expiring while the message is handled |
| `MaxDeliver` | server's, no limit | deliveries of a message at most; negative: no limit |
| `MaxAckPending` | server's, 1000 | delivered messages waiting for their ack, all instances together; keep it ≥ (`Concurrency` + `Prefetch`) × instances; negative: no limit |
| `Concurrency` | 1 | messages handled at the same time; above 1 the order is lost |
| `Prefetch` | `Concurrency` | messages pulled ahead of the handlers |
| `MaxProcessingTime` | 10 × `AckWait` (300s by default) | bound of one message's handling, retries included; past it the handler's `ctx` is cancelled, the in-progress acks stop and the message is naked; negative: no limit |

## Options

| Option | Default | Applies to |
|---|---|---|
| `WithName(string)` | `"nats"` | all; also the default `ConnectionName` |
| `WithLogger(*slog.Logger)` | `slog.Default()` | all |
| `WithConnectRetry(retry.Policy)` | `retry.ConnectPolicy()` | `Connection.Start`; once connected nats.go reconnects by itself |
| `WithTLSConfig(*tls.Config)` | none | `Connection` |
| `WithConnOptions(...nats.Option)` | none | `Connection`: any nats.go option, applied last; handlers set by them run after the module's own |
| `WithTracerProvider`, `WithMeterProvider`, `WithPropagators` | otel globals | `Publisher`, `Subscriber`, `Consumer` |
| `WithTelemetry(Telemetry)` | — | `Publisher`, `Subscriber`, `Consumer`: all three from the `otel` module's `Provider` when it is not global |
| `WithRedeliveryPolicy(retry.Policy)` | `DefaultRedeliveryPolicy()`: 5 attempts, 100ms doubling to 2s | `Subscriber`, `Consumer` (also the delay of a `Consumer`'s `Requeue`) |
| `WithStream(jetstream.StreamConfig)` | none: the stream must exist | `Consumer`: create or update the stream before the consumer |
| `WithConsumerConfig(func(*jetstream.ConsumerConfig))` | none | `Consumer`: settings `ConsumerConfig` lacks (`DeliverPolicy`, `BackOff`, …) |

## Lifecycle

| Component | Role | Start | Run | Shutdown | Healthcheck |
|---|---|---|---|---|---|
| `Connection` | Starter, Resource | connects with retry | — | drains (subscriptions left on it, buffered publishes) and closes; bounded by the deadline and `DrainTimeout`, then closed at once | connected / `ErrNotConnected` while reconnecting |
| `Publisher` | Resource | — (its connection connects) | — | refuses new calls, waits for the ones in flight, flushes; bounded by the deadline | — |
| `Subscriber` | Runner | — | subscribes and handles; drains on stop | bounds the drain; at the deadline cancels the handlers' context, the messages not handled are lost | subscribed / `ErrNotRunning` |
| `Consumer` | Runner | — | creates the stream (`WithStream`) and the consumer, pulls and handles; resubscribes with backoff after a failed subscription; on stop finishes the messages in flight and naks the prefetched ones | bounds the stop; at the deadline cancels the handlers' context and naks what was not acked | subscribed / `ErrNotConnected` while (re)subscribing |

Add these components to `app.App` without `app.WithReadiness()` unless the service cannot answer
requests without NATS: a reconnect should not take every instance out of the load balancer. Their
checks still appear in `HealthReport`.

**Registration order:** the connection, then publishers, then subscribers and consumers. `app.App`
stops the runners first — their handlers may still publish while they drain — then the publishers,
then the connection. A runner whose connection is closed under it returns `ErrClosed`.

One connection is usually enough: NATS has no per-connection flow control that would make
publishers hold up consumers. Use more (each with its own `WithName`) to separate credentials or
accounts.

While the client is reconnecting, `Publish` buffers up to `ReconnectBufferSize` and fails beyond
it; `Request` waits for its response until its ctx is done, `PublishJetStream` for its
acknowledgement until its ctx deadline (without one, jetstream's 5s default); subscriptions wait
and resume after the reconnect. A consumer resumes its pulls after a short outage; after a longer
one (missed heartbeats, about 30s) it subscribes again. `Connection.Healthcheck` reports
`ErrNotConnected` meanwhile.

## Core NATS: Subscriber and request-reply

Core NATS delivers **at most once** to the subscribers listening at the time: nothing is stored or
redelivered. Use a `Consumer` for messages that must not be lost.

`Publisher.Request(ctx, msg)` sends a request and returns the first response; it fails with
`ErrNoResponders` at once when nobody listens. A `Subscriber`'s handler answers with
`msg.Respond(ctx, resp)` (`ErrNoReply` for a message without a reply subject). Put responders in a
queue group so that each request is answered once.

A failed handler's message goes to the `ErrorHandler` (below); `Requeue` and `Reject` drop it —
core NATS cannot redeliver. On stop the subscription is drained: the server sends no more, the
messages the client already holds are handled, then `Run` returns.

## JetStream: Consumer

A `Handler` gets every message; `nil` acks it. While a message is handled — its attempts and the
retry delays between them — the consumer tells the server every `AckWait/2` that it is in progress,
so a slow handler does not get the message delivered again elsewhere — up to `MaxProcessingTime`.
Past it the handler's `ctx` is cancelled and the message is naked; a handler that ignores its `ctx`
no longer keeps the message in progress either, so the server delivers it again once `AckWait`
expires and `MaxDeliver` still applies: a hung handler cannot hold a message forever. Delivery is still **at least
once**: an ack lost with the connection, or an instance that dies mid-handling, makes the server
deliver the message again, so handlers must be idempotent (`msg.Metadata` has the stream sequence and the delivery count). The
handler's `ctx` is the consumer's work context: it is not cancelled when the consumer stops — the
message in flight finishes — only at `Shutdown`'s deadline; an attempt cut off there is not a
failure: the `ErrorHandler` is not asked, and the message is naked. A retry in place passes the same
`*Message` again, changes the handler made to it included. `JSON[T]` decodes the data into a `T`;
data that does not decode is a `retry.Permanent` error.

A handler error goes to the `ErrorHandler` passed to `NewConsumer` / `NewSubscriber`, which returns
one of:

| Action | Consumer | Subscriber |
|---|---|---|
| `ErrorActionRetry` | handled again after the `WithRedeliveryPolicy` delay, in the same worker; the message is kept in progress, so `AckWait` does not expire. Once the attempts are exhausted the `ErrorHandler` is asked again with `ErrRetriesExhausted` joined, and `Retry` counts as `Stop` | the same |
| `ErrorActionRequeue` | nak with the redelivery policy's delay for its delivery count: the server delivers it again later, to any instance, until `MaxDeliver` | dropped (logged) |
| `ErrorActionReject` | terminated (`AckTerm`): never delivered again; the server publishes a `MSG_TERMINATED` advisory (`$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.<stream>.<consumer>`) a dead-letter flow can capture | dropped (logged) |
| `ErrorActionStop` | `Run` returns an error wrapping `ErrStopped`; the message is naked, delivered again at once | `Run` returns an error wrapping `ErrStopped` |

There is no default strategy: choose it for production deliberately. `Discard` (recommended)
retries, then rejects — at once for `retry.Permanent` errors; capture the `MSG_TERMINATED`
advisories to keep the rejected messages. `StopOnFailure` retries, then stops: no message is lost,
but with `app.App` one message that can never be handled stops the whole service, and a `Consumer`
meets it again after every restart — a crash loop. A handler or `ErrorHandler` panic is recovered: the handler's
becomes an error, the `ErrorHandler`'s stops the component. Failures are logged with the subject
(and the stream, consumer, stream sequence and delivery count) — never the data or the headers.

The consumer is created with `AckPolicy: explicit` from `ConsumerConfig` and `WithConsumerConfig`
on every subscription, so a changed configuration is applied on the next start. If the
subscription fails — the consumer was deleted, the stream is gone, the server lost leadership — the
consumer subscribes again with a backoff (at once after a subscription that lasted 5s or more);
`Healthcheck` reports `ErrNotConnected` meanwhile. A `MaxRequestBatch` below `Prefetch` (or a
`MaxRequestExpires` below 30s) set with `WithConsumerConfig` makes the server refuse the pulls: the
consumer stays subscribed but gets nothing.

## Publishing

- `Publish(ctx, msgs...)` publishes with core NATS, in order, and returns once the client has taken
  the messages; the client sends them in the background, and `Publisher.Shutdown` flushes them.
- `PublishJetStream(ctx, msg, opts...)` returns the stream's acknowledgement (`*jetstream.PubAck`)
  once the message is stored. `jetstream.WithMsgID` makes the stream drop a duplicate within its
  duplicate window — set it to retry a publish safely; `jetstream.WithExpectStream`,
  `WithExpectLastSequence` and the like add optimistic concurrency checks. It fails with
  `jetstream.ErrNoStreamResponse` when no stream captures the subject.
- `Request(ctx, msg)` — see request-reply above.

The trace context is added to the headers; the caller's header map is not changed. `JSONMessage`
builds a message with JSON data and the `Content-Type` header.

## Beyond this module

`Connection.Conn()` returns the `*nats.Conn` and `Connection.JetStream()` a JetStream context for
what the module does not wrap: key-value and object stores, stream administration, statistics. Do
not close or drain the connection yourself; `Shutdown` does.

## Testing

The tests run against an embedded nats-server with JetStream (no Docker): reconnect after a server
restart, drain on stop, acks, naks and terminations, request-reply and trace propagation.
