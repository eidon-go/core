# kafka

[![Go Reference](https://pkg.go.dev/badge/github.com/eidon-go/core/contrib/kafka.svg)](https://pkg.go.dev/github.com/eidon-go/core/contrib/kafka)
[![Release](https://img.shields.io/github/v/tag/eidon-go/core?filter=contrib%2Fkafka%2Fv*&sort=semver&label=release)](https://github.com/eidon-go/core/releases)
[![Go Version](https://img.shields.io/github/go-mod/go-version/eidon-go/core?filename=contrib%2Fkafka%2Fgo.mod)](./go.mod)
[![codecov](https://codecov.io/gh/eidon-go/core/graph/badge.svg?flag=contrib-kafka)](https://codecov.io/gh/eidon-go/core?flags%5B0%5D=contrib-kafka)

Kafka on [franz-go](https://github.com/twmb/franz-go) as [`lifecycle`](../../lifecycle/README.md)
components:

- `Consumer` — a consumer group member that hands every message to a `Handler`, one at a time in
  partition order, and commits only what was handled. On stop it takes no new message, lets the one
  in flight finish and commits it. A failed message is retried in place, skipped (after a
  dead-letter topic, say) or stops the consumer — never dropped silently by default.
- `Producer` — synchronous publishing, flushed on shutdown; `JSONProducer[T]` on top of it.
- `Client` — a plain franz-go client with the same lifecycle, for admin requests and the rest.

OpenTelemetry (publish, receive and process spans with context propagation through record
headers, client metrics) is on by default through [kotel](https://pkg.go.dev/github.com/twmb/franz-go/plugin/kotel).

```sh
go get github.com/eidon-go/core/contrib/kafka
```

```go
cfg := kafka.Config{Brokers: []string{"kafka-1:9092", "kafka-2:9092"}}

producer, err := kafka.NewProducer(cfg)
if err != nil {
    return err
}

consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
    Config: cfg,
    Group:  "billing",
    Topics: []string{"orders"},
}, kafka.HandlerFunc(func(ctx context.Context, msg *kafka.Message) error {
    var o Order
    if err := json.Unmarshal(msg.Value, &o); err != nil {
        return retry.Permanent(err) // not worth retrying
    }

    return billing.Charge(ctx, o) // idempotent: delivery is at least once
}), kafka.WithErrorHandler(kafka.DeadLetter(producer, "orders.dlt")))
if err != nil {
    return err
}

a := app.New()
a.Add(producer) // a Resource: shut down after the consumer has drained
a.Add(consumer) // a Runner
return a.Run(ctx)
```

## Configuration

`Config` (Client, Producer) and `ConsumerConfig` (Consumer, embeds `Config`) are plain structs;
`Validate` reports every problem at once, each wrapping `ErrInvalidConfig`.

| Field | Meaning | Default |
|---|---|---|
| `Brokers` | seed brokers, `host:port` | required |
| `ClientID` | client ID sent to the brokers | franz-go's `kgo` |
| `Group` | consumer group | required (consumer) |
| `Topics` | topics to consume | required (consumer) |
| `Offset` | where a partition without a committed offset starts: `OffsetNewest` or `OffsetOldest` | `OffsetNewest` |
| `MaxPollRecords` | records taken per poll; rebalances wait while they are handled | `DefaultMaxPollRecords` (500) |

TLS goes through `WithTLSConfig`; SASL, compression, partitioners, timeouts and every other
franz-go setting through `WithKgoOpts`.

## Options

| Option | Applies to | Default |
|---|---|---|
| `WithName(string)` | all | `DefaultName` (`"kafka"`) |
| `WithLogger(*slog.Logger)` | all | `slog.Default()` |
| `WithConnectRetry(retry.Policy)` | all: the ping in `Start` | `retry.ConnectPolicy()`, logged |
| `WithTLSConfig(*tls.Config)` | all | plaintext |
| `WithTracerProvider` / `WithMeterProvider` / `WithPropagators` | all | the otel globals at construction time |
| `WithTelemetry` (all three from the `otel` module's `Provider` when it is not global) | all | — |
| `WithKgoOpts(...kgo.Opt)` | all, applied last | — |
| `WithErrorHandler(ErrorHandler)` | consumer | `DefaultErrorHandler` |
| `WithRedeliveryPolicy(retry.Policy)` | consumer | `DefaultRedeliveryPolicy()`: 5 attempts, 100ms doubling to 2s |

Options that change how the consumer consumes or commits (group and topic settings, commit and
rebalance callbacks) passed through `WithKgoOpts` break its guarantees.

## Lifecycle

| Component | `Start` | `Run` | `Shutdown` | `Healthcheck` |
|---|---|---|---|---|
| `Consumer` | creates the client (joins the group), pings with retry | consumes until stopped; returns `nil`, an error wrapping `ErrStopped` when the error handler stops it, or one wrapping `kgo.ErrMaxDecompress` when a partition can no longer be fetched (below) | stops `Run` (see below), then leaves the group and closes the client | `nil` while consuming, `ErrNotRunning` otherwise |
| `Producer` | creates the client, pings with retry | — | flushes buffered records until its ctx is done, then closes | pings |
| `Client` | creates the client, pings with retry | — | closes | pings |

`New*` do no network I/O: the franz-go client — whose background loops talk to the brokers — is
created in `Start`, so `Kgo()` is `nil` before it and `Publish` returns `ErrNotStarted`. A
`Shutdown` during `Start` ends its ping retries at once (`Start` returns `ErrClosed`). `Shutdown`
is idempotent and safe without `Start`; `Start` after `Shutdown` returns `ErrClosed`, a second
`Start` `ErrAlreadyStarted`. When the ctx of `Shutdown` is done, it returns an error wrapping
`ctx.Err()` and the close goes on in the background; a later `Shutdown` waits for it.

The consumer is built on [`lifecycle.Worker`](../../lifecycle/README.md#writing-a-component).
Following the [lifecycle contract](../../lifecycle/README.md#contract), the cancelled ctx of `Run`
and `Shutdown` are one stop:

1. The poll returns at once; records polled but not yet handled are not committed, so they are
   read again from the committed offset — with no commit yet, from `Offset`, so with
   `OffsetNewest` they are not.
2. The message in flight finishes: the handler's ctx is not derived from `Run`'s, it is cancelled
   only at the deadline of `Shutdown`. A stop during a redelivery delay ends the wait instead (the
   message is not committed).
3. The handled messages are committed, `Run` returns `nil`, and `Shutdown` leaves the group.

At the `Shutdown` deadline the handler's ctx is cancelled, `Run` returns and the client is closed
in the background; a handler that ignores its ctx keeps running until it returns by itself.

**Registration order:** a producer the handlers publish with (or a dead-letter producer) is a
plain `Resource`: register it **before** the consumer, so that `app.App` shuts it down after the
consumer has drained.

## Consuming

A `Handler` gets `*Message` (`Topic`, `Partition`, `Offset`, `Key`, `Value`, `Headers`,
`Timestamp`), one at a time, in partition order — partitions are handled sequentially, one
consumer goroutine. Its ctx carries the message's process span (a child of the publisher's trace)
and the values of `Run`'s ctx. A panic in the handler is recovered and treated as an error.

**Commits.** A message is committed once it, and every earlier message of its partition, is
handled or skipped. Offsets are committed every few seconds, when partitions are revoked in a
rebalance and when the consumer stops. Delivery is **at least once**: after a crash, a forced stop
or a lost partition, messages handled since the last commit come again — handlers must be
idempotent. Rebalances wait while a poll's records are handled (franz-go's
`BlockRebalanceOnPoll`), so no offset is committed for a partition the member no longer owns; keep
`MaxPollRecords` × handling time (with retries) well under the group's rebalance timeout
(franz-go default 60s).

**Errors.** When the handler returns an error, the `ErrorHandler` chooses an `ErrorAction`:

| Action | Effect |
|---|---|
| `ErrorActionRetry` | handle the same message again after the delay of the redelivery policy; the partition waits (order is kept). Once the policy allows no more attempts, the error handler is asked again with `ErrRetriesExhausted` joined to the error, and `Retry` then means `Stop` |
| `ErrorActionSkip` | commit the message as handled and go on |
| `ErrorActionStop` | `Run` returns an error wrapping `ErrStopped` and the handler's error, and `app.App` stops the service; the message is not committed (the earlier ones are), so it comes again after the restart |

Built-in handlers, following the usual practice of retrying transient errors, never retrying
poison messages and recovering once the attempts are exhausted:

- `DefaultErrorHandler` (the default): retry; stop once the attempts are exhausted or at once for
  an error marked with `retry.Permanent`. No message is dropped silently.
- `DeadLetter(publisher, topic)`: retry; once the attempts are exhausted, or at once for
  `retry.Permanent`, publish the message to `topic` and skip it. The dead letter keeps key, value
  and headers and adds `dlt-original-topic`, `dlt-original-partition`, `dlt-original-offset` and
  `dlt-error`. If that publish fails, the consumer stops.

Write your own `ErrorHandler` for anything else, e.g. skip validation errors and retry the rest
forever with `WithRedeliveryPolicy(retry.Policy{MaxAttempts: retry.Forever, ...})`.

**Fetch errors** (a broker down, a leader moving) are logged and the poll is retried with a
growing pause. One is fatal: a batch that would decompress past franz-go's bound
(`kgo.MaxDecompressBatchBytes`, 1 GiB by default) is never skipped, and franz-go stops fetching
its partition — `Run` then returns an error wrapping `kgo.ErrMaxDecompress` rather than consume
the other partitions while that one stalls unnoticed.

## Producing

`Producer.Publish(ctx, msgs...)` sends messages and waits until each is acknowledged;
`Produce(ctx, topic, key, value)` sends one. The trace context of `ctx` goes into the record
headers. Cancelling `ctx` is best effort: it ends the wait for records not sent yet, but a record
already in flight (the producer is idempotent) is waited for until the broker answers or the
delivery timeout passes, so `Publish` may return after `ctx` is done. An error does not mean the
broker rejected the record — it may have been written, and a retry may duplicate it. Depend on the `Publisher` interface to swap in a fake in tests.
`NewJSONProducer[T](publisher, topic).Produce(ctx, key, value)` encodes values as JSON with a
`content-type: application/json` header.

## Client

`Client.Kgo()` returns the franz-go client for everything else — admin requests through
`kadm.NewClient(c.Kgo())`, transactions, direct partition consumption. The client stays owned by
the component: do not close it, call `Shutdown`. `Producer.Kgo()` does the same for the producer.

## Testing

The module's tests run against franz-go's in-memory cluster
([`kfake`](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kfake)), which serves real Kafka
protocol on a local port — use it for your handlers' tests too:

```go
cluster, err := kfake.NewCluster(kfake.SeedTopics(1, "orders"))
// ...
cfg := kafka.Config{Brokers: cluster.ListenAddrs()}
```
