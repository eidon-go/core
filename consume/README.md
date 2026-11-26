# consume

Runs a message handler the way every message consumer of the integration modules does (`kafka`,
`nats`, `rabbitmq`): attempt after attempt as an error handler decides, a redelivery policy between
them, panics recovered, failures logged without the message's content — and transient errors of a
dependency waited out without using up the message's attempts. Application code rarely uses it
directly: it marks errors with `consume.Transient`, and the modules do the rest.

## In a handler

```go
func (h *Billing) Handle(ctx context.Context, msg *kafka.Message) error {
    var o Order
    if err := json.Unmarshal(msg.Value, &o); err != nil {
        return retry.Permanent(err) // the message is bad: no attempt can help
    }

    if err := h.db.Charge(ctx, o); err != nil {
        if isConnectionError(err) {
            return consume.Transient(err) // the database is down, not the message
        }

        return err // counted as an attempt; the ErrorHandler decides
    }

    return nil
}
```

A **transient** error is waited out with the transient policy (`consume.TransientPolicy()`: 1s
doubling up to 30s, until the stop) and the same attempt is made again: the error handler is not
asked and no attempt is counted, so a dependency outage neither dead-letters every message nor
stops the consumer. Once the policy's `MaxAttempts` transient failures in a row are reached, the
error counts as a failed attempt. The modules take the policy with `WithTransientPolicy`.

## In a broker module

```go
proc := consume.Processor[*Message]{
    Handle:     handler.Handle,
    Observe:    observe, // opens the process span around every attempt
    OnError:    func(ctx context.Context, m *Message, err error) consume.Action { ... },
    Redelivery: redelivery,
    Transient:  consume.TransientPolicy(),
    Exhausted:  ErrRetriesExhausted, // the module's own sentinel
    Logger:     logger,
    Name:       "nats",
}

res := proc.Process(stop, work, msg, consume.Delivery{Attrs: attrs})
switch res.Outcome {
case consume.Handled:              // ack
case consume.Requeued:             // give it back to the broker
case consume.Rejected:             // skip, terminate or dead-letter it
case consume.Stopped:              // stop the consumer with res.Err
case consume.Paused:               // pause the message's source, report unhealthy
case consume.Abandoned, consume.Yielded: // not settled: the broker delivers it again
}
```

| Field | |
|---|---|
| `Handle` | one attempt; a panic becomes the attempt's error (`"<Name>: handler panicked: …"`) |
| `Observe` | optional wrapper of every attempt (spans, metrics); `call` runs `Handle` with panics recovered |
| `OnError` | decides `Retry`, `Requeue`, `Reject`, `Stop` or `Pause` for a failed attempt; a panic in it counts as `Stop` |
| `Redelivery` | attempts and delays of `Retry`; `Retry` with no attempt left counts as `Stop`, and `OnError` gets `Exhausted` joined to the error once none is left |
| `Transient` | waits of a `Transient` error; the zero value counts it as an ordinary failure at once |
| `Exhausted` | the sentinel joined when attempts run out (default `consume.ErrRetriesExhausted`) |
| `Logger`, `Name` | failures are logged at Error, transient waits at Warn, as `"<Name> message handling failed"` with `Delivery.Attrs` |

`work` is the context of the attempts; an attempt it cuts off (Shutdown's deadline) is `Abandoned`
without asking `OnError` — the message did not fail. `stop` ends a wait at once (`Abandoned`).
`Delivery.Deadline` bounds the waits: a wait that would end past it is not started, and `Process`
returns `Yielded` with `Result.Attempts`; pass that back as `Delivery.Attempts` when the message
comes again, and its attempts go on where they were (Kafka uses this to yield to a rebalance).

`Process` may be called concurrently on one `Processor`.

### Pauses that end by themselves

`PauseBackoff` gives the length of a `Paused` outcome's pause for a module's `WithPauseFor`:

```go
pauses := consume.NewPauseBackoff(pauseFor, 15*time.Minute) // pauseFor 0: only Resume ends a pause

length := pauses.Begin(time.Now()) // on Paused; handlers that pause together share one pause
// ... hold the source; wait pauses.Remaining(time.Now()) or a Resume
pauses.End(time.Now(), expired)    // expired: it ended by itself
```

A pause lasts `pauseFor`; one that begins within the length of the previous pause after that
ended by itself lasts twice as long, up to the ceiling. A pause ended by hand does not lengthen
the next one.
