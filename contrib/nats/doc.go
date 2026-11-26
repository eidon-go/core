// Package nats provides NATS components on nats.go that follow the
// lifecycle contract of github.com/eidon-go/core/lifecycle:
//
//   - Connection — one client connection (Starter, Resource,
//     Healthchecker): Start connects with retry; nats.go then reconnects by
//     itself until Shutdown drains and closes the connection.
//   - Publisher — publishing (Resource): Publish (core NATS, at most once),
//     PublishJetStream (stored by a stream, acknowledged) and Request
//     (request-reply). Shutdown flushes.
//   - Subscriber — a core NATS subscription (Runner), optionally in a queue
//     group: every message goes to a Handler, which may answer a request
//     with Message.Respond. On stop the subscription is drained.
//   - Consumer — a durable JetStream pull consumer (Runner), created or
//     updated by Run: every message goes to a Handler and is acked once
//     handled; a failed one is retried, redelivered later, terminated or
//     stops the consumer, as the ErrorHandler given to NewConsumer decides
//     (Discard, StopOnFailure). On stop the messages in flight finish and
//     are acked, the prefetched ones are naked.
//
// Register the components in this order: the connection, then publishers,
// then subscribers and consumers. app.App stops the subscribers and
// consumers first (they drain, and their handlers may still publish), then
// the publishers, then the connection.
//
// Constructors do no I/O. OpenTelemetry tracing and metrics are on by
// default; trace context travels in the message headers.
//
// The package name is the same as nats.go's; import one of them under
// another name (natsgo "github.com/nats-io/nats.go").
package nats
