// Package rabbitmq provides RabbitMQ (AMQP 0-9-1) components on amqp091-go
// that follow the lifecycle contract of github.com/eidon-go/core/lifecycle:
//
//   - Connection — one AMQP connection (Starter, Resource, Healthchecker):
//     Start dials with retry, then a background loop redials whenever the
//     connection is lost, until Shutdown closes it. Publishers and consumers
//     open their channels on it and wait for it while it is down.
//   - Publisher — publishing with publisher confirms (Resource): Publish
//     returns once the broker has confirmed every message; Shutdown waits
//     for the confirms outstanding.
//   - Consumer — a queue consumer (Runner): every message goes to a Handler,
//     with Concurrency workers; a handled message is acked, a failed one is
//     retried, requeued, rejected (to the queue's dead-letter exchange) or
//     stops the consumer, as the ErrorHandler given to NewConsumer decides
//     (DeadLetter, StopOnFailure). On stop the broker stops delivering, the
//     messages in flight finish and are acked, the prefetched ones are
//     requeued.
//
// Use one Connection for publishers and another for consumers: the broker
// blocks a connection that publishes faster than it can take (flow control,
// resource alarms), and acks on a blocked connection wait as well.
//
// Register the components in this order: connections, then publishers, then
// consumers. app.App stops the consumers first (they drain, and their
// handlers may still publish), then the publishers, then the connections.
//
// Constructors do no network I/O. OpenTelemetry tracing and metrics are on
// by default; trace context travels in the message headers.
package rabbitmq
