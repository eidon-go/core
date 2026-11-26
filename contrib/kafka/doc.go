// Package kafka provides Kafka components on franz-go (kgo) that follow the
// lifecycle contract of github.com/eidon-go/core/lifecycle:
//
//   - Consumer — a consumer group member (Starter, Runner): every message
//     goes to a Handler, one at a time in partition order; only handled
//     messages are committed; on stop the message in flight finishes and is
//     committed. Failed messages are retried, skipped or stop the consumer,
//     as the ErrorHandler given to NewConsumer decides (DeadLetter,
//     StopOnFailure).
//   - Producer — synchronous publishing (Publisher), flushed on Shutdown;
//     JSONProducer on top of any Publisher.
//   - Client — a plain franz-go client with the same lifecycle.
//
// Constructors do no network I/O; Start pings the cluster with retry.
// OpenTelemetry tracing and metrics are on by default through kotel. Register
// a producer the handlers use before the consumer, so that it is shut down
// after the consumer has drained.
package kafka
