package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
)

// instrumentationName is the tracer and meter name.
const instrumentationName = "github.com/eidon-go/core/contrib/rabbitmq"

// defaultExchange is the semantic-convention name of the default exchange.
const defaultExchange = "amq.default"

// telemetry holds the tracer, the propagators and the instruments of a
// publisher or a consumer.
type telemetry struct {
	tracer      trace.Tracer
	propagators propagation.TextMapPropagator
	sent        metric.Int64Counter
	consumed    metric.Int64Counter
	duration    metric.Float64Histogram
}

func newTelemetry(o *options) *telemetry {
	meter := o.meterProvider.Meter(instrumentationName)

	return &telemetry{
		tracer:      o.tracerProvider.Tracer(instrumentationName),
		propagators: o.propagators,
		sent: counter(meter, "messaging.client.sent.messages",
			"Number of messages a producer attempted to send to the broker."),
		consumed: counter(meter, "messaging.client.consumed.messages",
			"Number of messages that were delivered to the application."),
		duration: histogram(meter, "messaging.process.duration", "Duration of processing operation."),
	}
}

// counter creates a message counter. The error of a failed one goes to the
// otel error handler and a no-op takes its place: a provider may return nil
// with its error.
func counter(meter metric.Meter, name, description string) metric.Int64Counter {
	c, err := meter.Int64Counter(name, metric.WithUnit("{message}"), metric.WithDescription(description))
	if err != nil || c == nil {
		otel.Handle(err)

		return noop.Int64Counter{} //nolint:exhaustruct_v5 // the embedded interface is unused
	}

	return c
}

// histogram creates a duration histogram in seconds, like counter.
func histogram(meter metric.Meter, name, description string) metric.Float64Histogram {
	h, err := meter.Float64Histogram(name, metric.WithUnit("s"), metric.WithDescription(description))
	if err != nil || h == nil {
		otel.Handle(err)

		return noop.Float64Histogram{} //nolint:exhaustruct_v5 // the embedded interface is unused
	}

	return h
}

// destination returns the semantic-convention name of an exchange.
func destination(exchange string) string {
	if exchange == "" {
		return defaultExchange
	}

	return exchange
}

// startPublish starts the span of a publish and returns the headers to send:
// msg.Headers with the trace context added.
func (t *telemetry) startPublish(ctx context.Context, msg *Message) (context.Context, trace.Span, amqp.Table) {
	dest := destination(msg.Exchange)
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemRabbitMQ,
		semconv.MessagingOperationTypeSend,
		semconv.MessagingOperationName("publish"),
		semconv.MessagingDestinationName(dest),
		semconv.MessagingMessageBodySize(len(msg.Body)),
	}

	if msg.RoutingKey != "" {
		attrs = append(attrs, semconv.MessagingRabbitMQDestinationRoutingKey(msg.RoutingKey))
	}

	if msg.MessageID != "" {
		attrs = append(attrs, semconv.MessagingMessageID(msg.MessageID))
	}

	ctx, span := t.tracer.Start(ctx, "publish "+dest, //nolint:spancheck // ended by endPublish
		trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(attrs...))

	headers := toTable(msg.Headers, 1)
	t.propagators.Inject(ctx, tableCarrier(headers))

	return ctx, span, headers //nolint:spancheck // ended by endPublish
}

// endPublish ends the span of a publish and, if it was handed to a channel
// (attempted), counts the message as sent: a message that never got that
// far (no connection) is not producer traffic.
func (t *telemetry) endPublish(ctx context.Context, span trace.Span, msg *Message, attempted bool, err error) {
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemRabbitMQ,
		semconv.MessagingOperationName("publish"),
		semconv.MessagingDestinationName(destination(msg.Exchange)),
	}

	if err != nil {
		attrs = append(attrs, semconv.ErrorTypeKey.String(errorType(err)))
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	span.End()

	if attempted {
		t.sent.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}

// startProcess starts the span of the processing of a consumed message, a
// child of the trace context in its headers.
func (t *telemetry) startProcess(ctx context.Context, queue string, msg *Message) (context.Context, trace.Span) {
	ctx = t.propagators.Extract(ctx, tableCarrier(msg.Headers))

	attrs := []attribute.KeyValue{
		semconv.MessagingSystemRabbitMQ,
		semconv.MessagingOperationTypeProcess,
		semconv.MessagingOperationName("process"),
		semconv.MessagingDestinationName(destination(msg.Exchange)),
		semconv.MessagingDestinationSubscriptionName(queue),
		semconv.MessagingMessageBodySize(len(msg.Body)),
	}

	if msg.RoutingKey != "" {
		attrs = append(attrs, semconv.MessagingRabbitMQDestinationRoutingKey(msg.RoutingKey))
	}

	if msg.MessageID != "" {
		attrs = append(attrs, semconv.MessagingMessageID(msg.MessageID))
	}

	return t.tracer.Start(ctx, "process "+queue, //nolint:spancheck // ended by endProcess
		trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(attrs...))
}

// endProcess ends the span of one processing attempt and records its
// duration.
func (t *telemetry) endProcess(
	ctx context.Context,
	span trace.Span,
	queue string,
	msg *Message,
	start time.Time,
	err error,
) {
	attrs := processAttrs(queue, msg)

	if err != nil {
		attrs = append(attrs, semconv.ErrorTypeKey.String(errorType(err)))
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	span.End()
	t.duration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attrs...))
}

// delivered counts a message delivered to the consumer.
func (t *telemetry) delivered(ctx context.Context, queue string, msg *Message) {
	t.consumed.Add(ctx, 1, metric.WithAttributes(processAttrs(queue, msg)...))
}

func processAttrs(queue string, msg *Message) []attribute.KeyValue {
	return []attribute.KeyValue{
		semconv.MessagingSystemRabbitMQ,
		semconv.MessagingOperationName("process"),
		semconv.MessagingDestinationName(destination(msg.Exchange)),
		semconv.MessagingDestinationSubscriptionName(queue),
	}
}

// errorType is the error.type attribute: the module's outcome for its own
// errors, the Go type of the error otherwise (a handler's).
func errorType(err error) string {
	for _, known := range []struct {
		err  error
		name string
	}{
		{ErrNacked, "nacked"},
		{ErrNotConfirmed, "not_confirmed"},
		{ErrNotConnected, "not_connected"},
		{ErrClosed, "closed"},
		{context.DeadlineExceeded, "timeout"},
		{context.Canceled, "canceled"},
	} {
		if errors.Is(err, known.err) {
			return known.name
		}
	}

	return fmt.Sprintf("%T", err)
}

// tableCarrier carries the trace context in AMQP headers. Values that are
// not strings (or []byte) are ignored when reading.
type tableCarrier amqp.Table

func (c tableCarrier) Get(key string) string {
	switch v := c[key].(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return ""
	}
}

func (c tableCarrier) Set(key, value string) { c[key] = value }

func (c tableCarrier) Keys() []string { return slices.Collect(maps.Keys(c)) }

var _ propagation.TextMapCarrier = tableCarrier(nil)
