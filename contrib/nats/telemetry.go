package nats

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	natsgo "github.com/nats-io/nats.go"
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
const instrumentationName = "github.com/eidon-go/core/contrib/nats"

// temporaryDestination is the semantic-convention name of a reply subject.
const temporaryDestination = "(temporary)"

// Operation names of the spans and metrics.
const (
	opPublish = "publish"
	opRequest = "request"
	opRespond = "respond"
	opProcess = "process"
)

// messagingSystem is the messaging.system attribute; the semantic
// conventions have no well-known value for NATS.
var messagingSystem = semconv.MessagingSystemKey.String("nats") //nolint:gochecknoglobals // constant attribute

// telemetry holds the tracer, the propagators and the instruments of a
// component.
type telemetry struct {
	tracer      trace.Tracer
	propagators propagation.TextMapPropagator
	sent        metric.Int64Counter
	consumed    metric.Int64Counter
	duration    metric.Float64Histogram
	paused      metric.Int64Gauge
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
		paused: gauge(
			meter,
			"nats.consumer.paused",
			"Whether a consumer is paused by ErrorActionPause (1) or not (0).",
		),
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

// gauge creates a consumer state gauge, like counter.
func gauge(meter metric.Meter, name, description string) metric.Int64Gauge {
	g, err := meter.Int64Gauge(name, metric.WithUnit("{consumer}"), metric.WithDescription(description))
	if err != nil || g == nil {
		otel.Handle(err)

		return noop.Int64Gauge{} //nolint:exhaustruct_v5 // the embedded interface is unused
	}

	return g
}

// setPaused records whether the consumer of durable on stream is paused.
func (t *telemetry) setPaused(ctx context.Context, stream, durable string, paused bool) {
	var v int64
	if paused {
		v = 1
	}

	t.paused.Record(ctx, v, metric.WithAttributes(
		messagingSystem,
		semconv.MessagingDestinationName(stream),
		semconv.MessagingConsumerGroupName(durable),
	))
}

// publishDestination returns the destination name of a send operation: the
// subject, or "(temporary)" for a response to a reply subject.
func publishDestination(operation string, msg *Message) string {
	if operation == opRespond {
		return temporaryDestination
	}

	return msg.Subject
}

// startPublish starts the span of a send operation (publish, request,
// respond) and returns the headers to send: msg.Header with the trace
// context added.
func (t *telemetry) startPublish(
	ctx context.Context,
	operation string,
	msg *Message,
) (context.Context, trace.Span, natsgo.Header) {
	dest := publishDestination(operation, msg)
	attrs := []attribute.KeyValue{
		messagingSystem,
		semconv.MessagingOperationTypeSend,
		semconv.MessagingOperationName(operation),
		semconv.MessagingDestinationName(dest),
		semconv.MessagingMessageBodySize(len(msg.Data)),
	}

	if operation == opRespond {
		attrs = append(attrs, semconv.MessagingDestinationTemporary(true))
	}

	kind := trace.SpanKindProducer
	if operation == opRequest {
		kind = trace.SpanKindClient
	}

	ctx, span := t.tracer.Start(ctx, operation+" "+dest,
		trace.WithSpanKind(kind), trace.WithAttributes(attrs...))

	header := maps.Clone(msg.Header)
	if header == nil {
		header = natsgo.Header{}
	}

	t.propagators.Inject(ctx, headerCarrier(header))

	return ctx, span, header
}

// endPublish ends the span of a send operation and counts the message as
// sent. The metric has no destination attribute: subjects often carry IDs,
// and a metric series per subject would grow without bound (the span has
// the subject).
func (t *telemetry) endPublish(ctx context.Context, span trace.Span, operation string, err error) {
	attrs := []attribute.KeyValue{
		messagingSystem,
		semconv.MessagingOperationName(operation),
	}

	if err != nil {
		errType := semconv.ErrorTypeKey.String(errorType(err))
		attrs = append(attrs, errType)
		span.SetAttributes(errType)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	span.End()
	t.sent.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// subscription describes what a Subscriber or a Consumer receives from, for
// the process spans and metrics.
type subscription struct {
	name  string // span name suffix: the subject pattern or the stream
	attrs []attribute.KeyValue
}

// startProcess starts the span of the processing of a received message, a
// child of the trace context in its headers.
func (t *telemetry) startProcess(ctx context.Context, sub *subscription, msg *Message) (context.Context, trace.Span) {
	ctx = t.propagators.Extract(ctx, headerCarrier(msg.Header))

	attrs := append(processAttrs(sub),
		semconv.MessagingOperationTypeProcess,
		semconv.MessagingMessageBodySize(len(msg.Data)),
	)

	if md := msg.Metadata; md != nil {
		attrs = append(attrs, semconv.MessagingMessageID(fmt.Sprintf("%s:%d", md.Stream, md.Sequence.Stream)))
	}

	return t.tracer.Start(ctx, opProcess+" "+sub.name, //nolint:spancheck // ended by endProcess
		trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(attrs...))
}

// endProcess ends the span of one processing attempt and records its
// duration.
func (t *telemetry) endProcess(
	ctx context.Context, span trace.Span, sub *subscription, start time.Time, err error,
) {
	attrs := processAttrs(sub)

	if err != nil {
		errType := semconv.ErrorTypeKey.String(errorType(err))
		attrs = append(attrs, errType)
		span.SetAttributes(errType)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	span.End()
	t.duration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attrs...))
}

// delivered counts a message delivered to the application.
func (t *telemetry) delivered(ctx context.Context, sub *subscription) {
	t.consumed.Add(ctx, 1, metric.WithAttributes(processAttrs(sub)...))
}

// processAttrs are the attributes of the process spans and metrics. The
// destination is the subscription's name (a subject pattern or a stream),
// not the message's subject: subjects often carry IDs.
func processAttrs(sub *subscription) []attribute.KeyValue {
	return append([]attribute.KeyValue{
		messagingSystem,
		semconv.MessagingOperationName(opProcess),
		semconv.MessagingDestinationName(sub.name),
	}, sub.attrs...)
}

// errorType is the error.type attribute: the module's outcome for its own
// errors, the Go type of the error otherwise (a handler's).
func errorType(err error) string {
	for _, known := range []struct {
		err  error
		name string
	}{
		{ErrNoResponders, "no_responders"},
		{natsgo.ErrTimeout, "timeout"},
		{natsgo.ErrConnectionClosed, "closed"},
		{ErrClosed, "closed"},
		{ErrNotStarted, "not_started"},
		{context.DeadlineExceeded, "timeout"},
		{context.Canceled, "canceled"},
	} {
		if errors.Is(err, known.err) {
			return known.name
		}
	}

	return fmt.Sprintf("%T", err)
}

// headerCarrier carries the trace context in NATS headers.
type headerCarrier natsgo.Header

func (c headerCarrier) Get(key string) string { return natsgo.Header(c).Get(key) }

func (c headerCarrier) Set(key, value string) { natsgo.Header(c).Set(key, value) }

func (c headerCarrier) Keys() []string { return slices.Collect(maps.Keys(c)) }

var _ propagation.TextMapCarrier = headerCarrier(nil)
