package nats_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/eidon-go/core/contrib/nats"
)

// testTelemetry implements nats.Telemetry with in-memory exporters.
type testTelemetry struct {
	spans  *tracetest.SpanRecorder
	tp     *sdktrace.TracerProvider
	reader *sdkmetric.ManualReader
	mp     *sdkmetric.MeterProvider
}

func newTestTelemetry() *testTelemetry {
	spans := tracetest.NewSpanRecorder()
	reader := sdkmetric.NewManualReader()

	return &testTelemetry{
		spans:  spans,
		tp:     sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)),
		reader: reader,
		mp:     sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)),
	}
}

func (t *testTelemetry) TracerProvider() trace.TracerProvider { return t.tp }
func (t *testTelemetry) MeterProvider() metric.MeterProvider  { return t.mp }
func (t *testTelemetry) Propagators() propagation.TextMapPropagator {
	return propagation.TraceContext{}
}

// span waits for the ended span with the name and returns it.
func (t *testTelemetry) span(tb testing.TB, name string) sdktrace.ReadOnlySpan {
	tb.Helper()

	var found sdktrace.ReadOnlySpan

	require.Eventually(tb, func() bool {
		for _, s := range t.spans.Ended() {
			if s.Name() == name {
				found = s

				return true
			}
		}

		return false
	}, waitFor, tick, "no span %q", name)

	return found
}

// metric returns the metric with the name.
func (t *testTelemetry) metric(tb testing.TB, name string) metricdata.Metrics {
	tb.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(tb, t.reader.Collect(tb.Context(), &rm))

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m
			}
		}
	}

	tb.Fatalf("no metric %q", name)

	return metricdata.Metrics{}
}

func attr(s sdktrace.ReadOnlySpan, key attribute.Key) attribute.Value {
	for _, kv := range s.Attributes() {
		if kv.Key == key {
			return kv.Value
		}
	}

	return attribute.Value{}
}

func TestTelemetry_PublishToConsumer(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	s.createStream(t, "ORDERS", "orders.>")
	conn := startConnection(t, s)
	tel := newTestTelemetry()

	got := newReceived()
	c := newConsumer(t, conn, ordersConsumer(), got, nats.WithTelemetry(tel))
	runInBackground(t, c)
	healthy(t, c)

	pub := newPublisher(t, conn, nats.WithTelemetry(tel))
	ctx, parent := tel.tp.Tracer("test").Start(t.Context(), "parent")

	_, err := pub.PublishJetStream(ctx, &nats.Message{Subject: "orders.created", Data: []byte("x")})
	require.NoError(t, err)
	parent.End()

	msg := got.next(t)
	assert.NotEmpty(t, msg.Header.Get("traceparent"))

	settled(t, s, "ORDERS", "billing")

	publish := tel.span(t, "publish orders.created")
	assert.Equal(t, trace.SpanKindProducer, publish.SpanKind())
	assert.Equal(t, parent.SpanContext().TraceID(), publish.SpanContext().TraceID())
	assert.Equal(t, "nats", attr(publish, "messaging.system").AsString())
	assert.Equal(t, "ORDERS:1", attr(publish, "messaging.message.id").AsString())

	var process sdktrace.ReadOnlySpan

	require.Eventually(t, func() bool {
		for _, s := range tel.spans.Ended() {
			if s.Name() == "process ORDERS" {
				process = s

				return true
			}
		}

		return false
	}, waitFor, tick)

	assert.Equal(t, trace.SpanKindConsumer, process.SpanKind())
	assert.Equal(t, publish.SpanContext().TraceID(), process.SpanContext().TraceID(), "one trace")
	assert.Equal(t, publish.SpanContext().SpanID(), process.Parent().SpanID())
	assert.Equal(t, "billing", attr(process, "messaging.consumer.group.name").AsString())

	assert.Equal(t, "messaging.client.sent.messages", tel.metric(t, "messaging.client.sent.messages").Name)
	assert.Equal(t, "messaging.client.consumed.messages", tel.metric(t, "messaging.client.consumed.messages").Name)
	assert.Equal(t, "messaging.process.duration", tel.metric(t, "messaging.process.duration").Name)
}

func TestTelemetry_RequestRespond(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	conn := startConnection(t, s)
	tel := newTestTelemetry()

	sub := newSubscriber(t, conn, nats.SubscriberConfig{Subject: "svc.echo", Queue: "svc"},
		nats.HandlerFunc(func(ctx context.Context, msg *nats.Message) error {
			return msg.Respond(ctx, &nats.Message{Data: msg.Data})
		}), nats.WithTelemetry(tel))
	runInBackground(t, sub)
	healthy(t, sub)

	pub := newPublisher(t, conn, nats.WithTelemetry(tel))

	_, err := pub.Request(t.Context(), &nats.Message{Subject: "svc.echo", Data: []byte("x")})
	require.NoError(t, err)

	request := tel.span(t, "request svc.echo")
	assert.Equal(t, trace.SpanKindClient, request.SpanKind())

	var respond sdktrace.ReadOnlySpan

	require.Eventually(t, func() bool {
		for _, s := range tel.spans.Ended() {
			if s.Name() == "respond (temporary)" {
				respond = s

				return true
			}
		}

		return false
	}, waitFor, tick)

	assert.True(t, attr(respond, "messaging.destination.temporary").AsBool())
	assert.Equal(t, request.SpanContext().TraceID(), respond.SpanContext().TraceID())

	process := tel.span(t, "process svc.echo")
	assert.Equal(t, "svc", attr(process, "messaging.consumer.group.name").AsString())
	assert.Equal(t, request.SpanContext().SpanID(), process.Parent().SpanID())
}

func TestTelemetry_HandlerErrorRecorded(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	conn := startConnection(t, s)
	tel := newTestTelemetry()

	sub := newSubscriberOnError(t, conn, nats.SubscriberConfig{Subject: "bad"},
		nats.HandlerFunc(func(context.Context, *nats.Message) error { panic("boom") }),
		nats.Discard, nats.WithTelemetry(tel))
	runInBackground(t, sub)
	healthy(t, sub)

	require.NoError(t, newPublisher(t, conn).Publish(t.Context(), &nats.Message{Subject: "bad"}))

	require.Eventually(t, func() bool {
		n := 0

		for _, s := range tel.spans.Ended() {
			if s.Name() == "process bad" {
				n++
			}
		}

		return n == 3
	}, waitFor, tick, "three attempts")

	process := tel.span(t, "process bad")
	assert.Equal(t, "*errors.errorString", attr(process, "error.type").AsString())
}

func TestWithTelemetry_Nil(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	pub := newPublisher(t, startConnection(t, s), nats.WithTelemetry(nil))

	require.NoError(t, pub.Publish(t.Context(), &nats.Message{Subject: "x"}))
}
