package propagator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/eidon-go/core/contrib/grpc/internal/propagator"
)

// Not parallel: it sets the otel global, which is the point of the test.
//
//nolint:paralleltest // sets the otel global propagator
func TestDefault(t *testing.T) {
	p := propagator.Default() // built before the global is set, as a component is

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1},
		SpanID:     trace.SpanID{2},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(t.Context(), sc)

	// No global propagator: W3C Trace Context and Baggage.
	carrier := propagation.MapCarrier{}
	p.Inject(ctx, carrier)

	assert.Equal(t, "00-01000000000000000000000000000000-0200000000000000-01", carrier.Get("traceparent"))
	assert.Equal(t, sc.TraceID(), trace.SpanContextFromContext(p.Extract(t.Context(), carrier)).TraceID())
	assert.ElementsMatch(t, []string{"traceparent", "tracestate", "baggage"}, p.Fields())

	// The global one, once set — also after the propagator was taken.
	otel.SetTextMapPropagator(propagation.Baggage{})

	carrier = propagation.MapCarrier{}
	p.Inject(ctx, carrier)

	assert.Empty(t, carrier.Get("traceparent"))
	assert.Equal(t, []string{"baggage"}, p.Fields())
}
