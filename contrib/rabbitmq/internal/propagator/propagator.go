// Package propagator holds the default propagator of the module's packages.
package propagator

import (
	"context"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// Default returns the propagator used without WithPropagators: the global
// one (otel.GetTextMapPropagator) once otel.SetTextMapPropagator has been
// called — also after the component was built — and W3C Trace Context and
// Baggage until then, so that a service that sets only a tracer provider
// still continues the traces of its callers.
//
//nolint:ireturn // the propagator interface of otel
func Default() propagation.TextMapPropagator { return global{} }

// fallback is the propagator until a global one is set.
//
//nolint:gochecknoglobals // stateless, shared by every component
var fallback = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})

// installed is set once the global propagator declares fields: from then on
// it is used without asking again.
//
//nolint:gochecknoglobals // mirrors the otel global it caches a fact about
var installed atomic.Bool

type global struct{}

func (g global) Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	g.current().Inject(ctx, carrier)
}

func (g global) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	return g.current().Extract(ctx, carrier)
}

func (g global) Fields() []string { return g.current().Fields() }

//nolint:ireturn // the propagator interface of otel
func (global) current() propagation.TextMapPropagator {
	p := otel.GetTextMapPropagator()
	if installed.Load() {
		return p
	}

	// The default global propagator is a no-op that declares no fields.
	if len(p.Fields()) == 0 {
		return fallback
	}

	installed.Store(true)

	return p
}
