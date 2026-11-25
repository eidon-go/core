package client

import (
	"crypto/tls"
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Option configures New.
type Option func(*options)

type options struct {
	tlsConfig      *tls.Config
	transportFuncs []func(*http.Transport)
	wrappers       []func(http.RoundTripper) http.RoundTripper
	tracerProvider trace.TracerProvider
	meterProvider  metric.MeterProvider
	propagators    propagation.TextMapPropagator
	otelOptions    []otelhttp.Option
}

func newOptions(opts []Option) options {
	var o options

	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}

	if o.tracerProvider == nil {
		o.tracerProvider = otel.GetTracerProvider()
	}

	if o.meterProvider == nil {
		o.meterProvider = otel.GetMeterProvider()
	}

	if o.propagators == nil {
		o.propagators = otel.GetTextMapPropagator()
	}

	return o
}

// WithTLSConfig sets the TLS client configuration: client certificates for
// mTLS, a private root CA pool, a minimum version. cfg is cloned. Default: Go's
// standard verification against the system roots.
func WithTLSConfig(cfg *tls.Config) Option {
	return func(o *options) { o.tlsConfig = cfg }
}

// WithTransport calls fn with the *http.Transport after New has configured it
// from Config and the options above — an escape hatch for the transport
// fields without a Config field (DisableCompression, Proxy, DialContext, …).
// Functions run in the order given, before the RoundTripper chain is built.
func WithTransport(fn func(*http.Transport)) Option {
	return func(o *options) {
		if fn != nil {
			o.transportFuncs = append(o.transportFuncs, fn)
		}
	}
}

// WithRoundTripper wraps the transport for concerns above it: auth headers,
// request signing, rate limiting, retries. wrap receives the next
// RoundTripper (closer to the wire). The first wrapper passed is the
// outermost — it sees the request first. Tracing is always innermost, so a
// client span covers one network round trip, not the time spent in wrappers
// (each retry gets its own span).
func WithRoundTripper(wrap func(http.RoundTripper) http.RoundTripper) Option {
	return func(o *options) {
		if wrap != nil {
			o.wrappers = append(o.wrappers, wrap)
		}
	}
}

// WithTracerProvider sets the tracer provider for client spans. Default: the
// global provider at New time. Pass a noop provider to disable tracing.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *options) { o.tracerProvider = tp }
}

// WithMeterProvider sets the meter provider for client metrics. Default: the
// global provider at New time. Pass a noop provider to disable metrics.
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(o *options) { o.meterProvider = mp }
}

// WithPropagators sets the propagators that inject the trace context into
// outgoing requests. Default: the global propagators at New time.
func WithPropagators(p propagation.TextMapPropagator) Option {
	return func(o *options) { o.propagators = p }
}

// WithOtelHTTPOptions passes extra options to otelhttp.NewTransport, applied
// after the ones derived from the options above — e.g.
// otelhttp.WithMetricAttributesFn to limit metric cardinality.
func WithOtelHTTPOptions(opts ...otelhttp.Option) Option {
	return func(o *options) { o.otelOptions = append(o.otelOptions, opts...) }
}
