package http2

import (
	"crypto/tls"
	"log/slog"
	"net"
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/eidon-go/core/lifecycle"
)

// DefaultName is the component name used in logs, errors and health reports
// unless WithName sets another one.
const DefaultName = "http2"

// Option configures New.
type Option func(*options)

type options struct {
	name           string
	logger         *slog.Logger
	listener       net.Listener
	tlsConfig      *tls.Config
	readiness      lifecycle.Healthchecker
	probes         bool
	tracerProvider trace.TracerProvider
	meterProvider  metric.MeterProvider
	propagators    propagation.TextMapPropagator
	otelOptions    []otelhttp.Option
	serverFuncs    []func(*http.Server)
}

func newOptions(opts []Option) options {
	var o options

	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}

	if o.name == "" {
		o.name = DefaultName
	}

	if o.logger == nil {
		o.logger = slog.Default()
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

// WithName sets the component name used in logs, errors, health reports and
// as the otelhttp operation name. Use it to tell two servers apart (a public
// API and an internal admin server). Default: DefaultName.
func WithName(name string) Option {
	return func(o *options) { o.name = name }
}

// WithLogger sets the logger for lifecycle messages and for the http.Server
// error log. Default: slog.Default() at New time.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

// WithListener makes Start use l instead of listening on Config.Addr — for a
// unix socket, systemd socket activation or a custom listener. The server
// owns l and closes it on Shutdown.
func WithListener(l net.Listener) Option {
	return func(o *options) { o.listener = l }
}

// WithTLSConfig serves HTTPS with cfg. cfg must provide a certificate
// (Certificates, GetCertificate or GetConfigForClient); HTTP/2 is negotiated
// via ALPN. cfg is cloned.
func WithTLSConfig(cfg *tls.Config) Option {
	return func(o *options) { o.tlsConfig = cfg }
}

// WithProbes serves LivenessPath and ReadinessPath in front of the handler.
// LivenessPath always answers 200; ReadinessPath answers 200 when
// readiness.Healthcheck returns nil and 503 otherwise. Pass the application
// (app.App implements lifecycle.Healthchecker), so that readiness fails as
// soon as shutdown begins while the server keeps serving during the
// shutdown delay. A nil readiness serves ReadinessPath like LivenessPath.
//
// Probe requests bypass the handler — and therefore its middleware, such as
// authentication — and are not traced.
func WithProbes(readiness lifecycle.Healthchecker) Option {
	return func(o *options) {
		o.probes = true
		o.readiness = readiness
	}
}

// WithTracerProvider sets the tracer provider for request spans. Default: the
// global provider at New time. Pass a noop provider to disable tracing.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *options) { o.tracerProvider = tp }
}

// WithMeterProvider sets the meter provider for request metrics. Default: the
// global provider at New time. Pass a noop provider to disable metrics.
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(o *options) { o.meterProvider = mp }
}

// WithPropagators sets the propagators used to extract the trace context from
// incoming requests. Default: the global propagators at New time.
func WithPropagators(p propagation.TextMapPropagator) Option {
	return func(o *options) { o.propagators = p }
}

// WithOtelHTTPOptions passes extra options to otelhttp.NewHandler, applied
// after the ones derived from the options above — e.g.
// otelhttp.WithMetricAttributesFn to limit metric cardinality, or
// otelhttp.WithFilter to skip noisy routes.
func WithOtelHTTPOptions(opts ...otelhttp.Option) Option {
	return func(o *options) { o.otelOptions = append(o.otelOptions, opts...) }
}

// WithHTTPServer calls fn with the underlying http.Server after New has
// configured it — an escape hatch for fields without an option (ConnState,
// ConnContext, Protocols, RegisterOnShutdown for hijacked connections such as
// WebSockets, …). Do not set Handler or Addr, and do not call methods that
// start or stop the server (Serve, Shutdown, Close).
func WithHTTPServer(fn func(*http.Server)) Option {
	return func(o *options) {
		if fn != nil {
			o.serverFuncs = append(o.serverFuncs, fn)
		}
	}
}
