package grpc

import (
	"crypto/tls"
	"log/slog"
	"net"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"

	"github.com/eidon-go/core/contrib/grpc/internal/propagator"
	"github.com/eidon-go/core/lifecycle"
)

// DefaultName is the component name used in logs, errors and health reports
// unless WithName sets another one.
const DefaultName = "grpc"

// Option configures New.
type Option func(*options)

type options struct {
	name               string
	logger             *slog.Logger
	listener           net.Listener
	tlsConfig          *tls.Config
	health             bool
	readiness          lifecycle.Healthchecker
	unaryInterceptors  []grpc.UnaryServerInterceptor
	streamInterceptors []grpc.StreamServerInterceptor
	tracerProvider     trace.TracerProvider
	meterProvider      metric.MeterProvider
	propagators        propagation.TextMapPropagator
	otelOptions        []otelgrpc.Option
	serverOptions      []grpc.ServerOption
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
		o.propagators = propagator.Default()
	}

	return o
}

// WithName sets the component name used in logs, errors and health reports.
// Use it to tell two servers apart. Default: DefaultName.
func WithName(name string) Option {
	return func(o *options) { o.name = name }
}

// WithLogger sets the logger for lifecycle messages. Default: slog.Default()
// at New time. grpc-go's own logging goes through the global grpclog package,
// which this module does not touch.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

// WithListener makes Start use l instead of listening on Config.Addr — for a
// unix socket, systemd socket activation or an in-memory listener (bufconn)
// in tests. The server owns l and closes it on Shutdown.
func WithListener(l net.Listener) Option {
	return func(o *options) { o.listener = l }
}

// WithTLSConfig serves over TLS with cfg. cfg must provide a certificate
// (Certificates, GetCertificate or GetConfigForClient); set ClientAuth and
// ClientCAs for mutual TLS. cfg is cloned.
func WithTLSConfig(cfg *tls.Config) Option {
	return func(o *options) { o.tlsConfig = cfg }
}

// WithHealth registers the standard grpc.health.v1.Health service, which
// Kubernetes gRPC probes, Envoy and grpc-health-probe query. Check answers
// SERVING while the server serves and readiness.Healthcheck returns nil, and
// NOT_SERVING otherwise. Pass the application (app.App implements
// lifecycle.Healthchecker), so that readiness fails as soon as shutdown
// begins while the server keeps serving during the shutdown delay. A nil
// readiness follows the server alone.
//
// The service answers for the empty name (the whole server) and for every
// registered service; other names get NOT_FOUND. Watch is not implemented
// (Unimplemented): probes use Check. Health RPCs bypass the interceptors
// added with WithUnaryInterceptors and WithStreamInterceptors — and
// therefore authentication — and are not traced.
func WithHealth(readiness lifecycle.Healthchecker) Option {
	return func(o *options) {
		o.health = true
		o.readiness = readiness
	}
}

// WithUnaryInterceptors appends unary server interceptors. Across all calls,
// the first one passed is the outermost.
func WithUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) Option {
	return func(o *options) {
		for _, i := range interceptors {
			if i != nil {
				o.unaryInterceptors = append(o.unaryInterceptors, i)
			}
		}
	}
}

// WithStreamInterceptors appends stream server interceptors. Across all
// calls, the first one passed is the outermost.
func WithStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) Option {
	return func(o *options) {
		for _, i := range interceptors {
			if i != nil {
				o.streamInterceptors = append(o.streamInterceptors, i)
			}
		}
	}
}

// WithTracerProvider sets the tracer provider for RPC spans. Default: the
// global provider at New time. Pass a noop provider to disable tracing.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *options) { o.tracerProvider = tp }
}

// WithMeterProvider sets the meter provider for RPC metrics. Default: the
// global provider at New time. Pass a noop provider to disable metrics.
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(o *options) { o.meterProvider = mp }
}

// WithPropagators sets the propagators used to extract the trace context from
// incoming metadata. Default: the global propagators once
// otel.SetTextMapPropagator is called (the otel module's Provider.SetGlobal
// does it), also after New, and W3C Trace Context and Baggage until then.
func WithPropagators(p propagation.TextMapPropagator) Option {
	return func(o *options) { o.propagators = p }
}

// Telemetry is the set of OpenTelemetry providers the server instruments
// with. The Provider of github.com/eidon-go/core/contrib/otel implements it.
type Telemetry interface {
	TracerProvider() trace.TracerProvider
	MeterProvider() metric.MeterProvider
	Propagators() propagation.TextMapPropagator
}

// WithTelemetry sets the tracer provider, the meter provider and the
// propagators from telemetry — the way to use an otel Provider that is not installed
// as the global one. WithTracerProvider alone keeps the default propagators
// (see WithPropagators). Options apply in order, so a WithTracerProvider,
// WithMeterProvider or WithPropagators after it replaces one of the three. A
// nil telemetry is ignored.
func WithTelemetry(telemetry Telemetry) Option {
	return func(o *options) {
		if telemetry == nil {
			return
		}

		o.tracerProvider = telemetry.TracerProvider()
		o.meterProvider = telemetry.MeterProvider()
		o.propagators = telemetry.Propagators()
	}
}

// WithOtelGRPCOptions passes extra options to otelgrpc.NewServerHandler,
// applied after the ones derived from the options above — e.g.
// otelgrpc.WithMetricAttributesFn, or otelgrpc.WithFilter to skip noisy
// methods (it replaces the filter that leaves health RPCs out).
func WithOtelGRPCOptions(opts ...otelgrpc.Option) Option {
	return func(o *options) { o.otelOptions = append(o.otelOptions, opts...) }
}

// WithServerOptions passes extra options to grpc.NewServer, applied after the
// ones New derives from Config and the options above, so they win where
// grpc-go keeps the last value — an escape hatch for settings without a
// Config field or option (a custom codec, credentials other than TLS, extra
// stats handlers, …). Interceptors added here with grpc.ChainUnaryInterceptor
// or grpc.ChainStreamInterceptor run inside the ones from
// WithUnaryInterceptors / WithStreamInterceptors and also see health RPCs; a
// single grpc.UnaryInterceptor or grpc.StreamInterceptor runs outermost,
// before every other interceptor.
func WithServerOptions(opts ...grpc.ServerOption) Option {
	return func(o *options) { o.serverOptions = append(o.serverOptions, opts...) }
}
