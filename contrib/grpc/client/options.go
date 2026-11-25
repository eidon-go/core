package client

import (
	"crypto/tls"
	"log/slog"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

// DefaultName is the component name used in logs and errors unless WithName
// sets another one.
const DefaultName = "grpc-client"

// Option configures New.
type Option func(*options)

type options struct {
	name               string
	logger             *slog.Logger
	tlsConfig          *tls.Config
	unaryInterceptors  []grpc.UnaryClientInterceptor
	streamInterceptors []grpc.StreamClientInterceptor
	tracerProvider     trace.TracerProvider
	meterProvider      metric.MeterProvider
	propagators        propagation.TextMapPropagator
	otelOptions        []otelgrpc.Option
	dialOptions        []grpc.DialOption
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

// WithName sets the component name used in logs and errors. Use it to tell
// the connections to different services apart. Default: DefaultName.
func WithName(name string) Option {
	return func(o *options) { o.name = name }
}

// WithLogger sets the logger for lifecycle messages. Default: slog.Default()
// at New time. grpc-go's own logging goes through the global grpclog package,
// which this package does not touch.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

// WithTLSConfig connects over TLS with cfg instead of the system roots — for
// private CAs and mutual TLS (set Certificates). cfg is cloned. It cannot be
// combined with Config.Plaintext.
func WithTLSConfig(cfg *tls.Config) Option {
	return func(o *options) { o.tlsConfig = cfg }
}

// WithUnaryInterceptors appends unary client interceptors — auth metadata,
// retries, logging. Across all calls, the first one passed is the outermost.
// They run outside the OpenTelemetry instrumentation, so each attempt of a
// retrying interceptor gets its own span.
func WithUnaryInterceptors(interceptors ...grpc.UnaryClientInterceptor) Option {
	return func(o *options) {
		for _, i := range interceptors {
			if i != nil {
				o.unaryInterceptors = append(o.unaryInterceptors, i)
			}
		}
	}
}

// WithStreamInterceptors appends stream client interceptors. Across all
// calls, the first one passed is the outermost.
func WithStreamInterceptors(interceptors ...grpc.StreamClientInterceptor) Option {
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

// WithPropagators sets the propagators used to inject the trace context into
// outgoing metadata. Default: the global propagators at New time.
func WithPropagators(p propagation.TextMapPropagator) Option {
	return func(o *options) { o.propagators = p }
}

// WithOtelGRPCOptions passes extra options to otelgrpc.NewClientHandler,
// applied after the ones derived from the options above — e.g.
// otelgrpc.WithFilter to skip long-lived streams.
func WithOtelGRPCOptions(opts ...otelgrpc.Option) Option {
	return func(o *options) { o.otelOptions = append(o.otelOptions, opts...) }
}

// WithDialOptions passes extra options to grpc.NewClient, applied after the
// ones New derives from Config and the options above, so they win where
// grpc-go keeps the last value — an escape hatch for settings without a
// Config field or option (per-RPC credentials, a service config with retry
// policies, a custom resolver, …). A grpc.WithDefaultServiceConfig here
// replaces the one set from Config.LoadBalancingPolicy; include the policy in
// it.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *options) { o.dialOptions = append(o.dialOptions, opts...) }
}
