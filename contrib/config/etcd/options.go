package etcd

import (
	"crypto/tls"
	"log/slog"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Option configures New.
type Option func(*options)

// DefaultClientLogLevel is the lowest level of the etcd client's own log
// messages that reach the logger. The client logs every retry of a request
// at Warn while the cluster is unreachable; config already reports failed
// reads and watches, so the default keeps only errors.
const DefaultClientLogLevel = slog.LevelError

type options struct {
	logger         *slog.Logger
	clientLogLevel slog.Leveler
	tlsConfig      *tls.Config
	tracerProvider trace.TracerProvider
	meterProvider  metric.MeterProvider
	propagators    propagation.TextMapPropagator
	clientFuncs    []func(*clientv3.Config)
}

func newOptions(opts []Option) options {
	var o options

	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}

	if o.logger == nil {
		o.logger = slog.Default()
	}

	if o.clientLogLevel == nil {
		o.clientLogLevel = DefaultClientLogLevel
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

// WithLogger sets the logger that receives the etcd client's own log
// messages (connection retries, endpoint changes), with the attribute
// component=etcd. Default: slog.Default() at New time.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

// WithClientLogLevel sets the lowest level of the etcd client's own messages
// that are passed to the logger, e.g. slog.LevelWarn to see its retries. The
// logger's handler filters as well. Default: DefaultClientLogLevel.
func WithClientLogLevel(level slog.Leveler) Option {
	return func(o *options) { o.clientLogLevel = level }
}

// WithTLSConfig enables TLS (and client certificates, if the config has
// them) for the connections to the cluster. New rejects http:// endpoints
// with it. Default: plain TCP, unless the first endpoint uses the https
// scheme (clientv3 picks the credentials for every endpoint from the first
// one's scheme).
func WithTLSConfig(c *tls.Config) Option {
	return func(o *options) { o.tlsConfig = c }
}

// WithTracerProvider sets the tracer provider for the spans of the etcd gRPC
// calls (reads, watch streams). Default: the global provider at New time —
// it forwards to a provider set later with otel.SetTracerProvider, so the
// KV may be built before telemetry is configured (from the configuration
// it reads). Pass a noop provider to disable tracing.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *options) { o.tracerProvider = tp }
}

// WithMeterProvider sets the meter provider for the metrics of the etcd gRPC
// calls. Default: the global provider at New time (forwarding, like the
// tracer provider). Pass a noop provider to disable metrics.
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(o *options) { o.meterProvider = mp }
}

// WithPropagators sets the propagators that inject the trace context into
// the etcd gRPC calls. Default: the global propagators at New time.
func WithPropagators(p propagation.TextMapPropagator) Option {
	return func(o *options) { o.propagators = p }
}

// WithClientConfig changes the clientv3.Config built from Config just before
// the client is created, for settings this package does not expose (message
// size limits, gRPC dial options, retry backoff). The functions run in
// order. DialOptions already holds the OpenTelemetry stats handler: append
// to it rather than replace it. Keep DialTimeout positive when Config has
// credentials: it bounds the authentication, and clientv3 does not bound it
// otherwise.
func WithClientConfig(f func(*clientv3.Config)) Option {
	return func(o *options) {
		if f != nil {
			o.clientFuncs = append(o.clientFuncs, f)
		}
	}
}
