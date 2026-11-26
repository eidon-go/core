package kafka

import (
	"crypto/tls"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/eidon-go/core/retry"
)

// DefaultName is the component name used in logs, errors and health reports
// unless WithName sets another one.
const DefaultName = "kafka"

// Backoff settings of DefaultRedeliveryPolicy and of the pause after polls
// that return only errors.
const (
	redeliveryAttempts     = 5
	redeliveryInitialDelay = 100 * time.Millisecond
	redeliveryMaxDelay     = 2 * time.Second
	fetchErrorInitialDelay = 100 * time.Millisecond
	fetchErrorMaxDelay     = 5 * time.Second
	backoffMultiplier      = 2
	backoffJitter          = 0.2
)

// DefaultRedeliveryPolicy returns the redelivery policy of a Consumer
// without WithRedeliveryPolicy: 5 attempts, delays from 100ms doubling up to
// 2s, 20% jitter — about 1.5 seconds of waiting, short enough not to hold a
// rebalance for long.
func DefaultRedeliveryPolicy() retry.Policy {
	return retry.Policy{ //nolint:exhaustruct_v5 // RetryIf and OnRetry unset
		MaxAttempts:  redeliveryAttempts,
		InitialDelay: redeliveryInitialDelay,
		Multiplier:   backoffMultiplier,
		MaxDelay:     redeliveryMaxDelay,
		Jitter:       backoffJitter,
	}
}

// Option configures NewClient, NewProducer and NewConsumer. Options that
// apply to one of them only say so; the others ignore them.
type Option func(*options)

type options struct {
	name             string
	logger           *slog.Logger
	connectRetry     *retry.Policy
	tlsConfig        *tls.Config
	tracerProvider   trace.TracerProvider
	meterProvider    metric.MeterProvider
	propagators      propagation.TextMapPropagator
	kgoOpts          []kgo.Opt
	errorHandler     ErrorHandler
	redeliveryPolicy *retry.Policy
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

	o.logger = o.logger.With(slog.String("component", o.name))

	if o.tracerProvider == nil {
		o.tracerProvider = otel.GetTracerProvider()
	}

	if o.meterProvider == nil {
		o.meterProvider = otel.GetMeterProvider()
	}

	if o.propagators == nil {
		o.propagators = otel.GetTextMapPropagator()
	}

	if o.errorHandler == nil {
		o.errorHandler = DefaultErrorHandler
	}

	return o
}

func (o *options) connectPolicy() retry.Policy {
	p := retry.ConnectPolicy()
	if o.connectRetry != nil {
		p = *o.connectRetry
	}

	return p.Logged(o.logger)
}

func (o *options) redelivery() retry.Policy {
	if o.redeliveryPolicy != nil {
		return *o.redeliveryPolicy
	}

	return DefaultRedeliveryPolicy()
}

// tracer returns the kotel tracer for the options; group is empty outside a
// consumer.
func (o *options) tracer(clientID, group string) *kotel.Tracer {
	topts := []kotel.TracerOpt{
		kotel.TracerProvider(o.tracerProvider),
		kotel.TracerPropagator(o.propagators),
	}

	if clientID != "" {
		topts = append(topts, kotel.ClientID(clientID))
	}

	if group != "" {
		topts = append(topts, kotel.ConsumerGroup(group))
	}

	return kotel.NewTracer(topts...)
}

// clientOpts assembles the franz-go options: the ones from cfg, then
// telemetry and TLS, then extra (the consumer's), then WithKgoOpts last, so
// that they win.
func (o *options) clientOpts(cfg Config, tracer *kotel.Tracer, extra ...kgo.Opt) []kgo.Opt {
	meter := kotel.NewMeter(kotel.MeterProvider(o.meterProvider))
	hooks := kotel.NewKotel(kotel.WithTracer(tracer), kotel.WithMeter(meter)).Hooks()

	opts := cfg.kgoOpts()
	opts = append(opts, kgo.WithHooks(hooks...))

	if o.tlsConfig != nil {
		opts = append(opts, kgo.DialTLSConfig(o.tlsConfig.Clone()))
	}

	opts = append(opts, extra...)

	return append(opts, o.kgoOpts...)
}

// WithName sets the component name used in logs, errors and health reports.
// Use it to tell two clients apart. Default: DefaultName.
func WithName(name string) Option {
	return func(o *options) { o.name = name }
}

// WithLogger sets the logger for lifecycle messages and message errors.
// Default: slog.Default() at construction time. franz-go's own logging is
// off unless set with WithKgoOpts(kgo.WithLogger(...)).
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

// WithConnectRetry sets the retry policy of Start, which pings the cluster
// until it answers. Default: retry.ConnectPolicy(). Retries are logged.
func WithConnectRetry(p retry.Policy) Option {
	return func(o *options) { o.connectRetry = &p }
}

// WithTLSConfig connects to the brokers over TLS with a clone of cfg.
func WithTLSConfig(cfg *tls.Config) Option {
	return func(o *options) { o.tlsConfig = cfg }
}

// WithTracerProvider sets the tracer provider for publish, receive and
// process spans. Default: the global provider at construction time. Pass a
// noop provider to disable tracing.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *options) { o.tracerProvider = tp }
}

// WithMeterProvider sets the meter provider for client metrics (connections,
// bytes and records produced and fetched). Default: the global provider at
// construction time. Pass a noop provider to disable metrics.
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(o *options) { o.meterProvider = mp }
}

// WithPropagators sets the propagators that inject the trace context into
// published record headers and extract it from consumed ones. Default: the
// global propagators at construction time, which propagate nothing until
// otel.SetTextMapPropagator is called (the otel module's Provider.SetGlobal
// does it); see WithTelemetry.
func WithPropagators(p propagation.TextMapPropagator) Option {
	return func(o *options) { o.propagators = p }
}

// Telemetry is the set of OpenTelemetry providers the module instruments
// with. The Provider of github.com/eidon-go/core/contrib/otel implements it.
type Telemetry interface {
	TracerProvider() trace.TracerProvider
	MeterProvider() metric.MeterProvider
	Propagators() propagation.TextMapPropagator
}

// WithTelemetry sets the tracer provider, the meter provider and the
// propagators from telemetry — the way to use an otel Provider that is not installed
// as the global one. WithTracerProvider alone leaves the propagators at the
// global default: spans are recorded, but the trace context is neither
// written to published records nor read from consumed ones, so traces break
// between producer and consumer. Options apply in order, so a
// WithTracerProvider, WithMeterProvider or WithPropagators after it replaces
// one of the three. A nil telemetry is ignored.
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

// WithKgoOpts passes extra options to kgo.NewClient, applied after the ones
// derived from the config and the options above, so they win — an escape
// hatch for SASL, compression, partitioners, timeouts, franz-go logging and
// the like. Options that change how a Consumer consumes or commits (group
// and topic settings, commit and rebalance callbacks) break its guarantees.
func WithKgoOpts(opts ...kgo.Opt) Option {
	return func(o *options) { o.kgoOpts = append(o.kgoOpts, opts...) }
}

// WithErrorHandler sets what a Consumer does with a message whose handler
// failed. Default: DefaultErrorHandler. Consumer only.
func WithErrorHandler(h ErrorHandler) Option {
	return func(o *options) { o.errorHandler = h }
}

// WithRedeliveryPolicy sets the delays between attempts at a message whose
// ErrorHandler returned ErrorActionRetry, and how many attempts there are
// (MaxAttempts counts the first one; retry.Forever retries until the
// consumer stops). RetryIf is not used — the ErrorHandler decides —, OnRetry
// is called before every wait. Default: DefaultRedeliveryPolicy(). Consumer
// only.
func WithRedeliveryPolicy(p retry.Policy) Option {
	return func(o *options) { o.redeliveryPolicy = &p }
}
