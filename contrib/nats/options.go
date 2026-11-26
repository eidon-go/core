package nats

import (
	"crypto/tls"
	"log/slog"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/eidon-go/core/retry"
)

// DefaultName is the component name used in logs, errors and health reports
// unless WithName sets another one.
const DefaultName = "nats"

// Backoff settings of DefaultRedeliveryPolicy and of the pause before a
// consumer subscribes again after a failure.
const (
	redeliveryAttempts      = 5
	redeliveryInitialDelay  = 100 * time.Millisecond
	redeliveryMaxDelay      = 2 * time.Second
	resubscribeInitialDelay = 100 * time.Millisecond
	resubscribeMaxDelay     = 10 * time.Second
	backoffMultiplier       = 2
	backoffJitter           = 0.2
)

// DefaultRedeliveryPolicy returns the redelivery policy of a Subscriber or
// a Consumer without WithRedeliveryPolicy: 5 attempts, delays from 100ms
// doubling up to 2s, 20% jitter — about 1.5 seconds of waiting.
func DefaultRedeliveryPolicy() retry.Policy {
	return retry.Policy{ //nolint:exhaustruct_v5 // RetryIf and OnRetry unset
		MaxAttempts:  redeliveryAttempts,
		InitialDelay: redeliveryInitialDelay,
		Multiplier:   backoffMultiplier,
		MaxDelay:     redeliveryMaxDelay,
		Jitter:       backoffJitter,
	}
}

// resubscribePolicy spaces out the attempts of a consumer to create its
// JetStream consumer and subscribe after a failure.
var resubscribePolicy = retry.Policy{ //nolint:gochecknoglobals,exhaustruct_v5 // read-only; only Delay is used
	InitialDelay: resubscribeInitialDelay,
	Multiplier:   backoffMultiplier,
	MaxDelay:     resubscribeMaxDelay,
	Jitter:       backoffJitter,
}

// Option configures NewConnection, NewPublisher, NewSubscriber and
// NewConsumer. Options that apply to some of them only say so; the others
// ignore them.
type Option func(*options)

type options struct {
	name             string
	logger           *slog.Logger
	connectRetry     *retry.Policy
	tlsConfig        *tls.Config
	connOptions      []natsgo.Option
	tracerProvider   trace.TracerProvider
	meterProvider    metric.MeterProvider
	propagators      propagation.TextMapPropagator
	errorHandler     ErrorHandler
	redeliveryPolicy *retry.Policy
	stream           *jetstream.StreamConfig
	consumerConfig   []func(*jetstream.ConsumerConfig)
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

// connectPolicy returns the policy of Connection.Start, logged.
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

// WithName sets the component name used in logs, errors and health reports
// (default DefaultName); for a Connection it is also the default
// Config.ConnectionName.
func WithName(name string) Option {
	return func(o *options) { o.name = name }
}

// WithLogger sets the logger; nil means slog.Default().
func WithLogger(logger *slog.Logger) Option {
	return func(o *options) { o.logger = logger }
}

// WithConnectRetry sets the retry policy of Connection.Start (default
// retry.ConnectPolicy()). Once connected, nats.go reconnects by itself
// (Config.ReconnectWait) until Shutdown. Connection only.
func WithConnectRetry(p retry.Policy) Option {
	return func(o *options) { o.connectRetry = &p }
}

// WithTLSConfig connects with TLS using cfg, as if Config.TLS were set.
// Connection only.
func WithTLSConfig(cfg *tls.Config) Option {
	return func(o *options) { o.tlsConfig = cfg }
}

// WithConnOptions adds nats.go options, applied after the ones built from
// Config, so they win. Handlers set by them (nats.DisconnectErrHandler,
// nats.ClosedHandler, …) are called after the Connection's own, which log.
// Connection only.
func WithConnOptions(opts ...natsgo.Option) Option {
	return func(o *options) { o.connOptions = append(o.connOptions, opts...) }
}

// WithTracerProvider sets the tracer provider (default: the otel global at
// construction). Publisher, Subscriber and Consumer.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *options) { o.tracerProvider = tp }
}

// WithMeterProvider sets the meter provider (default: the otel global at
// construction). Publisher, Subscriber and Consumer.
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(o *options) { o.meterProvider = mp }
}

// WithPropagators sets the propagators that write the trace context into the
// headers of published messages and read it from received ones (default:
// the otel global at construction, which propagates nothing until
// otel.SetTextMapPropagator is called — the otel module's
// Provider.SetGlobal does it; see WithTelemetry). Publisher, Subscriber and
// Consumer.
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
// propagators from telemetry — the way to use an otel Provider that is not
// installed as the global one. WithTracerProvider alone leaves the
// propagators at the global default: spans are recorded, but the trace
// context is neither written to published messages nor read from received
// ones, so traces break between publisher and subscriber. Options apply in
// order, so a WithTracerProvider, WithMeterProvider or WithPropagators after
// it replaces one of the three. A nil telemetry is ignored. Publisher,
// Subscriber and Consumer.
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

// WithErrorHandler sets what is done with a message whose handler failed
// (default DefaultErrorHandler). Subscriber and Consumer.
func WithErrorHandler(h ErrorHandler) Option {
	return func(o *options) { o.errorHandler = h }
}

// WithRedeliveryPolicy sets the attempts and delays of ErrorActionRetry
// (default DefaultRedeliveryPolicy()), and for a Consumer also the delay of
// ErrorActionRequeue; its RetryIf is ignored — the ErrorHandler decides —
// and its OnRetry is called before every delay of a retry. MaxAttempts 0 or
// 1 means no retry; retry.Forever retries until the stop. Subscriber and
// Consumer.
func WithRedeliveryPolicy(p retry.Policy) Option {
	return func(o *options) { o.redeliveryPolicy = &p }
}

// WithStream makes the Consumer create the stream from cfg, or update it to
// cfg, before it creates its consumer — on every (re)subscription. Without
// it the stream must exist. cfg.Name must be ConsumerConfig.Stream. Consumer
// only.
func WithStream(cfg jetstream.StreamConfig) Option {
	return func(o *options) { o.stream = &cfg }
}

// WithConsumerConfig changes the JetStream consumer configuration the
// Consumer builds from its ConsumerConfig before it creates or updates the
// consumer — for the settings ConsumerConfig does not have (DeliverPolicy,
// BackOff, Metadata, …). Several apply in order. Changing Durable or the
// AckPolicy (the Consumer acks explicitly) breaks it, and a MaxRequestBatch
// below ConsumerConfig.Prefetch (or a MaxRequestExpires below 30s, the
// pull expiry) makes the server refuse the pulls: the consumer then stays
// subscribed but gets nothing. Consumer only.
func WithConsumerConfig(fn func(*jetstream.ConsumerConfig)) Option {
	return func(o *options) {
		if fn != nil {
			o.consumerConfig = append(o.consumerConfig, fn)
		}
	}
}
