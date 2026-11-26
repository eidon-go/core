package rabbitmq

import (
	"crypto/tls"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/eidon-go/core/consume"
	"github.com/eidon-go/core/contrib/rabbitmq/internal/propagator"
	"github.com/eidon-go/core/retry"
)

// DefaultName is the component name used in logs, errors and health reports
// unless WithName sets another one. Give the connections of one service
// distinct names ("rabbitmq-publish", "rabbitmq-consume").
const DefaultName = "rabbitmq"

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

// DefaultRedeliveryPolicy returns the redelivery policy of a Consumer
// without WithRedeliveryPolicy: 5 attempts, delays from 100ms doubling up to
// 2s, 20% jitter — about 1.5 seconds of waiting.
func DefaultRedeliveryPolicy() retry.Policy {
	return retry.Policy{ //nolint:exhaustruct_v5 // RetryIf and OnRetry unset
		MaxAttempts:  redeliveryAttempts,
		InitialDelay: redeliveryInitialDelay,
		Multiplier:   backoffMultiplier,
		MaxDelay:     redeliveryMaxDelay,
		Jitter:       backoffJitter,
	}
}

// resubscribePolicy spaces out the attempts of a consumer to subscribe
// (declare the topology, consume) after a failure.
var resubscribePolicy = retry.Policy{ //nolint:gochecknoglobals,exhaustruct_v5 // read-only; only Delay is used
	InitialDelay: resubscribeInitialDelay,
	Multiplier:   backoffMultiplier,
	MaxDelay:     resubscribeMaxDelay,
	Jitter:       backoffJitter,
}

// Option configures NewConnection, NewPublisher and NewConsumer. Options that
// apply to some of them only say so; the others ignore them.
type Option func(*options)

type options struct {
	name             string
	logger           *slog.Logger
	connectRetry     *retry.Policy
	tlsConfig        *tls.Config
	tracerProvider   trace.TracerProvider
	meterProvider    metric.MeterProvider
	propagators      propagation.TextMapPropagator
	redeliveryPolicy *retry.Policy
	transientPolicy  *retry.Policy
	topologyMode     TopologyMode
	dial             dialFunc // tests only
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
		o.propagators = propagator.Default()
	}

	return o
}

// connectPolicy returns the policy of the first dial, logged.
func (o *options) connectPolicy() retry.Policy {
	p := retry.ConnectPolicy()
	if o.connectRetry != nil {
		p = *o.connectRetry
	}

	return p.Logged(o.logger)
}

// reconnectPolicy returns the policy of the redials after a lost connection:
// the delays of the connect policy, retried until Shutdown, logged.
func (o *options) reconnectPolicy() retry.Policy {
	p := retry.ConnectPolicy()
	if o.connectRetry != nil {
		p = *o.connectRetry
	}

	p.MaxAttempts = retry.Forever
	p.RetryIf = nil

	return p.Logged(o.logger)
}

func (o *options) redelivery() retry.Policy {
	if o.redeliveryPolicy != nil {
		return *o.redeliveryPolicy
	}

	return DefaultRedeliveryPolicy()
}

func (o *options) transient() retry.Policy {
	if o.transientPolicy != nil {
		return *o.transientPolicy
	}

	return consume.TransientPolicy()
}

// WithName sets the component name used in logs, errors and health reports
// (default DefaultName); it is also the default Config.ConnectionName.
func WithName(name string) Option {
	return func(o *options) { o.name = name }
}

// WithLogger sets the logger; nil means slog.Default().
func WithLogger(logger *slog.Logger) Option {
	return func(o *options) { o.logger = logger }
}

// WithConnectRetry sets the retry policy of Connection.Start (default
// retry.ConnectPolicy()). Its delays are also used between the redials after
// a lost connection, which go on until Shutdown. Connection only.
func WithConnectRetry(p retry.Policy) Option {
	return func(o *options) { o.connectRetry = &p }
}

// WithTLSConfig connects with TLS using cfg (cloned; ServerName defaults to
// Config.Host), as if Config.TLS were set. Connection only.
func WithTLSConfig(cfg *tls.Config) Option {
	return func(o *options) { o.tlsConfig = cfg }
}

// WithTracerProvider sets the tracer provider (default: the otel global at
// construction). Publisher and Consumer.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *options) { o.tracerProvider = tp }
}

// WithMeterProvider sets the meter provider (default: the otel global at
// construction). Publisher and Consumer.
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(o *options) { o.meterProvider = mp }
}

// WithPropagators sets the propagators that write the trace context into the
// headers of published messages and read it from consumed ones. Default: the
// global propagators once otel.SetTextMapPropagator is called (the otel
// module's Provider.SetGlobal does it), also after construction, and W3C Trace
// Context and Baggage until then. Publisher and Consumer.
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
// as the global one. WithTracerProvider alone keeps the default propagators
// (see WithPropagators). Options apply in order, so a WithTracerProvider,
// WithMeterProvider or WithPropagators after it replaces one of the three. A
// nil telemetry is ignored. Publisher and Consumer.
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

// WithRedeliveryPolicy sets the attempts and delays of ErrorActionRetry
// (default DefaultRedeliveryPolicy()); its RetryIf is ignored — the
// ErrorHandler decides — and its OnRetry is called before every delay.
// MaxAttempts 0 or 1 means no retry; retry.Forever retries until the
// consumer stops. Consumer only.
func WithRedeliveryPolicy(p retry.Policy) Option {
	return func(o *options) { o.redeliveryPolicy = &p }
}

// WithTransientPolicy sets the waits after a handler error marked with
// consume.Transient — a dependency that is down — before the same attempt is
// made again, without counting it or asking the ErrorHandler (default
// consume.TransientPolicy(): 1s doubling up to 30s, until the stop). The
// worker holds the message meanwhile. Once its MaxAttempts transient failures
// in a row are reached, the error counts as a failed attempt; a zero Policy
// counts it at once. Consumer only.
func WithTransientPolicy(p retry.Policy) Option {
	return func(o *options) { o.transientPolicy = &p }
}

// WithTopologyMode sets how the Consumer treats ConsumerConfig.Topology when
// it subscribes: declare it (TopologyDeclare, the default) or only check
// that it exists (TopologyVerify). Consumer only.
func WithTopologyMode(mode TopologyMode) Option {
	return func(o *options) { o.topologyMode = mode }
}

// withDial replaces the dialer (tests).
func withDial(d dialFunc) Option {
	return func(o *options) { o.dial = d }
}
