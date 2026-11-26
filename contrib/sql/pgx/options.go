package pgx

import (
	"log/slog"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/eidon-go/core/retry"
)

// DefaultName is the component name used in logs, errors and health reports
// unless WithName sets another one.
const DefaultName = "pgx"

// Option configures New, NewFromPool and NewFromDBTX.
type Option func(*options)

type options struct {
	name           string
	logger         *slog.Logger
	connectRetry   *retry.Policy
	tracerProvider trace.TracerProvider
	meterProvider  metric.MeterProvider
	otelPgxOpts    []otelpgx.Option
	poolConfig     []func(*pgxpool.Config)
	scanner        Scanner
}

// WithName sets the component name (default DefaultName); give two
// databases different names.
func WithName(name string) Option { return func(o *options) { o.name = name } }

// WithLogger sets the logger (default slog.Default()); it logs connect
// retries.
func WithLogger(logger *slog.Logger) Option { return func(o *options) { o.logger = logger } }

// WithConnectRetry sets the policy of the ping in Start (default
// retry.ConnectPolicy()). Retries are logged.
func WithConnectRetry(policy retry.Policy) Option {
	return func(o *options) { o.connectRetry = &policy }
}

// WithTracerProvider sets the tracer provider of the otelpgx tracer
// (default the otel global at New time). New only: a pool passed to
// NewFromPool keeps its own tracer.
func WithTracerProvider(provider trace.TracerProvider) Option {
	return func(o *options) { o.tracerProvider = provider }
}

// WithMeterProvider sets the meter provider of the otelpgx tracer and of
// the pool statistics (default the otel global at New time). New only.
// otelpgx offers no way to unregister the statistics: their callback stays
// with the provider after Shutdown (reporting the closed pool).
func WithMeterProvider(provider metric.MeterProvider) Option {
	return func(o *options) { o.meterProvider = provider }
}

// WithOtelPgxOptions adds options to the otelpgx tracer, after the
// providers — for example otelpgx.WithDisableAcquireTracer() against noisy
// acquire spans. New only.
func WithOtelPgxOptions(opts ...otelpgx.Option) Option {
	return func(o *options) { o.otelPgxOpts = append(o.otelPgxOpts, opts...) }
}

// WithPoolConfig changes the pgxpool configuration after Config and the
// tracer are applied (AfterConnect hooks, type registration, another
// tracer). New only.
func WithPoolConfig(fn func(*pgxpool.Config)) Option {
	return func(o *options) {
		if fn != nil {
			o.poolConfig = append(o.poolConfig, fn)
		}
	}
}

// WithScanner replaces the scanner of Get and Select (default
// DefaultScanner()).
func WithScanner(scanner Scanner) Option { return func(o *options) { o.scanner = scanner } }

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

	if o.scanner == nil {
		o.scanner = DefaultScanner()
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

func (o *options) tracer() *otelpgx.Tracer {
	return otelpgx.NewTracer(append([]otelpgx.Option{
		otelpgx.WithTracerProvider(o.tracerProvider),
		otelpgx.WithMeterProvider(o.meterProvider),
	}, o.otelPgxOpts...)...)
}
