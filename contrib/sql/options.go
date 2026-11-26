package sql

import (
	stdsql "database/sql"
	"log/slog"

	"github.com/XSAM/otelsql"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/eidon-go/core/retry"
)

// DefaultName is the component name used in logs, errors and health reports
// unless WithName sets another one.
const DefaultName = "sql"

// Option configures New and NewFromDB.
type Option func(*options)

type options struct {
	name           string
	logger         *slog.Logger
	connectRetry   *retry.Policy
	tracerProvider trace.TracerProvider
	meterProvider  metric.MeterProvider
	otelSQLOpts    []otelsql.Option
	scanner        Scanner
	defaultIso     stdsql.IsolationLevel
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

// WithTracerProvider sets the tracer provider of the otelsql instrumentation
// (default the otel global at New time). New only: NewFromDB does not
// instrument a *sql.DB it did not open.
func WithTracerProvider(provider trace.TracerProvider) Option {
	return func(o *options) { o.tracerProvider = provider }
}

// WithMeterProvider sets the meter provider of the otelsql instrumentation
// and of the connection pool metrics (default the otel global at New time).
// New only.
func WithMeterProvider(provider metric.MeterProvider) Option {
	return func(o *options) { o.meterProvider = provider }
}

// Telemetry is the set of OpenTelemetry providers the otelsql
// instrumentation uses. The Provider of github.com/eidon-go/core/contrib/otel
// implements it.
type Telemetry interface {
	TracerProvider() trace.TracerProvider
	MeterProvider() metric.MeterProvider
}

// WithTelemetry sets the tracer and meter providers from telemetry — the way to use
// an otel Provider that is not installed as the global one. Options apply in
// order, so a WithTracerProvider or WithMeterProvider after it replaces one
// of the two. A nil telemetry is ignored. New only.
func WithTelemetry(telemetry Telemetry) Option {
	return func(o *options) {
		if telemetry == nil {
			return
		}

		o.tracerProvider = telemetry.TracerProvider()
		o.meterProvider = telemetry.MeterProvider()
	}
}

// WithOtelSQLOptions adds options to the otelsql instrumentation (span
// names, filters, attributes, SQL commenter), after the providers. New only.
func WithOtelSQLOptions(opts ...otelsql.Option) Option {
	return func(o *options) { o.otelSQLOpts = append(o.otelSQLOpts, opts...) }
}

// WithScanner replaces the scanner of Get and Select (default
// DefaultScanner()).
func WithScanner(scanner Scanner) Option { return func(o *options) { o.scanner = scanner } }

// WithDefaultIsolation sets the server's default isolation level, the level
// a transaction begun with LevelDefault counts as when a joined or nested
// call asks for one (see WithIsolation). Default Read Committed, right for
// PostgreSQL and SQL Server; pass stdsql.LevelRepeatableRead for MySQL and
// MariaDB (InnoDB), so that a call asking for Repeatable Read may join.
func WithDefaultIsolation(level stdsql.IsolationLevel) Option {
	return func(o *options) { o.defaultIso = level }
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

func (o *options) otelSQL() []otelsql.Option {
	return append([]otelsql.Option{
		otelsql.WithTracerProvider(o.tracerProvider),
		otelsql.WithMeterProvider(o.meterProvider),
	}, o.otelSQLOpts...)
}
