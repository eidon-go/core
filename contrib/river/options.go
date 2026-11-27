package river

import (
	"fmt"
	"log/slog"

	"github.com/riverqueue/rivercontrib/otelriver"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	corepgx "github.com/eidon-go/core/contrib/sql/pgx"
)

// DefaultName is the component name without WithName.
const DefaultName = "river"

// Option configures New.
type Option func(*options)

type options struct {
	name           string
	logger         *slog.Logger
	tracerProvider trace.TracerProvider
	meterProvider  metric.MeterProvider
	otelConfig     []func(*otelriver.MiddlewareConfig)
	txFrom         *corepgx.DB
}

// WithName sets the component name (default DefaultName); give two clients
// different names.
func WithName(name string) Option { return func(o *options) { o.name = name } }

// WithLogger sets the logger (default slog.Default()). It is River's logger
// too, unless river.Config.Logger is set.
func WithLogger(logger *slog.Logger) Option { return func(o *options) { o.logger = logger } }

// WithTracerProvider sets the tracer provider of the otelriver middleware
// (default the otel global at New time).
func WithTracerProvider(provider trace.TracerProvider) Option {
	return func(o *options) { o.tracerProvider = provider }
}

// WithMeterProvider sets the meter provider of the otelriver middleware
// (default the otel global at New time).
func WithMeterProvider(provider metric.MeterProvider) Option {
	return func(o *options) { o.meterProvider = provider }
}

// Telemetry is the set of OpenTelemetry providers the otelriver middleware
// uses. The Provider of github.com/eidon-go/core/contrib/otel implements it.
type Telemetry interface {
	TracerProvider() trace.TracerProvider
	MeterProvider() metric.MeterProvider
}

// WithTelemetry sets the tracer and meter providers from telemetry — the way
// to use an otel Provider that is not installed as the global one. Options
// apply in order, so a WithTracerProvider or WithMeterProvider after it
// replaces one of the two. A nil telemetry is ignored.
func WithTelemetry(telemetry Telemetry) Option {
	return func(o *options) {
		if telemetry == nil {
			return
		}

		o.tracerProvider = telemetry.TracerProvider()
		o.meterProvider = telemetry.MeterProvider()
	}
}

// WithOtelRiverConfig adjusts the configuration of the otelriver middleware
// (semantic metrics, span names, the duration unit), after the providers.
// Trace propagation (the W3C trace context of the insert carried in the job's
// metadata, linked from the work span) is on by default; fn may turn it off.
func WithOtelRiverConfig(fn func(*otelriver.MiddlewareConfig)) Option {
	return func(o *options) {
		if fn != nil {
			o.otelConfig = append(o.otelConfig, fn)
		}
	}
}

// WithTxFrom sets the DB whose transactions Insert and InsertMany join
// (default the DB of the client). Use it when River runs on a pool of its
// own (a second corepgx.DB, sized for the workers) while the jobs are
// inserted in the transactions of the service's DB: both must connect to the
// same database, where River's tables are.
func WithTxFrom(db *corepgx.DB) Option { return func(o *options) { o.txFrom = db } }

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

	return o
}

func (o *options) middleware() (*otelriver.Middleware, error) {
	cfg := otelriver.MiddlewareConfig{ //nolint:exhaustruct_v5 // otelriver's defaults
		TracerProvider:         o.tracerProvider,
		MeterProvider:          o.meterProvider,
		EnableTracePropagation: true,
	}

	for _, fn := range o.otelConfig {
		fn(&cfg)
	}

	// otelriver panics on any other unit.
	switch cfg.DurationUnit {
	case "", "ms", "s":
	default:
		return nil, fmt.Errorf(
			"%w: otelriver DurationUnit %q (want \"ms\" or \"s\")",
			ErrInvalidConfig,
			cfg.DurationUnit,
		)
	}

	return otelriver.NewMiddleware(&cfg), nil
}
