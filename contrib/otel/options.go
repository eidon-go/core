package otel

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// DefaultName is the component name used in logs, errors and health reports
// unless WithName sets another one.
const DefaultName = "otel"

// Option configures New.
type Option func(*options)

type options struct {
	name            string
	logger          *slog.Logger
	tlsConfig       *tls.Config
	propagators     propagation.TextMapPropagator
	resourceOptions []resource.Option
	spanProcessors  []sdktrace.SpanProcessor
	metricReaders   []sdkmetric.Reader
	logProcessors   []sdklog.Processor
	traceOptions    []sdktrace.TracerProviderOption
	metricOptions   []sdkmetric.Option
	logOptions      []sdklog.LoggerProviderOption
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

	if o.propagators == nil {
		o.propagators = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	}

	return o
}

// WithName sets the component name used in logs, errors and health reports.
// Default: DefaultName.
func WithName(name string) Option {
	return func(o *options) { o.name = name }
}

// WithLogger sets the logger for errors the SDK reports while exporting (see
// Provider.SetGlobal). Default: slog.Default() at New time.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

// WithTLSConfig sets the TLS configuration of the OTLP exporters (a private
// CA, client certificates). It is used only with an "https" Config.Endpoint.
// Default: the system roots.
func WithTLSConfig(cfg *tls.Config) Option {
	return func(o *options) { o.tlsConfig = cfg }
}

// WithPropagators sets the propagators returned by Provider.Propagators and
// installed by Provider.SetGlobal. Default: W3C Trace Context and Baggage.
func WithPropagators(p propagation.TextMapPropagator) Option {
	return func(o *options) { o.propagators = p }
}

// WithResourceOptions adds resource options, such as detectors
// (resource.WithFromEnv, resource.WithHost, resource.WithContainer), to the
// resource shared by every signal. Attributes from Config take precedence over
// the same keys from these options. Repeated calls add up.
func WithResourceOptions(opts ...resource.Option) Option {
	return func(o *options) { o.resourceOptions = append(o.resourceOptions, opts...) }
}

// WithSpanProcessor adds a span processor — for example
// sdktrace.NewBatchSpanProcessor around another exporter — and turns traces
// on, alongside the OTLP exporter if Config.Traces is set. The Provider owns
// it and shuts it down; so does New if it fails. Repeated calls add up.
func WithSpanProcessor(p sdktrace.SpanProcessor) Option {
	return func(o *options) {
		if p != nil {
			o.spanProcessors = append(o.spanProcessors, p)
		}
	}
}

// WithMetricReader adds a metric reader — a periodic reader around another
// exporter, or a pull-based reader such as a Prometheus exporter — and turns
// metrics on, alongside the OTLP exporter if Config.Metrics is set. The
// Provider owns it and shuts it down; so does New if it fails. Repeated calls
// add up.
func WithMetricReader(r sdkmetric.Reader) Option {
	return func(o *options) {
		if r != nil {
			o.metricReaders = append(o.metricReaders, r)
		}
	}
}

// WithLogProcessor adds a log processor — for example
// sdklog.NewBatchProcessor around another exporter — and turns logs on,
// alongside the OTLP exporter if Config.Logs is set. The Provider owns it and
// shuts it down; so does New if it fails. Repeated calls add up.
func WithLogProcessor(p sdklog.Processor) Option {
	return func(o *options) {
		if p != nil {
			o.logProcessors = append(o.logProcessors, p)
		}
	}
}

// WithTracerProviderOptions passes options to the SDK tracer provider: a
// sampler, span limits, an ID generator. They apply after the module's own
// (resource, processors), so they can override them — sdktrace.WithResource
// here replaces the resource built from Config instead of merging with it.
// They are ignored while traces are off: turn traces on with Config.Traces or
// WithSpanProcessor, not with sdktrace.WithBatcher here. Repeated calls add up.
func WithTracerProviderOptions(opts ...sdktrace.TracerProviderOption) Option {
	return func(o *options) { o.traceOptions = append(o.traceOptions, opts...) }
}

// WithMeterProviderOptions passes options to the SDK meter provider: views
// (histogram buckets, dropped attributes), an exemplar filter, a cardinality
// limit. They apply after the module's own (resource, readers). They are
// ignored while metrics are off: turn metrics on with Config.Metrics or
// WithMetricReader, not with sdkmetric.WithReader here. Repeated calls add up.
func WithMeterProviderOptions(opts ...sdkmetric.Option) Option {
	return func(o *options) { o.metricOptions = append(o.metricOptions, opts...) }
}

// WithLoggerProviderOptions passes options to the SDK logger provider:
// attribute limits and the like. They apply after the module's own (resource,
// processors). They are ignored while logs are off: turn logs on with
// Config.Logs or WithLogProcessor. Repeated calls add up.
func WithLoggerProviderOptions(opts ...sdklog.LoggerProviderOption) Option {
	return func(o *options) { o.logOptions = append(o.logOptions, opts...) }
}

// anySignal reports whether cfg or the options turn at least one signal on.
func (o *options) anySignal(cfg Config) bool {
	return cfg.Traces || cfg.Metrics || cfg.Logs ||
		len(o.spanProcessors) > 0 || len(o.metricReaders) > 0 || len(o.logProcessors) > 0
}

// release shuts down the processors and readers that no built provider took
// over, when New fails.
func (o *options) release(ctx context.Context) error {
	errs := make([]error, 0, len(o.spanProcessors)+len(o.metricReaders)+len(o.logProcessors))

	for _, sp := range o.spanProcessors {
		errs = append(errs, sp.Shutdown(ctx))
	}

	for _, r := range o.metricReaders {
		errs = append(errs, r.Shutdown(ctx))
	}

	for _, lp := range o.logProcessors {
		errs = append(errs, lp.Shutdown(ctx))
	}

	o.spanProcessors, o.metricReaders, o.logProcessors = nil, nil, nil

	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("otel: release options: %w", err)
	}

	return nil
}
