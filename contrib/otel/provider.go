// Package otel sets up the OpenTelemetry SDK — traces, metrics and logs over
// OTLP/HTTP — and owns it as a lifecycle.Resource.
//
// New builds the providers without network I/O; exporters connect on their
// first export. Pass the providers to other components explicitly
// (http2.WithTracerProvider(p.TracerProvider()), …) or call SetGlobal once so
// that components which default to the otel globals pick them up. Register
// the Provider with app.App before the components that emit telemetry: it is
// then shut down after them and flushes what they recorded while stopping.
package otel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/url"
	"path"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/log"
	lognoop "go.opentelemetry.io/otel/log/noop"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/eidon-go/core/lifecycle"
)

// Provider owns the SDK tracer, meter and logger providers built by New. A
// signal that is off has a no-op provider. Provider is a lifecycle.Resource
// and a lifecycle.Namer.
type Provider struct {
	name        string
	logger      *slog.Logger
	propagators propagation.TextMapPropagator

	tracerProvider trace.TracerProvider
	meterProvider  metric.MeterProvider
	loggerProvider log.LoggerProvider

	// sdk holds the SDK providers of the signals that are on. Empty when
	// every signal is off.
	sdk []signalProvider

	shutdownOnce sync.Once
	stopping     atomic.Bool
	stopped      chan struct{} // closed when the first Shutdown returns
}

// signalProvider is an SDK tracer, meter or logger provider with the name of
// its signal, for error messages.
type signalProvider struct {
	signal string
	sdk    interface {
		ForceFlush(ctx context.Context) error
		Shutdown(ctx context.Context) error
	}
}

var (
	_ lifecycle.Resource = (*Provider)(nil)
	_ lifecycle.Namer    = (*Provider)(nil)
)

// New validates cfg and builds a provider for every signal that is on (see
// Config). It does no network I/O and changes no globals: call SetGlobal for
// that.
//
// The processors and readers passed as options belong to the Provider from
// here on: if New fails, it shuts them down along with whatever it built.
func New(cfg Config, opts ...Option) (*Provider, error) {
	o := newOptions(opts)

	p, err := newProvider(cfg, &o)
	if err != nil {
		// Nothing was recorded yet, so shutting down exports nothing.
		return nil, errors.Join(err, o.release(context.Background()))
	}

	return p, nil
}

func newProvider(cfg Config, o *options) (*Provider, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	cfg = cfg.withDefaults()
	cfg.Headers = maps.Clone(cfg.Headers) // the exporters read it on every request

	if o.tlsConfig != nil && cfg.Endpoint != "" && splitEndpoint(cfg.Endpoint).Scheme == schemeHTTP {
		return nil, fmt.Errorf("%w: WithTLSConfig needs an https endpoint, got %q", ErrInvalidConfig, cfg.Endpoint)
	}

	p := &Provider{ //nolint:exhaustruct_v5 // providers are set below, shutdown state starts at zero
		name:           o.name,
		logger:         o.logger.With(slog.String("component", o.name)),
		propagators:    o.propagators,
		tracerProvider: tracenoop.NewTracerProvider(),
		meterProvider:  metricnoop.NewMeterProvider(),
		loggerProvider: lognoop.NewLoggerProvider(),
		stopped:        make(chan struct{}),
	}

	if !o.anySignal(cfg) {
		return p, nil
	}

	res := p.newResource(cfg, o)

	if err := p.build(cfg, o, res); err != nil {
		return nil, errors.Join(err, p.Shutdown(context.Background()))
	}

	return p, nil
}

// Name returns the component name (DefaultName unless WithName set another).
func (p *Provider) Name() string { return p.name }

// TracerProvider returns the SDK tracer provider, or a no-op one when traces
// are off.
func (p *Provider) TracerProvider() trace.TracerProvider { return p.tracerProvider }

// MeterProvider returns the SDK meter provider, or a no-op one when metrics
// are off.
func (p *Provider) MeterProvider() metric.MeterProvider { return p.meterProvider }

// LoggerProvider returns the SDK logger provider, or a no-op one when logs are
// off. Bridge log/slog to it with go.opentelemetry.io/contrib/bridges/otelslog.
func (p *Provider) LoggerProvider() log.LoggerProvider { return p.loggerProvider }

// Propagators returns the propagators set by WithPropagators (W3C Trace
// Context and Baggage by default).
func (p *Provider) Propagators() propagation.TextMapPropagator { return p.propagators }

// SetGlobal installs the providers of the signals that are on, the
// propagators and an error handler that logs SDK errors (failed exports) with
// the module's logger as the otel globals. Providers of signals that are off
// leave their globals untouched. After Shutdown it does nothing.
//
// Tracers and meters taken from the globals before the first SetGlobal of the
// process follow it too: the otel globals delegate once, to the first
// providers set. A later SetGlobal (of another Provider) changes what the
// globals return from then on, but not what those early tracers and meters
// use.
func (p *Provider) SetGlobal() {
	if p.stopping.Load() {
		return
	}

	if _, ok := p.tracerProvider.(*sdktrace.TracerProvider); ok {
		otel.SetTracerProvider(p.tracerProvider)
	}

	if _, ok := p.meterProvider.(*sdkmetric.MeterProvider); ok {
		otel.SetMeterProvider(p.meterProvider)
	}

	if _, ok := p.loggerProvider.(*sdklog.LoggerProvider); ok {
		otel.SetLoggerProvider(p.loggerProvider)
	}

	otel.SetTextMapPropagator(p.propagators)
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		p.logger.WarnContext(context.Background(), "opentelemetry error", slog.Any("error", err))
	}))
}

// ForceFlush exports everything the signals have buffered, for a job that
// must not lose telemetry before it exits. It honors ctx.
func (p *Provider) ForceFlush(ctx context.Context) error {
	return p.each("flush", func(sp signalProvider) error { return sp.sdk.ForceFlush(ctx) })
}

// Shutdown flushes and shuts down the SDK providers concurrently, each bounded
// by ctx; telemetry recorded afterwards is dropped. The first call does it; a
// concurrent call waits for it until its own ctx is done, and calls after it
// return nil.
func (p *Provider) Shutdown(ctx context.Context) error {
	first := false

	p.shutdownOnce.Do(func() {
		first = true

		p.stopping.Store(true)
	})

	if !first {
		select {
		case <-p.stopped:
			return nil
		case <-ctx.Done():
			return fmt.Errorf("otel: shutdown: %w", ctx.Err())
		}
	}

	defer close(p.stopped)

	return p.each("shutdown", func(sp signalProvider) error { return sp.sdk.Shutdown(ctx) })
}

// build creates the SDK provider of every signal that is on. A built provider
// owns the processors and readers from o, so they are dropped from it.
func (p *Provider) build(cfg Config, o *options, res *resource.Resource) error {
	if cfg.Traces || len(o.spanProcessors) > 0 {
		tracerProvider, err := newTracerProvider(cfg, o, res)
		if err != nil {
			return fmt.Errorf("otel: traces: %w", err)
		}

		o.spanProcessors = nil
		p.tracerProvider = tracerProvider
		p.sdk = append(p.sdk, signalProvider{signal: "traces", sdk: tracerProvider})
	}

	if cfg.Metrics || len(o.metricReaders) > 0 {
		meterProvider, err := newMeterProvider(cfg, o, res)
		if err != nil {
			return fmt.Errorf("otel: metrics: %w", err)
		}

		o.metricReaders = nil
		p.meterProvider = meterProvider
		p.sdk = append(p.sdk, signalProvider{signal: "metrics", sdk: meterProvider})
	}

	if cfg.Logs || len(o.logProcessors) > 0 {
		loggerProvider, err := newLoggerProvider(cfg, o, res)
		if err != nil {
			return fmt.Errorf("otel: logs: %w", err)
		}

		o.logProcessors = nil
		p.loggerProvider = loggerProvider
		p.sdk = append(p.sdk, signalProvider{signal: "logs", sdk: loggerProvider})
	}

	return nil
}

// each calls call on every SDK provider concurrently and joins the errors.
func (p *Provider) each(action string, call func(signalProvider) error) error {
	errs := make([]error, len(p.sdk))

	var wg sync.WaitGroup

	for i, sp := range p.sdk {
		wg.Go(func() {
			if err := call(sp); err != nil {
				errs[i] = fmt.Errorf("otel: %s %s: %w", action, sp.signal, err)
			}
		})
	}

	wg.Wait()

	return errors.Join(errs...)
}

// newResource builds the resource shared by every signal. Detection problems
// (a detector failing, conflicting schema URLs, malformed
// OTEL_RESOURCE_ATTRIBUTES) are logged: the SDK still returns a usable
// resource, and telemetry setup must not keep a service from starting.
func (p *Provider) newResource(cfg Config, o *options) *resource.Resource {
	attrs := make([]attribute.KeyValue, 0, len(cfg.ResourceAttributes)+3) //nolint:mnd // the service attributes below

	for k, v := range cfg.ResourceAttributes {
		attrs = append(attrs, attribute.String(k, v))
	}

	attrs = append(attrs, semconv.ServiceName(cfg.ServiceName))

	if cfg.ServiceVersion != "" {
		attrs = append(attrs, semconv.ServiceVersion(cfg.ServiceVersion))
	}

	if cfg.Environment != "" {
		attrs = append(attrs, semconv.DeploymentEnvironmentNameKey.String(cfg.Environment))
	}

	// Later options win on conflicting keys, so Config comes last.
	ropts := make([]resource.Option, 0, len(o.resourceOptions)+3) //nolint:mnd // the three options around them
	ropts = append(ropts, resource.WithSchemaURL(semconv.SchemaURL), resource.WithTelemetrySDK())
	ropts = append(ropts, o.resourceOptions...)
	ropts = append(ropts, resource.WithAttributes(attrs...))

	res, err := resource.New(context.Background(), ropts...)
	if err != nil {
		p.logger.WarnContext(context.Background(), "otel: resource detection incomplete", slog.Any("error", err))
	}

	return res
}

func newTracerProvider(cfg Config, o *options, res *resource.Resource) (*sdktrace.TracerProvider, error) {
	topts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}

	if cfg.Traces {
		exp, err := otlptracehttp.New(context.Background(), traceExporterOptions(cfg, o)...)
		if err != nil {
			return nil, fmt.Errorf("otlp exporter: %w", err)
		}

		topts = append(topts, sdktrace.WithBatcher(exp))
	}

	for _, sp := range o.spanProcessors {
		topts = append(topts, sdktrace.WithSpanProcessor(sp))
	}

	return sdktrace.NewTracerProvider(append(topts, o.traceOptions...)...), nil
}

func newMeterProvider(cfg Config, o *options, res *resource.Resource) (*sdkmetric.MeterProvider, error) {
	mopts := []sdkmetric.Option{sdkmetric.WithResource(res)}

	if cfg.Metrics {
		exp, err := otlpmetrichttp.New(context.Background(), metricExporterOptions(cfg, o)...)
		if err != nil {
			return nil, fmt.Errorf("otlp exporter: %w", err)
		}

		mopts = append(mopts, sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(cfg.MetricInterval))))
	}

	for _, r := range o.metricReaders {
		mopts = append(mopts, sdkmetric.WithReader(r))
	}

	return sdkmetric.NewMeterProvider(append(mopts, o.metricOptions...)...), nil
}

func newLoggerProvider(cfg Config, o *options, res *resource.Resource) (*sdklog.LoggerProvider, error) {
	lopts := []sdklog.LoggerProviderOption{sdklog.WithResource(res)}

	if cfg.Logs {
		exp, err := otlploghttp.New(context.Background(), logExporterOptions(cfg, o)...)
		if err != nil {
			return nil, fmt.Errorf("otlp exporter: %w", err)
		}

		lopts = append(lopts, sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)))
	}

	for _, lp := range o.logProcessors {
		lopts = append(lopts, sdklog.WithProcessor(lp))
	}

	return sdklog.NewLoggerProvider(append(lopts, o.logOptions...)...), nil
}

// splitEndpoint parses an endpoint that Config.Validate accepted.
func splitEndpoint(raw string) *url.URL {
	u, _ := parseEndpoint(raw) // validated in New

	return u
}

// signalURL is the OTLP URL of one signal: the endpoint's base path plus
// /v1/<signal>. The exporters derive host, path and TLS from it (WithEndpointURL
// sets "insecure" from the scheme, over any OTEL_EXPORTER_OTLP_* variable).
func signalURL(endpoint, signal string) string {
	base := splitEndpoint(endpoint)
	base.Path = path.Join("/", base.Path, "v1", signal)

	return base.String()
}

// The TLS configuration is set explicitly for both schemes: with https the one
// from WithTLSConfig (nil keeps the exporter's default, which honors
// OTEL_EXPORTER_OTLP_CERTIFICATE), with http none at all, so that a
// certificate variable cannot turn a plain-HTTP exporter into an error.

func traceExporterOptions(cfg Config, o *options) []otlptracehttp.Option {
	opts := []otlptracehttp.Option{
		otlptracehttp.WithEndpointURL(signalURL(cfg.Endpoint, "traces")),
		otlptracehttp.WithTimeout(cfg.ExportTimeout),
	}

	if o.tlsConfig != nil || splitEndpoint(cfg.Endpoint).Scheme == schemeHTTP {
		opts = append(opts, otlptracehttp.WithTLSClientConfig(o.tlsConfig))
	}

	if len(cfg.Headers) > 0 {
		opts = append(opts, otlptracehttp.WithHeaders(cfg.Headers))
	}

	return opts
}

func metricExporterOptions(cfg Config, o *options) []otlpmetrichttp.Option {
	opts := []otlpmetrichttp.Option{
		otlpmetrichttp.WithEndpointURL(signalURL(cfg.Endpoint, "metrics")),
		otlpmetrichttp.WithTimeout(cfg.ExportTimeout),
	}

	if o.tlsConfig != nil || splitEndpoint(cfg.Endpoint).Scheme == schemeHTTP {
		opts = append(opts, otlpmetrichttp.WithTLSClientConfig(o.tlsConfig))
	}

	if len(cfg.Headers) > 0 {
		opts = append(opts, otlpmetrichttp.WithHeaders(cfg.Headers))
	}

	return opts
}

func logExporterOptions(cfg Config, o *options) []otlploghttp.Option {
	opts := []otlploghttp.Option{
		otlploghttp.WithEndpointURL(signalURL(cfg.Endpoint, "logs")),
		otlploghttp.WithTimeout(cfg.ExportTimeout),
	}

	if o.tlsConfig != nil || splitEndpoint(cfg.Endpoint).Scheme == schemeHTTP {
		opts = append(opts, otlploghttp.WithTLSClientConfig(o.tlsConfig))
	}

	if len(cfg.Headers) > 0 {
		opts = append(opts, otlploghttp.WithHeaders(cfg.Headers))
	}

	return opts
}
