package otel

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

const (
	// DefaultExportTimeout bounds one OTLP export request unless
	// Config.ExportTimeout sets another value.
	DefaultExportTimeout = 10 * time.Second

	// DefaultMetricInterval is how often metrics are collected and exported
	// unless Config.MetricInterval sets another value. It is the OpenTelemetry
	// SDK default.
	DefaultMetricInterval = time.Minute
)

// Config describes the service and where its telemetry goes. It is a plain
// struct: fill it with config getters, an env library or a literal.
//
// Traces, Metrics and Logs each turn on an OTLP/HTTP exporter to Endpoint for
// that signal. A signal can also be turned on with an option that adds a
// processor or reader of your own (WithSpanProcessor, WithMetricReader,
// WithLogProcessor) — with or without OTLP. A signal that is not turned on
// gets a no-op provider.
type Config struct {
	// ServiceName is the service.name resource attribute. Required.
	ServiceName string

	// ServiceVersion is the service.version resource attribute. Optional.
	ServiceVersion string

	// Environment is the deployment.environment.name resource attribute
	// ("production", "staging"). Optional.
	Environment string

	// ResourceAttributes are extra resource attributes (k8s.pod.name,
	// team, …). ServiceName, ServiceVersion and Environment take precedence
	// over the same keys here. Optional.
	ResourceAttributes map[string]string

	// Endpoint is the base URL of the OTLP/HTTP receiver, for example
	// "http://otel-collector:4318". The signal path (/v1/traces, /v1/metrics,
	// /v1/logs) is appended to it, as with OTEL_EXPORTER_OTLP_ENDPOINT. The
	// scheme selects TLS — "https" uses it (see WithTLSConfig), "http" does
	// not — whatever the OTEL_EXPORTER_OTLP_* variables say. Required when
	// Traces, Metrics or Logs is set.
	Endpoint string

	// Headers are sent with every OTLP request (authentication tokens). When
	// set, they replace OTEL_EXPORTER_OTLP_HEADERS rather than adding to it;
	// when empty, the exporters send the headers from that variable. New
	// copies the map. Optional.
	Headers map[string]string

	// ExportTimeout bounds each HTTP attempt of an OTLP export. Failed
	// attempts are retried with backoff; the whole export, retries included,
	// is bounded by the SDK processor's export timeout (30s by default, see
	// OTEL_BSP_EXPORT_TIMEOUT and OTEL_BLRP_EXPORT_TIMEOUT). Default:
	// DefaultExportTimeout.
	ExportTimeout time.Duration

	// Traces turns on the OTLP trace exporter.
	Traces bool

	// Metrics turns on the OTLP metric exporter.
	Metrics bool

	// Logs turns on the OTLP log exporter.
	Logs bool

	// MetricInterval is how often metrics are collected and pushed to the
	// OTLP exporter. Default: DefaultMetricInterval.
	MetricInterval time.Duration
}

// Validate reports every problem in c at once. Each error wraps
// ErrInvalidConfig.
func (c Config) Validate() error {
	var errs []error

	if c.ServiceName == "" {
		errs = append(errs, fmt.Errorf("%w: serviceName is required", ErrInvalidConfig))
	}

	if _, ok := c.ResourceAttributes[""]; ok {
		errs = append(errs, fmt.Errorf("%w: resourceAttributes has an empty key", ErrInvalidConfig))
	}

	if c.Endpoint != "" {
		if _, err := parseEndpoint(c.Endpoint); err != nil {
			errs = append(errs, fmt.Errorf("%w: endpoint %q: %w", ErrInvalidConfig, c.Endpoint, err))
		}
	} else if c.Traces || c.Metrics || c.Logs {
		errs = append(errs, fmt.Errorf("%w: endpoint is required when traces, metrics or logs are on",
			ErrInvalidConfig))
	}

	if c.ExportTimeout < 0 {
		errs = append(errs, fmt.Errorf("%w: exportTimeout must not be negative, got %s",
			ErrInvalidConfig, c.ExportTimeout))
	}

	if c.MetricInterval < 0 {
		errs = append(errs, fmt.Errorf("%w: metricInterval must not be negative, got %s",
			ErrInvalidConfig, c.MetricInterval))
	}

	return errors.Join(errs...)
}

func (c Config) withDefaults() Config {
	if c.ExportTimeout == 0 {
		c.ExportTimeout = DefaultExportTimeout
	}

	if c.MetricInterval == 0 {
		c.MetricInterval = DefaultMetricInterval
	}

	return c
}

const (
	schemeHTTP  = "http"
	schemeHTTPS = "https"
)

var (
	errScheme = errors.New("scheme must be http or https")
	errHost   = errors.New("host is missing")
	errExtra  = errors.New("query, fragment and user info are not allowed")
	errEscape = errors.New("path must not contain escaped slashes or other non-canonical escapes")
	errSignal = errors.New("is a base URL: drop the /v1/<signal> suffix, it is appended per signal")
)

// parseEndpoint checks that s is an http(s) base URL with a host and a path
// the exporters can take as is.
func parseEndpoint(s string) (*url.URL, error) {
	parsed, err := url.Parse(s)
	if err != nil {
		return nil, err //nolint:wrapcheck // wrapped by the caller with the field name
	}

	switch {
	case parsed.Scheme != schemeHTTP && parsed.Scheme != schemeHTTPS:
		return nil, errScheme
	case parsed.Host == "":
		return nil, errHost
	case parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil:
		return nil, errExtra
	case parsed.RawPath != "":
		// The exporters send the decoded path: "%2F" would become "/".
		return nil, errEscape
	case isSignalPath(parsed.Path):
		return nil, errSignal
	}

	return parsed, nil
}

func isSignalPath(p string) bool {
	p = strings.TrimSuffix(p, "/")

	return strings.HasSuffix(p, "/v1/traces") || strings.HasSuffix(p, "/v1/metrics") ||
		strings.HasSuffix(p, "/v1/logs")
}
