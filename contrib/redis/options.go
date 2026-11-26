package redis

import (
	"crypto/tls"
	"log/slog"

	"github.com/redis/go-redis/extra/redisotel/v9"
	goredis "github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/eidon-go/core/retry"
)

// DefaultName is the component name used in logs, errors and health reports
// unless WithName sets another one.
const DefaultName = "redis"

// Option configures New.
type Option func(*options)

type options struct {
	name           string
	logger         *slog.Logger
	connectRetry   *retry.Policy
	tracerProvider trace.TracerProvider
	meterProvider  metric.MeterProvider
	tracingOpts    []redisotel.TracingOption
	tlsConfig      *tls.Config
	universal      []func(*goredis.UniversalOptions)
}

// WithName sets the component name (default DefaultName); give two clients
// different names.
func WithName(name string) Option { return func(o *options) { o.name = name } }

// WithLogger sets the logger (default slog.Default()); it logs connect
// retries. go-redis's own logger is global and left alone.
func WithLogger(logger *slog.Logger) Option { return func(o *options) { o.logger = logger } }

// WithConnectRetry sets the policy of the ping in Start (default
// retry.ConnectPolicy()). Retries are logged.
func WithConnectRetry(policy retry.Policy) Option {
	return func(o *options) { o.connectRetry = &policy }
}

// WithTracerProvider sets the tracer provider of the command spans (default
// the otel global at New time).
func WithTracerProvider(provider trace.TracerProvider) Option {
	return func(o *options) { o.tracerProvider = provider }
}

// WithMeterProvider sets the meter provider of the command and connection
// pool metrics (default the otel global at New time).
func WithMeterProvider(provider metric.MeterProvider) Option {
	return func(o *options) { o.meterProvider = provider }
}

// Telemetry is the set of OpenTelemetry providers the instrumentation uses.
// The Provider of github.com/eidon-go/core/contrib/otel implements it.
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

// WithTracingOptions adds options to the redisotel tracing, after the
// tracer provider: redisotel.WithDialFilter(true) drops the spans of the
// pool's background dials, redisotel.WithDBStatement(false) the command
// arguments.
func WithTracingOptions(opts ...redisotel.TracingOption) Option {
	return func(o *options) { o.tracingOpts = append(o.tracingOpts, opts...) }
}

// WithTLSConfig connects over TLS with config (cloned), whatever Config.TLS
// says.
func WithTLSConfig(config *tls.Config) Option {
	return func(o *options) { o.tlsConfig = config }
}

// WithUniversalOptions edits the go-redis options built from Config before
// the client is created, for the settings Config does not cover (a
// CredentialsProvider, a Dialer, RouteByLatency, Protocol, …). Several
// apply in order.
func WithUniversalOptions(fn func(*goredis.UniversalOptions)) Option {
	return func(o *options) {
		if fn != nil {
			o.universal = append(o.universal, fn)
		}
	}
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

	return o
}

func (o *options) connectPolicy() retry.Policy {
	p := retry.ConnectPolicy()
	if o.connectRetry != nil {
		p = *o.connectRetry
	}

	return p.Logged(o.logger)
}

// universalOptions maps cfg and the options to go-redis options.
func (o *options) universalOptions(cfg Config) *goredis.UniversalOptions {
	universal := &goredis.UniversalOptions{ //nolint:exhaustruct_v5 // go-redis defaults for the rest; WithUniversalOptions
		Addrs:            append([]string(nil), cfg.Addrs...),
		MasterName:       cfg.MasterName,
		IsClusterMode:    cfg.Cluster,
		Username:         cfg.Username,
		Password:         cfg.Password,
		SentinelUsername: cfg.SentinelUsername,
		SentinelPassword: cfg.SentinelPassword,
		DB:               cfg.DB,
		PoolSize:         cfg.PoolSize,
		MinIdleConns:     cfg.MinIdleConns,
		DialTimeout:      cfg.DialTimeout,
		ReadTimeout:      disabled(cfg.ReadTimeout, -1),
		WriteTimeout:     disabled(cfg.WriteTimeout, -1),
		PoolTimeout:      cfg.PoolTimeout,
		ConnMaxIdleTime:  disabled(cfg.ConnMaxIdleTime, -1),
		ConnMaxLifetime:  cfg.ConnMaxLifetime,
		MaxRetries:       disabled(cfg.MaxRetries, -1),
		MinRetryBackoff:  cfg.MinRetryBackoff,
		MaxRetryBackoff:  cfg.MaxRetryBackoff,
	}

	switch {
	case universal.ConnMaxLifetime == 0:
		universal.ConnMaxLifetime = DefaultConnMaxLifetime
	case universal.ConnMaxLifetime < 0:
		universal.ConnMaxLifetime = 0 // go-redis: never
	}

	switch {
	case o.tlsConfig != nil:
		universal.TLSConfig = o.tlsConfig.Clone()
	case cfg.TLS:
		universal.TLSConfig = defaultTLS()
	}

	for _, fn := range o.universal {
		fn(universal)
	}

	return universal
}

// defaultTLS is the configuration of Config.TLS: the system roots, and the
// host of each address as the server name (tls.Dialer's default).
func defaultTLS() *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS12} //nolint:exhaustruct_v5 // the zero values are the point
}

// disabled maps a negative value (the documented "disabled") to go-redis's
// own value for it.
func disabled[T ~int | ~int64](value, goRedis T) T {
	if value < 0 {
		return goRedis
	}

	return value
}
