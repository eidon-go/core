// Package client builds an *http.Client for service-to-service and
// third-party API calls: a dedicated transport with a sized connection pool,
// dial, TLS and response-header timeouts, HTTP/2 (negotiated over TLS, or h2c
// with Config.UnencryptedHTTP2), OpenTelemetry instrumentation on by default,
// and RoundTripper wrappers for auth and similar concerns.
//
// The result is a plain *http.Client, not a lifecycle component: it holds no
// resources beyond pooled connections. Call CloseIdleConnections on shutdown
// if that matters to you.
package client

import (
	"net"
	"net/http"
	"slices"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// New validates cfg and builds a client. Requests carry no client-wide
// timeout: bound each one with its context.
func New(cfg Config, opts ...Option) (*http.Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	cfg = cfg.withDefaults()
	o := newOptions(opts)

	transport := &http.Transport{ //nolint:exhaustruct_v5 // the rest keep net/http defaults
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{ //nolint:exhaustruct_v5 // the rest keep net defaults
			Timeout:   cfg.DialTimeout,
			KeepAlive: cfg.KeepAlive,
		}).DialContext,
		// A custom DialContext or TLSClientConfig would otherwise turn off
		// net/http's automatic HTTP/2.
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          cfg.MaxIdleConns,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		MaxConnsPerHost:       cfg.MaxConnsPerHost,
		IdleConnTimeout:       cfg.IdleConnTimeout,
		TLSHandshakeTimeout:   cfg.TLSHandshakeTimeout,
		ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
		ExpectContinueTimeout: DefaultExpectContinueTimeout,
	}

	if o.tlsConfig != nil {
		transport.TLSClientConfig = o.tlsConfig.Clone()
	}

	if cfg.UnencryptedHTTP2 {
		protocols := new(http.Protocols)
		protocols.SetHTTP2(true)
		protocols.SetUnencryptedHTTP2(true)
		transport.Protocols = protocols
	}

	for _, fn := range o.transportFuncs {
		fn(transport)
	}

	var roundTripper http.RoundTripper = otelhttp.NewTransport(transport, append([]otelhttp.Option{
		otelhttp.WithTracerProvider(o.tracerProvider),
		otelhttp.WithMeterProvider(o.meterProvider),
		otelhttp.WithPropagators(o.propagators),
		otelhttp.WithSpanNameFormatter(spanName),
	}, o.otelOptions...)...)

	for _, wrap := range slices.Backward(o.wrappers) {
		roundTripper = wrap(roundTripper)
	}

	return &http.Client{Transport: roundTripper}, nil //nolint:exhaustruct_v5 // no client-wide Timeout on purpose
}

// spanName names a client span "{method}", following the HTTP semantic
// conventions (the URL template is unknown to a generic client).
func spanName(_ string, r *http.Request) string { return r.Method }
