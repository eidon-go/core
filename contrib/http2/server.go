// Package http2 runs an http.Handler as a lifecycle component: the listener is
// opened in Start (so a busy port fails the application start), requests are
// served in Run, and the server drains in-flight requests when it is stopped,
// force-closing the remaining connections at the Shutdown deadline.
//
// The server speaks HTTP/1.1 and HTTP/2 — over TLS with WithTLSConfig, or
// cleartext (h2c) with Config.UnencryptedHTTP2 — and is instrumented with
// otelhttp by default. Readiness and liveness probes are opt-in via
// WithProbes; panic recovery is in the middleware package.
package http2

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync/atomic"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/eidon-go/core/lifecycle"
)

// Server is an HTTP server that implements lifecycle.Starter, Runner,
// Healthchecker and Namer. A Server is single-use: once shut down it cannot be
// started again.
type Server struct {
	name   string
	logger *slog.Logger
	srv    *http.Server
	tls    bool

	// listener is preset by WithListener or opened by Start. It is written
	// only in Start, which the lifecycle adapter orders before Serve and
	// Drain.
	listener net.Listener
	addr     atomic.Pointer[net.Addr] // set once Start has succeeded

	lc *lifecycle.Server
}

var (
	_ lifecycle.Starter       = (*Server)(nil)
	_ lifecycle.Runner        = (*Server)(nil)
	_ lifecycle.Healthchecker = (*Server)(nil)
	_ lifecycle.Namer         = (*Server)(nil)
)

// New validates cfg and builds a server for handler. It does no network I/O:
// the listener is opened in Start.
func New(cfg Config, handler http.Handler, opts ...Option) (*Server, error) {
	if lifecycle.IsNil(handler) {
		return nil, ErrNilHandler
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	o := newOptions(opts)
	cfg = cfg.withDefaults()
	logger := o.logger.With(slog.String("component", o.name))

	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(cfg.UnencryptedHTTP2)
	protocols.SetHTTP2(o.tlsConfig != nil)

	srv := &http.Server{ //nolint:exhaustruct_v5 // the rest keep net/http defaults; TLSConfig is set below
		Addr:              cfg.Addr,
		Handler:           buildHandler(handler, o),
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		MaxHeaderBytes:    cfg.MaxHeaderBytes,
		Protocols:         protocols,
		// net/http logs client-side failures (TLS handshakes from scanners,
		// broken connections) here: noise rather than warnings.
		ErrorLog: slog.NewLogLogger(logger.Handler(), slog.LevelInfo),
	}

	if o.tlsConfig != nil {
		srv.TLSConfig = o.tlsConfig.Clone()
	}

	before := *protocols

	for _, fn := range o.serverFuncs {
		fn(srv)
	}

	// TLS may also come from WithHTTPServer: decide from the final server.
	useTLS := srv.TLSConfig != nil
	if useTLS && !hasCertificate(srv.TLSConfig) {
		return nil, fmt.Errorf("%w: TLS config has no certificate", ErrInvalidConfig)
	}

	// Unless WithHTTPServer chose the protocols itself, HTTP/2 follows TLS
	// (which WithHTTPServer may have set).
	if srv.Protocols == protocols && *protocols == before {
		protocols.SetHTTP2(useTLS)
	}

	return newServer(o, srv, logger, useTLS), nil
}

// newServer wires the Server to its lifecycle adapter.
func newServer(o options, srv *http.Server, logger *slog.Logger, useTLS bool) *Server {
	s := &Server{ //nolint:exhaustruct_v5 // listener and addr are set below / by Start
		name:   o.name,
		logger: logger,
		srv:    srv,
		tls:    useTLS,
	}

	if o.listener != nil {
		s.listener = lifecycle.ListenerOnce(o.listener)
	}

	s.lc = lifecycle.NewServer(lifecycle.ServerFuncs{
		Listen: s.listen,
		Serve:  s.serve,
		Drain:  s.drain,
		Close:  srv.Close,
		Logger: logger,
	})

	return s
}

// Name implements lifecycle.Namer.
func (s *Server) Name() string { return s.name }

// Addr returns the address the server listens on, or nil before Start. With
// port 0 in Config.Addr it reports the port that was picked.
func (s *Server) Addr() net.Addr {
	if addr := s.addr.Load(); addr != nil {
		return *addr
	}

	return nil
}

// Start opens the listener (or takes the one from WithListener). It returns
// ErrAlreadyStarted when called twice and ErrClosed after Shutdown.
// Implements lifecycle.Starter.
func (s *Server) Start(ctx context.Context) error {
	return wrap(s.lc.Start(ctx))
}

// Run serves requests until the server is stopped, then returns nil once the
// in-flight requests have finished or been cut off. Implements
// lifecycle.Runner.
//
// Cancelling ctx starts the graceful stop: the listener closes and in-flight
// requests drain, with no deadline of Run's own — Shutdown bounds the drain
// and force-closes the connections when its ctx is done (app.App always calls
// it; standalone code must too). Request contexts are not derived from ctx,
// so cancelling it does not abort the requests being drained.
func (s *Server) Run(ctx context.Context) error {
	return wrap(s.lc.Run(ctx))
}

// Shutdown stops accepting connections and waits for in-flight requests to
// complete. When ctx is done first, it closes every remaining connection and
// returns an error wrapping ctx.Err(). Shutdown shares the stop that a
// cancelled Run has begun, so shutdown hooks run once. It is idempotent and
// safe without Start or Run. Implements lifecycle.Resource.
//
// Hijacked connections (WebSockets) are neither tracked nor closed by
// net/http; register a hook for them with WithHTTPServer and
// http.Server.RegisterOnShutdown.
func (s *Server) Shutdown(ctx context.Context) error {
	if err := s.lc.Shutdown(ctx); err != nil {
		return fmt.Errorf("http2: shutdown: %w", err)
	}

	return nil
}

// Healthcheck reports whether the server is accepting requests: nil while Run
// serves, ErrNotServing before that and once the stop has begun. Implements
// lifecycle.Healthchecker.
func (s *Server) Healthcheck(ctx context.Context) error {
	return s.lc.Healthcheck(ctx) //nolint:wrapcheck // ErrNotServing, re-exported by this package
}

// listen opens the listener unless WithListener preset one; called by Start.
func (s *Server) listen(ctx context.Context) error {
	if s.listener == nil {
		ln, err := lifecycle.ListenTCP(ctx, s.srv.Addr)
		if err != nil {
			return fmt.Errorf("listen on %s: %w", s.srv.Addr, err)
		}

		s.listener = ln
	}

	addr := s.listener.Addr()
	s.addr.Store(&addr)
	s.logger.InfoContext(ctx, "http server listening",
		slog.String("addr", addr.String()), slog.Bool("tls", s.tls))

	return nil
}

// drain is the graceful stop: http.Server.Shutdown marks the server as
// shutting down before it closes the listeners, so a Serve in progress
// returns ErrServerClosed rather than an accept error. Serve closes the
// listener it was given; this one may never have been served (Shutdown after
// Start, before Run), so it is closed here too.
func (s *Server) drain(ctx context.Context) error {
	err := s.srv.Shutdown(ctx)

	if s.listener != nil {
		_ = s.listener.Close()
	}

	return err //nolint:wrapcheck // wrapped by Shutdown
}

func (s *Server) serve() error {
	var err error
	if s.tls {
		// Certificates come from TLSConfig, which ServeTLS also sets up for
		// HTTP/2 negotiation.
		err = s.srv.ServeTLS(s.listener, "", "")
	} else {
		err = s.srv.Serve(s.listener)
	}

	if err == nil || errors.Is(err, http.ErrServerClosed) {
		return nil
	}

	return fmt.Errorf("serve: %w", err)
}

// wrap prefixes an error of Start or Run, including the lifecycle state
// errors, with the module name.
func wrap(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("http2: %w", err)
}

// buildHandler wraps handler with otelhttp and, outermost, the probes, so that
// probe requests are not traced.
func buildHandler(handler http.Handler, o options) http.Handler {
	otelOpts := append([]otelhttp.Option{
		otelhttp.WithTracerProvider(o.tracerProvider),
		otelhttp.WithMeterProvider(o.meterProvider),
		otelhttp.WithPropagators(o.propagators),
		otelhttp.WithSpanNameFormatter(spanName),
	}, o.otelOptions...)

	h := otelhttp.NewHandler(handler, o.name, otelOpts...)

	if o.probes {
		h = probes(h, o.readiness)
	}

	return h
}

// spanName names a server span "{method} {route}" once http.ServeMux has
// matched a pattern (otelhttp renames the span after the handler returns),
// and "{method}" otherwise, following the HTTP semantic conventions.
func spanName(_ string, r *http.Request) string {
	switch {
	case r.Pattern == "":
		return r.Method
	case strings.Contains(r.Pattern, " "): // the pattern names the method
		return r.Pattern
	default:
		return r.Method + " " + r.Pattern
	}
}

func hasCertificate(cfg *tls.Config) bool {
	return len(cfg.Certificates) > 0 || cfg.GetCertificate != nil || cfg.GetConfigForClient != nil
}
