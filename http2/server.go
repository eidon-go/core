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
	"sync"
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

	mu       sync.Mutex
	listener net.Listener // preset by WithListener, or opened by Start
	started  bool
	running  bool
	closed   bool

	serving atomic.Bool

	// The one graceful stop of srv, started by whichever comes first: the
	// cancellation of Run's ctx or Shutdown.
	stopOnce    sync.Once
	cancelDrain context.CancelFunc
	drained     chan struct{} // closed when the stop has finished
	drainErr    error         // set before drained is closed
	stoppedOnce sync.Once
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

	var listener net.Listener
	if o.listener != nil {
		listener = &onceCloseListener{Listener: o.listener} //nolint:exhaustruct_v5 // once state starts at zero
	}

	return &Server{ //nolint:exhaustruct_v5 // run state starts at its zero value
		name:     o.name,
		logger:   logger,
		srv:      srv,
		tls:      useTLS,
		listener: listener,
		drained:  make(chan struct{}),
	}, nil
}

// Name implements lifecycle.Namer.
func (s *Server) Name() string { return s.name }

// Addr returns the address the server listens on, or nil before Start. With
// port 0 in Config.Addr it reports the port that was picked.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return nil
	}

	return s.listener.Addr()
}

// Start opens the listener (or takes the one from WithListener). Implements
// lifecycle.Starter.
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch {
	case s.closed:
		return ErrClosed
	case s.started:
		return ErrAlreadyStarted
	}

	if s.listener == nil {
		var lc net.ListenConfig

		ln, err := lc.Listen(ctx, "tcp", s.srv.Addr)
		if err != nil {
			return fmt.Errorf("http2: listen on %s: %w", s.srv.Addr, err)
		}

		s.listener = &onceCloseListener{Listener: ln} //nolint:exhaustruct_v5 // once state starts at zero
	}

	s.started = true
	s.logger.InfoContext(ctx, "http server listening",
		slog.String("addr", s.listener.Addr().String()), slog.Bool("tls", s.tls))

	return nil
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
	s.mu.Lock()

	switch {
	case s.closed:
		s.mu.Unlock()

		return nil
	case !s.started:
		s.mu.Unlock()

		return ErrNotStarted
	case s.running:
		s.mu.Unlock()

		return ErrAlreadyStarted
	}

	s.running = true
	listener := s.listener
	s.serving.Store(true)
	s.mu.Unlock()

	served := make(chan error, 1)

	go func() { served <- s.serve(listener) }()

	select {
	case err := <-served:
		if err != nil {
			s.serving.Store(false)

			return err
		}

		// Serve returns nil only once the stop has begun (Shutdown).
		<-s.drained

		return nil
	case <-ctx.Done():
	}

	s.logger.InfoContext(ctx, "http server draining")
	s.beginStop(ctx)
	<-s.drained

	return <-served
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
	s.mu.Lock()
	s.closed = true
	listener := s.listener
	s.mu.Unlock()

	s.beginStop(ctx)

	var err error

	// A finished stop wins over an expired ctx: select would pick at random
	// when both are ready and report a clean stop as forced.
	select {
	case <-s.drained:
		err = s.drainErr
	default:
		select {
		case <-s.drained:
			err = s.drainErr
		case <-ctx.Done():
			err = ctx.Err()

			s.cancelDrain()
			_ = s.srv.Close()
			<-s.drained // prompt: the drain sees its ctx cancelled
		}
	}

	// Serve closes the listener it was given; this one may never have been
	// served (Shutdown after Start, before Run).
	if listener != nil {
		_ = listener.Close()
	}

	s.stoppedOnce.Do(func() {
		s.logger.InfoContext(ctx, "http server stopped", slog.Bool("forced", err != nil))
	})

	return wrapShutdown(err)
}

// Healthcheck reports whether the server is accepting requests: nil while Run
// serves, ErrNotServing before that and once the stop has begun. Implements
// lifecycle.Healthchecker.
func (s *Server) Healthcheck(context.Context) error {
	if s.serving.Load() {
		return nil
	}

	return ErrNotServing
}

// beginStop starts the graceful stop of the http.Server once. The stop keeps
// ctx's values but not its cancellation (Run's ctx is already cancelled);
// Shutdown cancels it through cancelDrain when its own ctx is done.
func (s *Server) beginStop(ctx context.Context) {
	s.stopOnce.Do(func() {
		s.serving.Store(false)

		drainCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		s.cancelDrain = cancel

		go func() {
			// http.Server.Shutdown marks the server as shutting down before it
			// closes the listeners, so a Serve in progress returns
			// ErrServerClosed rather than an accept error.
			if err := s.srv.Shutdown(drainCtx); err != nil && drainCtx.Err() == nil {
				s.drainErr = err
			}

			cancel()
			close(s.drained)
		}()
	})
}

func (s *Server) serve(listener net.Listener) error {
	var err error
	if s.tls {
		// Certificates come from TLSConfig, which ServeTLS also sets up for
		// HTTP/2 negotiation.
		err = s.srv.ServeTLS(listener, "", "")
	} else {
		err = s.srv.Serve(listener)
	}

	if err == nil || errors.Is(err, http.ErrServerClosed) {
		return nil
	}

	return fmt.Errorf("http2: serve: %w", err)
}

func wrapShutdown(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("http2: shutdown: %w", err)
}

// onceCloseListener closes the listener once: Serve closes the listener it is
// given, and Shutdown closes it again in case Run never served it. Not every
// net.Listener tolerates a second Close.
type onceCloseListener struct {
	net.Listener

	once sync.Once
	err  error
}

func (l *onceCloseListener) Close() error {
	l.once.Do(func() { l.err = l.Listener.Close() })

	return l.err
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
