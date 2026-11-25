// Package grpc runs a gRPC server as a lifecycle component: the listener is
// opened in Start (so a busy port fails the application start), RPCs are
// served in Run, and the server drains in-flight RPCs when it is stopped,
// force-closing the remaining connections at the Shutdown deadline.
//
// Services are registered on the Server itself — it is a
// grpc.ServiceRegistrar, so generated RegisterXxxServer functions and
// reflection.Register accept it. The server is instrumented with otelgrpc by
// default; the standard health service is opt-in via WithHealth; panic
// recovery is in the middleware package.
package grpc

import (
	"cmp"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/stats"

	"github.com/eidon-go/core/lifecycle"
)

// Server is a gRPC server that implements lifecycle.Starter, Runner,
// Healthchecker and Namer. A Server is single-use: once shut down it cannot be
// started again.
type Server struct {
	name   string
	logger *slog.Logger
	srv    *grpc.Server
	addr   string
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
	draining    chan struct{} // closed when the stop begins
	drained     chan struct{} // closed when GracefulStop has returned
	cutoffOnce  sync.Once
	cutoff      chan struct{} // closed when Shutdown's ctx ended the drain
	stoppedOnce sync.Once
}

var (
	_ lifecycle.Starter       = (*Server)(nil)
	_ lifecycle.Runner        = (*Server)(nil)
	_ lifecycle.Healthchecker = (*Server)(nil)
	_ lifecycle.Namer         = (*Server)(nil)
	_ grpc.ServiceRegistrar   = (*Server)(nil)
)

// New validates cfg and builds a server. It does no network I/O: the listener
// is opened in Start. Register services on the returned Server before Run.
func New(cfg Config, opts ...Option) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	o := newOptions(opts)
	if o.tlsConfig != nil && !hasCertificate(o.tlsConfig) {
		return nil, fmt.Errorf("%w: TLS config has no certificate", ErrInvalidConfig)
	}

	s := &Server{ //nolint:exhaustruct_v5 // run state starts at its zero value
		name:     o.name,
		logger:   o.logger.With(slog.String("component", o.name)),
		addr:     cmp.Or(cfg.Addr, DefaultAddr),
		tls:      o.tlsConfig != nil,
		draining: make(chan struct{}),
		drained:  make(chan struct{}),
		cutoff:   make(chan struct{}),
	}

	if o.listener != nil {
		s.listener = &onceCloseListener{Listener: o.listener} //nolint:exhaustruct_v5 // once state starts at zero
	}

	s.srv = grpc.NewServer(serverOptions(cfg, o)...)

	if o.health {
		registerHealth(s, o.readiness)
	}

	return s, nil
}

// serverOptions assembles the grpc.ServerOptions: Config, TLS, the otelgrpc
// stats handler, the interceptor chains, then WithServerOptions.
func serverOptions(cfg Config, o options) []grpc.ServerOption {
	otelOpts := append([]otelgrpc.Option{
		otelgrpc.WithTracerProvider(o.tracerProvider),
		otelgrpc.WithMeterProvider(o.meterProvider),
		otelgrpc.WithPropagators(o.propagators),
	}, o.otelOptions...)
	if o.health {
		otelOpts = append([]otelgrpc.Option{otelgrpc.WithFilter(notHealth)}, otelOpts...)
	}

	opts := cfg.serverOptions()
	opts = append(opts,
		grpc.StatsHandler(otelgrpc.NewServerHandler(otelOpts...)),
		grpc.ChainUnaryInterceptor(unaryChain(o.unaryInterceptors, o.health)),
		grpc.ChainStreamInterceptor(streamChain(o.streamInterceptors, o.health)),
	)

	if o.tlsConfig != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(o.tlsConfig.Clone())))
	}

	return append(opts, o.serverOptions...)
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

// RegisterService registers a service and its implementation, typically
// through a generated RegisterXxxServer function. It implements
// grpc.ServiceRegistrar. It panics when called after Run, for a service
// registered twice and for an implementation of the wrong type — cases in
// which grpc.Server would exit the process.
func (s *Server) RegisterService(desc *grpc.ServiceDesc, impl any) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		panic("grpc: RegisterService called after Run")
	}

	if _, ok := s.srv.GetServiceInfo()[desc.ServiceName]; ok {
		panic(fmt.Sprintf("grpc: service %q registered twice", desc.ServiceName))
	}

	if impl != nil && desc.HandlerType != nil {
		if want := reflect.TypeOf(desc.HandlerType).Elem(); !reflect.TypeOf(impl).Implements(want) {
			panic(fmt.Sprintf("grpc: %T does not implement %v for service %q", impl, want, desc.ServiceName))
		}
	}

	s.srv.RegisterService(desc, impl)
}

// GetServiceInfo reports the registered services, so that
// reflection.Register accepts the Server.
func (s *Server) GetServiceInfo() map[string]grpc.ServiceInfo {
	return s.srv.GetServiceInfo()
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

		ln, err := lc.Listen(ctx, "tcp", s.addr)
		if err != nil {
			return fmt.Errorf("grpc: listen on %s: %w", s.addr, err)
		}

		s.listener = &onceCloseListener{Listener: ln} //nolint:exhaustruct_v5 // once state starts at zero
	}

	s.started = true
	s.logger.InfoContext(ctx, "grpc server listening",
		slog.String("addr", s.listener.Addr().String()), slog.Bool("tls", s.tls))

	return nil
}

// Run serves RPCs until the server is stopped, then returns nil once the
// in-flight RPCs have finished or been cut off. Implements lifecycle.Runner.
//
// Cancelling ctx starts the graceful stop: the listener closes, clients are
// told to go away (GOAWAY) and in-flight RPCs drain, with no deadline of
// Run's own — Shutdown bounds the drain and force-closes the connections when
// its ctx is done (app.App always calls it; standalone code must too). RPC
// contexts are not derived from ctx, so cancelling it does not abort the RPCs
// being drained.
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
		s.serving.Store(false)

		return err // nil only once a stop has finished: Serve waits for it
	case <-s.cutoff:
		return nil
	case <-ctx.Done():
	}

	s.logger.InfoContext(ctx, "grpc server draining")
	s.beginStop()

	select {
	case err := <-served:
		return err
	case <-s.cutoff:
		return nil
	}
}

// Shutdown stops accepting connections and waits for in-flight RPCs to
// complete. When ctx is done first, it closes every remaining connection —
// cancelling the RPCs still running — and returns an error wrapping
// ctx.Err(). Shutdown shares the stop that a cancelled Run has begun. It is
// idempotent and safe without Start or Run. Implements lifecycle.Resource.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	listener := s.listener
	s.mu.Unlock()

	s.beginStop()

	var err error

	// A finished stop wins over an expired ctx: select would pick at random
	// when both are ready and report a clean stop as forced.
	select {
	case <-s.drained:
	case <-s.cutoff:
	default:
		select {
		case <-s.drained:
		case <-s.cutoff:
		case <-ctx.Done():
			err = ctx.Err()

			s.forceStop()
		}
	}

	// Serve closes the listener it was given; this one may never have been
	// served (Shutdown after Start, before Run).
	if listener != nil {
		_ = listener.Close()
	}

	s.stoppedOnce.Do(func() {
		s.logger.InfoContext(ctx, "grpc server stopped", slog.Bool("forced", err != nil))
	})

	if err != nil {
		return fmt.Errorf("grpc: shutdown: %w", err)
	}

	return nil
}

// Healthcheck reports whether the server is accepting RPCs: nil while Run
// serves, ErrNotServing before that and once the stop has begun. Implements
// lifecycle.Healthchecker.
func (s *Server) Healthcheck(context.Context) error {
	if s.serving.Load() {
		return nil
	}

	return ErrNotServing
}

// Draining returns a channel that is closed when the stop begins (Run's ctx
// cancelled or Shutdown called). Long-lived streaming handlers (subscriptions,
// watches) select on it to end their RPC, so that the drain finishes before
// the Shutdown deadline instead of being forced.
func (s *Server) Draining() <-chan struct{} { return s.draining }

// beginStop starts the graceful stop of the grpc.Server once.
func (s *Server) beginStop() {
	s.stopOnce.Do(func() {
		s.serving.Store(false)
		close(s.draining)

		go func() {
			s.srv.GracefulStop()
			close(s.drained)
		}()
	})
}

// forceStop closes every connection and ends the wait of Run and Shutdown,
// once. Stop runs in the background: it closes the connections right away,
// but GracefulStop keeps waiting — holding grpc.Server's lock, which Stop then
// also needs — for handlers that ignore their cancelled context, so neither
// Stop nor Serve may return until those handlers do.
func (s *Server) forceStop() {
	s.cutoffOnce.Do(func() {
		go s.srv.Stop()

		close(s.cutoff)
	})
}

// serve runs grpc.Server.Serve. Once a stop has begun, Serve returns only
// after it has finished, with nil (or ErrServerStopped when the stop came
// before Serve).
func (s *Server) serve(listener net.Listener) error {
	err := s.srv.Serve(listener)
	if err == nil || errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}

	return fmt.Errorf("grpc: serve: %w", err)
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

// notHealth keeps health RPCs out of the otelgrpc instrumentation: probes
// call them every few seconds.
func notHealth(info *stats.RPCTagInfo) bool {
	return !isHealthMethod(info.FullMethodName)
}

func isHealthMethod(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, "/"+healthServiceName+"/")
}

func hasCertificate(cfg *tls.Config) bool {
	return len(cfg.Certificates) > 0 || cfg.GetCertificate != nil || cfg.GetConfigForClient != nil
}
