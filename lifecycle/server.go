package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
)

// Errors returned by Server. Components that re-export them wrap them with
// their own prefix where they return them.
var (
	// ErrNotStarted is returned by Run when ServerFuncs.Listen is set and
	// Start has not been called.
	ErrNotStarted = errors.New("lifecycle: Run called before Start")

	// ErrAlreadyStarted is returned by a second Start or a second Run: a
	// Server is single-use, like the servers it wraps.
	ErrAlreadyStarted = errors.New("lifecycle: already started")

	// ErrClosed is returned by Start after Shutdown.
	ErrClosed = errors.New("lifecycle: closed")

	// ErrNotServing is returned by Healthcheck while the server is not
	// serving: before Run, and once the stop has begun.
	ErrNotServing = errors.New("lifecycle: not serving")
)

// ServerFuncs describes a server by what its library already provides; see
// NewServer.
//
// Ordering: Listen returns before Serve or Drain is called (Start and
// Shutdown are serialized, so a Shutdown during Start waits for Listen), so
// the three may share state written by Listen without locking. Serve and
// Drain may run concurrently; Close may run concurrently with both. A panic
// in any of them is recovered and turned into an error.
type ServerFuncs struct {
	// Listen, if set, is called by Start, e.g. to open the listener, so that
	// a port conflict fails the startup. Run then requires Start.
	Listen func(ctx context.Context) error

	// Serve serves until the server is stopped (required). It returns nil
	// once Drain or Close has stopped it; map the library's "server closed"
	// error to nil.
	Serve func() error

	// Drain stops accepting work and waits for the work in flight, until ctx
	// is cancelled (required); it must then return promptly. It is called
	// once, also when Serve never ran, so it must release what Listen
	// acquired (e.g. close a listener Serve never took). ctx keeps the values
	// of the stop's ctx; it is cancelled at Shutdown's deadline, together
	// with the call to Close. If Drain fails, Close is called too, so that
	// Serve returns. For a graceful stop that takes no ctx, use
	// DrainBlocking.
	Drain func(ctx context.Context) error

	// Close force-closes every connection (required). It is called at most
	// once, in its own goroutine, when Shutdown's deadline passes before the
	// drain has finished or when Drain fails; nothing waits for it.
	Close func() error

	// Logger, if set, logs the start of the stop ("server draining") and its
	// end ("server stopped", with forced). Pass the component's logger: it
	// already carries the component's name.
	Logger *slog.Logger
}

// Server implements the stop state machine of the lifecycle contract for a
// server, from four functions: Start (Listen), Run (Serve), the one graceful
// stop shared by Run's cancellation and Shutdown (Drain), the force-close at
// Shutdown's deadline (Close), and Healthcheck (serving or not). It is a
// Starter, Runner and Healthchecker.
//
// It takes care of: Start/Run/Shutdown in any order and from any goroutine,
// a single Drain, Run after Shutdown returning nil at once, Run returning
// only once the drain has finished or been cut off, Healthcheck reporting
// ErrNotServing from the moment the stop begins, and Close never blocking
// Shutdown past its deadline.
//
// A component usually keeps a *Server in an unexported field and delegates
// Start, Run, Shutdown and Healthcheck to it, wrapping the errors with its
// own prefix. Embedding would make every method added to Server later part
// of the component's API.
type Server struct {
	funcs ServerFuncs

	mu      sync.Mutex
	started bool
	running bool
	closed  bool

	serving atomic.Bool

	stopOnce    sync.Once
	draining    chan struct{} // closed when the stop begins
	drained     chan struct{} // closed when Drain has returned
	drainErr    error         // set before drained is closed
	cancelDrain context.CancelFunc

	cutoffOnce sync.Once
	cutoff     chan struct{} // closed when the drain was cut off and Close called
}

// NewServer returns a Server for funcs. It panics if Serve, Drain or Close
// is nil.
func NewServer(funcs ServerFuncs) *Server {
	if funcs.Serve == nil || funcs.Drain == nil || funcs.Close == nil {
		panic("lifecycle: NewServer: Serve, Drain and Close are required")
	}

	return &Server{ //nolint:exhaustruct_v5 // state starts at zero
		funcs:    funcs,
		draining: make(chan struct{}),
		drained:  make(chan struct{}),
		cutoff:   make(chan struct{}),
	}
}

// Start calls Listen, if set. It returns ErrClosed after Shutdown and
// ErrAlreadyStarted when called twice. Implements Starter.
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch {
	case s.closed:
		return ErrClosed
	case s.started:
		return ErrAlreadyStarted
	}

	if s.funcs.Listen != nil {
		if err := protect("listen", func() error { return s.funcs.Listen(ctx) }); err != nil {
			return err
		}
	}

	s.started = true

	return nil
}

// Run serves until the server is stopped, then returns once the work in
// flight has drained or been cut off. Cancelling ctx begins the drain, with
// no deadline of Run's own: Shutdown bounds it. Run returns nil at once after
// Shutdown, ErrNotStarted when Listen is set and Start was not called,
// ErrAlreadyStarted when called twice, and the error of Serve — after the
// drain if a stop has begun. Implements Runner.
func (s *Server) Run(ctx context.Context) error {
	if run, err := s.claimRun(); !run {
		return err
	}

	served := make(chan error, 1)

	go func() { served <- protect("serve", s.funcs.Serve) }()

	select {
	case err := <-served:
		s.serving.Store(false)

		// Some servers (net/http) return from Serve as soon as the stop
		// begins, and Serve may fail while the stop runs: either way the
		// work in flight still drains.
		s.awaitStop()

		return err
	case <-s.cutoff:
		return nil
	case <-ctx.Done():
	}

	s.beginStop(ctx)

	select {
	case <-s.drained:
	case <-s.cutoff:
		return nil
	}

	// The drain has finished, so Serve returns promptly.
	select {
	case err := <-served:
		return err
	case <-s.cutoff:
		return nil
	}
}

// Shutdown begins the drain, if Run's cancellation has not, and waits for it
// until ctx is done; then it cuts the drain off (Close, in the background)
// and returns ctx.Err(). A drain that has finished wins over an expired ctx.
// The error of Drain goes to the first call only; later calls return nil
// once the drain has finished, or ctx.Err() if their own ctx ends first. It
// is safe without Start or Run; a Shutdown during Start waits for Listen.
// Implements Resource.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	first := !s.closed
	s.closed = true
	s.mu.Unlock()

	s.beginStop(ctx)

	select {
	case <-s.drained:
	default:
		select {
		case <-s.drained:
		case <-ctx.Done():
			s.forceStop()

			return ctx.Err() //nolint:wrapcheck // the caller's own context error
		}
	}

	if first {
		return s.drainErr
	}

	return nil
}

// Healthcheck returns nil while Run serves, ErrNotServing before that and
// once the stop has begun. Implements Healthchecker.
func (s *Server) Healthcheck(context.Context) error {
	if s.serving.Load() {
		return nil
	}

	return ErrNotServing
}

// Draining returns a channel that is closed when the stop begins (Run's ctx
// cancelled or Shutdown called). Long-lived handlers (streams, watches)
// select on it to end their work, so that the drain finishes before the
// Shutdown deadline instead of being cut off.
func (s *Server) Draining() <-chan struct{} { return s.draining }

// claimRun marks the server as running, or reports why Run must return at
// once (with nil after Shutdown).
func (s *Server) claimRun() (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch {
	case s.closed:
		return false, nil
	case !s.started && s.funcs.Listen != nil:
		return false, ErrNotStarted
	case s.running:
		return false, ErrAlreadyStarted
	}

	s.started, s.running = true, true
	s.serving.Store(true)

	return true, nil
}

// beginStop starts the one drain. Its ctx keeps the values of ctx but not
// its cancellation (Run's ctx is already cancelled); forceStop cancels it. A
// failed Drain is cut off too: Serve may still be running.
func (s *Server) beginStop(ctx context.Context) {
	s.stopOnce.Do(func() {
		s.serving.Store(false)
		close(s.draining)

		drainCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		s.cancelDrain = cancel

		s.log(ctx, "server draining")

		go func() {
			err := protect("drain", func() error { return s.funcs.Drain(drainCtx) })
			cutOff := drainCtx.Err() != nil

			if err != nil && !cutOff {
				s.drainErr = err
				s.forceStop()
			}

			// Logged before drained is closed, so that a Shutdown that
			// returns has its stop logged.
			if s.drainErr != nil {
				s.log(ctx, "server stopped", slog.Bool("forced", true), slog.Any("error", s.drainErr))
			} else {
				s.log(ctx, "server stopped", slog.Bool("forced", cutOff))
			}

			cancel()
			close(s.drained)
		}()
	})
}

// awaitStop waits for a stop that has begun to finish or be cut off; it
// returns at once when no stop has begun.
func (s *Server) awaitStop() {
	select {
	case <-s.draining:
	default:
		return
	}

	select {
	case <-s.drained:
	case <-s.cutoff:
	}
}

// forceStop cuts the drain off once: Close runs in its own goroutine, since
// some libraries (grpc-go) block it on handlers that ignore cancellation.
// It is called only after beginStop, which sets cancelDrain.
func (s *Server) forceStop() {
	s.cutoffOnce.Do(func() {
		s.cancelDrain()

		go func() { _ = protect("close", s.funcs.Close) }()

		close(s.cutoff)
	})
}

func (s *Server) log(ctx context.Context, msg string, attrs ...any) {
	if s.funcs.Logger != nil {
		s.funcs.Logger.InfoContext(ctx, msg, attrs...)
	}
}

// protect calls fn, turning a panic into an error: Serve, Drain and Close
// run in goroutines of their own, where a panic would end the process.
func protect(what string, call func() error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("lifecycle: %s panicked: %v", what, p) //nolint:err113 // carries the panic value
		}
	}()

	return call()
}

// DrainBlocking adapts a graceful stop without ctx (grpc.Server.GracefulStop)
// to ServerFuncs.Drain: stop runs in its own goroutine, and the drain returns
// when it does or when ctx is cancelled — Close then makes stop return.
func DrainBlocking(stop func()) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		done := make(chan struct{})

		go func() {
			defer close(done)

			stop()
		}()

		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Compile-time checks.
var (
	_ Starter       = (*Server)(nil)
	_ Runner        = (*Server)(nil)
	_ Healthchecker = (*Server)(nil)
)
