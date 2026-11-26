package lifecycle_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
)

// fakeServer behaves like net/http by default: Serve returns as soon as the
// stop begins, while Drain waits for the work in flight (hold), until its ctx
// is cancelled. With grpcStyle, Serve returns only once the drain has
// succeeded or Close was called, like grpc-go.
type fakeServer struct {
	hold        chan struct{} // in-flight work; nil means none
	lingers     chan struct{} // if set, a cut-off Drain returns only once it is closed
	grpcStyle   bool
	serveErr    error // returned by Serve at once
	stopErr     error // returned by Serve once the stop has begun
	drainErr    error
	drainPanic  bool
	closePanic  bool
	servePanics bool

	stopOnce   sync.Once
	stop       chan struct{}
	endOnce    sync.Once
	end        chan struct{} // grpcStyle: closed by a successful drain or Close
	served     chan struct{} // closed when Serve has returned
	drains     atomic.Int32
	closes     atomic.Int32
	drainEnded atomic.Bool
}

func newFakeServer() *fakeServer {
	return &fakeServer{stop: make(chan struct{}), end: make(chan struct{}), served: make(chan struct{})}
}

func (f *fakeServer) endServe() { f.endOnce.Do(func() { close(f.end) }) }

func (f *fakeServer) drain(ctx context.Context) error {
	f.drains.Add(1)
	f.stopOnce.Do(func() { close(f.stop) })

	defer f.drainEnded.Store(true)

	if f.drainPanic {
		panic("drain boom")
	}

	if f.hold == nil {
		if f.drainErr == nil {
			f.endServe()
		}

		return f.drainErr
	}

	select {
	case <-f.hold:
		f.endServe()

		return f.drainErr
	case <-ctx.Done():
		if f.lingers != nil {
			<-f.lingers
		}

		return ctx.Err()
	}
}

func (f *fakeServer) funcs() lifecycle.ServerFuncs {
	return lifecycle.ServerFuncs{
		Listen: func(context.Context) error { return nil },
		Serve: func() error {
			defer close(f.served)

			if f.servePanics {
				panic("serve boom")
			}

			if f.serveErr != nil {
				return f.serveErr
			}

			if f.grpcStyle {
				<-f.end
			} else {
				<-f.stop
			}

			return f.stopErr
		},
		Drain: f.drain,
		Close: func() error {
			f.closes.Add(1)
			f.stopOnce.Do(func() { close(f.stop) })
			f.endServe()

			if f.closePanic {
				panic("close boom")
			}

			return nil
		},
	}
}

func newTestServer(f *fakeServer) *lifecycle.Server { return lifecycle.NewServer(f.funcs()) }

func runServer(ctx context.Context, s *lifecycle.Server) <-chan error {
	done := make(chan error, 1)

	go func() { done <- s.Run(ctx) }()

	return done
}

// syncBuffer is a bytes.Buffer safe for a logger writing from the drain
// goroutine while the test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

func waitServing(t *testing.T, s *lifecycle.Server) {
	t.Helper()

	require.Eventually(t, func() bool { return s.Healthcheck(t.Context()) == nil }, 2*time.Second, time.Millisecond)
}

func TestServer_Contract(t *testing.T) {
	t.Parallel()

	lifecycletest.Runner(t, func(*testing.T) lifecycle.Runner { return newTestServer(newFakeServer()) })
}

func TestServer_StateErrors(t *testing.T) {
	t.Parallel()

	t.Run("Run before Start", func(t *testing.T) {
		t.Parallel()

		require.ErrorIs(t, newTestServer(newFakeServer()).Run(t.Context()), lifecycle.ErrNotStarted)
	})

	t.Run("Run without Listen needs no Start", func(t *testing.T) {
		t.Parallel()

		f := newFakeServer()
		funcs := f.funcs()
		funcs.Listen = nil
		s := lifecycle.NewServer(funcs)

		done := runServer(t.Context(), s)
		waitServing(t, s)
		require.NoError(t, s.Shutdown(t.Context()))
		require.NoError(t, <-done)
	})

	t.Run("Start twice", func(t *testing.T) {
		t.Parallel()

		s := newTestServer(newFakeServer())
		require.NoError(t, s.Start(t.Context()))
		require.ErrorIs(t, s.Start(t.Context()), lifecycle.ErrAlreadyStarted)
	})

	t.Run("Run twice", func(t *testing.T) {
		t.Parallel()

		s := newTestServer(newFakeServer())
		require.NoError(t, s.Start(t.Context()))

		done := runServer(t.Context(), s)
		waitServing(t, s)
		require.ErrorIs(t, s.Run(t.Context()), lifecycle.ErrAlreadyStarted)
		require.NoError(t, s.Shutdown(t.Context()))
		require.NoError(t, <-done)
	})

	t.Run("Start after Shutdown", func(t *testing.T) {
		t.Parallel()

		s := newTestServer(newFakeServer())
		require.NoError(t, s.Shutdown(t.Context()))
		require.ErrorIs(t, s.Start(t.Context()), lifecycle.ErrClosed)
	})

	t.Run("Listen error", func(t *testing.T) {
		t.Parallel()

		errBusy := errors.New("address in use")
		funcs := newFakeServer().funcs()
		funcs.Listen = func(context.Context) error { return errBusy }
		s := lifecycle.NewServer(funcs)

		require.ErrorIs(t, s.Start(t.Context()), errBusy)
		require.ErrorIs(t, s.Run(t.Context()), lifecycle.ErrNotStarted)
		require.NoError(t, s.Shutdown(t.Context()))
	})
}

func TestServer_Healthcheck(t *testing.T) {
	t.Parallel()

	s := newTestServer(newFakeServer())
	require.ErrorIs(t, s.Healthcheck(t.Context()), lifecycle.ErrNotServing, "before Run")

	require.NoError(t, s.Start(t.Context()))

	done := runServer(t.Context(), s)
	waitServing(t, s)

	require.NoError(t, s.Shutdown(t.Context()))
	require.ErrorIs(t, s.Healthcheck(t.Context()), lifecycle.ErrNotServing, "after the stop began")
	require.NoError(t, <-done)
}

// Run must not return while the work in flight drains, even though Serve
// (like net/http's) has already returned.
func TestServer_RunWaitsForTheDrain(t *testing.T) {
	t.Parallel()

	f := newFakeServer()
	f.hold = make(chan struct{})
	s := newTestServer(f)
	require.NoError(t, s.Start(t.Context()))

	ctx, cancel := context.WithCancel(t.Context())
	done := runServer(ctx, s)
	waitServing(t, s)

	cancel()
	<-f.served

	select {
	case err := <-done:
		t.Fatalf("Run returned %v while the drain is still running", err)
	case <-time.After(50 * time.Millisecond):
	}

	select {
	case <-s.Draining():
	default:
		t.Fatal("Draining is not closed once the stop began")
	}

	close(f.hold)
	require.NoError(t, <-done)
	assert.True(t, f.drainEnded.Load())
	require.NoError(t, s.Shutdown(t.Context()))
	assert.Equal(t, int32(1), f.drains.Load(), "cancellation and Shutdown share one drain")
	assert.Zero(t, f.closes.Load())
}

// At Shutdown's deadline the drain is cut off: Close is called, Shutdown
// returns the ctx error, Run returns nil, a later Shutdown returns nil.
func TestServer_ShutdownDeadlineCutsOff(t *testing.T) {
	t.Parallel()

	f := newFakeServer()
	f.hold = make(chan struct{}) // never released
	s := newTestServer(f)
	require.NoError(t, s.Start(t.Context()))

	done := runServer(t.Context(), s)
	waitServing(t, s)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, s.Shutdown(ctx), context.DeadlineExceeded)
	require.NoError(t, <-done)
	require.Eventually(t, func() bool { return f.closes.Load() == 1 }, 2*time.Second, time.Millisecond)
	require.NoError(t, s.Shutdown(t.Context()))
	assert.Equal(t, int32(1), f.closes.Load(), "Close runs once")
}

// slowListen returns funcs whose Listen blocks until release is closed and
// reports through listening that it has begun.
func slowListen(f *fakeServer) (lifecycle.ServerFuncs, chan struct{}, chan struct{}) {
	listening, release := make(chan struct{}), make(chan struct{})
	funcs := f.funcs()
	funcs.Listen = func(context.Context) error {
		close(listening)
		<-release

		return nil
	}

	return funcs, listening, release
}

func TestServer_ShutdownDuringListenWaitsForIt(t *testing.T) {
	t.Parallel()

	f := newFakeServer()
	funcs, listening, release := slowListen(f)
	s := lifecycle.NewServer(funcs)

	started := make(chan error, 1)

	go func() { started <- s.Start(t.Context()) }()

	<-listening
	require.ErrorIs(t, s.Start(t.Context()), lifecycle.ErrAlreadyStarted, "a second Start during Listen")

	shut := make(chan error, 1)

	go func() { shut <- s.Shutdown(t.Context()) }()

	// Shutdown has begun once a Start sees it.
	require.Eventually(t, func() bool { return errors.Is(s.Start(t.Context()), lifecycle.ErrClosed) },
		2*time.Second, time.Millisecond)
	close(release)

	require.ErrorIs(t, <-started, lifecycle.ErrClosed)
	require.NoError(t, <-shut)
	assert.Equal(t, int32(1), f.drains.Load())
}

func TestServer_ShutdownDuringListenHonorsItsContext(t *testing.T) {
	t.Parallel()

	f := newFakeServer()
	funcs, listening, release := slowListen(f)
	s := lifecycle.NewServer(funcs)

	started := make(chan error, 1)

	go func() { started <- s.Start(t.Context()) }()

	<-listening

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, s.Shutdown(ctx), context.Canceled, "Shutdown does not wait for Listen past its ctx")

	close(release)

	require.ErrorIs(t, <-started, lifecycle.ErrClosed)
	require.Eventually(t, func() bool { return f.closes.Load() == 1 }, 2*time.Second, time.Millisecond,
		"the stop follows once Listen returns")
}

func TestServer_DrainErrorGoesToFirstShutdown(t *testing.T) {
	t.Parallel()

	errDrain := errors.New("hook failed")
	f := newFakeServer()
	f.drainErr = errDrain
	s := newTestServer(f)

	require.ErrorIs(t, s.Shutdown(t.Context()), errDrain)
	require.NoError(t, s.Shutdown(t.Context()))
}

func TestServer_ServeErrorFailsRun(t *testing.T) {
	t.Parallel()

	errServe := errors.New("accept failed")
	f := newFakeServer()
	f.serveErr = errServe
	s := newTestServer(f)
	require.NoError(t, s.Start(t.Context()))

	require.ErrorIs(t, s.Run(t.Context()), errServe)
	require.ErrorIs(t, s.Healthcheck(t.Context()), lifecycle.ErrNotServing)
	require.NoError(t, s.Shutdown(t.Context()))
}

// Shutdown between Start and Run drains (releasing what Listen acquired),
// and Run afterwards returns nil at once without serving.
func TestServer_ShutdownBeforeRun(t *testing.T) {
	t.Parallel()

	f := newFakeServer()
	s := newTestServer(f)
	require.NoError(t, s.Start(t.Context()))
	require.NoError(t, s.Shutdown(t.Context()))
	assert.Equal(t, int32(1), f.drains.Load())

	require.NoError(t, s.Run(t.Context()))

	select {
	case <-f.served:
		t.Fatal("Serve ran after Shutdown")
	default:
	}
}

func TestServer_Logs(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	funcs := newFakeServer().funcs()
	funcs.Logger = slog.New(slog.NewTextHandler(&buf, nil))
	s := lifecycle.NewServer(funcs)
	require.NoError(t, s.Start(t.Context()))

	ctx, cancel := context.WithCancel(t.Context())
	done := runServer(ctx, s)
	waitServing(t, s)
	cancel()
	require.NoError(t, <-done)
	require.NoError(t, s.Shutdown(t.Context()))

	assert.Contains(t, buf.String(), `msg="server draining"`)
	assert.Contains(t, buf.String(), `msg="server stopped" forced=false`)
}

// The stop is logged also when Shutdown begins it, and a cut-off drain is
// logged as forced.
func TestServer_LogsForcedStopFromShutdown(t *testing.T) {
	t.Parallel()

	var buf syncBuffer

	f := newFakeServer()
	f.hold = make(chan struct{}) // never released
	funcs := f.funcs()
	funcs.Logger = slog.New(slog.NewTextHandler(&buf, nil))
	s := lifecycle.NewServer(funcs)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, s.Shutdown(ctx), context.DeadlineExceeded)

	assert.Contains(t, buf.String(), `msg="server draining"`)
	require.Eventually(t, func() bool { return strings.Contains(buf.String(), `msg="server stopped" forced=true`) },
		2*time.Second, time.Millisecond)
}

// Once the drain has been cut off, the stop is over: a later Shutdown
// returns nil at once, even with an expired ctx and while the cut-off drain
// is still returning (grpc-go's GracefulStop behind a stuck handler).
func TestServer_ShutdownAfterCutoffReturnsAtOnce(t *testing.T) {
	t.Parallel()

	f := newFakeServer()
	f.hold = make(chan struct{})    // never released
	f.lingers = make(chan struct{}) // the cut-off drain does not return yet

	t.Cleanup(func() { close(f.lingers) })

	s := newTestServer(f)
	require.NoError(t, s.Start(t.Context()))

	done := runServer(t.Context(), s)
	waitServing(t, s)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, s.Shutdown(ctx), context.DeadlineExceeded)
	require.NoError(t, <-done)
	assert.False(t, f.drainEnded.Load(), "the drain is still returning")

	expired, expire := context.WithCancel(t.Context())
	expire()

	for range 10 {
		require.NoError(t, s.Shutdown(expired))
	}
}

// A waiting first Shutdown gets the Drain error even though the failed
// Drain also cuts the stop off.
func TestServer_FailedDrainErrorReachesWaitingShutdown(t *testing.T) {
	t.Parallel()

	errDrain := errors.New("hook failed")
	f := newFakeServer()
	f.grpcStyle = true
	f.hold = make(chan struct{})
	f.drainErr = errDrain
	s := newTestServer(f)

	shutdown := make(chan error, 1)

	go func() { shutdown <- s.Shutdown(t.Context()) }()

	<-s.Draining()
	close(f.hold)

	require.ErrorIs(t, <-shutdown, errDrain)
}

// A Serve that fails once the stop has begun does not make Run return
// before the drain.
func TestServer_ServeErrorDuringStopWaitsForTheDrain(t *testing.T) {
	t.Parallel()

	errClosing := errors.New("use of closed connection")
	f := newFakeServer()
	f.hold = make(chan struct{})
	f.stopErr = errClosing
	s := newTestServer(f)
	require.NoError(t, s.Start(t.Context()))

	done := runServer(t.Context(), s)
	waitServing(t, s)

	shutdown := make(chan error, 1)

	go func() { shutdown <- s.Shutdown(t.Context()) }()

	<-f.served

	select {
	case err := <-done:
		t.Fatalf("Run returned %v while the drain is still running", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(f.hold)
	require.ErrorIs(t, <-done, errClosing)
	assert.True(t, f.drainEnded.Load())
	require.NoError(t, <-shutdown)
}

// grpc-style: Serve returns only after the drain. A failed Drain must still
// end Serve (through Close), or Run would never return.
func TestServer_FailedDrainClosesTheServer(t *testing.T) {
	t.Parallel()

	errDrain := errors.New("hook failed")
	f := newFakeServer()
	f.grpcStyle = true
	f.drainErr = errDrain
	s := newTestServer(f)
	require.NoError(t, s.Start(t.Context()))

	done := runServer(t.Context(), s)
	waitServing(t, s)

	require.ErrorIs(t, s.Shutdown(t.Context()), errDrain)
	require.NoError(t, <-done)
	require.Eventually(t, func() bool { return f.closes.Load() == 1 }, 2*time.Second, time.Millisecond)
}

// grpc-style on the Run-ctx path: Run returns once the drain has finished.
func TestServer_GRPCStyleStopFromRunContext(t *testing.T) {
	t.Parallel()

	f := newFakeServer()
	f.grpcStyle = true
	f.hold = make(chan struct{})
	s := newTestServer(f)
	require.NoError(t, s.Start(t.Context()))

	ctx, cancel := context.WithCancel(t.Context())
	done := runServer(ctx, s)
	waitServing(t, s)
	cancel()
	<-s.Draining()

	close(f.hold)
	require.NoError(t, <-done)
	assert.True(t, f.drainEnded.Load())
	require.NoError(t, s.Shutdown(t.Context()))
	assert.Zero(t, f.closes.Load())
}

// A second Shutdown whose ctx ends before the drain reports it, even though
// it is not the first call; the first, with a longer ctx, still succeeds
// once the drain has been cut off.
func TestServer_ConcurrentShutdownWithShortContext(t *testing.T) {
	t.Parallel()

	f := newFakeServer()
	f.hold = make(chan struct{}) // never released
	s := newTestServer(f)
	require.NoError(t, s.Start(t.Context()))

	done := runServer(t.Context(), s)
	waitServing(t, s)

	first := make(chan error, 1)

	go func() { first <- s.Shutdown(t.Context()) }()

	<-s.Draining()

	short, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, s.Shutdown(short), context.DeadlineExceeded, "the force-close is not reported as clean")
	require.NoError(t, <-done)
	require.NoError(t, <-first)
	require.Eventually(t, func() bool { return f.closes.Load() == 1 }, 2*time.Second, time.Millisecond)
}

// A drain that has finished wins over an expired ctx.
func TestServer_FinishedDrainWinsOverExpiredContext(t *testing.T) {
	t.Parallel()

	f := newFakeServer()
	s := newTestServer(f)
	require.NoError(t, s.Shutdown(t.Context()))

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()

	for range 100 {
		require.NoError(t, s.Shutdown(cancelled))
	}

	assert.Zero(t, f.closes.Load())
}

func TestServer_PanicsBecomeErrors(t *testing.T) {
	t.Parallel()

	t.Run("Serve", func(t *testing.T) {
		t.Parallel()

		f := newFakeServer()
		f.servePanics = true
		s := newTestServer(f)
		require.NoError(t, s.Start(t.Context()))
		require.ErrorContains(t, s.Run(t.Context()), "lifecycle: serve panicked: serve boom")
		require.NoError(t, s.Shutdown(t.Context()))
	})

	t.Run("Drain", func(t *testing.T) {
		t.Parallel()

		f := newFakeServer()
		f.drainPanic = true
		s := newTestServer(f)

		require.ErrorContains(t, s.Shutdown(t.Context()), "lifecycle: drain panicked: drain boom")
		require.Eventually(t, func() bool { return f.closes.Load() == 1 }, 2*time.Second, time.Millisecond)
	})

	t.Run("Close", func(t *testing.T) {
		t.Parallel()

		f := newFakeServer()
		f.drainErr = errors.New("hook failed")
		f.closePanic = true
		s := newTestServer(f)

		require.Error(t, s.Shutdown(t.Context()))
		require.Eventually(t, func() bool { return f.closes.Load() == 1 }, 2*time.Second, time.Millisecond)
	})

	t.Run("Listen", func(t *testing.T) {
		t.Parallel()

		funcs := newFakeServer().funcs()
		funcs.Listen = func(context.Context) error { panic("listen boom") }
		s := lifecycle.NewServer(funcs)

		require.ErrorContains(t, s.Start(t.Context()), "lifecycle: listen panicked: listen boom")
		require.NoError(t, s.Shutdown(t.Context()))
	})
}

func TestNewServer_PanicsWithoutRequiredFuncs(t *testing.T) {
	t.Parallel()

	assert.Panics(t, func() { lifecycle.NewServer(lifecycle.ServerFuncs{}) })
}

func TestDrainBlocking(t *testing.T) {
	t.Parallel()

	require.NoError(t, lifecycle.DrainBlocking(func() {})(t.Context()))

	release := make(chan struct{})
	defer close(release)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, lifecycle.DrainBlocking(func() { <-release })(ctx), context.DeadlineExceeded)
}
