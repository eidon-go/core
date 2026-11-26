package app_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/app"
)

// barrierServer's Shutdown waits until every server sharing the barrier is
// in Shutdown: it completes only if the runners are shut down concurrently.
type barrierServer struct {
	*server

	barrier *sync.WaitGroup
}

func (s *barrierServer) Shutdown(ctx context.Context) error {
	s.barrier.Done()

	released := make(chan struct{})

	go func() {
		s.barrier.Wait()
		close(released)
	}()

	select {
	case <-released:
		return s.server.Shutdown(ctx)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestShutdown_RunnersConcurrently(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	barrier := &sync.WaitGroup{}
	barrier.Add(2)

	first := &barrierServer{server: newServer("first", rec), barrier: barrier}
	second := &barrierServer{server: newServer("second", rec), barrier: barrier}

	a := newApp(app.WithShutdownTimeout(10 * time.Second))
	addAll(a, first, second)

	ctx, cancel := context.WithCancel(t.Context())
	done := runAsync(ctx, a)

	<-first.running
	<-second.running
	cancel()

	require.NoError(t, wait(t, done), "sequential shutdown would deadlock on the barrier until the timeout")
	assert.ElementsMatch(t, []string{"shutdown first", "shutdown second"}, rec.list())
}

// forceClosedServer drains on ctx cancellation until Shutdown force-closes it
// at the deadline; Run then returns a moment after Shutdown — like a server
// whose serve loop notices the closed connections.
type forceClosedServer struct {
	running chan struct{}
	forced  chan struct{}
	once    sync.Once
}

func (s *forceClosedServer) Run(ctx context.Context) error {
	close(s.running)
	<-ctx.Done()
	<-s.forced
	time.Sleep(5 * time.Millisecond) // the serve loop winds down after the force-close

	return nil
}

func (s *forceClosedServer) Shutdown(ctx context.Context) error {
	<-ctx.Done()
	s.once.Do(func() { close(s.forced) })

	return ctx.Err()
}

func TestShutdown_RunnerReturningAfterForceCloseIsNotStuck(t *testing.T) {
	t.Parallel()

	srv := &forceClosedServer{running: make(chan struct{}), forced: make(chan struct{})}

	a := newApp(app.WithShutdownTimeout(20 * time.Millisecond))
	a.Add(srv)

	ctx, cancel := context.WithCancel(t.Context())
	done := runAsync(ctx, a)

	<-srv.running
	cancel()

	err := wait(t, done)
	require.ErrorIs(t, err, app.ErrShutdownTimeout, "the forced Shutdown is reported")
	assert.NotContains(t, err.Error(), "still running", "Run returned within the force-close grace")
}

// ctxWorker follows the contract by stopping when its ctx is cancelled.
type ctxWorker struct {
	running  chan struct{}
	returned atomic.Bool
}

func newCtxWorker() *ctxWorker { return &ctxWorker{running: make(chan struct{})} }

func (w *ctxWorker) Run(ctx context.Context) error {
	close(w.running)
	<-ctx.Done()
	w.returned.Store(true)

	return nil
}

func (w *ctxWorker) Shutdown(context.Context) error { return nil }

// The shutdown delay must keep contract-following runners running: they stop
// only when shutdown starts, after the delay.
func TestShutdownDelay_KeepsRunnersRunning(t *testing.T) {
	t.Parallel()

	w := newCtxWorker()

	a := newApp(app.WithShutdownDelay(300 * time.Millisecond))
	a.Add(w)

	ctx, cancel := context.WithCancel(t.Context())
	done := runAsync(ctx, a)

	<-w.running
	cancel()

	eventually(t, func() bool { return errors.Is(a.Healthcheck(t.Context()), app.ErrNotReady) })
	time.Sleep(100 * time.Millisecond)
	assert.False(t, w.returned.Load(), "runner must keep running during the delay")

	require.NoError(t, wait(t, done))
	assert.True(t, w.returned.Load())
}

func TestShutdownDelay_EndsWhenRunnerFails(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	fail := make(chan struct{})
	r := &failOnSignal{fail: fail, err: errBoom}

	a := newApp(app.WithShutdownDelay(time.Hour))
	a.Add(r)

	ctx, cancel := context.WithCancel(t.Context())
	done := runAsync(ctx, a)

	eventually(t, func() bool { return a.Healthcheck(t.Context()) == nil })
	cancel()
	eventually(t, func() bool { return errors.Is(a.Healthcheck(t.Context()), app.ErrNotReady) })
	close(fail)

	require.ErrorIs(t, wait(t, done), errBoom)
}

type failOnSignal struct {
	fail chan struct{}
	err  error
}

func (f *failOnSignal) Run(ctx context.Context) error {
	select {
	case <-f.fail:
		return f.err
	case <-ctx.Done():
		return nil
	}
}

func (f *failOnSignal) Shutdown(context.Context) error { return nil }

// lazyClient sets its client in Start and dereferences it in Shutdown: it
// assumes Shutdown never follows a failed or skipped Start.
type lazyClient struct{ conn *struct{ open bool } }

func (l *lazyClient) Name() string { return "lazy" }

func (l *lazyClient) Start(context.Context) error {
	l.conn = &struct{ open bool }{open: true}

	return nil
}

func (l *lazyClient) Shutdown(context.Context) error {
	l.conn.open = false

	return nil
}

// A component that panics in Shutdown after a failed startup does not crash
// the process or hide the start error, and the others are still shut down.
func TestRun_PanickingShutdownAfterFailedStartIsReported(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	errStart := errors.New("no connection")
	first := &resource{name: "first", rec: rec}
	failing := &starter{resource: resource{name: "failing", rec: rec}, startErr: errStart}

	a := newApp()
	addAll(a, first, failing, &lazyClient{})

	err := wait(t, runAsync(t.Context(), a))
	require.ErrorIs(t, err, errStart)
	require.ErrorIs(t, err, app.ErrShutdownPanicked)
	require.ErrorContains(t, err, "shutdown lazy")
	assert.Contains(t, rec.list(), "shutdown first")
}
