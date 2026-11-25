package app_test

import (
	"context"
	"sync"
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
	a.Add(first, second)

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
