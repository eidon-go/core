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
	"github.com/eidon-go/core/lifecycle"
)

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

// Cancelling right after the app is running must always be a clean stop,
// whichever way the runners' return is observed.
func TestRun_CancelWhileRunningIsAlwaysClean(t *testing.T) {
	t.Parallel()

	for range 100 {
		w1, w2 := newCtxWorker(), newCtxWorker()

		a := newApp()
		a.Add(w1, w2)

		ctx, cancel := context.WithCancel(t.Context())
		done := runAsync(ctx, a)

		<-w1.running
		<-w2.running
		cancel()

		require.NoError(t, wait(t, done))
	}
}

type nop struct{}

func (*nop) Shutdown(context.Context) error { return nil }

type holder struct{ v any }

func (holder) Shutdown(context.Context) error { return nil }

func TestAdd_DuplicateDetectionEdgeCases(t *testing.T) {
	t.Parallel()

	assert.NotPanics(t, func() { newApp().Add(&nop{}, &nop{}) }, "distinct zero-size pointers may share an address")
	assert.NotPanics(t, func() { newApp().Add(holder{[]int{1}}, holder{[]int{2}}) }, "values are never compared")
	assert.NotPanics(t, func() { newApp().Add(holder{1}, holder{1}) }, "equal values are distinct components")
}

func TestRun_GroupWithoutStartersIsShutDownAfterStartFailure(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	errStart := errors.New("refused")
	failing := &starter{resource: resource{name: "db", rec: rec}, startErr: errStart}
	plain := &resource{name: "cache", rec: rec}

	a := newApp()
	a.Add(failing, lifecycle.GroupNamed("infra", plain))

	require.ErrorIs(t, wait(t, runAsync(t.Context(), a)), errStart)
	assert.Contains(t, rec.list(), "shutdown cache", "a group without starters holds resources and must be shut down")
}

func TestHealthReport_GroupWithoutCheckersIsNotListed(t *testing.T) {
	t.Parallel()

	a := newApp()
	a.Add(lifecycle.GroupNamed("infra", &resource{name: "cache", rec: &recorder{}}))

	assert.Empty(t, a.HealthReport(t.Context()))
}

// barrierChecker blocks until all checkers of a barrier have entered, which
// only happens if they run concurrently.
type barrierChecker struct {
	name    string
	entered *sync.WaitGroup
}

func (b *barrierChecker) Name() string                   { return b.name }
func (b *barrierChecker) Shutdown(context.Context) error { return nil }

func (b *barrierChecker) Healthcheck(ctx context.Context) error {
	b.entered.Done()

	done := make(chan struct{})

	go func() {
		b.entered.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestHealthReport_RunsChecksConcurrently(t *testing.T) {
	t.Parallel()

	var entered sync.WaitGroup

	entered.Add(3)

	a := newApp()
	a.Add(
		&barrierChecker{name: "a", entered: &entered},
		&barrierChecker{name: "b", entered: &entered},
		&barrierChecker{name: "c", entered: &entered},
	)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	for _, r := range a.HealthReport(ctx) {
		require.NoError(t, r.Err, "%s: checks must run concurrently", r.Name)
	}
}
