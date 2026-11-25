package lifecycle_test

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
)

// blockingLoop returns a loop that runs until stop is done.
func blockingLoop(stop, _ context.Context) error {
	<-stop.Done()

	return stop.Err()
}

func runWorker(ctx context.Context, w *lifecycle.Worker) <-chan error {
	done := make(chan error, 1)

	go func() { done <- w.Run(ctx) }()

	return done
}

func waitRunning(t *testing.T, w *lifecycle.Worker) {
	t.Helper()

	require.Eventually(t, func() bool { return w.Healthcheck(t.Context()) == nil }, 2*time.Second, time.Millisecond)
}

// inFlightLoop takes one item of work at once, then handles it on work until
// release is closed or work is cancelled; ended receives why it ended.
type inFlightLoop struct {
	taken   chan struct{}
	release chan struct{}
	ended   chan error
}

func newInFlightLoop() *inFlightLoop {
	return &inFlightLoop{taken: make(chan struct{}), release: make(chan struct{}), ended: make(chan error, 1)}
}

func (l *inFlightLoop) run(_, work context.Context) error {
	close(l.taken)

	select {
	case <-l.release:
		l.ended <- nil
	case <-work.Done():
		l.ended <- work.Err()
	}

	return nil
}

func TestWorker_Contract(t *testing.T) {
	t.Parallel()

	lifecycletest.Runner(t, func(*testing.T) lifecycle.Runner {
		return lifecycle.NewWorker(lifecycle.WorkerFuncs{Run: blockingLoop})
	})
}

func TestWorker_StateErrors(t *testing.T) {
	t.Parallel()

	t.Run("Run twice", func(t *testing.T) {
		t.Parallel()

		w := lifecycle.NewWorker(lifecycle.WorkerFuncs{Run: blockingLoop})
		done := runWorker(t.Context(), w)
		waitRunning(t, w)

		require.ErrorIs(t, w.Run(t.Context()), lifecycle.ErrAlreadyStarted)
		require.NoError(t, w.Shutdown(t.Context()))
		require.NoError(t, <-done)
	})

	t.Run("Run after Shutdown does not run the loop", func(t *testing.T) {
		t.Parallel()

		var ran atomic.Bool

		w := lifecycle.NewWorker(lifecycle.WorkerFuncs{Run: func(stop, work context.Context) error {
			ran.Store(true)

			return blockingLoop(stop, work)
		}})

		require.NoError(t, w.Shutdown(t.Context()))
		require.NoError(t, w.Run(t.Context()))
		assert.False(t, ran.Load())
	})
}

func TestWorker_Healthcheck(t *testing.T) {
	t.Parallel()

	w := lifecycle.NewWorker(lifecycle.WorkerFuncs{Run: blockingLoop})
	require.ErrorIs(t, w.Healthcheck(t.Context()), lifecycle.ErrNotRunning)

	ctx, cancel := context.WithCancel(t.Context())
	done := runWorker(ctx, w)
	waitRunning(t, w)

	cancel()
	require.NoError(t, <-done)
	require.ErrorIs(t, w.Healthcheck(t.Context()), lifecycle.ErrNotRunning)
	require.NoError(t, w.Shutdown(t.Context()))
}

// Cancelling Run's ctx stops taking work, but the work in flight finishes on
// its own context; Run waits for it.
func TestWorker_InFlightWorkFinishesAfterStop(t *testing.T) {
	t.Parallel()

	l := newInFlightLoop()
	w := lifecycle.NewWorker(lifecycle.WorkerFuncs{Run: l.run})

	ctx, cancel := context.WithCancel(t.Context())
	done := runWorker(ctx, w)

	<-l.taken

	cancel()
	require.Eventually(t, func() bool { return w.Healthcheck(t.Context()) != nil }, 2*time.Second, time.Millisecond)

	select {
	case err := <-done:
		t.Fatalf("Run returned (%v) before the work in flight finished", err)
	default:
	}

	close(l.release)
	require.NoError(t, <-done)
	require.NoError(t, <-l.ended, "the work was not cancelled")
	require.NoError(t, w.Shutdown(t.Context()))
}

// Shutdown's deadline cancels the work context; Run returns once the loop has.
func TestWorker_ShutdownDeadlineCancelsWork(t *testing.T) {
	t.Parallel()

	l := newInFlightLoop()
	w := lifecycle.NewWorker(lifecycle.WorkerFuncs{Run: l.run})
	done := runWorker(t.Context(), w)

	<-l.taken

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, w.Shutdown(ctx), context.DeadlineExceeded)
	require.NoError(t, <-done)
	require.ErrorIs(t, <-l.ended, context.Canceled)
	require.NoError(t, w.Shutdown(t.Context()), "a Shutdown after the cutoff returns at once")
}

// A loop that ignores both contexts is abandoned at Shutdown's deadline: Run
// and Shutdown still return.
func TestWorker_StuckLoopIsAbandoned(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	defer close(release)

	started := make(chan struct{})
	w := lifecycle.NewWorker(lifecycle.WorkerFuncs{Run: func(context.Context, context.Context) error {
		close(started)
		<-release

		return nil
	}})
	done := runWorker(t.Context(), w)

	<-started

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, w.Shutdown(ctx), context.DeadlineExceeded)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Run did not return after the cutoff")
	}
}

func TestWorker_LoopErrorFailsRun(t *testing.T) {
	t.Parallel()

	errLoop := errors.New("fetch failed")
	w := lifecycle.NewWorker(
		lifecycle.WorkerFuncs{Run: func(context.Context, context.Context) error { return errLoop }},
	)

	require.ErrorIs(t, w.Run(t.Context()), errLoop)
	require.ErrorIs(t, w.Healthcheck(t.Context()), lifecycle.ErrNotRunning)
	require.NoError(t, w.Shutdown(t.Context()))
}

// context.Canceled is a clean stop only once the stop has begun.
func TestWorker_CanceledBeforeStopIsAnError(t *testing.T) {
	t.Parallel()

	w := lifecycle.NewWorker(lifecycle.WorkerFuncs{Run: func(context.Context, context.Context) error {
		return context.Canceled
	}})

	require.ErrorIs(t, w.Run(t.Context()), context.Canceled)
}

func TestWorker_PanicBecomesError(t *testing.T) {
	t.Parallel()

	w := lifecycle.NewWorker(lifecycle.WorkerFuncs{Run: func(context.Context, context.Context) error { panic("boom") }})

	err := w.Run(t.Context())
	require.ErrorContains(t, err, "lifecycle: run panicked: boom")
	require.NoError(t, w.Shutdown(t.Context()))
}

func TestWorker_WorkKeepsRunContextValues(t *testing.T) {
	t.Parallel()

	type key struct{}

	got := make(chan any, 2)
	w := lifecycle.NewWorker(lifecycle.WorkerFuncs{Run: func(stop, work context.Context) error {
		got <- stop.Value(key{})

		got <- work.Value(key{})

		return nil
	}})

	require.NoError(t, w.Run(context.WithValue(t.Context(), key{}, "v")))
	assert.Equal(t, "v", <-got)
	assert.Equal(t, "v", <-got)
}

func TestWorker_Logs(t *testing.T) {
	t.Parallel()

	t.Run("graceful", func(t *testing.T) {
		t.Parallel()

		var buf syncBuffer

		w := lifecycle.NewWorker(
			lifecycle.WorkerFuncs{Run: blockingLoop, Logger: slog.New(slog.NewTextHandler(&buf, nil))},
		)
		done := runWorker(t.Context(), w)
		waitRunning(t, w)

		require.NoError(t, w.Shutdown(t.Context()))
		require.NoError(t, <-done)
		assert.Contains(t, buf.String(), `msg="worker stopping"`)
		assert.Contains(t, buf.String(), `msg="worker stopped" forced=false`)
	})

	t.Run("forced", func(t *testing.T) {
		t.Parallel()

		var buf syncBuffer

		l := newInFlightLoop()
		w := lifecycle.NewWorker(lifecycle.WorkerFuncs{Run: l.run, Logger: slog.New(slog.NewTextHandler(&buf, nil))})
		done := runWorker(t.Context(), w)

		<-l.taken

		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()

		require.ErrorIs(t, w.Shutdown(ctx), context.DeadlineExceeded)
		require.NoError(t, <-done)
		assert.Contains(t, buf.String(), `msg="worker stopped" forced=true`)
		assert.NotContains(t, buf.String(), "forced=false")
	})
}

func TestNewWorker_PanicsWithoutRun(t *testing.T) {
	t.Parallel()

	assert.Panics(t, func() { lifecycle.NewWorker(lifecycle.WorkerFuncs{}) })
}
