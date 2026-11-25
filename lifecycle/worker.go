package lifecycle

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
)

// ErrNotRunning is returned by Worker.Healthcheck while the worker is not
// running: before Run, once the stop has begun and after its loop has
// returned.
var ErrNotRunning = errors.New("lifecycle: not running")

// WorkerFuncs describes a worker — a message consumer, a scheduler, a poll
// loop — by the loop that does its work; see NewWorker.
type WorkerFuncs struct {
	// Run does the work until the worker is stopped (required). It gets two
	// contexts:
	//
	//   - stop is cancelled when the stop begins: Run's ctx cancelled or
	//     Shutdown called. The loop then takes no new work (it fetches with
	//     stop, so the fetch returns at once).
	//   - work is for the work in flight — the message being handled, a
	//     final commit. It keeps the values of Run's ctx and is cancelled
	//     only when Shutdown's deadline passes, so that work can finish
	//     after the stop has begun but not beyond the bound of the stop.
	//
	// Run returns once the work in flight has finished, with nil; an error
	// that is context.Canceled after the stop has begun counts as nil.
	// Returning before the stop — with nil or an error — ends the worker,
	// and app.App shuts the application down. A panic is recovered and
	// turned into an error.
	Run func(stop, work context.Context) error

	// Logger, if set, logs the start of the stop ("worker stopping") and its
	// end ("worker stopped", with forced). Pass the component's logger: it
	// already carries the component's name.
	Logger *slog.Logger
}

// Worker implements the stop state machine of the lifecycle contract for a
// loop that takes work (fetches messages, waits for ticks) and handles it:
// the cancellation of Run's ctx and Shutdown make one stop, during which the
// work in flight finishes on its own context until Shutdown's deadline cuts
// it off. It is a Runner and Healthchecker.
//
// It takes care of: Run and Shutdown in any order and from any goroutine, a
// single stop, Run after Shutdown returning nil at once, Run returning once
// the loop has returned or the stop has been cut off, Shutdown returning at
// its deadline even when the loop ignores work's cancellation (the loop is
// then abandoned), and Healthcheck reporting ErrNotRunning from the moment
// the stop begins.
//
// What the worker uses — a client, a connection — is not released by the
// Worker: the component's Shutdown calls Worker.Shutdown first and releases
// its client afterwards (typically through a Releaser), also when Run was
// never called. Connecting belongs in the component's Start.
//
// A component usually keeps a *Worker in an unexported field and delegates
// Run, Shutdown and Healthcheck to it; embedding would make every method
// added to Worker later part of the component's API.
type Worker struct {
	funcs WorkerFuncs

	mu      sync.Mutex
	running bool // Run has been claimed
	closed  bool // Shutdown has been called

	healthy atomic.Bool

	cancelStop context.CancelFunc // set when Run is claimed
	cancelWork context.CancelFunc // set when Run is claimed

	stopOnce sync.Once
	stopping chan struct{} // closed when the stop begins
	done     chan struct{} // closed when the loop has returned

	cutoffOnce sync.Once
	cutoff     chan struct{} // closed when the stop was cut off at Shutdown's deadline
}

// NewWorker returns a Worker for funcs. It panics if Run is nil.
func NewWorker(funcs WorkerFuncs) *Worker {
	if funcs.Run == nil {
		panic("lifecycle: NewWorker: Run is required")
	}

	return &Worker{ //nolint:exhaustruct_v5 // state starts at zero
		funcs:    funcs,
		stopping: make(chan struct{}),
		done:     make(chan struct{}),
		cutoff:   make(chan struct{}),
	}
}

// Run runs the loop until the worker is stopped, then returns once the loop
// has returned or the stop has been cut off by Shutdown's deadline.
// Cancelling ctx begins the stop, with no deadline of Run's own: Shutdown
// bounds it. Run returns nil at once after Shutdown, ErrAlreadyStarted when
// called twice, and the error of the loop. Implements Runner.
func (w *Worker) Run(ctx context.Context) error {
	stop, work, ok, err := w.claimRun(ctx)
	if !ok {
		return err
	}

	stopOnCancel := context.AfterFunc(ctx, func() { w.beginStop(ctx) })
	defer stopOnCancel()

	result := make(chan error, 1)

	go func() {
		err := protect("run", func() error { return w.funcs.Run(stop, work) })
		if errors.Is(err, context.Canceled) && stop.Err() != nil {
			err = nil
		}

		w.finish(ctx)

		result <- err
	}()

	select {
	case err := <-result:
		return err
	case <-w.cutoff:
		return nil
	}
}

// Shutdown begins the stop, if Run's cancellation has not, and waits for the
// loop to return until ctx is done; then it cuts the stop off — work is
// cancelled, Run returns — and returns ctx.Err(). A loop that has returned
// wins over an expired ctx. Once the stop has been cut off, every Shutdown
// waiting then and every later one returns nil at once. It is safe without
// Run, and Run after it returns nil. Implements Resource.
func (w *Worker) Shutdown(ctx context.Context) error {
	w.mu.Lock()
	w.closed = true
	running := w.running
	w.mu.Unlock()

	if !running {
		return nil
	}

	// A loop that has already returned (it failed) has nothing to stop.
	select {
	case <-w.done:
		return nil
	default:
	}

	w.beginStop(ctx)

	select {
	case <-w.done:
		return nil
	case <-w.cutoff:
		return nil
	default:
	}

	select {
	case <-w.done:
		return nil
	case <-w.cutoff:
		return nil
	case <-ctx.Done():
		w.forceStop(ctx)

		return ctx.Err() //nolint:wrapcheck // the caller's own context error
	}
}

// Healthcheck returns nil while Run runs the loop, ErrNotRunning before
// that, once the stop has begun and after the loop has returned. Implements
// Healthchecker.
func (w *Worker) Healthcheck(context.Context) error {
	if w.healthy.Load() {
		return nil
	}

	return ErrNotRunning
}

// claimRun marks the worker as running and creates the loop's contexts, or
// reports why Run must return at once (with nil after Shutdown).
func (w *Worker) claimRun(ctx context.Context) (context.Context, context.Context, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	switch {
	case w.closed:
		return nil, nil, false, nil
	case w.running:
		return nil, nil, false, ErrAlreadyStarted
	}

	w.running = true
	w.healthy.Store(true)

	stop, cancelStop := context.WithCancel(ctx)
	work, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	w.cancelStop, w.cancelWork = cancelStop, cancelWork

	return stop, work, true, nil
}

// beginStop starts the one stop: the loop's stop context is cancelled. It is
// called only after claimRun, which sets cancelStop.
func (w *Worker) beginStop(ctx context.Context) {
	w.stopOnce.Do(func() {
		w.healthy.Store(false)
		close(w.stopping)
		w.cancelStop()

		w.log(ctx, "worker stopping")
	})
}

// finish runs when the loop has returned: the worker is no longer healthy,
// its contexts are released and Shutdown stops waiting.
func (w *Worker) finish(ctx context.Context) {
	w.healthy.Store(false)

	select {
	case <-w.stopping:
		select {
		case <-w.cutoff:
		default:
			w.log(ctx, "worker stopped", slog.Bool("forced", false))
		}
	default:
	}

	w.cancelStop()
	w.cancelWork()
	close(w.done)
}

// forceStop cuts the stop off once: work is cancelled and Run returns
// without waiting for the loop. It is called only after beginStop.
func (w *Worker) forceStop(ctx context.Context) {
	w.cutoffOnce.Do(func() {
		// cutoff first, so that a loop returning on work's cancellation
		// does not log a graceful stop.
		close(w.cutoff)
		w.cancelWork()

		w.log(ctx, "worker stopped", slog.Bool("forced", true))
	})
}

func (w *Worker) log(ctx context.Context, msg string, attrs ...any) {
	if w.funcs.Logger != nil {
		w.funcs.Logger.InfoContext(ctx, msg, attrs...)
	}
}

// Compile-time checks.
var (
	_ Runner        = (*Worker)(nil)
	_ Healthchecker = (*Worker)(nil)
)
