package nats

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"

	"github.com/eidon-go/core/lifecycle"
)

// runner is the Runner side shared by Subscriber and Consumer: a
// lifecycle.Worker that runs the component's loop, and whether the loop is
// subscribed.
type runner struct {
	worker        *lifecycle.Worker
	subscribed    atomic.Bool
	notSubscribed error // Healthcheck's error while running unsubscribed
}

func newRunner(loop func(stop, work context.Context) error, logger *slog.Logger, notSubscribed error) *runner {
	return &runner{ //nolint:exhaustruct_v5 // subscribed starts false
		worker:        lifecycle.NewWorker(lifecycle.WorkerFuncs{Run: loop, Logger: logger}),
		notSubscribed: notSubscribed,
	}
}

// run runs the loop; its errors already carry the module prefix.
func (r *runner) run(ctx context.Context) error {
	err := r.worker.Run(ctx)
	if errors.Is(err, ErrStopped) || errors.Is(err, ErrClosed) || errors.Is(err, ErrNotStarted) {
		return err //nolint:wrapcheck // the loop's errors carry the module prefix
	}

	return wrap(err)
}

func (r *runner) shutdown(ctx context.Context) error {
	return wrap(r.worker.Shutdown(ctx))
}

func (r *runner) healthcheck(ctx context.Context) error {
	if err := r.worker.Healthcheck(ctx); err != nil {
		return wrap(err)
	}

	if !r.subscribed.Load() {
		return r.notSubscribed
	}

	return nil
}

// markSubscribed reports the loop as subscribed until the returned function
// is called.
func (r *runner) markSubscribed() func() {
	r.subscribed.Store(true)

	return func() { r.subscribed.Store(false) }
}
