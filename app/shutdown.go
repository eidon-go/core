package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/eidon-go/core/lifecycle"
)

// forceCloseGrace is how long App still waits for a Shutdown after the
// shutdown timeout has expired: a Shutdown called with a done ctx is expected
// to force-close promptly, and gets this long to do it before it is
// abandoned.
const forceCloseGrace = 100 * time.Millisecond

// runWait lets shutdown wait for runners between the two shutdown phases.
type runWait struct {
	runners []lifecycle.Runner
	results chan runResult
	done    <-chan struct{}
	// first is the index of the runner whose return triggered shutdown
	// (already reported), or -1.
	first int
}

// shutdown shuts components down under one timeout: every runner
// concurrently, then (if wait is set) waits for their Run to return, then
// every other component in reverse order.
func (a *App) shutdown(ctx context.Context, components []lifecycle.Resource, wait *runWait) error {
	a.state.Store(int32(stateStopping))

	timeout := a.shutdownTimeout
	if timeout <= 0 {
		timeout = DefaultShutdownTimeout
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()

	errs := a.shutdownRunners(shutdownCtx, components)

	if wait != nil {
		errs = append(errs, wait.collect(shutdownCtx)...)
	}

	for _, c := range slices.Backward(components) {
		if _, ok := c.(lifecycle.Runner); !ok {
			errs = append(errs, a.shutdownOne(shutdownCtx, c, "resource"))
		}
	}

	err := errors.Join(errs...)
	if err != nil {
		a.Logger().ErrorContext(shutdownCtx, "app stopped with errors", slog.Any("err", err))
	} else {
		a.Logger().InfoContext(shutdownCtx, "app stopped")
	}

	return err
}

// shutdownRunners shuts every runner down concurrently, so that one runner's
// drain does not eat into the budget of the others: runners do not depend on
// each other, only on the resources shut down after them. The errors are in
// reverse registration order.
func (a *App) shutdownRunners(ctx context.Context, components []lifecycle.Resource) []error {
	var runners []lifecycle.Resource

	for _, c := range slices.Backward(components) {
		if _, ok := c.(lifecycle.Runner); ok {
			runners = append(runners, c)
		}
	}

	errs := make([]error, len(runners))

	var wg sync.WaitGroup

	for idx, r := range runners {
		wg.Go(func() { errs[idx] = a.shutdownOne(ctx, r, "runner") })
	}

	wg.Wait()

	return errs
}

// shutdownOne calls c.Shutdown and stops waiting forceCloseGrace after ctx
// expires: a Shutdown that ignores ctx is abandoned (its goroutine leaks) and
// reported with ErrShutdownTimeout, so one stuck component cannot block the
// rest. A Shutdown that panics is reported as ErrShutdownPanicked: Shutdown
// also runs after a failed startup, on components that were never started,
// and one faulty component must not hide the start error or skip the rest.
func (a *App) shutdownOne(ctx context.Context, c lifecycle.Resource, kind string) error {
	done := make(chan error, 1)
	begin := time.Now()

	go func() { done <- callShutdown(ctx, c) }()

	err := awaitShutdown(ctx, done, begin)

	a.observe(ctx, PhaseShutdown, c, time.Since(begin), err)

	if err != nil {
		name := lifecycle.NameOf(c)
		a.Logger().ErrorContext(ctx, kind+" shutdown failed", slog.String("component", name), slog.Any("err", err))

		return fmt.Errorf("shutdown %s: %w", name, err)
	}

	return nil
}

// callShutdown calls c.Shutdown, turning a panic into an error: it runs in its
// own goroutine, where an unrecovered panic ends the process.
func callShutdown(ctx context.Context, c lifecycle.Resource) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%w: %v", ErrShutdownPanicked, p)
		}
	}()

	return c.Shutdown(ctx) //nolint:wrapcheck // wrapped by shutdownOne with the component's name
}

func awaitShutdown(ctx context.Context, done <-chan error, begin time.Time) error {
	select {
	case err := <-done:
		return timeoutErr(ctx, err)
	case <-ctx.Done():
	}

	grace := time.NewTimer(forceCloseGrace)
	defer grace.Stop()

	select {
	case err := <-done:
		return timeoutErr(ctx, err)
	case <-grace.C:
		return fmt.Errorf("%w: abandoned after %v", ErrShutdownTimeout, time.Since(begin).Round(time.Millisecond))
	}
}

// timeoutErr marks an error returned after the deadline as a timeout.
func timeoutErr(ctx context.Context, err error) error {
	if err != nil && ctx.Err() != nil && !errors.Is(err, ErrShutdownTimeout) {
		return fmt.Errorf("%w: %w", ErrShutdownTimeout, err)
	}

	return err
}

// collect waits until every Run has returned or ctx expires, and returns the
// errors runners returned after shutdown was requested, plus
// ErrShutdownTimeout naming the runners that are still running. After ctx
// expires, runners get forceCloseGrace to return — the same grace their
// Shutdown gets to force-close, which is what makes Run return.
func (w *runWait) collect(ctx context.Context) []error {
	select {
	case <-w.done:
	case <-ctx.Done():
		grace := time.NewTimer(forceCloseGrace)

		select {
		case <-w.done:
		case <-grace.C:
		}

		grace.Stop()
	}

	// Indexes, not the runners themselves: a Runner may be a non-comparable
	// type and cannot be a map key.
	returned := make([]bool, len(w.runners))
	if w.first >= 0 {
		returned[w.first] = true
	}

	errs := w.drain(returned)

	var stuck []string

	for idx, r := range w.runners {
		if !returned[idx] {
			stuck = append(stuck, lifecycle.NameOf(r))
		}
	}

	if len(stuck) > 0 {
		errs = append(errs, fmt.Errorf("%w: still running: %s", ErrShutdownTimeout, strings.Join(stuck, ", ")))
	}

	return errs
}

// drain reads the results available without blocking, marks their runners as
// returned and returns their errors.
func (w *runWait) drain(returned []bool) []error {
	var errs []error

	for {
		select {
		case res := <-w.results:
			returned[res.idx] = true

			if err := runErr(res, false); err != nil {
				errs = append(errs, err)
			}
		default:
			return errs
		}
	}
}
