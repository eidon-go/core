// Package lifecycletest checks that components follow the lifecycle contract
// (see package lifecycle). Integration modules run it against their
// components so that every component behaves the same under app.App:
//
//	func TestServerContract(t *testing.T) {
//	    lifecycletest.Runner(t, func(t *testing.T) lifecycle.Runner {
//	        return newTestServer(t)
//	    }, lifecycletest.WithTimeout(30*time.Second))
//	}
//
// Every check builds a fresh component with the given constructor.
package lifecycletest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/eidon-go/core/lifecycle"
)

// DefaultTimeout bounds every wait in the checks unless WithTimeout sets
// another value.
const DefaultTimeout = 10 * time.Second

// settle is how long a check lets Run get going before stopping it.
const settle = 50 * time.Millisecond

// forceCloseBound bounds how long Run may take to return once a Shutdown
// whose ctx was done has returned. app.App waits 100ms before it reports the
// runner as still running; the margin covers slow CI machines.
const forceCloseBound = time.Second

var (
	errReturnedEarly = errors.New("Run returned early; it must block until Shutdown or ctx cancellation")
	errTimeout       = errors.New("did not return in time")
	errPanicked      = errors.New("panicked")
)

// Option configures the checks.
type Option func(*options)

type options struct {
	timeout time.Duration
}

// WithTimeout bounds every wait in the checks (default DefaultTimeout): a
// component that takes longer to return from Start, Run, Shutdown or
// Healthcheck fails the check. Raise it for components that start slowly,
// e.g. against a container.
func WithTimeout(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.timeout = d
		}
	}
}

func newOptions(opts []Option) options {
	o := options{timeout: DefaultTimeout}
	for _, opt := range opts {
		opt(&o)
	}

	return o
}

// Resource checks the Resource contract, plus the Starter and Healthchecker
// contracts when the component implements them:
//
//   - Shutdown without Start succeeds.
//   - Shutdown with an already cancelled ctx returns promptly (any error).
//   - Start followed by Shutdown succeeds.
//   - Start with an already cancelled ctx returns promptly (any error), and
//     the Shutdown that follows succeeds: app.App shuts down every component
//     after a failed startup, including the one whose Start failed. This
//     reaches the failure path only of a Start that honors ctx; other
//     failures (a busy port, a refused connection) need tests of their own.
//   - A second Shutdown succeeds (Shutdown is idempotent).
//   - Healthcheck returns promptly (any result) before Start and after
//     Shutdown.
//
// A panic in any method fails the check.
func Resource(t *testing.T, newResource func(t *testing.T) lifecycle.Resource, opts ...Option) {
	t.Helper()

	o := newOptions(opts)

	t.Run("ShutdownWithoutStart", func(t *testing.T) {
		fail(t, checkShutdownWithoutStart(t.Context(), newResource(t), o.timeout))
	})

	t.Run("ShutdownWithCancelledContext", func(t *testing.T) {
		fail(t, checkShutdownCancelled(t.Context(), newResource(t), o.timeout))
	})

	t.Run("StartThenShutdown", func(t *testing.T) {
		fail(t, checkStartThenShutdown(t.Context(), newResource(t), o.timeout))
	})

	t.Run("ShutdownAfterCancelledStart", func(t *testing.T) {
		fail(t, checkShutdownAfterCancelledStart(t.Context(), newResource(t), o.timeout))
	})

	t.Run("ShutdownTwice", func(t *testing.T) {
		fail(t, checkShutdownTwice(t.Context(), newResource(t), o.timeout))
	})

	t.Run("HealthcheckAroundLifecycle", func(t *testing.T) {
		fail(t, checkHealthcheck(t.Context(), newResource(t), o.timeout))
	})
}

// Runner checks the Runner contract on top of Resource:
//
//   - Run keeps running until Shutdown, then returns nil.
//   - Run keeps running until its ctx is cancelled, then returns nil (or
//     context.Canceled).
//   - Run called after Shutdown returns nil promptly.
//   - The app.App stop order: Run's ctx is cancelled, then Shutdown is
//     called with an already expired ctx; Shutdown returns promptly (any
//     error) and Run returns nil.
//   - Cancelling Run's ctx and calling Shutdown at the same time: both
//     return, Run with nil, Shutdown with nil.
//
// For a Starter, Start is called before Run in every check.
func Runner(t *testing.T, newRunner func(t *testing.T) lifecycle.Runner, opts ...Option) {
	t.Helper()

	o := newOptions(opts)

	Resource(t, func(t *testing.T) lifecycle.Resource {
		t.Helper()

		return newRunner(t)
	}, opts...)

	t.Run("ShutdownStopsRun", func(t *testing.T) {
		fail(t, checkShutdownStopsRun(t.Context(), newRunner(t), o.timeout))
	})

	t.Run("CancelStopsRun", func(t *testing.T) {
		fail(t, checkCancelStopsRun(t.Context(), newRunner(t), o.timeout))
	})

	t.Run("RunAfterShutdown", func(t *testing.T) {
		fail(t, checkRunAfterShutdown(t.Context(), newRunner(t), o.timeout))
	})

	t.Run("CancelThenExpiredShutdown", func(t *testing.T) {
		fail(t, checkCancelThenExpiredShutdown(t.Context(), newRunner(t), o.timeout))
	})

	t.Run("ConcurrentCancelAndShutdown", func(t *testing.T) {
		fail(t, checkConcurrentCancelAndShutdown(t.Context(), newRunner(t), o.timeout))
	})
}

func fail(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatal(err)
	}
}

func checkShutdownWithoutStart(ctx context.Context, r lifecycle.Resource, timeout time.Duration) error {
	return within("Shutdown", timeout, func() error { return r.Shutdown(ctx) })
}

func checkShutdownCancelled(ctx context.Context, r lifecycle.Resource, timeout time.Duration) error {
	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	// Any error is acceptable (typically ctx.Err()); hanging is not.
	return within("Shutdown with a cancelled ctx", timeout, func() error {
		_ = r.Shutdown(cancelled)

		return nil
	})
}

func checkStartThenShutdown(ctx context.Context, r lifecycle.Resource, timeout time.Duration) error {
	if err := start(ctx, r, timeout); err != nil {
		return err
	}

	return within("Shutdown after Start", timeout, func() error { return r.Shutdown(ctx) })
}

func checkShutdownAfterCancelledStart(ctx context.Context, r lifecycle.Resource, timeout time.Duration) error {
	if s, ok := r.(lifecycle.Starter); ok {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()

		// Start may fail (typically) or succeed; hanging is not acceptable.
		err := within("Start with a cancelled ctx", timeout, func() error {
			_ = s.Start(cancelled)

			return nil
		})
		if err != nil {
			return err
		}
	}

	return within("Shutdown after a cancelled Start", timeout, func() error { return r.Shutdown(ctx) })
}

func checkShutdownTwice(ctx context.Context, r lifecycle.Resource, timeout time.Duration) error {
	if err := start(ctx, r, timeout); err != nil {
		return err
	}

	if err := within("Shutdown", timeout, func() error { return r.Shutdown(ctx) }); err != nil {
		return err
	}

	return within("second Shutdown", timeout, func() error { return r.Shutdown(ctx) })
}

func checkHealthcheck(ctx context.Context, r lifecycle.Resource, timeout time.Duration) error {
	checker, ok := r.(lifecycle.Healthchecker)
	if !ok {
		return nil
	}

	check := func(when string) error {
		return within("Healthcheck "+when, timeout, func() error {
			_ = checker.Healthcheck(ctx) // any result; it must just return

			return nil
		})
	}

	return errors.Join(
		check("before Start"),
		start(ctx, r, timeout),
		within("Shutdown", timeout, func() error { return r.Shutdown(ctx) }),
		check("after Shutdown"),
	)
}

func checkShutdownStopsRun(ctx context.Context, r lifecycle.Runner, timeout time.Duration) error {
	if err := start(ctx, r, timeout); err != nil {
		return err
	}

	done := runAsync(ctx, r)
	if err := stillRunning("before Shutdown", done); err != nil {
		return err
	}

	if err := within("Shutdown", timeout, func() error { return r.Shutdown(ctx) }); err != nil {
		return err
	}

	return returned("Run after Shutdown", done, timeout)
}

func checkCancelStopsRun(ctx context.Context, r lifecycle.Runner, timeout time.Duration) error {
	if err := start(ctx, r, timeout); err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := runAsync(runCtx, r)
	if err := stillRunning("before cancel", done); err != nil {
		return err
	}

	cancel()

	if err := returned("Run after cancel", done, timeout); err != nil {
		return err
	}

	return within("Shutdown after Run returned", timeout, func() error { return r.Shutdown(ctx) })
}

func checkRunAfterShutdown(ctx context.Context, r lifecycle.Runner, timeout time.Duration) error {
	if err := start(ctx, r, timeout); err != nil {
		return err
	}

	if err := within("Shutdown", timeout, func() error { return r.Shutdown(ctx) }); err != nil {
		return err
	}

	return returned("Run after Shutdown", runAsync(ctx, r), timeout)
}

// checkCancelThenExpiredShutdown follows app.App at its shutdown deadline:
// Run's ctx is cancelled, then Shutdown gets a ctx that is already done and
// must force-close.
func checkCancelThenExpiredShutdown(ctx context.Context, r lifecycle.Runner, timeout time.Duration) error {
	if err := start(ctx, r, timeout); err != nil {
		return err
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	done := runAsync(runCtx, r)
	if err := stillRunning("before cancel", done); err != nil {
		return err
	}

	cancelRun()

	expired, expire := context.WithCancel(ctx)
	expire()

	// Any error is acceptable from a Shutdown whose ctx is done; hanging or
	// panicking is not.
	if err := within("Shutdown with an expired ctx after cancel", timeout, func() error {
		_ = r.Shutdown(expired)

		return nil
	}); err != nil {
		return err
	}

	return returned("Run after cancel and an expired Shutdown", done, min(timeout, forceCloseBound))
}

func checkConcurrentCancelAndShutdown(ctx context.Context, r lifecycle.Runner, timeout time.Duration) error {
	if err := start(ctx, r, timeout); err != nil {
		return err
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	done := runAsync(runCtx, r)
	if err := stillRunning("before cancel and Shutdown", done); err != nil {
		return err
	}

	go cancelRun()

	return errors.Join(
		within("Shutdown concurrent with cancel", timeout, func() error { return r.Shutdown(ctx) }),
		returned("Run after concurrent cancel and Shutdown", done, timeout),
	)
}

func start(ctx context.Context, r lifecycle.Resource, timeout time.Duration) error {
	s, ok := r.(lifecycle.Starter)
	if !ok {
		return nil
	}

	return within("Start", timeout, func() error { return s.Start(ctx) })
}

func runAsync(ctx context.Context, r lifecycle.Runner) <-chan error {
	done := make(chan error, 1)

	go func() { done <- protect(func() error { return r.Run(ctx) }) }()

	return done
}

// stillRunning fails if Run returns within the settle period: a Runner must
// block until it is stopped.
func stillRunning(when string, done <-chan error) error {
	select {
	case err := <-done:
		return fmt.Errorf("%w: returned %w %s", errReturnedEarly, err, when)
	case <-time.After(settle):
		return nil
	}
}

// returned expects Run to return nil (context.Canceled is accepted) in time.
func returned(what string, done <-chan error, timeout time.Duration) error {
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("%s: returned %w, want nil", what, err)
		}

		return nil
	case <-time.After(timeout):
		return fmt.Errorf("%s: %w (%v)", what, errTimeout, timeout)
	}
}

// within runs fn and fails if it does not return nil in time.
func within(what string, timeout time.Duration, fn func() error) error {
	done := make(chan error, 1)

	go func() { done <- protect(fn) }()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("%s: returned %w, want nil", what, err)
		}

		return nil
	case <-time.After(timeout):
		return fmt.Errorf("%s: %w (%v)", what, errTimeout, timeout)
	}
}

// protect turns a panic in fn into an error, so that a component that panics
// (closing a channel twice, say) fails the check instead of the test binary.
func protect(call func() error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%w: %v", errPanicked, p)
		}
	}()

	return call()
}
