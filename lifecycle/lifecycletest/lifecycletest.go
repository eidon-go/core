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

var (
	errReturnedEarly = errors.New("Run returned early; it must block until Shutdown or ctx cancellation")
	errTimeout       = errors.New("did not return in time")
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
//   - Healthcheck returns promptly (any result) before Start and after
//     Shutdown.
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

func start(ctx context.Context, r lifecycle.Resource, timeout time.Duration) error {
	s, ok := r.(lifecycle.Starter)
	if !ok {
		return nil
	}

	return within("Start", timeout, func() error { return s.Start(ctx) })
}

func runAsync(ctx context.Context, r lifecycle.Runner) <-chan error {
	done := make(chan error, 1)

	go func() { done <- r.Run(ctx) }()

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

	go func() { done <- fn() }()

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
