// Package lifecycletest checks that components follow the lifecycle contract
// (see package lifecycle). Integration modules run it against their
// components so that every component behaves the same under app.App:
//
//	func TestServerContract(t *testing.T) {
//	    lifecycletest.Runner(t, func(t *testing.T) lifecycle.Runner {
//	        return newTestServer(t)
//	    })
//	}
//
// Every check builds a fresh component with the given constructor.
package lifecycletest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/eidon-go/core/lifecycle"
)

// Timeout bounds every wait in the checks: a component that takes longer to
// return from Start, Run or Shutdown fails the check.
const Timeout = 5 * time.Second

// settle is how long a check lets Run get going before stopping it.
const settle = 50 * time.Millisecond

// Resource checks the Resource contract (and the Starter contract when the
// component implements it):
//
//   - Shutdown without Start or Run succeeds.
//   - Shutdown with an already cancelled ctx returns promptly (any error).
//   - For a Starter: Start followed by Shutdown succeeds.
func Resource(t *testing.T, newResource func(t *testing.T) lifecycle.Resource) {
	t.Helper()

	t.Run("ShutdownWithoutStart", func(t *testing.T) {
		r := newResource(t)
		within(t, "Shutdown", func() error { return r.Shutdown(t.Context()) })
	})

	t.Run("ShutdownWithCancelledContext", func(t *testing.T) {
		r := newResource(t)

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		// Any error is acceptable (typically ctx.Err()); hanging is not.
		within(t, "Shutdown", func() error {
			_ = r.Shutdown(ctx)

			return nil
		})
	})

	if _, ok := newResource(t).(lifecycle.Starter); ok {
		t.Run("StartThenShutdown", func(t *testing.T) {
			r := newResource(t)
			s, _ := r.(lifecycle.Starter)

			within(t, "Start", func() error { return s.Start(t.Context()) })
			within(t, "Shutdown", func() error { return r.Shutdown(t.Context()) })
		})
	}
}

// Runner checks the Runner contract on top of Resource:
//
//   - Shutdown makes a running Run return nil.
//   - Cancelling the ctx passed to Run makes it return nil (or
//     context.Canceled).
//   - Run called after Shutdown returns nil promptly.
//
// For a Starter, Start is called before Run in every check.
func Runner(t *testing.T, newRunner func(t *testing.T) lifecycle.Runner) {
	t.Helper()

	Resource(t, func(t *testing.T) lifecycle.Resource {
		t.Helper()

		return newRunner(t)
	})

	t.Run("ShutdownStopsRun", func(t *testing.T) {
		r := started(t, newRunner(t))
		done := runAsync(t.Context(), r)

		time.Sleep(settle)
		within(t, "Shutdown", func() error { return r.Shutdown(t.Context()) })
		wait(t, "Run after Shutdown", done)
	})

	t.Run("CancelStopsRun", func(t *testing.T) {
		r := started(t, newRunner(t))

		ctx, cancel := context.WithCancel(t.Context())
		done := runAsync(ctx, r)

		time.Sleep(settle)
		cancel()
		wait(t, "Run after cancel", done)
		within(t, "Shutdown", func() error { return r.Shutdown(t.Context()) })
	})

	t.Run("RunAfterShutdown", func(t *testing.T) {
		r := started(t, newRunner(t))

		within(t, "Shutdown", func() error { return r.Shutdown(t.Context()) })
		wait(t, "Run after Shutdown", runAsync(t.Context(), r))
	})
}

func started(t *testing.T, r lifecycle.Runner) lifecycle.Runner {
	t.Helper()

	if s, ok := r.(lifecycle.Starter); ok {
		within(t, "Start", func() error { return s.Start(t.Context()) })
	}

	return r
}

func runAsync(ctx context.Context, r lifecycle.Runner) <-chan error {
	done := make(chan error, 1)

	go func() { done <- r.Run(ctx) }()

	return done
}

// wait expects Run to return nil (context.Canceled is accepted) in time.
func wait(t *testing.T, what string, done <-chan error) {
	t.Helper()

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("%s: returned %v, want nil", what, err)
		}
	case <-time.After(Timeout):
		t.Fatalf("%s: did not return within %v", what, Timeout)
	}
}

// within runs fn and fails the test if it does not return nil in time.
func within(t *testing.T, what string, fn func() error) {
	t.Helper()

	done := make(chan error, 1)

	go func() { done <- fn() }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s: returned %v, want nil", what, err)
		}
	case <-time.After(Timeout):
		t.Fatalf("%s: did not return within %v", what, Timeout)
	}
}
