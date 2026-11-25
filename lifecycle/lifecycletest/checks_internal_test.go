package lifecycletest

import (
	"context"
	"errors"
	"testing"
	"time"
)

const short = 200 * time.Millisecond

// earlyRunner returns from Run at once: a contract violation.
type earlyRunner struct{}

func (earlyRunner) Run(context.Context) error      { return nil }
func (earlyRunner) Shutdown(context.Context) error { return nil }

// deafRunner ignores both Shutdown and ctx.
type deafRunner struct{}

func (deafRunner) Run(context.Context) error      { select {} }
func (deafRunner) Shutdown(context.Context) error { return nil }

// erroringRunner returns an error after Shutdown.
type erroringRunner struct{ stop chan struct{} }

func (r *erroringRunner) Run(ctx context.Context) error {
	select {
	case <-r.stop:
	case <-ctx.Done():
	}

	return errors.New("server closed")
}

func (r *erroringRunner) Shutdown(context.Context) error {
	select {
	case <-r.stop:
	default:
		close(r.stop)
	}

	return nil
}

// stuckShutdown ignores its ctx.
type stuckShutdown struct{}

func (stuckShutdown) Shutdown(context.Context) error {
	time.Sleep(time.Hour)

	return nil
}

// stuckShutdownRunner stops on ctx, but its Shutdown ignores ctx.
type stuckShutdownRunner struct{ stuckShutdown }

func (stuckShutdownRunner) Run(ctx context.Context) error {
	<-ctx.Done()

	return nil
}

// closedOnce fails a second Shutdown: not idempotent.
type closedOnce struct{ closed bool }

func (c *closedOnce) Shutdown(context.Context) error {
	if c.closed {
		return errors.New("already closed")
	}

	c.closed = true

	return nil
}

// doubleStopRunner stops both on ctx and on Shutdown by closing the same
// channel without a guard: cancellation and Shutdown together panic.
type doubleStopRunner struct{ stop chan struct{} }

func (r *doubleStopRunner) Run(ctx context.Context) error {
	select {
	case <-ctx.Done():
		close(r.stop)
	case <-r.stop:
	}

	return nil
}

func (r *doubleStopRunner) Shutdown(context.Context) error {
	<-r.stop // waits for Run's stop, then closes it again
	close(r.stop)

	return nil
}

// panickingShutdown panics in Shutdown.
type panickingShutdown struct{}

func (panickingShutdown) Shutdown(context.Context) error { panic("boom") }

func TestChecksDetectViolations(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	tests := []struct {
		name string
		err  error
	}{
		{"Run returns at once", checkShutdownStopsRun(ctx, earlyRunner{}, short)},
		{"Run ignores Shutdown", checkShutdownStopsRun(ctx, deafRunner{}, short)},
		{"Run ignores ctx", checkCancelStopsRun(ctx, deafRunner{}, short)},
		{"Run errors after Shutdown", checkShutdownStopsRun(ctx, &erroringRunner{stop: make(chan struct{})}, short)},
		{"Run after Shutdown hangs", checkRunAfterShutdown(ctx, deafRunner{}, short)},
		{"Shutdown ignores cancelled ctx", checkShutdownCancelled(ctx, stuckShutdown{}, short)},
		{"second Shutdown fails", checkShutdownTwice(ctx, &closedOnce{}, short)},
		{"Shutdown panics", checkShutdownWithoutStart(ctx, panickingShutdown{}, short)},
		{"expired Shutdown after cancel hangs", checkCancelThenExpiredShutdown(ctx, stuckShutdownRunner{}, short)},
		{
			"cancel and Shutdown close twice",
			checkConcurrentCancelAndShutdown(ctx, &doubleStopRunner{stop: make(chan struct{})}, short),
		},
	}

	for _, tt := range tests {
		if tt.err == nil {
			t.Errorf("%s: check passed, want a failure", tt.name)
		}
	}
}

func TestWithTimeout(t *testing.T) {
	t.Parallel()

	if got := newOptions(nil).timeout; got != DefaultTimeout {
		t.Errorf("default timeout = %v", got)
	}

	if got := newOptions([]Option{WithTimeout(time.Minute)}).timeout; got != time.Minute {
		t.Errorf("WithTimeout = %v", got)
	}

	if got := newOptions([]Option{WithTimeout(-1)}).timeout; got != DefaultTimeout {
		t.Errorf("non-positive timeout must keep the default, got %v", got)
	}
}
