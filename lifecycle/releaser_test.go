package lifecycle_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
)

func TestReleaser_Contract(t *testing.T) {
	t.Parallel()

	t.Run("ShutdownOnce", func(t *testing.T) {
		t.Parallel()

		lifecycletest.Resource(t, func(*testing.T) lifecycle.Resource {
			return lifecycle.ShutdownOnce(func(context.Context) error { return nil })
		})
	})

	t.Run("CloseOnce", func(t *testing.T) {
		t.Parallel()

		lifecycletest.Resource(t, func(*testing.T) lifecycle.Resource {
			return lifecycle.CloseOnce(func() error { return nil })
		})
	})
}

// The release runs once; its error goes to the first Shutdown only.
func TestReleaser_ReleasesOnce(t *testing.T) {
	t.Parallel()

	errClose := errors.New("flush failed")

	for name, newReleaser := range map[string]func(*atomic.Int32) *lifecycle.Releaser{
		"ShutdownOnce": func(calls *atomic.Int32) *lifecycle.Releaser {
			return lifecycle.ShutdownOnce(func(context.Context) error {
				calls.Add(1)

				return errClose
			})
		},
		"CloseOnce": func(calls *atomic.Int32) *lifecycle.Releaser {
			return lifecycle.CloseOnce(func() error {
				calls.Add(1)

				return errClose
			})
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var calls atomic.Int32

			c := newReleaser(&calls)
			assert.False(t, c.Closing())

			require.ErrorIs(t, c.Shutdown(t.Context()), errClose)
			assert.True(t, c.Closing())
			require.NoError(t, c.Shutdown(t.Context()), "a repeated Shutdown returns nil")
			assert.Equal(t, int32(1), calls.Load())

			select {
			case <-c.Done():
			default:
				t.Fatal("Done is not closed after the release")
			}
		})
	}
}

// A blocking Close outlives the Shutdown deadline: Shutdown returns the ctx
// error, the close finishes on its own, and a later Shutdown waits for it.
func TestCloseOnce_DeadlineDoesNotBlock(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	c := lifecycle.CloseOnce(func() error {
		<-release

		return nil
	})

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, c.Shutdown(ctx), context.DeadlineExceeded)

	close(release)
	<-c.Done()
	require.NoError(t, c.Shutdown(t.Context()))
}

// A Shutdown concurrent with a running release waits with its own ctx, not
// for the release (sync.Once would block it).
func TestShutdownOnce_ConcurrentCallerHonorsItsContext(t *testing.T) {
	t.Parallel()

	entered, release := make(chan struct{}), make(chan struct{})
	c := lifecycle.ShutdownOnce(func(context.Context) error {
		close(entered)
		<-release

		return nil
	})

	first := make(chan error, 1)

	go func() { first <- c.Shutdown(t.Context()) }()

	<-entered

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, c.Shutdown(cancelled), context.Canceled)

	close(release)
	require.NoError(t, <-first)
	require.NoError(t, c.Shutdown(t.Context()))
}

// A finished release wins over an expired ctx.
func TestReleaser_FinishedReleaseWinsOverExpiredContext(t *testing.T) {
	t.Parallel()

	c := lifecycle.CloseOnce(func() error { return nil })
	require.NoError(t, c.Shutdown(t.Context()))

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()

	for range 100 {
		require.NoError(t, c.Shutdown(cancelled))
	}
}

func TestReleaser_PanicBecomesError(t *testing.T) {
	t.Parallel()

	c := lifecycle.CloseOnce(func() error { panic("double close") })

	require.ErrorContains(t, c.Shutdown(t.Context()), "release panicked: double close")
	require.NoError(t, c.Shutdown(t.Context()))
}

func TestReleaser_PanicsOnNilRelease(t *testing.T) {
	t.Parallel()

	assert.Panics(t, func() { lifecycle.CloseOnce(nil) })
	assert.Panics(t, func() { lifecycle.ShutdownOnce(nil) })
}

func TestReleaser_ContextIsCancelledBeforeRelease(t *testing.T) {
	t.Parallel()

	var r *lifecycle.Releaser

	var causeInRelease error

	r = lifecycle.ShutdownOnce(func(context.Context) error {
		causeInRelease = context.Cause(r.Context()) //nolint:contextcheck // the close signal under test

		return nil
	})

	require.NoError(t, r.Context().Err())
	assert.False(t, r.Closing())

	require.NoError(t, r.Shutdown(t.Context()))

	require.ErrorIs(t, causeInRelease, lifecycle.ErrClosed)
	assert.True(t, r.Closing())
}

func TestReleaser_Do(t *testing.T) {
	t.Parallel()

	errConnect := errors.New("connection refused")

	tests := []struct {
		name string
		fn   func(ctx context.Context) error
		want error
	}{
		{name: "success", fn: func(context.Context) error { return nil }, want: nil},
		{name: "failure", fn: func(context.Context) error { return errConnect }, want: errConnect},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := lifecycle.CloseOnce(func() error { return nil })

			err := r.Do(t.Context(), tt.fn)
			if tt.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.want)
			}
		})
	}
}

func TestReleaser_DoAfterShutdown(t *testing.T) {
	t.Parallel()

	r := lifecycle.CloseOnce(func() error { return nil })
	require.NoError(t, r.Shutdown(t.Context()))

	called := false
	err := r.Do(t.Context(), func(context.Context) error {
		called = true

		return nil
	})

	require.ErrorIs(t, err, lifecycle.ErrClosed)
	assert.False(t, called)
}

func TestReleaser_ShutdownEndsDo(t *testing.T) {
	t.Parallel()

	r := lifecycle.CloseOnce(func() error { return nil })
	started := make(chan struct{})
	result := make(chan error, 1)

	var cause error

	go func() {
		result <- r.Do(t.Context(), func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			cause = context.Cause(ctx)

			return ctx.Err()
		})
	}()

	<-started
	require.NoError(t, r.Shutdown(t.Context()))

	require.ErrorIs(t, <-result, lifecycle.ErrClosed)
	assert.ErrorIs(t, cause, lifecycle.ErrClosed)
}

func TestReleaser_DoKeepsCallerCancellation(t *testing.T) {
	t.Parallel()

	r := lifecycle.CloseOnce(func() error { return nil })

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := r.Do(ctx, func(ctx context.Context) error { return ctx.Err() })

	require.ErrorIs(t, err, context.Canceled)
	assert.NotErrorIs(t, err, lifecycle.ErrClosed)
}
