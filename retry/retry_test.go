package retry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/retry"
)

var errTransient = errors.New("transient")

// failing returns an op that fails n times, then succeeds, counting calls.
func failing(n int, calls *int) func(context.Context) error {
	return func(context.Context) error {
		*calls++
		if *calls <= n {
			return errTransient
		}

		return nil
	}
}

func TestDo_ZeroPolicySingleAttempt(t *testing.T) {
	t.Parallel()

	calls := 0
	err := retry.Policy{}.Do(t.Context(), failing(5, &calls))

	require.ErrorIs(t, err, errTransient)
	assert.Equal(t, 1, calls)
}

func TestDo_SucceedsAfterRetries(t *testing.T) {
	t.Parallel()

	calls := 0
	err := retry.Policy{MaxAttempts: 3}.Do(t.Context(), failing(2, &calls))

	require.NoError(t, err)
	assert.Equal(t, 3, calls)
}

func TestDo_MaxAttemptsIsTotal(t *testing.T) {
	t.Parallel()

	calls := 0
	err := retry.Policy{MaxAttempts: 3}.Do(t.Context(), failing(10, &calls))

	require.ErrorIs(t, err, errTransient)
	assert.Equal(t, 3, calls)
}

func TestDo_Permanent(t *testing.T) {
	t.Parallel()

	errFatal := errors.New("bad credentials")
	calls := 0

	err := retry.Policy{MaxAttempts: 5}.Do(t.Context(), func(context.Context) error {
		calls++

		return retry.Permanent(errFatal)
	})

	require.ErrorIs(t, err, errFatal)
	require.ErrorIs(t, err, retry.ErrPermanent)
	require.EqualError(t, err, "bad credentials")
	assert.Equal(t, 1, calls)
	assert.NoError(t, retry.Permanent(nil))
}

func TestDo_RetryIf(t *testing.T) {
	t.Parallel()

	calls := 0
	p := retry.Policy{MaxAttempts: 5, RetryIf: func(err error) bool { return !errors.Is(err, errTransient) }}

	require.ErrorIs(t, p.Do(t.Context(), failing(10, &calls)), errTransient)
	assert.Equal(t, 1, calls)
}

func TestDo_OnRetry(t *testing.T) {
	t.Parallel()

	type event struct {
		attempt int
		delay   time.Duration
	}

	var events []event

	p := retry.Policy{
		MaxAttempts:  3,
		InitialDelay: time.Millisecond,
		Multiplier:   2,
		OnRetry: func(attempt int, err error, delay time.Duration) {
			require.ErrorIs(t, err, errTransient)

			events = append(events, event{attempt, delay})
		},
	}

	calls := 0
	require.ErrorIs(t, p.Do(t.Context(), failing(10, &calls)), errTransient)
	assert.Equal(t, []event{{1, time.Millisecond}, {2, 2 * time.Millisecond}}, events)
}

func TestDo_ContextCancelledWhileWaiting(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	calls := 0

	p := retry.Policy{MaxAttempts: 5, InitialDelay: time.Hour}
	err := p.Do(ctx, func(context.Context) error {
		calls++

		cancel()

		return errTransient
	})

	require.ErrorIs(t, err, errTransient)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, calls)
}

func TestDoValue(t *testing.T) {
	t.Parallel()

	calls := 0
	v, err := retry.DoValue(t.Context(), retry.Policy{MaxAttempts: 3}, func(context.Context) (string, error) {
		calls++
		if calls < 2 {
			return "", errTransient
		}

		return "conn", nil
	})

	require.NoError(t, err)
	assert.Equal(t, "conn", v)
}

func TestDelay(t *testing.T) {
	t.Parallel()

	p := retry.Policy{InitialDelay: 100 * time.Millisecond, Multiplier: 2, MaxDelay: 300 * time.Millisecond}

	assert.Equal(t, 100*time.Millisecond, p.Delay(1))
	assert.Equal(t, 200*time.Millisecond, p.Delay(2))
	assert.Equal(t, 300*time.Millisecond, p.Delay(3), "capped")
	assert.Equal(t, 300*time.Millisecond, p.Delay(1000), "no overflow")

	constant := retry.Policy{InitialDelay: 50 * time.Millisecond}
	assert.Equal(t, 50*time.Millisecond, constant.Delay(7))

	jittered := retry.Policy{InitialDelay: 100 * time.Millisecond, Jitter: 0.5}
	for range 100 {
		d := jittered.Delay(1)
		assert.GreaterOrEqual(t, d, 50*time.Millisecond)
		assert.LessOrEqual(t, d, 100*time.Millisecond)
	}
}
