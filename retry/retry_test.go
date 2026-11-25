package retry_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
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

	var events []retry.Attempt

	p := retry.Policy{
		MaxAttempts:  3,
		InitialDelay: time.Millisecond,
		Multiplier:   2,
		OnRetry: func(ctx context.Context, a retry.Attempt) {
			require.NoError(t, ctx.Err())

			events = append(events, a)
		},
	}

	calls := 0
	require.ErrorIs(t, p.Do(t.Context(), failing(10, &calls)), errTransient)
	assert.Equal(t, []retry.Attempt{
		{Number: 1, Err: errTransient, Delay: time.Millisecond},
		{Number: 2, Err: errTransient, Delay: 2 * time.Millisecond},
	}, events)
}

func TestDo_Forever(t *testing.T) {
	t.Parallel()

	calls := 0
	err := retry.Policy{MaxAttempts: retry.Forever}.Do(t.Context(), failing(50, &calls))

	require.NoError(t, err)
	assert.Equal(t, 51, calls)
}

func TestDo_ForeverStopsOnContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	calls := 0
	err := retry.Policy{MaxAttempts: retry.Forever, InitialDelay: time.Millisecond}.Do(ctx, failing(1<<30, &calls))

	require.ErrorIs(t, err, errTransient)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Positive(t, calls)
}

func TestDo_ContextDoneBeforeFirstAttempt(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	calls := 0
	err := retry.Policy{MaxAttempts: 5}.Do(ctx, failing(10, &calls))

	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, calls)
}

// With a zero delay the timer and ctx.Done race; the operation must not be
// called again after ctx is done.
func TestDo_ZeroDelayStopsOnContext(t *testing.T) {
	t.Parallel()

	for range 100 {
		ctx, cancel := context.WithCancel(t.Context())
		calls := 0

		err := retry.Policy{MaxAttempts: 10}.Do(ctx, func(context.Context) error {
			calls++

			cancel()

			return errTransient
		})

		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, calls)
	}
}

func TestDo_PermanentWinsOverRetryIf(t *testing.T) {
	t.Parallel()

	calls := 0
	p := retry.Policy{MaxAttempts: 5, RetryIf: func(error) bool { return true }}

	err := p.Do(t.Context(), func(context.Context) error {
		calls++

		return retry.Permanent(errTransient)
	})

	require.ErrorIs(t, err, errTransient)
	assert.Equal(t, 1, calls)
}

func TestDoValue_ZeroOnError(t *testing.T) {
	t.Parallel()

	v, err := retry.DoValue(t.Context(), retry.Policy{MaxAttempts: 2}, func(context.Context) (string, error) {
		return "partial", errTransient
	})

	require.ErrorIs(t, err, errTransient)
	assert.Empty(t, v)
}

func TestDelay_JitterIsClamped(t *testing.T) {
	t.Parallel()

	negative := retry.Policy{InitialDelay: 10 * time.Millisecond, Jitter: -1}
	assert.Equal(t, 10*time.Millisecond, negative.Delay(1))

	tooLarge := retry.Policy{InitialDelay: 10 * time.Millisecond, Jitter: 5}
	for range 100 {
		d := tooLarge.Delay(1)
		assert.GreaterOrEqual(t, d, time.Duration(0))
		assert.LessOrEqual(t, d, 10*time.Millisecond)
	}
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

func TestDelay_NoOverflow(t *testing.T) {
	t.Parallel()

	p := retry.Policy{InitialDelay: 1, Multiplier: 2}

	for _, attempt := range []int{63, 64, 65, 1000} {
		assert.Positive(t, p.Delay(attempt), "attempt %d", attempt)
	}
}

// When the operation fails because ctx is done, no retry is reported and the
// ctx error is not duplicated.
func TestDo_NoRetryReportedAfterContextDone(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	retries := 0

	p := retry.Policy{
		MaxAttempts: 5,
		OnRetry:     func(context.Context, retry.Attempt) { retries++ },
	}

	err := p.Do(ctx, func(ctx context.Context) error {
		cancel()

		return ctx.Err()
	})

	require.ErrorIs(t, err, context.Canceled)
	require.EqualError(t, err, "context canceled")
	assert.Zero(t, retries)
}

func TestErrPermanentMessage(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "retry: permanent error", retry.ErrPermanent.Error())
	assert.Equal(t, 10*time.Millisecond, retry.Policy{InitialDelay: 10 * time.Millisecond}.Delay(1))
}

func TestConnectPolicy(t *testing.T) {
	t.Parallel()

	p := retry.ConnectPolicy()
	assert.Equal(t, 10, p.MaxAttempts)
	assert.Equal(t, 200*time.Millisecond, p.InitialDelay)
	assert.Equal(t, 5*time.Second, p.MaxDelay)
	assert.Nil(t, p.OnRetry)

	p.MaxAttempts = 1
	assert.Equal(t, 10, retry.ConnectPolicy().MaxAttempts, "every call returns a fresh copy")

	noJitter := retry.ConnectPolicy()
	noJitter.Jitter = 0

	var total time.Duration
	for attempt := 1; attempt < noJitter.MaxAttempts; attempt++ {
		total += noJitter.Delay(attempt)
	}

	assert.Equal(t, 26200*time.Millisecond, total, "documented as at most about 26 seconds")
}

func TestLogged(t *testing.T) {
	t.Parallel()

	var (
		buf   bytes.Buffer
		calls int
	)

	p := retry.Policy{MaxAttempts: 2, OnRetry: func(context.Context, retry.Attempt) { calls++ }}
	logged := p.Logged(slog.New(slog.NewTextHandler(&buf, nil)).With("component", "redis"))

	err := logged.Do(t.Context(), func(context.Context) error { return errors.New("connection refused") })
	require.EqualError(t, err, "connection refused")

	assert.Equal(t, 1, calls, "the existing OnRetry is still called")
	assert.Contains(
		t,
		buf.String(),
		`level=WARN msg=retrying component=redis attempt=1 delay=0s error="connection refused"`,
	)
	assert.Equal(t, 1, bytes.Count(buf.Bytes(), []byte("retrying")))

	assert.NotNil(t, retry.Policy{}.Logged(nil).OnRetry, "a nil logger means slog.Default()")
}
