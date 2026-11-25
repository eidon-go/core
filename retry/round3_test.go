package retry_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/retry"
)

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
