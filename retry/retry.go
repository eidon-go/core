// Package retry retries an operation according to a Policy: bounded attempts,
// exponential backoff with an optional cap and jitter, a predicate deciding
// which errors are retryable, and a hook for logging or metrics.
//
// Integrations use it to smooth transient failures at startup — a database or
// cache that is not reachable yet the moment the pod boots — and accept a
// Policy as an option so callers can tune it:
//
//	p := retry.Policy{MaxAttempts: 5, InitialDelay: 200 * time.Millisecond, Multiplier: 2, MaxDelay: 5 * time.Second}
//	err := p.Do(ctx, func(ctx context.Context) error { return db.PingContext(ctx) })
package retry

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"time"
)

// ErrPermanent marks an error as not retryable; see Permanent.
var ErrPermanent = errors.New("permanent error")

// Permanent wraps err so that Policy.Do stops retrying and returns it
// immediately. The result matches both err and ErrPermanent with errors.Is.
// Permanent(nil) returns nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}

	return &permanentError{err: err}
}

type permanentError struct{ err error }

func (e *permanentError) Error() string   { return e.err.Error() }
func (e *permanentError) Unwrap() []error { return []error{e.err, ErrPermanent} }

// Policy describes how an operation is retried. The zero value makes a single
// attempt (no retries). Policy is a value type: copy and adjust it freely.
type Policy struct {
	// MaxAttempts is the total number of attempts, including the first one.
	// Values <= 1 mean a single attempt.
	MaxAttempts int

	// InitialDelay is the delay before the second attempt. Zero retries
	// immediately.
	InitialDelay time.Duration

	// Multiplier grows the delay after every retry: delay(n) =
	// InitialDelay * Multiplier^(n-1). Values <= 1 keep the delay constant.
	Multiplier float64

	// MaxDelay caps the delay. Zero means no cap.
	MaxDelay time.Duration

	// Jitter randomizes every delay down by up to this fraction, in [0, 1]:
	// 0.2 picks a delay in [0.8*d, d]. It spreads out retries of many
	// instances that failed at the same moment.
	Jitter float64

	// RetryIf reports whether an error is retryable. nil retries every error.
	// Errors wrapped with Permanent are never retried, whatever RetryIf says.
	RetryIf func(err error) bool

	// OnRetry, if set, is called before waiting for the next attempt with the
	// number of the attempt that failed (starting at 1), its error and the
	// delay before the next one.
	OnRetry func(attempt int, err error, delay time.Duration)
}

// Do calls op until it succeeds, returns a non-retryable error, the attempts
// are exhausted or ctx is done. It returns nil on success, otherwise the last
// error of op; if ctx ends while waiting, that error joined with ctx.Err().
func (p Policy) Do(ctx context.Context, operation func(ctx context.Context) error) error {
	_, err := DoValue(ctx, p, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, operation(ctx)
	})

	return err
}

// DoValue is Policy.Do for operations that return a value, e.g. a connection.
func DoValue[T any](ctx context.Context, p Policy, operation func(ctx context.Context) (T, error)) (T, error) {
	var timer *time.Timer

	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for attempt := 1; ; attempt++ {
		v, err := operation(ctx)
		if err == nil {
			return v, nil
		}

		if attempt >= p.MaxAttempts || !p.retryable(err) {
			return v, err
		}

		delay := p.Delay(attempt)
		if p.OnRetry != nil {
			p.OnRetry(attempt, err, delay)
		}

		if timer == nil {
			timer = time.NewTimer(delay)
		} else {
			timer.Reset(delay)
		}

		select {
		case <-ctx.Done():
			var zero T

			return zero, errors.Join(err, ctx.Err())
		case <-timer.C:
		}
	}
}

// Delay returns the delay to wait after the given failed attempt (starting
// at 1), with jitter applied.
func (p Policy) Delay(attempt int) time.Duration {
	d := float64(p.InitialDelay)
	if p.Multiplier > 1 && attempt > 1 {
		d *= math.Pow(p.Multiplier, float64(attempt-1))
	}

	if p.MaxDelay > 0 && d > float64(p.MaxDelay) {
		d = float64(p.MaxDelay)
	}

	if d > math.MaxInt64 {
		d = math.MaxInt64
	}

	if j := min(max(p.Jitter, 0), 1); j > 0 {
		d -= d * j * rand.Float64() //nolint:gosec // jitter does not need a cryptographic source
	}

	return time.Duration(d)
}

func (p Policy) retryable(err error) bool {
	if errors.Is(err, ErrPermanent) {
		return false
	}

	return p.RetryIf == nil || p.RetryIf(err)
}
