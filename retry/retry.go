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
	"log/slog"
	"math"
	"math/rand/v2"
	"time"
)

// ErrPermanent marks an error as not retryable; see Permanent.
var ErrPermanent = errors.New("retry: permanent error")

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

// Forever as Policy.MaxAttempts retries until the operation succeeds, returns
// a non-retryable error or ctx is done.
const Forever = -1

// The values of ConnectPolicy.
const (
	connectAttempts     = 10
	connectInitialDelay = 200 * time.Millisecond
	connectMultiplier   = 2
	connectMaxDelay     = 5 * time.Second
	connectJitter       = 0.2
)

// ConnectPolicy returns the policy integrations use by default to connect in
// Start: 10 attempts, delays from 200ms doubling up to 5s, 20% jitter — at
// most about 26 seconds of waiting, enough for a dependency that starts with
// the service, short enough for a misconfiguration to fail the startup
// visibly. It is a function, not a variable, so that no caller can change it
// for the others.
func ConnectPolicy() Policy {
	return Policy{ //nolint:exhaustruct_v5 // no RetryIf/OnRetry by default
		MaxAttempts:  connectAttempts,
		InitialDelay: connectInitialDelay,
		Multiplier:   connectMultiplier,
		MaxDelay:     connectMaxDelay,
		Jitter:       connectJitter,
	}
}

// Policy describes how an operation is retried. The zero value makes a single
// attempt (no retries). Policy is a value type: copy and adjust it freely.
type Policy struct {
	// MaxAttempts is the total number of attempts, including the first one.
	// 0 and 1 mean a single attempt; a negative value (Forever) means no
	// limit other than ctx.
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
	// A Policy shared by concurrent Do calls has its RetryIf and OnRetry
	// called concurrently; callbacks that touch shared state must
	// synchronize it.
	RetryIf func(err error) bool

	// OnRetry, if set, is called before waiting for the next attempt, e.g. to
	// log it. ctx is the context passed to Do.
	OnRetry func(ctx context.Context, a Attempt)
}

// Attempt describes a failed attempt that will be retried. Fields may be
// added in later versions.
type Attempt struct {
	// Number is the number of the failed attempt, starting at 1.
	Number int
	// Err is the error the attempt returned.
	Err error
	// Delay is the wait before the next attempt.
	Delay time.Duration
}

// Logged returns a copy of p whose OnRetry also logs every retry with
// logger.WarnContext ("retrying", with attempt, delay and error), after
// calling the OnRetry p already has. Pass the component's logger: it already
// carries the component's name. A nil logger means slog.Default().
//
//	err := c.connectRetry.Logged(c.logger).Do(ctx, c.ping)
func (p Policy) Logged(logger *slog.Logger) Policy {
	if logger == nil {
		logger = slog.Default()
	}

	next := p.OnRetry

	p.OnRetry = func(ctx context.Context, a Attempt) {
		if next != nil {
			next(ctx, a)
		}

		logger.WarnContext(ctx, "retrying",
			slog.Int("attempt", a.Number),
			slog.Duration("delay", a.Delay),
			slog.Any("error", a.Err))
	}

	return p
}

// Do calls operation until it succeeds, returns a non-retryable error, the
// attempts are exhausted or ctx is done; ctx is checked before every attempt.
// It returns nil on success, otherwise the last error of operation; if ctx
// ends first, that error joined with ctx.Err() (or just ctx.Err() if ctx was
// done before the first attempt).
func (p Policy) Do(ctx context.Context, operation func(ctx context.Context) error) error {
	_, err := DoValue(ctx, p, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, operation(ctx)
	})

	return err
}

// DoValue is Policy.Do for operations that return a value, e.g. a connection.
// On any error it returns the zero value of T.
func DoValue[T any](ctx context.Context, p Policy, operation func(ctx context.Context) (T, error)) (T, error) {
	var zero T

	if err := ctx.Err(); err != nil {
		return zero, err //nolint:wrapcheck // the caller's own context error
	}

	var sleeper sleeper
	defer sleeper.stop()

	for attempt := 1; ; attempt++ {
		v, err := operation(ctx)
		if err == nil {
			return v, nil
		}

		if p.exhausted(attempt) || !p.retryable(err) {
			return zero, err
		}

		// The operation may have failed because ctx is done: do not report a
		// retry that will not happen.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return zero, joinCtx(err, ctxErr)
		}

		delay := p.Delay(attempt)
		if p.OnRetry != nil {
			p.OnRetry(ctx, Attempt{Number: attempt, Err: err, Delay: delay})
		}

		if ctxErr := sleeper.sleep(ctx, delay); ctxErr != nil {
			return zero, joinCtx(err, ctxErr)
		}
	}
}

// joinCtx joins the last error with the ctx error, unless it already wraps it.
func joinCtx(err, ctxErr error) error {
	if errors.Is(err, ctxErr) {
		return err
	}

	return errors.Join(err, ctxErr)
}

// sleeper reuses one timer across waits.
type sleeper struct{ timer *time.Timer }

// sleep waits for d or until ctx is done and returns ctx.Err(). ctx is
// checked after the wait as well: with a zero delay both can be ready at
// once.
func (s *sleeper) sleep(ctx context.Context, d time.Duration) error {
	if s.timer == nil {
		s.timer = time.NewTimer(d)
	} else {
		s.timer.Reset(d)
	}

	select {
	case <-ctx.Done():
	case <-s.timer.C:
	}

	return ctx.Err() //nolint:wrapcheck // the caller's own context error
}

func (s *sleeper) stop() {
	if s.timer != nil {
		s.timer.Stop()
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

	// Clamp in integers: float64(math.MaxInt64) is 2^63, and converting a
	// float >= 2^63 to int64 is implementation-defined (negative on amd64).
	delay := time.Duration(math.MaxInt64)
	if d < math.MaxInt64 {
		delay = time.Duration(d)
	}

	if j := min(max(p.Jitter, 0), 1); j > 0 {
		delay -= time.Duration(float64(delay) * j * rand.Float64()) //nolint:gosec // jitter needs no crypto source
	}

	return delay
}

// exhausted reports whether attempt was the last one allowed.
func (p Policy) exhausted(attempt int) bool {
	return p.MaxAttempts >= 0 && attempt >= p.MaxAttempts
}

func (p Policy) retryable(err error) bool {
	if errors.Is(err, ErrPermanent) {
		return false
	}

	return p.RetryIf == nil || p.RetryIf(err)
}
