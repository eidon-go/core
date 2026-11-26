package consume_test

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/consume"
	"github.com/eidon-go/core/retry"
)

var (
	errDown    = errors.New("down")
	errInvalid = errors.New("invalid")
)

// fast is a redelivery policy of n attempts without real waits.
func fast(n int) retry.Policy {
	return retry.Policy{MaxAttempts: n, InitialDelay: time.Millisecond}
}

// script is a handler that returns its errors in turn, then nil.
type script struct {
	errs  []error
	calls atomic.Int32
}

func (s *script) handle(context.Context, string) error {
	n := int(s.calls.Add(1))
	if n <= len(s.errs) {
		return s.errs[n-1]
	}

	return nil
}

// recorder is an OnError that records what it saw and answers with action.
type recorder struct {
	action consume.Action
	seen   []error
}

func (r *recorder) onError(_ context.Context, _ string, err error) consume.Action {
	r.seen = append(r.seen, err)

	return r.action
}

func newProcessor(h *script, onError *recorder, redelivery retry.Policy) *consume.Processor[string] {
	return &consume.Processor[string]{
		Handle:     h.handle,
		OnError:    onError.onError,
		Redelivery: redelivery,
		Logger:     slog.New(slog.DiscardHandler),
		Name:       "test",
	}
}

func TestProcess_Outcomes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		errs       []error
		action     consume.Action
		redelivery retry.Policy
		want       consume.Outcome
		attempts   int
		asked      int
		wantErr    error
	}{
		{name: "handled at once", want: consume.Handled, attempts: 1},
		{
			name: "retried, then handled", errs: []error{errDown}, action: consume.Retry,
			redelivery: fast(3), want: consume.Handled, attempts: 2, asked: 1,
		},
		{
			name: "retries exhausted stop", errs: []error{errDown, errDown, errDown}, action: consume.Retry,
			redelivery: fast(2), want: consume.Stopped, attempts: 2, asked: 2, wantErr: consume.ErrRetriesExhausted,
		},
		{
			name: "requeue", errs: []error{errDown}, action: consume.Requeue,
			redelivery: fast(3), want: consume.Requeued, attempts: 1, asked: 1, wantErr: errDown,
		},
		{
			name: "reject", errs: []error{errInvalid}, action: consume.Reject,
			redelivery: fast(3), want: consume.Rejected, attempts: 1, asked: 1, wantErr: errInvalid,
		},
		{
			name: "stop", errs: []error{errDown}, action: consume.Stop,
			redelivery: fast(3), want: consume.Stopped, attempts: 1, asked: 1, wantErr: errDown,
		},
		{
			name: "unknown action stops", errs: []error{errDown}, action: consume.Action(42),
			redelivery: fast(3), want: consume.Stopped, attempts: 1, asked: 1, wantErr: errDown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := &script{errs: tt.errs}
			onError := &recorder{action: tt.action}

			res := newProcessor(h, onError, tt.redelivery).Process(t.Context(), t.Context(), "m", consume.Delivery{})

			assert.Equal(t, tt.want, res.Outcome)
			assert.Equal(t, tt.attempts, res.Attempts)
			assert.Len(t, onError.seen, tt.asked)

			if tt.wantErr == nil {
				require.NoError(t, res.Err)
			} else {
				require.ErrorIs(t, res.Err, tt.wantErr)
			}
		})
	}
}

// Once the attempts are exhausted, OnError gets Exhausted joined — the
// module's own sentinel when one is set.
func TestProcess_Exhausted(t *testing.T) {
	t.Parallel()

	errOwn := errors.New("mod: redelivery attempts exhausted")

	h := &script{errs: []error{errDown, errDown}}
	onError := &recorder{action: consume.Retry}
	p := newProcessor(h, onError, fast(2))
	p.Exhausted = errOwn

	res := p.Process(t.Context(), t.Context(), "m", consume.Delivery{})

	require.Len(t, onError.seen, 2)
	require.NotErrorIs(t, onError.seen[0], errOwn, "an attempt is left")
	require.ErrorIs(t, onError.seen[1], errOwn)
	require.ErrorIs(t, onError.seen[1], errDown)
	require.NotErrorIs(t, onError.seen[1], consume.ErrRetriesExhausted)
	assert.Equal(t, consume.Stopped, res.Outcome)
}

func TestProcess_Panics(t *testing.T) {
	t.Parallel()

	t.Run("in Handle: an error for OnError", func(t *testing.T) {
		t.Parallel()

		onError := &recorder{action: consume.Reject}
		p := &consume.Processor[string]{
			Handle:  func(context.Context, string) error { panic("boom") },
			OnError: onError.onError,
			Logger:  slog.New(slog.DiscardHandler),
		}

		res := p.Process(t.Context(), t.Context(), "m", consume.Delivery{})

		assert.Equal(t, consume.Rejected, res.Outcome)
		require.Len(t, onError.seen, 1)
		require.ErrorContains(t, onError.seen[0], "consume: handler panicked: boom", "default name")
	})

	t.Run("in OnError: stop", func(t *testing.T) {
		t.Parallel()

		p := newProcessor(&script{errs: []error{errDown}}, &recorder{}, fast(3))
		p.OnError = func(context.Context, string, error) consume.Action { panic("again") }

		res := p.Process(t.Context(), t.Context(), "m", consume.Delivery{})

		assert.Equal(t, consume.Stopped, res.Outcome)
		require.ErrorIs(t, res.Err, errDown)
	})
}

// Observe wraps every attempt and sees its error, a recovered panic
// included.
func TestProcess_Observe(t *testing.T) {
	t.Parallel()

	var observed []error

	onError := &recorder{action: consume.Retry}
	calls := 0
	p := newProcessor(&script{}, onError, fast(3))
	p.Handle = func(context.Context, string) error {
		calls++
		if calls == 1 {
			panic("boom")
		}

		return nil
	}
	p.Observe = func(ctx context.Context, msg string, call func(context.Context) error) error {
		assert.Equal(t, "m", msg)

		err := call(ctx)
		observed = append(observed, err)

		return err
	}

	res := p.Process(t.Context(), t.Context(), "m", consume.Delivery{})

	assert.Equal(t, consume.Handled, res.Outcome)
	require.Len(t, observed, 2)
	require.EqualError(t, observed[0], "test: handler panicked: boom")
	require.NoError(t, observed[1])
}

// An attempt cut off by work's cancellation did not fail: OnError is not
// asked.
func TestProcess_WorkCancelledAbandons(t *testing.T) {
	t.Parallel()

	work, cancel := context.WithCancel(t.Context())
	onError := &recorder{action: consume.Stop}
	p := newProcessor(&script{}, onError, fast(3))
	p.Handle = func(ctx context.Context, _ string) error {
		cancel()

		return ctx.Err()
	}

	res := p.Process(t.Context(), work, "m", consume.Delivery{})

	assert.Equal(t, consume.Abandoned, res.Outcome)
	assert.Equal(t, 0, res.Attempts, "the cut-off attempt does not count")
	assert.Empty(t, onError.seen)
}

// The stop ends a redelivery wait at once.
func TestProcess_StopDuringWaitAbandons(t *testing.T) {
	t.Parallel()

	stop, cancel := context.WithCancel(t.Context())
	h := &script{errs: []error{errDown}}

	var retries atomic.Int32

	p := newProcessor(h, &recorder{action: consume.Retry}, retry.Policy{
		MaxAttempts:  3,
		InitialDelay: time.Hour,
		OnRetry: func(context.Context, retry.Attempt) {
			retries.Add(1)
			cancel()
		},
	})

	res := p.Process(stop, t.Context(), "m", consume.Delivery{})

	assert.Equal(t, consume.Abandoned, res.Outcome)
	assert.Equal(t, 1, res.Attempts)
	require.ErrorIs(t, res.Err, errDown)
	assert.EqualValues(t, 1, h.calls.Load())
	assert.EqualValues(t, 1, retries.Load())
}

// A wait that would end past the deadline is not waited: Process yields,
// and a later Process resumes the attempts where they were.
func TestProcess_DeadlineYieldsAndResumes(t *testing.T) {
	t.Parallel()

	onError := &recorder{action: consume.Retry}
	h := &script{errs: []error{errDown, errDown}}
	p := newProcessor(h, onError, retry.Policy{MaxAttempts: 2, InitialDelay: time.Hour})

	res := p.Process(t.Context(), t.Context(), "m", consume.Delivery{Deadline: time.Now().Add(time.Minute)})

	assert.Equal(t, consume.Yielded, res.Outcome)
	assert.Equal(t, 1, res.Attempts)
	assert.EqualValues(t, 1, h.calls.Load(), "no wait, no second attempt")

	res = p.Process(t.Context(), t.Context(), "m", consume.Delivery{Attempts: res.Attempts})

	assert.Equal(t, consume.Stopped, res.Outcome, "the resumed attempt was the last one")
	assert.Equal(t, 2, res.Attempts)
	require.ErrorIs(t, res.Err, consume.ErrRetriesExhausted)
}

func TestProcess_Transient(t *testing.T) {
	t.Parallel()

	transient := consume.Transient(errDown)

	tests := []struct {
		name      string
		errs      []error
		policy    retry.Policy
		want      consume.Outcome
		attempts  int
		asked     int
		waits     int32
		wantCalls int32
	}{
		{
			name: "waited out, not counted", errs: []error{transient, transient},
			policy: fast(retry.Forever), want: consume.Handled, attempts: 1, waits: 2, wantCalls: 3,
		},
		{
			// The second transient failure in a row counts as attempt 1; the
			// third starts a new streak.
			name: "counted once the policy is exhausted", errs: []error{transient, transient, transient},
			policy: fast(2), want: consume.Handled, attempts: 2, asked: 1, waits: 2, wantCalls: 4,
		},
		{
			name: "zero policy counts it at once", errs: []error{transient},
			want: consume.Handled, attempts: 2, asked: 1, wantCalls: 2,
		},
		{
			name: "a message error in between resets the streak", errs: []error{transient, errInvalid, transient},
			policy: fast(2), want: consume.Handled, attempts: 2, asked: 1, waits: 2, wantCalls: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var waits atomic.Int32

			h := &script{errs: tt.errs}
			onError := &recorder{action: consume.Retry}
			p := newProcessor(h, onError, fast(3))
			p.Transient = tt.policy
			p.Transient.OnRetry = func(context.Context, retry.Attempt) { waits.Add(1) }

			res := p.Process(t.Context(), t.Context(), "m", consume.Delivery{})

			assert.Equal(t, tt.want, res.Outcome)
			assert.Equal(t, tt.attempts, res.Attempts)
			assert.Len(t, onError.seen, tt.asked)
			assert.Equal(t, tt.waits, waits.Load())
			assert.Equal(t, tt.wantCalls, h.calls.Load())
		})
	}
}

// A transient wait past the deadline yields without counting the attempt.
func TestProcess_TransientYields(t *testing.T) {
	t.Parallel()

	h := &script{errs: []error{errInvalid, consume.Transient(errDown)}}
	p := newProcessor(h, &recorder{action: consume.Retry}, fast(3))
	p.Transient = retry.Policy{MaxAttempts: retry.Forever, InitialDelay: time.Hour}

	res := p.Process(t.Context(), t.Context(), "m", consume.Delivery{Deadline: time.Now().Add(time.Minute)})

	assert.Equal(t, consume.Yielded, res.Outcome)
	assert.Equal(t, 1, res.Attempts, "only the failed attempt counts")
	require.NoError(t, res.Err)
}

func TestTransient(t *testing.T) {
	t.Parallel()

	require.NoError(t, consume.Transient(nil))

	err := consume.Transient(errDown)
	require.ErrorIs(t, err, errDown)
	require.ErrorIs(t, err, consume.ErrTransient)
	require.EqualError(t, err, "down")

	policy := consume.TransientPolicy()
	assert.Equal(t, retry.Forever, policy.MaxAttempts)
	assert.Equal(t, 30*time.Second, policy.MaxDelay)
}

func TestStrings(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{"retry", "requeue", "reject", "stop", "action(9)"}, []string{
		consume.Retry.String(), consume.Requeue.String(), consume.Reject.String(), consume.Stop.String(),
		consume.Action(9).String(),
	})
	assert.Equal(t, []string{"handled", "requeued", "rejected", "stopped", "abandoned", "yielded", "outcome(9)"},
		[]string{
			consume.Handled.String(), consume.Requeued.String(), consume.Rejected.String(), consume.Stopped.String(),
			consume.Abandoned.String(), consume.Yielded.String(), consume.Outcome(9).String(),
		})
}

// Process may be called concurrently on one Processor.
func TestProcess_Concurrent(t *testing.T) {
	t.Parallel()

	var handled atomic.Int32

	p := &consume.Processor[int]{
		Handle: func(context.Context, int) error {
			handled.Add(1)

			return nil
		},
		OnError: func(context.Context, int, error) consume.Action { return consume.Stop },
	}

	done := make(chan consume.Result, 16)
	for i := range cap(done) {
		go func() { done <- p.Process(t.Context(), t.Context(), i, consume.Delivery{}) }()
	}

	for range cap(done) {
		assert.Equal(t, consume.Handled, (<-done).Outcome)
	}

	assert.EqualValues(t, cap(done), handled.Load())
}
