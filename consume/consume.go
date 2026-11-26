// Package consume runs a message handler the way every message consumer of
// the integration modules does: attempt after attempt as an error handler
// decides, with a redelivery policy between them, panics recovered, failures
// logged without the message's content — and waits out transient errors of a
// dependency without using up the message's attempts.
//
// A broker module builds one Processor per consumer, calls Process for every
// message and settles the message with the broker according to the Outcome:
//
//	res := c.proc.Process(stop, work, msg, consume.Delivery{Attrs: attrs})
//	switch res.Outcome {
//	case consume.Handled:  ack
//	case consume.Requeued: return it to the broker
//	case consume.Rejected: skip, terminate or dead-letter it
//	case consume.Stopped:  stop the consumer with res.Err
//	case consume.Paused:   pause the message's source; leave the message to the broker
//	case consume.Abandoned, consume.Yielded: leave it to the broker
//	}
//
// Handlers mark an error of a dependency — a database that is down — with
// Transient. Process then waits with the Transient policy and tries again,
// without asking the error handler and without counting an attempt, so a
// dependency outage neither dead-letters every message nor stops the
// consumer.
package consume

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/eidon-go/core/retry"
)

// ErrRetriesExhausted is joined to the error an error handler gets once the
// redelivery policy allows no more attempts, unless Processor.Exhausted names
// another.
var ErrRetriesExhausted = errors.New("consume: redelivery attempts exhausted")

// ErrTransient marks an error of a dependency rather than of the message; see
// Transient.
var ErrTransient = errors.New("consume: transient error")

// Transient wraps err to mark it as an error of a dependency — a database or
// a service that is down — rather than of the message: Process waits and
// tries again without counting an attempt or asking the error handler. The
// result matches both err and ErrTransient with errors.Is. Transient(nil)
// returns nil.
func Transient(err error) error {
	if err == nil {
		return nil
	}

	return &transientError{err: err}
}

type transientError struct{ err error }

func (e *transientError) Error() string   { return e.err.Error() }
func (e *transientError) Unwrap() []error { return []error{e.err, ErrTransient} }

// The values of TransientPolicy.
const (
	transientInitialDelay = time.Second
	transientMultiplier   = 2
	transientMaxDelay     = 30 * time.Second
	transientJitter       = 0.2
)

// TransientPolicy returns the policy modules use by default to wait out
// transient errors: delays from 1s doubling up to 30s, 20% jitter, without a
// limit — only the stop (or a Delivery's deadline) ends the waiting.
func TransientPolicy() retry.Policy {
	return retry.Policy{ //nolint:exhaustruct_v5 // no RetryIf, no OnRetry
		MaxAttempts:  retry.Forever,
		InitialDelay: transientInitialDelay,
		Multiplier:   transientMultiplier,
		MaxDelay:     transientMaxDelay,
		Jitter:       transientJitter,
	}
}

// Action is what an error handler decides about a failed attempt.
type Action int

const (
	// Retry makes another attempt after the redelivery policy's delay. Once
	// the policy allows no more attempts it counts as Stop.
	Retry Action = iota
	// Requeue gives the message back to the broker, to be delivered again.
	Requeue
	// Reject gives the message up: the consumer skips it, or the broker
	// terminates or dead-letters it.
	Reject
	// Stop stops the consumer.
	Stop
	// Pause leaves the message unsettled and pauses the consumer's intake of
	// its source (a partition, a subscription, a queue): the process keeps
	// running, the consumer reports unhealthy until it is resumed or
	// restarted, and the message is delivered again then.
	Pause
)

// String returns the action's name.
func (a Action) String() string {
	switch a {
	case Retry:
		return "retry"
	case Requeue:
		return "requeue"
	case Reject:
		return "reject"
	case Stop:
		return "stop"
	case Pause:
		return "pause"
	default:
		return fmt.Sprintf("action(%d)", int(a))
	}
}

// Outcome is what became of a message in Process.
type Outcome int

const (
	// Handled: the handler succeeded.
	Handled Outcome = iota
	// Requeued: the error handler chose Requeue.
	Requeued
	// Rejected: the error handler chose Reject.
	Rejected
	// Stopped: the error handler chose Stop, Retry with no attempts left, or
	// an unknown action, or it panicked.
	Stopped
	// Abandoned: the stop began during a wait, or work was cancelled during
	// an attempt. The message neither succeeded nor failed: leave it to the
	// broker.
	Abandoned
	// Yielded: a wait would have ended past the Delivery's deadline. The
	// message is not settled; Result.Attempts is where its attempts resume.
	Yielded
	// Paused: the error handler chose Pause. The message is not settled;
	// the consumer pauses its source.
	Paused
)

// String returns the outcome's name.
func (o Outcome) String() string {
	switch o {
	case Handled:
		return "handled"
	case Requeued:
		return "requeued"
	case Rejected:
		return "rejected"
	case Stopped:
		return "stopped"
	case Abandoned:
		return "abandoned"
	case Yielded:
		return "yielded"
	case Paused:
		return "paused"
	default:
		return fmt.Sprintf("outcome(%d)", int(o))
	}
}

// Processor runs a handler on messages of type M. Its fields are set once,
// before the first Process; Process may then be called concurrently.
type Processor[M any] struct {
	// Handle handles one attempt at msg. A panic in it is recovered and
	// becomes the attempt's error. Required.
	Handle func(ctx context.Context, msg M) error

	// Observe, if set, wraps every attempt — a module opens its process span
	// in it. call runs Handle, with panics already recovered, and returns
	// its error; Observe returns that error (or another).
	Observe func(ctx context.Context, msg M, call func(context.Context) error) error

	// OnError decides what to do about a failed attempt; err has Exhausted
	// joined once Redelivery allows no more attempts. A panic in it is
	// logged and counts as Stop. Required.
	OnError func(ctx context.Context, msg M, err error) Action

	// Redelivery sets the attempts at a message and the delays between them
	// when OnError returns Retry.
	Redelivery retry.Policy

	// Transient sets the waits after an attempt that failed with an error
	// marked with Transient, before the same attempt is made again (see
	// TransientPolicy). Once its MaxAttempts transient failures in a row are
	// reached, the error counts as a failed attempt. The zero value makes no
	// waits: transient errors count like any other.
	Transient retry.Policy

	// Exhausted is joined to the error OnError gets once Redelivery allows
	// no more attempts. Default: ErrRetriesExhausted.
	Exhausted error

	// Logger logs failed attempts and transient waits. Default:
	// slog.Default().
	Logger *slog.Logger

	// Name starts the log messages and the panic errors, e.g. "kafka".
	// Default: "consume".
	Name string
}

// Delivery describes one call of Process.
type Delivery struct {
	// Attrs are logged with every failure: the broker's coordinates of the
	// message, never its content.
	Attrs []slog.Attr

	// Attempts made at the message by an earlier Process that yielded
	// (Result.Attempts); the next attempt is the one after them.
	Attempts int

	// Deadline, if not zero, ends Process with Yielded instead of a wait
	// that would end past it.
	Deadline time.Time
}

// Result is the outcome of Process.
type Result struct {
	// Outcome is what became of the message.
	Outcome Outcome

	// Err is the error OnError last saw (Exhausted joined, if so); nil when
	// Handled, and when Abandoned or Yielded before any failure.
	Err error

	// Attempts is the number of attempts counted at the message so far,
	// Delivery.Attempts included; transient failures do not count.
	Attempts int
}

// Process handles msg: it calls Handle on work until an attempt succeeds or
// OnError gives the message up, waiting between attempts unless stop is
// done first. An attempt cut off by work's cancellation (Shutdown's
// deadline) is Abandoned without asking OnError, since the message did not
// fail.
func (p *Processor[M]) Process(stop, work context.Context, msg M, d Delivery) Result {
	attempt := d.Attempts + 1
	transient := 0

	for {
		err := p.attempt(work, msg)
		if err == nil {
			return Result{Outcome: Handled, Err: nil, Attempts: attempt}
		}

		if work.Err() != nil {
			return Result{Outcome: Abandoned, Err: nil, Attempts: attempt - 1}
		}

		if errors.Is(err, ErrTransient) && !exhausted(p.Transient, transient+1) {
			transient++
			p.log(work, slog.LevelWarn, "message handling failed on a transient error, waiting", d.Attrs,
				slog.Int("transient_failures", transient), slog.Any("error", err))

			if out, ok := p.wait(stop, work, d.Deadline, p.Transient, transient, err); !ok {
				return Result{Outcome: out, Err: nil, Attempts: attempt - 1}
			}

			continue
		}

		transient = 0

		if res, done := p.failed(stop, work, msg, d, attempt, err); done {
			return res
		}

		attempt++
	}
}

// failed asks OnError about failed attempt and logs it. Unless OnError
// chose Retry and an attempt is left, it returns the Result and true; for
// Retry it waits the redelivery delay and returns false, or the Result of an
// interrupted wait and true.
func (p *Processor[M]) failed(stop, work context.Context, msg M, d Delivery, attempt int, err error) (Result, bool) {
	last := exhausted(p.Redelivery, attempt)
	if last {
		err = errors.Join(p.exhaustedErr(), err)
	}

	action := p.decide(work, msg, err)
	p.log(work, slog.LevelError, "message handling failed", d.Attrs,
		slog.Int("attempt", attempt), slog.String("action", action.String()), slog.Any("error", err))

	switch {
	case action == Requeue:
		return Result{Outcome: Requeued, Err: err, Attempts: attempt}, true
	case action == Reject:
		return Result{Outcome: Rejected, Err: err, Attempts: attempt}, true
	case action == Pause:
		return Result{Outcome: Paused, Err: err, Attempts: attempt}, true
	case action == Retry && !last:
		if out, ok := p.wait(stop, work, d.Deadline, p.Redelivery, attempt, err); !ok {
			return Result{Outcome: out, Err: err, Attempts: attempt}, true
		}

		return Result{}, false //nolint:exhaustruct_v5 // not used
	default:
		return Result{Outcome: Stopped, Err: err, Attempts: attempt}, true
	}
}

// attempt calls Handle, through Observe if set; a panic is an error.
func (p *Processor[M]) attempt(ctx context.Context, msg M) error {
	call := func(ctx context.Context) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("%s: handler panicked: %v", p.name(), r) //nolint:err113 // carries the panic value
			}
		}()

		return p.Handle(ctx, msg)
	}

	if p.Observe == nil {
		return call(ctx)
	}

	return p.Observe(ctx, msg, call)
}

// decide asks OnError; a panic in it means Stop.
func (p *Processor[M]) decide(ctx context.Context, msg M, err error) Action {
	action := Stop

	func() {
		defer func() {
			if r := recover(); r != nil {
				p.logger().ErrorContext(ctx, p.name()+" error handler panicked", slog.Any("panic", r))
			}
		}()

		action = p.OnError(ctx, msg, err)
	}()

	return action
}

// wait sleeps for policy's delay after the n-th failure. It returns false
// with Yielded if the delay would end past deadline (not waiting then), or
// with Abandoned if the stop began first.
func (p *Processor[M]) wait(
	stop, work context.Context, deadline time.Time, policy retry.Policy, n int, err error,
) (Outcome, bool) {
	delay := policy.Delay(n)
	if !deadline.IsZero() && time.Until(deadline) < delay {
		return Yielded, false
	}

	if policy.OnRetry != nil {
		policy.OnRetry(work, retry.Attempt{Number: n, Err: err, Delay: delay})
	}

	// The stop may have begun just as the delay ended: no new attempt then.
	if !sleep(stop, delay) || stop.Err() != nil {
		return Abandoned, false
	}

	return Handled, true
}

// log logs msg, prefixed with the processor's name, with attrs and extra.
func (p *Processor[M]) log(ctx context.Context, level slog.Level, msg string, attrs []slog.Attr, extra ...slog.Attr) {
	all := make([]slog.Attr, 0, len(attrs)+len(extra))
	all = append(all, attrs...)
	all = append(all, extra...)

	p.logger().LogAttrs(ctx, level, p.name()+" "+msg, all...)
}

func (p *Processor[M]) logger() *slog.Logger {
	if p.Logger == nil {
		return slog.Default()
	}

	return p.Logger
}

func (p *Processor[M]) name() string {
	if p.Name == "" {
		return "consume"
	}

	return p.Name
}

func (p *Processor[M]) exhaustedErr() error {
	if p.Exhausted == nil {
		return ErrRetriesExhausted
	}

	return p.Exhausted
}

// exhausted reports whether policy allows no attempt after the n-th.
func exhausted(policy retry.Policy, n int) bool {
	return policy.MaxAttempts >= 0 && n >= max(policy.MaxAttempts, 1)
}

// sleep waits for d unless ctx is done first; it reports whether it waited.
func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
