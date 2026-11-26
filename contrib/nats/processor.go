package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/eidon-go/core/retry"
)

// outcome is what becomes of a message after its handling.
type outcome int

const (
	outcomeAck     outcome = iota // handled
	outcomeRequeue                // ErrorActionRequeue
	outcomeReject                 // ErrorActionReject
	outcomeAbandon                // cut off by the stop: neither handled nor failed
	outcomeStop                   // ErrorActionStop (or exhausted Retry, unknown action)
	outcomeRetry                  // another attempt (internal to processor)
)

// processor runs the handler of a Subscriber or a Consumer on one message,
// with the attempts the ErrorHandler and the redelivery policy allow.
type processor struct {
	handler      Handler
	errorHandler ErrorHandler
	redelivery   retry.Policy
	logger       *slog.Logger
	tel          *telemetry
	sub          *subscription
}

// process handles msg. A stop during a redelivery delay abandons it, and so
// does an attempt cut off by Shutdown's deadline: the ErrorHandler is not
// asked about it, since the message did not fail. With outcomeStop the
// error is the one the ErrorHandler saw.
func (p *processor) process(
	stop, work context.Context, msg *Message, logAttrs []slog.Attr,
) (outcome, error) {
	p.tel.delivered(work, p.sub)

	for attempt := 1; ; attempt++ {
		err := p.handleOnce(work, msg)
		if err == nil {
			return outcomeAck, nil
		}

		if work.Err() != nil {
			return outcomeAbandon, nil //nolint:nilerr // cut off, not failed
		}

		out, err := p.failed(work, msg, logAttrs, attempt, err)
		if out != outcomeRetry {
			return out, err
		}

		if !p.waitRedelivery(stop, work, attempt, err) {
			return outcomeAbandon, nil
		}
	}
}

// failed asks the ErrorHandler about a failed attempt, logs it and returns
// the outcome — outcomeRetry for another attempt — and the error as the
// ErrorHandler saw it (ErrRetriesExhausted joined once the redelivery
// policy allows no more attempts).
func (p *processor) failed(
	work context.Context, msg *Message, logAttrs []slog.Attr, attempt int, err error,
) (outcome, error) {
	exhausted := p.redelivery.MaxAttempts >= 0 && attempt >= max(p.redelivery.MaxAttempts, 1)
	if exhausted {
		err = errors.Join(ErrRetriesExhausted, err)
	}

	action := p.decide(work, msg, err)
	p.logFailure(work, logAttrs, attempt, action, err)

	switch {
	case action == ErrorActionRequeue:
		return outcomeRequeue, nil
	case action == ErrorActionReject:
		return outcomeReject, nil
	case action == ErrorActionRetry && !exhausted:
		return outcomeRetry, err
	default:
		return outcomeStop, err
	}
}

// handleOnce calls the handler in the message's process span; a panic is an
// error.
func (p *processor) handleOnce(work context.Context, msg *Message) (err error) {
	ctx, span := p.tel.startProcess(work, p.sub, msg)
	start := time.Now()

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("nats: handler panicked: %v", r) //nolint:err113 // carries the panic value
		}

		p.tel.endProcess(ctx, span, p.sub, start, err)
	}()

	return p.handler.Handle(trace.ContextWithSpan(ctx, span), msg) //nolint:wrapcheck // goes to the ErrorHandler as is
}

// decide asks the ErrorHandler; a panic in it stops the component.
func (p *processor) decide(ctx context.Context, msg *Message, err error) ErrorAction {
	action := ErrorActionStop

	func() {
		defer func() {
			if r := recover(); r != nil {
				p.logger.ErrorContext(ctx, "nats error handler panicked", slog.Any("panic", r))
			}
		}()

		action = p.errorHandler(ctx, msg, err)
	}()

	return action
}

// logFailure logs a failed attempt; never the data or the headers, which
// may carry personal data or secrets.
func (p *processor) logFailure(ctx context.Context, attrs []slog.Attr, attempt int, action ErrorAction, err error) {
	attrs = append(attrs,
		slog.Int("attempt", attempt), slog.String("action", action.String()), slog.Any("error", err))
	p.logger.LogAttrs(ctx, slog.LevelError, "nats message handling failed", attrs...)
}

// waitRedelivery waits for the redelivery delay after the failed attempt,
// unless the stop begins first; it reports whether a new attempt may start.
func (p *processor) waitRedelivery(stop, work context.Context, attempt int, err error) bool {
	delay := p.redelivery.Delay(attempt)
	if p.redelivery.OnRetry != nil {
		p.redelivery.OnRetry(work, retry.Attempt{Number: attempt, Err: err, Delay: delay})
	}

	// The stop may have begun just as the delay ended: no new attempt then.
	return sleep(stop, delay) && stop.Err() == nil
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
