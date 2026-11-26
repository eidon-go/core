package rabbitmq

import (
	"context"
	"errors"

	"github.com/eidon-go/core/retry"
)

// DefaultErrorHandler is the ErrorHandler of a Consumer without
// WithErrorHandler. It retries an error (see WithRedeliveryPolicy) and stops
// the consumer once the attempts are exhausted or for an error marked with
// retry.Permanent: no message is ever dropped silently. Use DeadLetter, or an
// ErrorHandler of your own, to go on past a message that cannot be handled.
func DefaultErrorHandler(_ context.Context, _ *Message, err error) ErrorAction {
	if final(err) {
		return ErrorActionStop
	}

	return ErrorActionRetry
}

// DeadLetter is an ErrorHandler that retries an error like
// DefaultErrorHandler, but once the attempts are exhausted — or at once for
// an error marked with retry.Permanent (a message that cannot be decoded) —
// rejects the message, so that the broker moves it to the dead-letter
// exchange of its queue. Declare the queue with "x-dead-letter-exchange":
// without one the broker drops rejected messages.
func DeadLetter(_ context.Context, _ *Message, err error) ErrorAction {
	if final(err) {
		return ErrorActionReject
	}

	return ErrorActionRetry
}

// final reports whether err allows no more attempts.
func final(err error) bool {
	return errors.Is(err, ErrRetriesExhausted) || errors.Is(err, retry.ErrPermanent)
}
