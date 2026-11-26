package rabbitmq

import (
	"context"
	"errors"

	"github.com/eidon-go/core/retry"
)

// StopOnFailure is an ErrorHandler that retries an error (see
// WithRedeliveryPolicy) and stops the consumer once the attempts are
// exhausted or for an error marked with retry.Permanent: no message is ever
// dropped. Run then returns an error, so with app.App the whole service
// stops — and after a restart it meets the same message again. Prefer
// DeadLetter in production.
func StopOnFailure(_ context.Context, _ *Message, err error) ErrorAction {
	if final(err) {
		return ErrorActionStop
	}

	return ErrorActionRetry
}

// DeadLetter is an ErrorHandler that retries an error like
// StopOnFailure, but once the attempts are exhausted — or at once for
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
