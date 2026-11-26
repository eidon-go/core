package nats

import (
	"context"
	"errors"

	"github.com/eidon-go/core/retry"
)

// DefaultErrorHandler is the ErrorHandler of a Subscriber or a Consumer
// without WithErrorHandler. It retries an error (see WithRedeliveryPolicy)
// and stops the component once the attempts are exhausted or for an error
// marked with retry.Permanent: no message is ever dropped silently. Use
// Discard, or an ErrorHandler of your own, to go on past a message that
// cannot be handled.
func DefaultErrorHandler(_ context.Context, _ *Message, err error) ErrorAction {
	if final(err) {
		return ErrorActionStop
	}

	return ErrorActionRetry
}

// Discard is an ErrorHandler that retries an error like DefaultErrorHandler,
// but once the attempts are exhausted — or at once for an error marked with
// retry.Permanent (a message that cannot be decoded) — rejects the message:
// a Consumer terminates it (the server publishes a MSG_TERMINATED advisory,
// see ErrorActionReject), a Subscriber drops it. Both log it.
func Discard(_ context.Context, _ *Message, err error) ErrorAction {
	if final(err) {
		return ErrorActionReject
	}

	return ErrorActionRetry
}

// final reports whether err allows no more attempts.
func final(err error) bool {
	return errors.Is(err, ErrRetriesExhausted) || errors.Is(err, retry.ErrPermanent)
}
