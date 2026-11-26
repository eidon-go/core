package nats

import (
	"context"
	"errors"

	"github.com/eidon-go/core/retry"
)

// StopOnFailure is an ErrorHandler that retries an error (see
// WithRedeliveryPolicy) and stops the component once the attempts are
// exhausted or for an error marked with retry.Permanent: no message is ever
// dropped. Run then returns an error, so with app.App the whole service
// stops — and a Consumer meets the same message again after a restart.
// Prefer Discard (with an advisory listener or a dead-letter stream) in
// production.
func StopOnFailure(_ context.Context, _ *Message, err error) ErrorAction {
	if final(err) {
		return ErrorActionStop
	}

	return ErrorActionRetry
}

// Discard is an ErrorHandler that retries an error like StopOnFailure,
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
