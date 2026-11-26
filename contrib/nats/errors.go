package nats

import (
	"errors"
	"fmt"

	natsgo "github.com/nats-io/nats.go"

	"github.com/eidon-go/core/lifecycle"
)

var (
	// ErrInvalidConfig is wrapped by every error from Validate, by the
	// constructors for invalid arguments and by the publish methods for an
	// invalid message (nil, or without a subject).
	ErrInvalidConfig = errors.New("nats: invalid config")

	// ErrNotStarted is returned while the Connection has not been started
	// (or its Start failed): by its Healthcheck, by the Publisher and by the
	// Run of a Subscriber or a Consumer. It is lifecycle.ErrNotStarted.
	ErrNotStarted = lifecycle.ErrNotStarted

	// ErrAlreadyStarted is returned by a second Connection.Start and by a
	// second Run. It is lifecycle.ErrAlreadyStarted.
	ErrAlreadyStarted = lifecycle.ErrAlreadyStarted

	// ErrClosed is returned after Shutdown: by Connection.Start, by the
	// Publisher once it or its Connection is shut down, and by the Run of a
	// Subscriber or a Consumer whose Connection is shut down while it runs.
	// It is lifecycle.ErrClosed.
	ErrClosed = lifecycle.ErrClosed

	// ErrNotRunning is returned by the Healthcheck of a Subscriber or a
	// Consumer while Run is not running. It is lifecycle.ErrNotRunning.
	ErrNotRunning = lifecycle.ErrNotRunning

	// ErrNotConnected is returned by Connection.Healthcheck while the client
	// is disconnected and reconnecting, and by the Healthcheck of a Consumer
	// while it is (re)creating its consumer.
	ErrNotConnected = errors.New("nats: not connected")

	// ErrNoReply is returned by Message.Respond for a message that has no
	// reply subject or that did not come from a Subscriber.
	ErrNoReply = errors.New("nats: message has no reply subject")

	// ErrNoResponders is returned by Publisher.Request when no subscriber
	// listens on the subject. It is the nats.go error of the same name.
	ErrNoResponders = natsgo.ErrNoResponders

	// ErrRetriesExhausted is joined to the handler's error passed to the
	// ErrorHandler once the redelivery policy allows no more attempts.
	ErrRetriesExhausted = errors.New("nats: redelivery attempts exhausted")

	// ErrStopped is wrapped by the error Run returns when the ErrorHandler
	// chose ErrorActionStop; the handler's error is wrapped too.
	ErrStopped = errors.New("nats: stopped on a handler error")
)

// wrap prefixes err with the module name; nil stays nil.
func wrap(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("nats: %w", err)
}
