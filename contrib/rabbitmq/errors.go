package rabbitmq

import (
	"errors"
	"fmt"

	"github.com/eidon-go/core/lifecycle"
)

var (
	// ErrInvalidConfig is wrapped by every error from Validate, by the
	// constructors for invalid arguments and by Publish for an invalid
	// message (a nil one, a header value AMQP cannot carry).
	ErrInvalidConfig = errors.New("rabbitmq: invalid config")

	// ErrNotStarted is returned while the Connection has not been started:
	// by its Healthcheck, by Publish and by Consumer.Run. It is
	// lifecycle.ErrNotStarted.
	ErrNotStarted = lifecycle.ErrNotStarted

	// ErrAlreadyStarted is returned by a second Connection.Start or
	// Consumer.Run. It is lifecycle.ErrAlreadyStarted.
	ErrAlreadyStarted = lifecycle.ErrAlreadyStarted

	// ErrClosed is returned after Shutdown: by Connection.Start, by Publish
	// and by the users of a Connection that was shut down. It is
	// lifecycle.ErrClosed.
	ErrClosed = lifecycle.ErrClosed

	// ErrNotRunning is returned by Consumer.Healthcheck while Run is not
	// consuming. It is lifecycle.ErrNotRunning.
	ErrNotRunning = lifecycle.ErrNotRunning

	// ErrNotConnected is returned by Connection.Healthcheck while the
	// connection is down and being redialed, and by Consumer.Healthcheck
	// while the consumer waits for it or subscribes again.
	ErrNotConnected = errors.New("rabbitmq: not connected")

	// ErrNacked is returned by Publish when the broker refused a message
	// (basic.nack): it was not routed to its queues.
	ErrNacked = errors.New("rabbitmq: message nacked by the broker")

	// ErrNotConfirmed is returned by Publish when the channel closed before
	// the broker confirmed a message (the connection was lost, or Shutdown
	// gave up waiting): the message may or may not have been delivered.
	ErrNotConfirmed = errors.New("rabbitmq: publish not confirmed")

	// ErrRetriesExhausted is joined to the handler's error passed to the
	// ErrorHandler once the redelivery policy allows no more attempts.
	ErrRetriesExhausted = errors.New("rabbitmq: redelivery attempts exhausted")

	// ErrStopped is wrapped by the error Consumer.Run returns when the
	// ErrorHandler chose ErrorActionStop; the handler's error is wrapped too.
	ErrStopped = errors.New("rabbitmq: consumer stopped on a handler error")

	// ErrPaused is returned by Consumer.Healthcheck while an
	// ErrorActionPause holds the consumer unsubscribed, until
	// Consumer.Resume or the end of the WithPauseFor length.
	ErrPaused = errors.New("rabbitmq: consumer paused on a handler error")

	// ErrTopology is wrapped by the error of Consumer.Run when, with
	// TopologyVerify, an exchange or queue of the Topology does not exist.
	ErrTopology = errors.New("rabbitmq: topology does not exist")
)

// wrap prefixes err with the module name; nil stays nil.
func wrap(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("rabbitmq: %w", err)
}
