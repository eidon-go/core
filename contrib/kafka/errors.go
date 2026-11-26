package kafka

import (
	"errors"

	"github.com/eidon-go/core/lifecycle"
)

var (
	// ErrInvalidConfig is wrapped by every error from Validate and by the
	// constructors for invalid arguments.
	ErrInvalidConfig = errors.New("kafka: invalid config")

	// ErrNotStarted is returned by Consumer.Run when Start has not been
	// called: the consumer joins its group in Start. It is
	// lifecycle.ErrNotStarted.
	ErrNotStarted = lifecycle.ErrNotStarted

	// ErrAlreadyStarted is returned by a second Consumer.Run. It is
	// lifecycle.ErrAlreadyStarted.
	ErrAlreadyStarted = lifecycle.ErrAlreadyStarted

	// ErrClosed is returned by Start after Shutdown. It is lifecycle.ErrClosed.
	ErrClosed = lifecycle.ErrClosed

	// ErrNotRunning is returned by Consumer.Healthcheck while the consumer
	// is not consuming. It is lifecycle.ErrNotRunning.
	ErrNotRunning = lifecycle.ErrNotRunning

	// ErrRetriesExhausted is joined to the handler's error passed to the
	// ErrorHandler once the redelivery policy allows no more attempts.
	ErrRetriesExhausted = errors.New("kafka: redelivery attempts exhausted")

	// ErrStopped is wrapped by the error Consumer.Run returns when the
	// ErrorHandler chose ErrorActionStop; the handler's error is wrapped too.
	ErrStopped = errors.New("kafka: consumer stopped on a handler error")

	// ErrPaused is returned by Consumer.Healthcheck while an ErrorActionPause
	// holds partitions paused, until Consumer.Resume.
	ErrPaused = errors.New("kafka: consumer paused on a handler error")
)
