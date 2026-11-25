package grpc

import (
	"errors"

	"github.com/eidon-go/core/lifecycle"
)

var (
	// ErrNotStarted is returned by Run when Start has not been called: the
	// listener is opened in Start. It is lifecycle.ErrNotStarted.
	ErrNotStarted = lifecycle.ErrNotStarted

	// ErrAlreadyStarted is returned by a second Start or a second Run. A
	// Server is single-use, like the grpc.Server it wraps. It is
	// lifecycle.ErrAlreadyStarted.
	ErrAlreadyStarted = lifecycle.ErrAlreadyStarted

	// ErrClosed is returned by Start after Shutdown. It is lifecycle.ErrClosed.
	ErrClosed = lifecycle.ErrClosed

	// ErrNotServing is returned by Healthcheck while the server is not
	// accepting RPCs: before Run, or once shutdown has begun. It is
	// lifecycle.ErrNotServing.
	ErrNotServing = lifecycle.ErrNotServing

	// ErrInvalidConfig is wrapped by every error from Config.Validate and by
	// New for invalid options.
	ErrInvalidConfig = errors.New("grpc: invalid config")
)
