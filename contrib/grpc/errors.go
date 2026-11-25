package grpc

import "errors"

var (
	// ErrNotStarted is returned by Run when Start has not been called: the
	// listener is opened in Start.
	ErrNotStarted = errors.New("grpc: Run called before Start")

	// ErrAlreadyStarted is returned by a second Start or a second Run. A
	// Server is single-use, like the grpc.Server it wraps.
	ErrAlreadyStarted = errors.New("grpc: server already started")

	// ErrClosed is returned by Start after Shutdown.
	ErrClosed = errors.New("grpc: server closed")

	// ErrNotServing is returned by Healthcheck while the server is not
	// accepting RPCs: before Run, or once shutdown has begun.
	ErrNotServing = errors.New("grpc: server is not serving")

	// ErrInvalidConfig is wrapped by every error from Config.Validate and by
	// New for invalid options.
	ErrInvalidConfig = errors.New("grpc: invalid config")
)
