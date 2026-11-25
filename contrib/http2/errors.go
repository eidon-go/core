package http2

import "errors"

var (
	// ErrNilHandler is returned by New when the handler is nil.
	ErrNilHandler = errors.New("http2: nil handler")

	// ErrNotStarted is returned by Run when Start has not been called: the
	// listener is opened in Start.
	ErrNotStarted = errors.New("http2: Run called before Start")

	// ErrAlreadyStarted is returned by a second Start or a second Run. A
	// Server is single-use, like the http.Server it wraps.
	ErrAlreadyStarted = errors.New("http2: server already started")

	// ErrClosed is returned by Start after Shutdown.
	ErrClosed = errors.New("http2: server closed")

	// ErrNotServing is returned by Healthcheck while the server is not
	// accepting requests: before Run, or once shutdown has begun.
	ErrNotServing = errors.New("http2: server is not serving")

	// ErrInvalidConfig is wrapped by every error from Config.Validate.
	ErrInvalidConfig = errors.New("http2: invalid config")
)
