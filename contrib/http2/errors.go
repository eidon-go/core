package http2

import (
	"errors"

	"github.com/eidon-go/core/lifecycle"
)

var (
	// ErrNilHandler is returned by New when the handler is nil.
	ErrNilHandler = errors.New("http2: nil handler")

	// ErrNotStarted is returned by Run when Start has not been called: the
	// listener is opened in Start. It is lifecycle.ErrNotStarted.
	ErrNotStarted = lifecycle.ErrNotStarted

	// ErrAlreadyStarted is returned by a second Start or a second Run. A
	// Server is single-use, like the http.Server it wraps. It is
	// lifecycle.ErrAlreadyStarted.
	ErrAlreadyStarted = lifecycle.ErrAlreadyStarted

	// ErrClosed is returned by Start after Shutdown. It is lifecycle.ErrClosed.
	ErrClosed = lifecycle.ErrClosed

	// ErrNotServing is returned by Healthcheck while the server is not
	// accepting requests: before Run, or once shutdown has begun. It is
	// lifecycle.ErrNotServing.
	ErrNotServing = lifecycle.ErrNotServing

	// ErrInvalidConfig is wrapped by every error from Config.Validate.
	ErrInvalidConfig = errors.New("http2: invalid config")
)
