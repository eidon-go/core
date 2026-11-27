package river

import (
	"errors"

	"github.com/eidon-go/core/lifecycle"
)

var (
	// ErrInvalidConfig is wrapped by New for a nil DB and for a river.Config
	// River rejects.
	ErrInvalidConfig = errors.New("river: invalid config")

	// ErrNotStarted is returned by Run, Insert and InsertMany before Start,
	// and by Start when the DB is not started yet (register the client
	// after the DB). It is lifecycle.ErrNotStarted.
	ErrNotStarted = lifecycle.ErrNotStarted

	// ErrAlreadyStarted is returned by a second Start or Run. It is
	// lifecycle.ErrAlreadyStarted.
	ErrAlreadyStarted = lifecycle.ErrAlreadyStarted

	// ErrClosed is returned by Start after Shutdown. It is lifecycle.ErrClosed.
	ErrClosed = lifecycle.ErrClosed

	// ErrNotRunning is returned by Healthcheck while Run is not running. It
	// is lifecycle.ErrNotRunning.
	ErrNotRunning = lifecycle.ErrNotRunning

	// ErrStopped is returned by Run when the River client stops on its own,
	// before the component is stopped.
	ErrStopped = errors.New("river: client stopped")
)
