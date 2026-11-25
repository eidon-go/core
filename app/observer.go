package app

import (
	"context"
	"time"

	"github.com/eidon-go/core/lifecycle"
)

// Phase is a lifecycle phase of a component.
type Phase int

// Phases reported in Event.
const (
	PhaseStart Phase = iota + 1
	PhaseRun
	PhaseShutdown
)

func (p Phase) String() string {
	switch p {
	case PhaseStart:
		return "start"
	case PhaseRun:
		return "run"
	case PhaseShutdown:
		return "shutdown"
	default:
		return "unknown"
	}
}

// Event describes one finished lifecycle call of a component: Start, Run or
// Shutdown. Fields may be added in later versions.
type Event struct {
	Phase Phase
	// Component is lifecycle.NameOf(component).
	Component string
	// Duration is how long the call took.
	Duration time.Duration
	// Err is the error the call returned (for an abandoned Shutdown,
	// ErrShutdownTimeout).
	Err error
}

// WithObserver adds a function called after every Start, Run and Shutdown of
// a component, e.g. to record metrics or traces of the lifecycle. Observers
// are called synchronously from the goroutine that made the call, so they
// must be fast and safe for concurrent use. Repeated options add observers.
func WithObserver(fn func(ctx context.Context, e Event)) Option {
	return func(a *App) {
		if fn != nil {
			a.observers = append(a.observers, fn)
		}
	}
}

func (a *App) observe(ctx context.Context, phase Phase, c any, d time.Duration, err error) {
	if len(a.observers) == 0 {
		return
	}

	e := Event{Phase: phase, Component: lifecycle.NameOf(c), Duration: d, Err: err}
	for _, fn := range a.observers {
		fn(ctx, e)
	}
}
