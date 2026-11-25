package lifecycle

import "context"

// ShutdownFunc adapts a function to Resource, e.g. to register a cleanup
// that has no component of its own:
//
//	a.Add(lifecycle.ShutdownFunc(tracerProvider.Shutdown))
type ShutdownFunc func(ctx context.Context) error

// Shutdown implements Resource by calling f.
func (f ShutdownFunc) Shutdown(ctx context.Context) error { return f(ctx) }

// HealthcheckFunc adapts a function to Healthchecker. It is also a Resource
// with a no-op Shutdown, so it can be registered with app.App on its own:
//
//	a.Add(lifecycle.HealthcheckFunc(func(ctx context.Context) error { return queue.Ping(ctx) }))
type HealthcheckFunc func(ctx context.Context) error

// Healthcheck implements Healthchecker by calling f.
func (f HealthcheckFunc) Healthcheck(ctx context.Context) error { return f(ctx) }

// Shutdown implements Resource; it does nothing.
func (HealthcheckFunc) Shutdown(context.Context) error { return nil }

// Compile-time checks.
var (
	_ Resource      = ShutdownFunc(nil)
	_ Healthchecker = HealthcheckFunc(nil)
	_ Resource      = HealthcheckFunc(nil)
)
