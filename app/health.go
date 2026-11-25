package app

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/eidon-go/core/lifecycle"
)

// Healthcheck reports readiness. It returns ErrNotReady unless the app is
// running — before Run, while starting and from the moment shutdown is
// requested — and otherwise runs every registered lifecycle.Healthchecker
// concurrently and joins the failures, each prefixed with the component's
// name. App satisfies lifecycle.Healthchecker, so it can back a /readyz
// endpoint directly.
//
// The app counts as running as soon as every runner has been launched, not
// when each has finished binding; components that need to be truly ready
// (e.g. a server listening) should do that work in Start or report it in
// their own Healthcheck.
func (a *App) Healthcheck(ctx context.Context) error {
	if s := state(a.state.Load()); s != stateRunning {
		return fmt.Errorf("%w: %s", ErrNotReady, s)
	}

	var errs []error

	for _, r := range a.HealthReport(ctx) {
		if r.Err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", r.Name, r.Err))
		}
	}

	// Shutdown may have been requested while the checks ran: a check that
	// started before it must not report the app as ready after it.
	if s := state(a.state.Load()); s != stateRunning {
		return fmt.Errorf("%w: %s", ErrNotReady, s)
	}

	return errors.Join(errs...)
}

// HealthResult is the outcome of one component's health check.
type HealthResult struct {
	// Name is lifecycle.NameOf(component).
	Name string
	// Err is nil when the component is healthy.
	Err error
}

// HealthReport runs every registered lifecycle.Healthchecker concurrently and
// returns one result per checker, in registration order, whatever the app's
// own state. Components that do not implement lifecycle.Healthchecker are not
// included. A Healthcheck that panics is reported as ErrHealthcheckPanicked.
func (a *App) HealthReport(ctx context.Context) []HealthResult {
	a.mu.Lock()
	components := slices.Clone(a.components)
	a.mu.Unlock()

	var checkers []lifecycle.Healthchecker

	results := make([]HealthResult, 0, len(components))

	for _, c := range components {
		if hc, ok := c.(lifecycle.Healthchecker); ok {
			checkers = append(checkers, hc)
			results = append(results, HealthResult{Name: lifecycle.NameOf(c), Err: nil})
		}
	}

	var wg sync.WaitGroup

	for idx, checker := range checkers {
		wg.Go(func() {
			checkCtx := ctx

			if a.healthcheckTimeout > 0 {
				var cancel context.CancelFunc

				checkCtx, cancel = context.WithTimeout(ctx, a.healthcheckTimeout)
				defer cancel()
			}

			results[idx].Err = runCheck(checkCtx, checker)
		})
	}

	wg.Wait()

	return results
}

// runCheck calls checker.Healthcheck, turning a panic into an error: checks
// run in their own goroutines, where an unrecovered panic ends the process.
func runCheck(ctx context.Context, checker lifecycle.Healthchecker) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%w: %v", ErrHealthcheckPanicked, p)
		}
	}()

	return checker.Healthcheck(ctx) //nolint:wrapcheck // reported under the component's name
}
