// Package app runs the components of a service — every lifecycle.Resource
// registered with Add — through one managed lifecycle:
//
//  1. Start: every lifecycle.Starter is started in registration order. A
//     failure shuts down what was started and Run returns the error.
//  2. Run: every lifecycle.Runner runs concurrently until the context passed
//     to Run is cancelled, a shutdown signal arrives (SIGINT, SIGTERM by
//     default), or any runner returns.
//  3. Shutdown: under one timeout, runners are shut down in reverse
//     registration order (which makes their Run return), App waits for their
//     Run to return, then the remaining resources are shut down in reverse
//     registration order.
//
// A typical main:
//
//	func main() {
//	    a := app.New(app.WithShutdownTimeout(30 * time.Second))
//	    a.Add(telemetry, db, cache) // resources, in dependency order
//	    a.Add(grpcServer, httpServer)
//	    if err := a.Run(context.Background()); err != nil {
//	        log.Fatal(err)
//	    }
//	}
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/eidon-go/core/lifecycle"
)

// Errors returned by Run.
var (
	// ErrAlreadyStarted is returned when Run is called more than once.
	ErrAlreadyStarted = errors.New("app: already started")
	// ErrRunnerStopped is the cause reported when a Runner's Run returns nil
	// before shutdown was requested.
	ErrRunnerStopped = errors.New("runner stopped unexpectedly")
	// ErrShutdownTimeout is reported when runners did not return from Run
	// before the shutdown timeout expired.
	ErrShutdownTimeout = errors.New("app: shutdown timed out")
)

// DefaultShutdownTimeout bounds the shutdown phase unless WithShutdownTimeout
// sets another value.
const DefaultShutdownTimeout = 30 * time.Second

// App is the lifecycle orchestrator. Create it with New; the zero value is
// also usable and behaves like New() without options.
//
// Add is meant for setup and must not be called after Run. Healthcheck,
// HealthReport and Logger are safe for concurrent use at any time.
type App struct {
	logger             *slog.Logger
	signals            []os.Signal
	signalsSet         bool
	startTimeout       time.Duration
	shutdownTimeout    time.Duration
	healthcheckTimeout time.Duration

	mu         sync.Mutex
	started    bool
	components []lifecycle.Resource
}

// Option configures an App.
type Option func(*App)

// WithLogger sets the logger for lifecycle events. nil means slog.Default().
// App never changes the process-wide default logger; call slog.SetDefault in
// main if you want that.
func WithLogger(l *slog.Logger) Option {
	return func(a *App) { a.logger = l }
}

// WithShutdownTimeout sets the budget for the whole shutdown phase, shared by
// every component. Values <= 0 mean DefaultShutdownTimeout.
func WithShutdownTimeout(d time.Duration) Option {
	return func(a *App) { a.shutdownTimeout = d }
}

// WithStartTimeout bounds the start phase (all Starter.Start calls together).
// Values <= 0 mean no limit other than the context passed to Run.
func WithStartTimeout(d time.Duration) Option {
	return func(a *App) { a.startTimeout = d }
}

// WithSignals sets the OS signals that trigger shutdown (default SIGINT and
// SIGTERM). WithSignals() with no arguments disables signal handling; the app
// then stops only when the context passed to Run is cancelled or a runner
// returns.
//
// After the first signal the default signal behavior is restored, so a second
// Ctrl+C terminates a shutdown that hangs.
func WithSignals(sigs ...os.Signal) Option {
	return func(a *App) {
		a.signals = slices.Clone(sigs)
		a.signalsSet = true
	}
}

// WithHealthcheckTimeout bounds each component's Healthcheck call in
// Healthcheck and HealthReport. Values <= 0 mean no limit other than the
// caller's context.
func WithHealthcheckTimeout(d time.Duration) Option {
	return func(a *App) { a.healthcheckTimeout = d }
}

// New creates an App. Defaults: slog.Default() logger, SIGINT and SIGTERM,
// DefaultShutdownTimeout, no start or healthcheck timeout.
func New(opts ...Option) *App {
	a := &App{} //nolint:exhaustruct_v5 // zero values are the documented defaults
	for _, opt := range opts {
		opt(a)
	}

	return a
}

// Add registers components in dependency order: a component should be added
// after the components it uses. Start runs in this order, shutdown in reverse.
// A component that implements lifecycle.Runner is run; every component is shut
// down.
//
// Add panics if a component is nil (including a typed nil pointer) or if it is
// called after Run: both are programming errors.
func (a *App) Add(components ...lifecycle.Resource) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.started {
		panic("app: Add called after Run")
	}

	for i, c := range components {
		if lifecycle.IsNil(c) {
			panic(fmt.Sprintf("app: Add: component %d (%T) is nil", i, c))
		}
	}

	a.components = append(a.components, components...)
}

// Run starts, runs and shuts down every registered component (see the package
// documentation) and blocks until shutdown completes.
//
// Run returns nil after a clean stop: ctx cancelled or a signal received, and
// every component shut down without error. Otherwise it returns the cause —
// a Start error, a Runner error or ErrRunnerStopped — joined with any
// shutdown errors and ErrShutdownTimeout. Run may be called only once;
// later calls return ErrAlreadyStarted.
func (a *App) Run(ctx context.Context) error {
	components, err := a.begin()
	if err != nil {
		return err
	}

	logger := a.Logger()

	ctx, stopSignals := a.notifyContext(ctx)
	defer stopSignals()

	logger.InfoContext(ctx, "app starting", slog.Int("components", len(components)))

	live, startErr := a.start(ctx, components)
	if startErr != nil {
		if ctx.Err() != nil {
			// Stopped by ctx or a signal during startup: a clean stop.
			logger.InfoContext(ctx, "app stopped during startup", slog.Any("err", startErr))

			startErr = nil
		} else {
			logger.ErrorContext(ctx, "app startup failed", slog.Any("err", startErr))
		}

		return errors.Join(startErr, a.shutdown(ctx, live, nil))
	}

	return a.runAndShutdown(ctx, live)
}

// Healthcheck runs every registered lifecycle.Healthchecker concurrently and
// returns nil if all are healthy, or the joined errors, each prefixed with the
// component's name. It satisfies lifecycle.Healthchecker, so an App can back a
// /readyz endpoint directly.
func (a *App) Healthcheck(ctx context.Context) error {
	var errs []error

	for _, r := range a.HealthReport(ctx) {
		if r.Err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", r.Name, r.Err))
		}
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
// returns one result per checker, in registration order. Components that do
// not implement lifecycle.Healthchecker are not included.
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

			results[idx].Err = checker.Healthcheck(checkCtx)
		})
	}

	wg.Wait()

	return results
}

// Logger returns the logger set with WithLogger, or slog.Default().
func (a *App) Logger() *slog.Logger {
	if a.logger == nil {
		return slog.Default()
	}

	return a.logger
}

// begin marks the app as started and snapshots the registered components.
func (a *App) begin() ([]lifecycle.Resource, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.started {
		return nil, ErrAlreadyStarted
	}

	a.started = true

	return slices.Clone(a.components), nil
}

// notifyContext layers the configured signals on top of ctx. After the first
// signal it restores the default behavior, so a second signal kills the
// process even if shutdown hangs.
func (a *App) notifyContext(ctx context.Context) (context.Context, func()) {
	sigs := a.signals
	if !a.signalsSet {
		sigs = []os.Signal{os.Interrupt, syscall.SIGTERM}
	}

	// signal.NotifyContext without signals would relay every signal
	// (including SIGURG used by the runtime), so "no signals" must skip it.
	if len(sigs) == 0 {
		return ctx, func() {}
	}

	sigCtx, stop := signal.NotifyContext(ctx, sigs...)
	unregister := context.AfterFunc(sigCtx, stop)

	return sigCtx, func() {
		unregister()
		stop()
	}
}

// start calls Start on every Starter in registration order. It returns the
// components that must be shut down: everything up to the last successful
// Start, plus every non-Starter (those were constructed by the caller and
// hold resources whether or not startup completed).
func (a *App) start(ctx context.Context, components []lifecycle.Resource) ([]lifecycle.Resource, error) {
	if a.startTimeout > 0 {
		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(ctx, a.startTimeout)
		defer cancel()
	}

	live := make([]lifecycle.Resource, 0, len(components))

	for idx, c := range components {
		s, ok := c.(lifecycle.Starter)
		if !ok {
			live = append(live, c)

			continue
		}

		if err := s.Start(ctx); err != nil {
			for _, rest := range components[idx+1:] {
				if _, isStarter := rest.(lifecycle.Starter); !isStarter {
					live = append(live, rest)
				}
			}

			return live, fmt.Errorf("start %s: %w", lifecycle.NameOf(c), err)
		}

		live = append(live, c)
	}

	return live, nil
}

type runResult struct {
	idx    int // index in the runners slice
	runner lifecycle.Runner
	err    error
}

// runAndShutdown runs every Runner until ctx is done or one of them returns,
// then shuts everything down.
func (a *App) runAndShutdown(ctx context.Context, components []lifecycle.Resource) error {
	logger := a.Logger()

	var runners []lifecycle.Runner

	for _, c := range components {
		if r, ok := c.(lifecycle.Runner); ok {
			runners = append(runners, r)
		}
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	results := make(chan runResult, len(runners))

	var wg sync.WaitGroup

	for i, r := range runners {
		wg.Go(func() {
			results <- runResult{idx: i, runner: r, err: r.Run(runCtx)}
		})
	}

	allDone := make(chan struct{})

	go func() {
		wg.Wait()
		close(allDone)
	}()

	logger.InfoContext(ctx, "app running", slog.Int("runners", len(runners)))

	wait := &runWait{runners: runners, results: results, done: allDone, first: -1}

	var cause error

	// With no runners, results never receives: the app runs until ctx is done.
	select {
	case <-ctx.Done():
		logger.InfoContext(ctx, "app stopping", slog.String("reason", "context done or signal"))
	case res := <-results:
		wait.first = res.idx
		cause = runErr(res, true)
		logger.ErrorContext(ctx, "app stopping: runner stopped",
			slog.String("component", lifecycle.NameOf(res.runner)),
			slog.Any("err", cause),
		)
	}

	cancelRun()

	return errors.Join(cause, a.shutdown(ctx, components, wait))
}

// runWait lets shutdown wait for runners between the two shutdown phases.
type runWait struct {
	runners []lifecycle.Runner
	results <-chan runResult
	done    <-chan struct{}
	// first is the index of the runner whose return triggered shutdown
	// (already reported), or -1.
	first int
}

// shutdown shuts components down under one timeout: runners in reverse
// order, then (if wait is set) waits for their Run to return, then every
// other component in reverse order.
func (a *App) shutdown(ctx context.Context, components []lifecycle.Resource, wait *runWait) error {
	timeout := a.shutdownTimeout
	if timeout <= 0 {
		timeout = DefaultShutdownTimeout
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()

	var errs []error

	for _, c := range slices.Backward(components) {
		if _, ok := c.(lifecycle.Runner); ok {
			errs = append(errs, a.shutdownOne(shutdownCtx, c, "runner"))
		}
	}

	if wait != nil {
		errs = append(errs, wait.collect(shutdownCtx)...)
	}

	for _, c := range slices.Backward(components) {
		if _, ok := c.(lifecycle.Runner); !ok {
			errs = append(errs, a.shutdownOne(shutdownCtx, c, "resource"))
		}
	}

	err := errors.Join(errs...)
	if err != nil {
		a.Logger().ErrorContext(shutdownCtx, "app stopped with errors", slog.Any("err", err))
	} else {
		a.Logger().InfoContext(shutdownCtx, "app stopped")
	}

	return err
}

func (a *App) shutdownOne(ctx context.Context, c lifecycle.Resource, kind string) error {
	if err := c.Shutdown(ctx); err != nil {
		a.Logger().ErrorContext(ctx, kind+" shutdown failed",
			slog.String("component", lifecycle.NameOf(c)),
			slog.Any("err", err),
		)

		return fmt.Errorf("shutdown %s: %w", lifecycle.NameOf(c), err)
	}

	return nil
}

// collect waits until every Run has returned or ctx expires, and returns the
// errors runners returned after shutdown was requested, plus
// ErrShutdownTimeout naming the runners that are still running.
func (w *runWait) collect(ctx context.Context) []error {
	select {
	case <-w.done:
	case <-ctx.Done():
	}

	// Indexes, not the runners themselves: a Runner may be a non-comparable
	// type and cannot be a map key.
	returned := make([]bool, len(w.runners))
	if w.first >= 0 {
		returned[w.first] = true
	}

	errs := w.drain(returned)

	var stuck []string

	for idx, r := range w.runners {
		if !returned[idx] {
			stuck = append(stuck, lifecycle.NameOf(r))
		}
	}

	if len(stuck) > 0 {
		errs = append(errs, fmt.Errorf("%w: still running: %s", ErrShutdownTimeout, strings.Join(stuck, ", ")))
	}

	return errs
}

// drain reads the results available without blocking, marks their runners as
// returned and returns their errors.
func (w *runWait) drain(returned []bool) []error {
	var errs []error

	for {
		select {
		case res := <-w.results:
			returned[res.idx] = true

			if err := runErr(res, false); err != nil {
				errs = append(errs, err)
			}
		default:
			return errs
		}
	}
}

// runErr turns a Run result into the error to report. Before shutdown was
// requested (early), any return is unexpected; after it, only real errors
// count.
func runErr(res runResult, early bool) error {
	switch {
	case res.err == nil || errors.Is(res.err, context.Canceled):
		if early {
			return fmt.Errorf("run %s: %w", lifecycle.NameOf(res.runner), ErrRunnerStopped)
		}

		return nil
	default:
		return fmt.Errorf("run %s: %w", lifecycle.NameOf(res.runner), res.err)
	}
}

// Compile-time check: an App can be used wherever a Healthchecker is expected.
var _ lifecycle.Healthchecker = (*App)(nil)
