// Package app runs the components of a service — every lifecycle.Resource
// registered with Add — through one managed lifecycle:
//
//  1. Start: every lifecycle.Starter is started in registration order. A
//     failure shuts down every component — started or not — and Run
//     returns the error.
//  2. Run: every lifecycle.Runner runs concurrently until the context passed
//     to Run is cancelled, a shutdown signal arrives (SIGINT, SIGTERM by
//     default), or any runner returns.
//  3. Shutdown: readiness fails at once (see Healthcheck), the optional
//     shutdown delay lets load balancers notice, then under one timeout the
//     context of every Run is cancelled, every runner is shut down
//     concurrently (which makes its Run return), App waits for their Run to
//     return, and the remaining components are shut down in reverse
//     registration order.
//
// A typical main:
//
//	func main() {
//	    a := app.New(app.WithShutdownTimeout(30 * time.Second))
//	    a.Add(telemetry) // dependencies first
//	    a.Add(db, app.WithReadiness()) // no requests can be served without it
//	    a.Add(cache)                   // degraded without it, still serving
//	    a.Add(grpcServer)
//	    a.Add(httpServer)
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
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/eidon-go/core/lifecycle"
)

// Errors returned by Run and Healthcheck.
var (
	// ErrAlreadyStarted is returned when Run is called more than once.
	ErrAlreadyStarted = errors.New("app: already started")
	// ErrRunnerStopped is the cause reported when a Runner's Run returns nil
	// before shutdown was requested.
	ErrRunnerStopped = errors.New("app: runner stopped unexpectedly")
	// ErrShutdownTimeout is reported for every component that did not finish
	// in time: a runner whose Run had not returned, or a Shutdown that had
	// not returned (it is abandoned, together with its goroutine) or that
	// failed after the deadline.
	ErrShutdownTimeout = errors.New("app: shutdown timed out")
	// ErrNotReady is returned by Healthcheck while the app is not serving:
	// before Run, during startup, and from the moment shutdown is requested.
	ErrNotReady = errors.New("app: not ready")
	// ErrHealthcheckPanicked is the result reported for a component whose
	// Healthcheck panicked: the panic is recovered, so a faulty check fails
	// readiness instead of killing the process.
	ErrHealthcheckPanicked = errors.New("app: healthcheck panicked")
	// ErrShutdownPanicked is reported for a component whose Shutdown
	// panicked: the panic is recovered and the other components are still
	// shut down.
	ErrShutdownPanicked = errors.New("app: shutdown panicked")
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
	shutdownDelay      time.Duration
	healthcheckTimeout time.Duration
	observers          []func(context.Context, Event)

	state atomic.Int32 // a state

	mu         sync.Mutex
	started    bool
	components []lifecycle.Resource
	readiness  []bool // readiness[i]: components[i] was added WithReadiness
}

// state is the externally visible phase of the app, used for readiness.
type state int32

const (
	stateIdle state = iota
	stateStarting
	stateRunning
	stateStopping
	stateStopped
)

func (s state) String() string {
	switch s {
	case stateIdle:
		return "not started"
	case stateStarting:
		return "starting"
	case stateRunning:
		return "running"
	case stateStopping:
		return "stopping"
	case stateStopped:
		return "stopped"
	default:
		return fmt.Sprintf("state(%d)", int32(s))
	}
}

// Option configures an App.
type Option func(*App)

// WithLogger sets the logger for lifecycle events. nil means slog.Default().
// App never changes the process-wide default logger; call slog.SetDefault in
// main if you want that.
func WithLogger(logger *slog.Logger) Option {
	return func(a *App) { a.logger = logger }
}

// WithShutdownTimeout sets the budget for the whole shutdown phase, shared by
// every component (the shutdown delay is not included). Values <= 0 mean
// DefaultShutdownTimeout.
//
// After the budget is spent, each component still shutting down gets 100ms
// more to force-close before it is abandoned and reported with
// ErrShutdownTimeout, so the phase can overrun by up to 100ms per pending
// component. Size the timeout (and e.g. the Kubernetes termination grace
// period) accordingly.
func WithShutdownTimeout(d time.Duration) Option {
	return func(a *App) { a.shutdownTimeout = d }
}

// WithShutdownDelay keeps runners serving for d after a shutdown signal (or
// cancellation of the context passed to Run) while Healthcheck already
// reports ErrNotReady, so load balancers stop routing traffic before servers
// stop accepting it. Typical values are a few seconds, matching the readiness
// probe period. The delay is skipped when shutdown is caused by a failing
// runner. Default: no delay.
func WithShutdownDelay(d time.Duration) Option {
	return func(a *App) { a.shutdownDelay = d }
}

// WithStartTimeout bounds the start phase (all Starter.Start calls together).
// Values <= 0 mean no limit other than the context passed to Run.
// The bound is cooperative: Start is called synchronously, so a Starter that
// ignores its context delays the start phase past the timeout.
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
// DefaultShutdownTimeout, no start timeout, shutdown delay or healthcheck
// timeout.
func New(opts ...Option) *App {
	a := &App{} //nolint:exhaustruct_v5 // zero values are the documented defaults
	for _, opt := range opts {
		opt(a)
	}

	return a
}

// AddOption configures how Add registers one component.
type AddOption func(*registration)

// registration is what Add records about one component besides the
// component itself.
type registration struct {
	readiness bool
}

// WithReadiness makes the component's lifecycle.Healthchecker count toward
// App.Healthcheck: while it fails, the app is not ready. Use it for the
// dependencies without which the service cannot serve requests at all — the
// primary database, say. Without it a component's check only appears in
// HealthReport, so a cache or a message consumer that is down degrades the
// service instead of taking every instance out of the load balancer. It has
// no effect on a component that does not implement lifecycle.Healthchecker.
func WithReadiness() AddOption {
	return func(r *registration) { r.readiness = true }
}

// Add registers a component. Components are added in dependency order: a
// component should be added after the components it uses. Start runs in this
// order, shutdown in reverse. A component that implements lifecycle.Runner is
// run; every component is shut down. Its Healthcheck counts toward
// App.Healthcheck only if it is added WithReadiness.
//
// Add panics if the component is nil (including a typed nil pointer), if the
// same component is added twice, or if it is called after Run: all are
// programming errors.
//
// "The same component" means the same pointer. Non-pointer values and
// pointers to zero-size types are never treated as duplicates: equal values
// need not be the same resource, and distinct zero-size values may share an
// address.
func (a *App) Add(component lifecycle.Resource, opts ...AddOption) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.started {
		panic("app: Add called after Run")
	}

	if lifecycle.IsNil(component) {
		panic(fmt.Sprintf("app: Add: component (%T) is nil", component))
	}

	if isRegistered(component, a.components) {
		panic(fmt.Sprintf("app: Add: component %s is already registered", lifecycle.NameOf(component)))
	}

	var reg registration
	for _, opt := range opts {
		opt(&reg)
	}

	a.components = append(a.components, component)
	a.readiness = append(a.readiness, reg.readiness)
}

// isRegistered reports whether c is already in list. Only pointer components
// are compared, by address: comparing arbitrary values could panic (a struct
// holding a slice in an interface field) or report distinct values as equal,
// and pointers to zero-size types may share an address.
func isRegistered(c lifecycle.Resource, list []lifecycle.Resource) bool {
	value := reflect.ValueOf(c)
	if value.Kind() != reflect.Pointer || value.Type().Elem().Size() == 0 {
		return false
	}

	for _, other := range list {
		ov := reflect.ValueOf(other)
		if ov.Kind() == reflect.Pointer && ov.Type() == value.Type() && ov.Pointer() == value.Pointer() {
			return true
		}
	}

	return false
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

	defer a.state.Store(int32(stateStopped))

	ctx, stopSignals := a.notifyContext(ctx)
	defer stopSignals()

	logger := a.Logger()
	logger.InfoContext(ctx, "app starting", slog.Int("components", len(components)))

	startErr := a.start(ctx, components)

	if ctx.Err() != nil {
		// Stopped by ctx or a signal during startup: a clean stop, whatever
		// the starters returned.
		logger.InfoContext(ctx, "app stopped during startup", slog.Any("cause", context.Cause(ctx)))

		return a.shutdown(ctx, components, nil)
	}

	if startErr != nil {
		logger.ErrorContext(ctx, "app startup failed", slog.Any("err", startErr))

		return errors.Join(startErr, a.shutdown(ctx, components, nil))
	}

	return a.runAndShutdown(ctx, components)
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
	a.state.Store(int32(stateStarting))

	return slices.Clone(a.components), nil
}

// notifyContext layers the configured signals on top of ctx. After the first
// signal it restores the default behavior, so a second signal kills the
// process even if shutdown hangs. When shutdown is started by cancelling ctx
// instead, signals stay handled (a SIGTERM during that shutdown does not kill
// the process).
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
	unregister := context.AfterFunc(sigCtx, func() {
		if ctx.Err() == nil { // done because of a signal, not because of ctx
			stop()
		}
	})

	return sigCtx, func() {
		unregister()
		stop()
	}
}

// start calls Start on every Starter in registration order, stopping at the
// first failure or when ctx is done. Whatever it returns, every component is
// shut down afterwards: any of them may hold resources from construction on,
// and the one that failed may hold some from its partial Start.
func (a *App) start(ctx context.Context, components []lifecycle.Resource) error {
	if a.startTimeout > 0 {
		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(ctx, a.startTimeout)
		defer cancel()
	}

	for _, c := range components {
		s, ok := c.(lifecycle.Starter)
		if !ok {
			continue
		}

		err := ctx.Err()
		if err == nil {
			begin := time.Now()
			err = s.Start(ctx)
			a.observe(ctx, PhaseStart, c, time.Since(begin), err)
		}

		if err != nil {
			return fmt.Errorf("start %s: %w", lifecycle.NameOf(c), err)
		}
	}

	return nil
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

	// Runners keep running until shutdown actually starts — after the
	// shutdown delay — not when ctx is cancelled.
	runCtx, cancelRun := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelRun()

	wait := a.startRunners(ctx, runCtx, components)

	a.state.Store(int32(stateRunning))
	logger.InfoContext(ctx, "app running", slog.Int("runners", len(wait.runners)))

	var cause error

	// With no runners, results never receives: the app runs until ctx is done.
	select {
	case <-ctx.Done():
		a.state.Store(int32(stateStopping))
		logger.InfoContext(ctx, "app stopping", slog.Any("cause", context.Cause(ctx)))
		cause = a.drainDelay(ctx, wait)
	case res := <-wait.results:
		a.state.Store(int32(stateStopping))

		wait.first = res.idx
		// If ctx is already done, both select cases were ready: this is a
		// clean stop that happened to be observed through the runner.
		cause = runErr(res, ctx.Err() == nil)
		logger.Log(ctx, levelFor(cause), "app stopping: runner stopped",
			slog.String("component", lifecycle.NameOf(res.runner)), slog.Any("err", cause))
	}

	cancelRun()

	return errors.Join(cause, a.shutdown(ctx, components, wait))
}

// startRunners runs every Runner among components in its own goroutine and
// returns what shutdown needs to wait for them. Runs are observed with ctx.
func (a *App) startRunners(ctx, runCtx context.Context, components []lifecycle.Resource) *runWait {
	var runners []lifecycle.Runner

	for _, c := range components {
		if r, ok := c.(lifecycle.Runner); ok {
			runners = append(runners, r)
		}
	}

	results := make(chan runResult, len(runners))

	var wg sync.WaitGroup

	for idx, r := range runners {
		wg.Go(func() {
			if runCtx.Err() != nil {
				// Shutdown began before this goroutine got to run: do not
				// start a runner that is already being shut down.
				results <- runResult{idx: idx, runner: r, err: nil}

				return
			}

			begin := time.Now()
			err := r.Run(runCtx)
			a.observe(ctx, PhaseRun, r, time.Since(begin), err)

			results <- runResult{idx: idx, runner: r, err: err}
		})
	}

	allDone := make(chan struct{})

	go func() {
		wg.Wait()
		close(allDone)
	}()

	return &runWait{runners: runners, results: results, done: allDone, first: -1}
}

func levelFor(err error) slog.Level {
	if err != nil {
		return slog.LevelError
	}

	return slog.LevelInfo
}

// drainDelay waits for the shutdown delay, if any, while readiness already
// fails and runners keep serving. A runner that returns during the delay
// ends it; its error, if any, is returned.
func (a *App) drainDelay(ctx context.Context, wait *runWait) error {
	if a.shutdownDelay <= 0 {
		return nil
	}

	a.Logger().InfoContext(ctx, "app draining before shutdown", slog.Duration("delay", a.shutdownDelay))

	timer := time.NewTimer(a.shutdownDelay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case res := <-wait.results:
		wait.first = res.idx

		return runErr(res, false)
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
