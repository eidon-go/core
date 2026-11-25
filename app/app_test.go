package app_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/app"
	"github.com/eidon-go/core/lifecycle"
)

// recorder collects lifecycle events from every fake in a test.
type recorder struct {
	mu     sync.Mutex
	events []string
}

func (r *recorder) add(event string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.events = append(r.events, event)
}

func (r *recorder) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.events...)
}

// resource is a plain Resource.
type resource struct {
	name        string
	rec         *recorder
	shutdownErr error
	hang        bool // Shutdown ignores ctx and blocks for a long time
}

func (r *resource) Name() string { return r.name }

func (r *resource) Shutdown(context.Context) error {
	r.rec.add("shutdown " + r.name)

	if r.hang {
		time.Sleep(time.Hour)
	}

	return r.shutdownErr
}

type starter struct {
	resource

	startErr error
	blockCtx bool // Start blocks until ctx is done
}

func (s *starter) Start(ctx context.Context) error {
	s.rec.add("start " + s.name)

	if s.blockCtx {
		<-ctx.Done()

		return ctx.Err()
	}

	return s.startErr
}

// server behaves like a real server: Run blocks until Shutdown is called
// (ignoring ctx on purpose) and returns nil; Run after Shutdown returns at
// once.
type server struct {
	name    string
	rec     *recorder
	running chan struct{}
	runOnce sync.Once
	stop    chan struct{}
	once    sync.Once
}

func newServer(name string, rec *recorder) *server {
	return &server{name: name, rec: rec, running: make(chan struct{}), stop: make(chan struct{})}
}

func (s *server) Name() string { return s.name }

func (s *server) Run(context.Context) error {
	s.runOnce.Do(func() { close(s.running) })
	<-s.stop

	return nil
}

func (s *server) Shutdown(context.Context) error {
	s.rec.add("shutdown " + s.name)
	s.once.Do(func() { close(s.stop) })

	return nil
}

// worker returns from Run when ctx is cancelled, or with runErr right away.
type worker struct {
	name   string
	rec    *recorder
	runErr error
	early  bool // return runErr immediately instead of waiting for ctx
	hang   bool // ignore ctx and Shutdown entirely
}

func (w *worker) Name() string { return w.name }

func (w *worker) Run(ctx context.Context) error {
	switch {
	case w.hang:
		select {}
	case w.early:
		return w.runErr
	default:
		<-ctx.Done()

		return ctx.Err()
	}
}

func (w *worker) Shutdown(context.Context) error {
	w.rec.add("shutdown " + w.name)

	return nil
}

type checker struct {
	name  string
	err   error
	delay time.Duration
}

func (c *checker) Name() string { return c.name }

func (c *checker) Shutdown(context.Context) error { return nil }

func (c *checker) Healthcheck(ctx context.Context) error {
	select {
	case <-time.After(c.delay):
		return c.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newApp(opts ...app.Option) *app.App {
	base := make([]app.Option, 0, 3+len(opts))
	base = append(base,
		app.WithSignals(),
		app.WithLogger(slog.New(slog.DiscardHandler)),
		app.WithShutdownTimeout(2*time.Second),
	)

	return app.New(append(base, opts...)...)
}

// runAsync runs a in the background and returns its result channel.
func runAsync(ctx context.Context, a *app.App) <-chan error {
	done := make(chan error, 1)

	go func() { done <- a.Run(ctx) }()

	return done
}

func wait(t *testing.T, done <-chan error) error {
	t.Helper()

	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")

		return nil
	}
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	require.Eventually(t, cond, 2*time.Second, 2*time.Millisecond)
}

func cancelled(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	return ctx
}

func TestRun_CleanStopOrder(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	db := &starter{resource: resource{name: "db", rec: rec}}
	cache := &resource{name: "cache", rec: rec}
	grpcSrv := newServer("grpc", rec)
	httpSrv := newServer("http", rec)

	a := newApp()
	a.Add(db, cache)
	a.Add(grpcSrv, httpSrv)

	ctx, cancel := context.WithCancel(t.Context())
	done := runAsync(ctx, a)

	<-grpcSrv.running
	<-httpSrv.running
	cancel()

	require.NoError(t, wait(t, done))
	assert.Equal(t, []string{
		"start db",
		"shutdown http", "shutdown grpc", // runners first, reverse order
		"shutdown cache", "shutdown db", // then resources, reverse order
	}, rec.list())
}

func TestRun_ShutdownMakesServerReturn(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	srv := newServer("http", rec) // ignores ctx, stops only on Shutdown

	a := newApp(app.WithShutdownTimeout(time.Second))
	a.Add(srv)

	ctx, cancel := context.WithCancel(t.Context())
	done := runAsync(ctx, a)

	<-srv.running
	cancel()

	require.NoError(t, wait(t, done))
}

func TestRun_RunnerErrorStopsApp(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	errBoom := errors.New("boom")
	srv := newServer("http", rec)
	failing := &worker{name: "consumer", rec: rec, runErr: errBoom, early: true}
	db := &resource{name: "db", rec: rec}

	a := newApp(app.WithShutdownDelay(time.Hour)) // skipped for a failing runner
	a.Add(db, srv, failing)

	err := wait(t, runAsync(t.Context(), a))
	require.ErrorIs(t, err, errBoom)
	require.ErrorContains(t, err, "run consumer: boom")
	assert.Contains(t, rec.list(), "shutdown http")
	assert.Contains(t, rec.list(), "shutdown db")
}

func TestRun_RunnerReturningNilEarlyStopsApp(t *testing.T) {
	t.Parallel()

	rec := &recorder{}

	a := newApp()
	a.Add(&worker{name: "job", rec: rec, early: true}, &worker{name: "other", rec: rec})

	err := wait(t, runAsync(t.Context(), a))
	require.ErrorIs(t, err, app.ErrRunnerStopped)
	require.ErrorContains(t, err, "job")
}

// A cancelled ctx makes runners return at once; that must never be reported
// as ErrRunnerStopped, whichever select case wins.
func TestRun_CancelledContextIsAlwaysClean(t *testing.T) {
	t.Parallel()

	for range 200 {
		rec := &recorder{}

		a := newApp()
		a.Add(&worker{name: "a", rec: rec}, &worker{name: "b", rec: rec})

		require.NoError(t, a.Run(cancelled(t)))
	}
}

// When a runner fails at once, other runners may not have entered Run yet;
// they must not be started after their Shutdown, and a server returning nil
// after Shutdown is not an error.
func TestRun_ImmediateFailureWithManyRunners(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")

	for range 50 {
		rec := &recorder{}

		a := newApp()
		for range 10 {
			a.Add(newServer("srv", rec))
		}

		a.Add(&worker{name: "failing", rec: rec, runErr: errBoom, early: true})

		err := wait(t, runAsync(t.Context(), a))
		require.ErrorIs(t, err, errBoom)
		require.NotErrorIs(t, err, app.ErrShutdownTimeout)
	}
}

func TestRun_HangingRunnerHitsShutdownTimeout(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	db := &resource{name: "db", rec: rec}

	a := newApp(app.WithShutdownTimeout(100 * time.Millisecond))
	a.Add(db, &worker{name: "stuck", rec: rec, hang: true})

	ctx, cancel := context.WithCancel(t.Context())
	done := runAsync(ctx, a)

	time.Sleep(20 * time.Millisecond)
	cancel()

	err := wait(t, done)
	require.ErrorIs(t, err, app.ErrShutdownTimeout)
	require.ErrorContains(t, err, "still running: stuck")
	assert.Contains(t, rec.list(), "shutdown db", "resources are shut down even after a timeout")
}

func TestRun_HangingShutdownIsAbandoned(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	stuck := &resource{name: "pool", rec: rec, hang: true}
	first := &resource{name: "first", rec: rec}

	a := newApp(app.WithShutdownTimeout(50 * time.Millisecond))
	a.Add(first, stuck)

	start := time.Now()
	err := wait(t, runAsync(cancelled(t), a))

	require.ErrorIs(t, err, app.ErrShutdownTimeout)
	require.ErrorContains(t, err, "shutdown pool")
	assert.Less(t, time.Since(start), time.Second, "Run must not wait for the stuck Shutdown")
	assert.Contains(t, rec.list(), "shutdown first", "later components still get Shutdown")
}

func TestRun_StartFailureRollsBack(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	errStart := errors.New("no connection")
	first := &starter{resource: resource{name: "first", rec: rec}}
	plain := &resource{name: "plain", rec: rec}
	failing := &starter{resource: resource{name: "failing", rec: rec}, startErr: errStart}
	notStarted := &starter{resource: resource{name: "not-started", rec: rec}}
	later := &resource{name: "later", rec: rec}
	srv := newServer("http", rec)

	a := newApp()
	a.Add(first, plain, failing, notStarted, later, srv)

	err := wait(t, runAsync(t.Context(), a))
	require.ErrorIs(t, err, errStart)
	require.ErrorContains(t, err, "start failing: no connection")
	assert.Equal(t, []string{
		"start first", "start failing",
		// runners first (http was constructed, so it is shut down), then the
		// rest in reverse; failing and not-started are never shut down.
		"shutdown http", "shutdown later", "shutdown plain", "shutdown first",
	}, rec.list())
}

func TestRun_CancelDuringStartIsCleanStop(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	blocking := &starter{resource: resource{name: "slow", rec: rec}, blockCtx: true}
	next := &starter{resource: resource{name: "next", rec: rec}}

	a := newApp()
	a.Add(blocking, next)

	ctx, cancel := context.WithCancel(t.Context())
	done := runAsync(ctx, a)

	time.Sleep(20 * time.Millisecond)
	cancel()

	require.NoError(t, wait(t, done))
	assert.NotContains(t, rec.list(), "start next", "no Start after cancellation")
}

func TestRun_StartTimeout(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	blocking := &starter{resource: resource{name: "slow", rec: rec}, blockCtx: true}

	a := newApp(app.WithStartTimeout(50 * time.Millisecond))
	a.Add(blocking)

	err := wait(t, runAsync(t.Context(), a))
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRun_RunnerThatIsAlsoStarter(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	srv := &startingServer{server: newServer("http", rec)}

	a := newApp()
	a.Add(srv)

	ctx, cancel := context.WithCancel(t.Context())
	done := runAsync(ctx, a)

	<-srv.running
	cancel()

	require.NoError(t, wait(t, done))
	assert.Equal(t, []string{"start http", "shutdown http"}, rec.list())
}

type startingServer struct{ *server }

func (s *startingServer) Start(context.Context) error {
	s.rec.add("start " + s.name)

	return nil
}

func TestRun_Group(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	db := &starter{resource: resource{name: "db", rec: rec}}
	cache := &starter{resource: resource{name: "cache", rec: rec}}
	srv := newServer("http", rec)

	a := newApp()
	a.Add(lifecycle.GroupNamed("infra", db, cache), srv)

	ctx, cancel := context.WithCancel(t.Context())
	done := runAsync(ctx, a)

	<-srv.running
	cancel()

	require.NoError(t, wait(t, done))
	assert.Equal(t, []string{"start db", "start cache", "shutdown http", "shutdown cache", "shutdown db"}, rec.list())
}

func TestRun_NoRunnersBlocksUntilCancel(t *testing.T) {
	t.Parallel()

	rec := &recorder{}

	a := newApp()
	a.Add(&resource{name: "db", rec: rec})

	ctx, cancel := context.WithCancel(t.Context())
	done := runAsync(ctx, a)

	select {
	case err := <-done:
		t.Fatalf("Run returned before cancel: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	cancel()
	require.NoError(t, wait(t, done))
	assert.Equal(t, []string{"shutdown db"}, rec.list())
}

func TestRun_ShutdownErrorsAreJoined(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	errA := errors.New("a failed")
	errB := errors.New("b failed")

	a := newApp()
	a.Add(&resource{name: "a", rec: rec, shutdownErr: errA}, &resource{name: "b", rec: rec, shutdownErr: errB})

	err := wait(t, runAsync(cancelled(t), a))
	require.ErrorIs(t, err, errA)
	require.ErrorIs(t, err, errB)
	require.ErrorContains(t, err, "shutdown a: a failed")
}

func TestRun_Twice(t *testing.T) {
	t.Parallel()

	a := newApp()

	require.NoError(t, a.Run(cancelled(t)))
	require.ErrorIs(t, a.Run(cancelled(t)), app.ErrAlreadyStarted)
}

func TestRun_SignalTriggersShutdown(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	srv := newServer("http", rec)

	a := newApp(app.WithSignals(syscall.SIGUSR1))
	a.Add(srv)

	done := runAsync(t.Context(), a)

	<-srv.running
	require.NoError(t, syscall.Kill(syscall.Getpid(), syscall.SIGUSR1))

	require.NoError(t, wait(t, done))
}

// WithSignals() must not catch every signal (signal.NotifyContext without
// signals relays all of them, including SIGWINCH and runtime SIGURG).
func TestRun_NoSignalsIgnoresUnrelatedSignals(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	srv := newServer("http", rec)

	a := newApp() // WithSignals()
	a.Add(srv)

	ctx, cancel := context.WithCancel(t.Context())
	done := runAsync(ctx, a)

	<-srv.running
	require.NoError(t, syscall.Kill(syscall.Getpid(), syscall.SIGWINCH))

	select {
	case err := <-done:
		t.Fatalf("app stopped on SIGWINCH: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()
	require.NoError(t, wait(t, done))
}

func TestReadiness(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	gate := make(chan struct{})
	slowStart := &gatedStarter{resource: resource{name: "db", rec: rec}, gate: gate}
	srv := newServer("http", rec)

	a := newApp(app.WithShutdownDelay(200 * time.Millisecond))
	a.Add(slowStart, srv)

	require.ErrorIs(t, a.Healthcheck(t.Context()), app.ErrNotReady, "before Run")

	ctx, cancel := context.WithCancel(t.Context())
	done := runAsync(ctx, a)

	eventually(t, func() bool { return slowStart.entered.Load() })
	require.ErrorIs(t, a.Healthcheck(t.Context()), app.ErrNotReady, "while starting")

	close(gate)
	<-srv.running
	eventually(t, func() bool { return a.Healthcheck(t.Context()) == nil })

	cancel()
	eventually(t, func() bool { return errors.Is(a.Healthcheck(t.Context()), app.ErrNotReady) })
	assert.NotContains(t, rec.list(), "shutdown http", "runners keep serving during the shutdown delay")

	require.NoError(t, wait(t, done))
	require.ErrorIs(t, a.Healthcheck(t.Context()), app.ErrNotReady, "after Run")
}

type gatedStarter struct {
	resource

	gate    chan struct{}
	entered atomic.Bool
}

func (g *gatedStarter) Start(context.Context) error {
	g.entered.Store(true)
	<-g.gate

	return nil
}

func TestObserver(t *testing.T) {
	t.Parallel()

	rec := &recorder{}

	var (
		mu     sync.Mutex
		events []string
	)

	a := newApp(app.WithObserver(func(_ context.Context, e app.Event) {
		mu.Lock()
		defer mu.Unlock()

		events = append(events, e.Phase.String()+" "+e.Component)
	}))

	srv := newServer("http", rec)
	a.Add(&starter{resource: resource{name: "db", rec: rec}}, srv)

	ctx, cancel := context.WithCancel(t.Context())
	done := runAsync(ctx, a)

	<-srv.running
	cancel()
	require.NoError(t, wait(t, done))

	mu.Lock()
	defer mu.Unlock()

	assert.ElementsMatch(t, []string{"start db", "run http", "shutdown http", "shutdown db"}, events)
}

func TestAdd_Panics(t *testing.T) {
	t.Parallel()

	var nilResource *resource

	assert.PanicsWithValue(t, "app: Add: component 1 (<nil>) is nil", func() {
		newApp().Add(&resource{}, nil)
	})
	assert.Panics(t, func() { newApp().Add(nilResource) }, "typed nil")

	dup := &resource{name: "db"}

	assert.PanicsWithValue(t, "app: Add: component db is already registered", func() { newApp().Add(dup, dup) })
	assert.Panics(t, func() {
		a := newApp()
		a.Add(dup)
		a.Add(dup)
	})

	// Non-comparable components cannot be detected but must not panic.
	f := lifecycle.ShutdownFunc(func(context.Context) error { return nil })

	assert.NotPanics(t, func() { newApp().Add(f, f) })

	a := newApp()
	require.NoError(t, a.Run(cancelled(t)))

	assert.PanicsWithValue(t, "app: Add called after Run", func() { a.Add(&resource{}) })
}

func TestHealthcheck(t *testing.T) {
	t.Parallel()

	errDown := errors.New("down")
	rec := &recorder{}
	srv := newServer("http", rec)

	a := newApp()
	a.Add(
		&checker{name: "db", delay: 50 * time.Millisecond},
		&checker{name: "cache", err: errDown, delay: 50 * time.Millisecond},
		&resource{name: "plain", rec: rec},
		lifecycle.HealthcheckFunc(func(context.Context) error { return nil }),
		srv,
	)

	start := time.Now()
	report := a.HealthReport(t.Context())

	assert.Less(t, time.Since(start), 95*time.Millisecond, "checks run concurrently")
	assert.Equal(t, []app.HealthResult{
		{Name: "db", Err: nil},
		{Name: "cache", Err: errDown},
		{Name: "lifecycle.HealthcheckFunc", Err: nil},
	}, report)

	ctx, cancel := context.WithCancel(t.Context())
	done := runAsync(ctx, a)

	<-srv.running

	var err error

	eventually(t, func() bool {
		err = a.Healthcheck(t.Context())

		return !errors.Is(err, app.ErrNotReady)
	})
	require.ErrorIs(t, err, errDown)
	require.EqualError(t, err, "cache: down")

	cancel()
	require.NoError(t, wait(t, done))
}

func TestHealthcheckTimeout(t *testing.T) {
	t.Parallel()

	a := newApp(app.WithHealthcheckTimeout(20 * time.Millisecond))
	a.Add(&checker{name: "slow", delay: time.Second})

	report := a.HealthReport(t.Context())
	require.Len(t, report, 1)
	require.ErrorIs(t, report[0].Err, context.DeadlineExceeded)
}

func TestHealthcheckConcurrentWithAdd(t *testing.T) {
	t.Parallel()

	a := newApp()

	var wg sync.WaitGroup

	wg.Go(func() {
		for range 100 {
			a.Add(&checker{name: "c"})
		}
	})
	wg.Go(func() {
		for range 100 {
			_ = a.HealthReport(t.Context())
		}
	})
	wg.Wait()
}

func TestZeroValueAndNilLogger(t *testing.T) {
	t.Parallel()

	before := slog.Default()

	a := app.New(app.WithLogger(nil), app.WithSignals())
	assert.Same(t, before, a.Logger())
	assert.Same(t, before, slog.Default(), "WithLogger must not change the default logger")

	var zero app.App

	var calls atomic.Int32

	zero.Add(lifecycle.ShutdownFunc(func(context.Context) error {
		calls.Add(1)

		return nil
	}))

	require.NoError(t, zero.Run(cancelled(t)))
	assert.Equal(t, int32(1), calls.Load())
}
