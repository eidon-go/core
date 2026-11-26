package http2_test

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/eidon-go/core/app"
	"github.com/eidon-go/core/contrib/http2"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
)

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "hello")
	})
}

func TestServer_Contract(t *testing.T) {
	t.Parallel()

	lifecycletest.Runner(t, func(t *testing.T) lifecycle.Runner {
		t.Helper()

		return newServer(t, okHandler())
	})
}

func TestServer_Serves(t *testing.T) {
	t.Parallel()

	srv := newServer(t, okHandler())
	assert.Nil(t, srv.Addr(), "no address before Start")
	require.ErrorIs(t, srv.Healthcheck(t.Context()), http2.ErrNotServing)
	assert.Equal(t, http2.DefaultName, srv.Name())

	done := serve(t.Context(), t, srv)

	resp := get(t.Context(), newClient(), baseURL(srv)+"/")
	require.NoError(t, resp.err)
	assert.Equal(t, http.StatusOK, resp.status)
	assert.Equal(t, 1, resp.proto, "HTTP/2 is off on cleartext by default")
	assert.Equal(t, "hello", resp.body)
	require.NoError(t, srv.Healthcheck(t.Context()))

	require.NoError(t, srv.Shutdown(t.Context()))
	require.NoError(t, waitFor(t, done))
	require.ErrorIs(t, srv.Healthcheck(t.Context()), http2.ErrNotServing)
}

func TestServer_ShutdownDrainsInFlightRequests(t *testing.T) {
	t.Parallel()

	h := newBlockingHandler()
	srv := newServer(t, h)
	done := serveObserved(t.Context(), t, srv, h)

	inFlight := getAsync(t.Context(), newClient(), baseURL(srv)+"/")
	waitFor(t, h.entered)

	shutdown := make(chan error, 1)

	go func() { shutdown <- srv.Shutdown(t.Context()) }()

	// Shutdown has begun once the server stops reporting ready.
	require.Eventually(t, func() bool { return srv.Healthcheck(t.Context()) != nil }, testTimeout, time.Millisecond)
	close(h.release)

	resp := waitFor(t, inFlight)
	require.NoError(t, resp.err)
	assert.Equal(t, "done", resp.body)
	require.NoError(t, waitFor(t, shutdown))

	res := waitFor(t, done)
	require.NoError(t, res.err)
	assert.True(t, res.handlerDone, "Run returns once the drain is over")
}

func TestServer_ShutdownForceClosesWhenContextIsDone(t *testing.T) {
	t.Parallel()

	h := newBlockingHandler()
	srv := newServer(t, h)
	done := serve(t.Context(), t, srv)

	inFlight := getAsync(t.Context(), newClient(), baseURL(srv)+"/")
	waitFor(t, h.entered)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := srv.Shutdown(ctx)
	require.ErrorIs(t, err, context.Canceled)

	require.Error(t, waitFor(t, inFlight).err, "the in-flight request is cut off")
	require.NoError(t, waitFor(t, done))
}

func TestServer_CancelDrainsInFlightRequests(t *testing.T) {
	t.Parallel()

	h := newBlockingHandler()
	srv := newServer(t, h)

	runCtx, cancelRun := context.WithCancel(t.Context())
	done := serveObserved(runCtx, t, srv, h)

	inFlight := getAsync(t.Context(), newClient(), baseURL(srv)+"/")
	waitFor(t, h.entered)

	cancelRun()
	require.Eventually(t, func() bool { return srv.Healthcheck(t.Context()) != nil }, testTimeout, time.Millisecond)
	close(h.release)

	resp := waitFor(t, inFlight)
	require.NoError(t, resp.err)
	assert.Equal(t, "done", resp.body, "request context is not cancelled with Run's ctx")

	res := waitFor(t, done)
	require.NoError(t, res.err)
	assert.True(t, res.handlerDone, "Run returns only after the in-flight request finished")
}

func TestServer_CancelDrainIsBoundedByShutdown(t *testing.T) {
	t.Parallel()

	h := newBlockingHandler()
	srv := newServer(t, h)

	runCtx, cancelRun := context.WithCancel(t.Context())
	done := serveObserved(runCtx, t, srv, h)

	inFlight := getAsync(t.Context(), newClient(), baseURL(srv)+"/")
	waitFor(t, h.entered)

	// What app.App does: cancel Run's ctx, then call Shutdown with a deadline.
	cancelRun()

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, srv.Shutdown(ctx), context.DeadlineExceeded)

	shutdownReturned := time.Now()

	require.Error(t, waitFor(t, inFlight).err)

	res := waitFor(t, done)
	require.NoError(t, res.err)
	// Together with Shutdown; the bound leaves room for slow CI under -race
	// while still catching net/http's up-to-500ms shutdown poll.
	assert.Less(t, res.returnedTime.Sub(shutdownReturned), 300*time.Millisecond,
		"Run returns together with Shutdown")
}

func TestServer_ShutdownHooksRunOnce(t *testing.T) {
	t.Parallel()

	var hooks atomic.Int32

	srv := newServer(t, okHandler(), http2.WithHTTPServer(func(s *http.Server) {
		s.RegisterOnShutdown(func() { hooks.Add(1) })
	}))

	runCtx, cancelRun := context.WithCancel(t.Context())
	done := serve(runCtx, t, srv)
	require.NoError(t, get(t.Context(), newClient(), baseURL(srv)+"/").err)

	// app.App's order: cancel, then Shutdown — plus a repeated Shutdown.
	cancelRun()
	require.NoError(t, srv.Shutdown(t.Context()))
	require.NoError(t, srv.Shutdown(t.Context()))
	require.NoError(t, waitFor(t, done))

	require.Eventually(t, func() bool { return hooks.Load() >= 1 }, testTimeout, time.Millisecond)
	require.Never(t, func() bool { return hooks.Load() > 1 }, 50*time.Millisecond, 5*time.Millisecond)
}

// A finished stop is reported as clean even when the ctx of a later Shutdown
// has expired too: both are ready, and the finished stop must win.
func TestServer_FinishedStopWinsOverExpiredContext(t *testing.T) {
	t.Parallel()

	srv := newServer(t, okHandler())
	done := serve(t.Context(), t, srv)
	require.NoError(t, srv.Shutdown(t.Context()))
	require.NoError(t, waitFor(t, done))

	expired, expire := context.WithCancel(t.Context())
	expire()

	for range 300 {
		require.NoError(t, srv.Shutdown(expired))
	}
}

func TestServer_ProtocolsFromHTTPServerAreKept(t *testing.T) {
	t.Parallel()

	serverTLS, roots := selfSignedTLS(t)
	srv := newServer(t, okHandler(), http2.WithTLSConfig(serverTLS),
		http2.WithHTTPServer(func(s *http.Server) { s.Protocols.SetHTTP2(false) }))
	serve(t.Context(), t, srv)

	client := &http.Client{Timeout: testTimeout, Transport: &http.Transport{
		TLSClientConfig:   &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
		ForceAttemptHTTP2: true,
	}}

	resp := get(t.Context(), client, "https://"+srv.Addr().String()+"/")
	require.NoError(t, resp.err)
	assert.Equal(t, 1, resp.proto, "HTTP/2 disabled in place through WithHTTPServer stays disabled")
}

func TestServer_ListenerClosedOnce(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  bool
	}{
		{name: "served", run: true},
		{name: "never served", run: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var lc net.ListenConfig

			inner, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)

			ln := &countingListener{Listener: inner, done: make(chan struct{})}
			srv := newServer(t, okHandler(), http2.WithListener(ln))

			if tt.run {
				done := serve(t.Context(), t, srv)
				require.NoError(t, get(t.Context(), newClient(), baseURL(srv)+"/").err)
				require.NoError(t, srv.Shutdown(t.Context()))
				require.NoError(t, waitFor(t, done))
			} else {
				require.NoError(t, srv.Start(t.Context()))
				require.NoError(t, srv.Shutdown(t.Context()))
			}

			require.NoError(t, srv.Shutdown(t.Context()))
			assert.Equal(t, int32(1), ln.closes.Load())
		})
	}
}

func TestServer_TLSFromHTTPServer(t *testing.T) {
	t.Parallel()

	serverTLS, roots := selfSignedTLS(t)
	srv := newServer(t, okHandler(), http2.WithHTTPServer(func(s *http.Server) { s.TLSConfig = serverTLS }))
	serve(t.Context(), t, srv)

	client := &http.Client{Timeout: testTimeout, Transport: &http.Transport{
		TLSClientConfig:   &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
		ForceAttemptHTTP2: true,
	}}

	resp := get(t.Context(), client, "https://"+srv.Addr().String()+"/")
	require.NoError(t, resp.err)
	assert.Equal(t, 2, resp.proto)

	_, err := http2.New(http2.Config{}, okHandler(), http2.WithHTTPServer(func(s *http.Server) {
		s.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}))
	require.ErrorIs(t, err, http2.ErrInvalidConfig, "a TLS config without a certificate is rejected")
}

func TestServer_StateErrors(t *testing.T) {
	t.Parallel()

	t.Run("RunBeforeStart", func(t *testing.T) {
		t.Parallel()

		srv := newServer(t, okHandler())
		require.ErrorIs(t, srv.Run(t.Context()), http2.ErrNotStarted)
	})

	t.Run("StartTwice", func(t *testing.T) {
		t.Parallel()

		srv := newServer(t, okHandler())
		require.NoError(t, srv.Start(t.Context()))
		require.ErrorIs(t, srv.Start(t.Context()), http2.ErrAlreadyStarted)
	})

	t.Run("RunTwice", func(t *testing.T) {
		t.Parallel()

		srv := newServer(t, okHandler())
		done := serve(t.Context(), t, srv)

		// A served request proves the background Run is the first one.
		require.NoError(t, get(t.Context(), newClient(), baseURL(srv)+"/").err)
		require.ErrorIs(t, srv.Run(t.Context()), http2.ErrAlreadyStarted)
		require.NoError(t, srv.Shutdown(t.Context()))
		require.NoError(t, waitFor(t, done))
	})

	t.Run("StartAfterShutdown", func(t *testing.T) {
		t.Parallel()

		srv := newServer(t, okHandler())
		require.NoError(t, srv.Shutdown(t.Context()))
		require.ErrorIs(t, srv.Start(t.Context()), http2.ErrClosed)
		require.NoError(t, srv.Run(t.Context()), "Run after Shutdown returns nil")
	})

	t.Run("ShutdownTwice", func(t *testing.T) {
		t.Parallel()

		srv := newServer(t, okHandler())
		require.NoError(t, srv.Start(t.Context()))
		require.NoError(t, srv.Shutdown(t.Context()))
		require.NoError(t, srv.Shutdown(t.Context()))
	})
}

func TestServer_ShutdownAfterStartReleasesPort(t *testing.T) {
	t.Parallel()

	srv := newServer(t, okHandler())
	require.NoError(t, srv.Start(t.Context()))
	addr := srv.Addr().String()
	require.NoError(t, srv.Shutdown(t.Context()))

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", addr)
	require.NoError(t, err, "the listener opened by Start is closed without Run")
	require.NoError(t, ln.Close())
}

func TestServer_StartFailsOnBusyPort(t *testing.T) {
	t.Parallel()

	var lc net.ListenConfig

	busy, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = busy.Close() })

	srv := newServerWithConfig(t, http2.Config{Addr: busy.Addr().String()}, okHandler())
	err = srv.Start(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "http2: listen on")
	assert.Nil(t, srv.Addr())
}

func TestServer_WithListener(t *testing.T) {
	t.Parallel()

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := newServerWithConfig(t, http2.Config{Addr: "invalid:1"}, okHandler(), http2.WithListener(ln))
	done := serve(t.Context(), t, srv)
	assert.Equal(t, ln.Addr(), srv.Addr())

	resp := get(t.Context(), newClient(), baseURL(srv)+"/")
	require.NoError(t, resp.err)
	assert.Equal(t, "hello", resp.body)

	require.NoError(t, srv.Shutdown(t.Context()))
	require.NoError(t, waitFor(t, done))
}

func TestServer_UnencryptedHTTP2(t *testing.T) {
	t.Parallel()

	srv := newServerWithConfig(t, http2.Config{Addr: "127.0.0.1:0", UnencryptedHTTP2: true}, okHandler())
	serve(t.Context(), t, srv)

	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)

	client := &http.Client{Timeout: testTimeout, Transport: &http.Transport{Protocols: protocols}}

	resp := get(t.Context(), client, baseURL(srv)+"/")
	require.NoError(t, resp.err)
	assert.Equal(t, 2, resp.proto)
	assert.Equal(t, "hello", resp.body)
}

func TestServer_TLS(t *testing.T) {
	t.Parallel()

	serverTLS, roots := selfSignedTLS(t)
	srv := newServer(t, okHandler(), http2.WithTLSConfig(serverTLS))
	serve(t.Context(), t, srv)

	client := &http.Client{Timeout: testTimeout, Transport: &http.Transport{
		TLSClientConfig:   &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
		ForceAttemptHTTP2: true,
	}}

	resp := get(t.Context(), client, "https://"+srv.Addr().String()+"/")
	require.NoError(t, resp.err)
	assert.Equal(t, 2, resp.proto, "HTTP/2 is negotiated over TLS")
	assert.Equal(t, "hello", resp.body)
}

func TestServer_Probes(t *testing.T) {
	t.Parallel()

	// The handler rejects everything, like an auth middleware would.
	deny := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})

	notReady := lifecycle.HealthcheckFunc(func(context.Context) error { return errors.New("db is down") })
	ready := lifecycle.HealthcheckFunc(func(context.Context) error { return nil })

	tests := []struct {
		name      string
		opts      []http2.Option
		path      string
		status    int
		forbidden string
	}{
		{name: "liveness", opts: []http2.Option{http2.WithProbes(notReady)}, path: http2.LivenessPath, status: 200},
		{name: "ready", opts: []http2.Option{http2.WithProbes(ready)}, path: http2.ReadinessPath, status: 200},
		{
			name: "not ready", opts: []http2.Option{http2.WithProbes(notReady)},
			path: http2.ReadinessPath, status: 503, forbidden: "db is down",
		},
		{name: "nil readiness", opts: []http2.Option{http2.WithProbes(nil)}, path: http2.ReadinessPath, status: 200},
		{name: "other paths", opts: []http2.Option{http2.WithProbes(ready)}, path: "/api", status: 401},
		{name: "probes off", path: http2.LivenessPath, status: 401},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := newServer(t, deny, tt.opts...)
			serve(t.Context(), t, srv)

			resp := get(t.Context(), newClient(), baseURL(srv)+tt.path)
			require.NoError(t, resp.err)
			assert.Equal(t, tt.status, resp.status)

			if tt.forbidden != "" {
				assert.NotContains(t, resp.body, tt.forbidden, "readiness errors are not exposed")
			}
		})
	}
}

func TestServer_Tracing(t *testing.T) {
	t.Parallel()

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /users/{id}", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "user")
	})
	mux.HandleFunc("/any", func(http.ResponseWriter, *http.Request) {})

	srv := newServer(t, mux, http2.WithTracerProvider(tp), http2.WithProbes(nil))
	serve(t.Context(), t, srv)

	client := newClient()
	for _, path := range []string{"/users/42", "/any", "/missing", http2.LivenessPath} {
		require.NoError(t, get(t.Context(), client, baseURL(srv)+path).err)
	}

	require.Eventually(t, func() bool { return len(rec.Ended()) >= 3 }, testTimeout, time.Millisecond)

	ended := rec.Ended()
	names := make([]string, 0, len(ended))

	for _, span := range ended {
		names = append(names, span.Name())
	}

	assert.ElementsMatch(t, []string{"GET /users/{id}", "GET /any", "GET"}, names, "probes are not traced")
}

// telemetry stands in for the Provider of the otel module.
type telemetry struct{ tp trace.TracerProvider }

func (t telemetry) TracerProvider() trace.TracerProvider { return t.tp }

func (telemetry) MeterProvider() metric.MeterProvider { return metricnoop.NewMeterProvider() }

func (telemetry) Propagators() propagation.TextMapPropagator { return propagation.TraceContext{} }

// With WithTelemetry alone, the server continues the trace of the request.
func TestServer_WithTelemetry(t *testing.T) {
	t.Parallel()

	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))

	srv := newServer(t, http.NotFoundHandler(), http2.WithTelemetry(nil), http2.WithTelemetry(telemetry{tp: tp}))
	serve(t.Context(), t, srv)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, baseURL(srv)+"/", nil)
	require.NoError(t, err)
	req.Header.Set("Traceparent", "00-"+traceID+"-00f067aa0ba902b7-01")

	resp, err := newClient().Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	require.Eventually(t, func() bool { return len(rec.Ended()) == 1 }, testTimeout, time.Millisecond)
	assert.Equal(t, traceID, rec.Ended()[0].SpanContext().TraceID().String(), "the incoming trace context is kept")
}

func TestServer_WithApp(t *testing.T) {
	t.Parallel()

	h := newBlockingHandler()
	mux := http.NewServeMux()
	mux.Handle("/slow", h)

	a := app.New(app.WithSignals(), app.WithShutdownTimeout(testTimeout))
	srv := newServer(t, mux, http2.WithProbes(a))
	a.Add(srv)

	ctx, stop := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() { done <- a.Run(ctx) }()

	client := newClient()

	// Start has run (the address is known) once the app reports ready.
	require.Eventually(t, func() bool { return a.Healthcheck(t.Context()) == nil }, testTimeout, time.Millisecond)
	assert.Equal(t, http.StatusOK, get(t.Context(), client, baseURL(srv)+http2.ReadinessPath).status)

	inFlight := getAsync(t.Context(), client, baseURL(srv)+"/slow")
	waitFor(t, h.entered)

	stop()
	require.Eventually(t, func() bool { return srv.Healthcheck(t.Context()) != nil }, testTimeout, time.Millisecond)

	close(h.release)

	resp := waitFor(t, inFlight)
	require.NoError(t, resp.err)
	assert.Equal(t, "done", resp.body, "the app drains in-flight requests on shutdown")
	require.NoError(t, waitFor(t, done))
}

func TestNew_Errors(t *testing.T) {
	t.Parallel()

	_, err := http2.New(http2.Config{}, nil)
	require.ErrorIs(t, err, http2.ErrNilHandler)

	var nilFunc http.HandlerFunc

	_, err = http2.New(http2.Config{}, nilFunc)
	require.ErrorIs(t, err, http2.ErrNilHandler)

	_, err = http2.New(http2.Config{Addr: "no-port", MaxHeaderBytes: -1}, okHandler())
	require.ErrorIs(t, err, http2.ErrInvalidConfig)
	assert.Contains(t, err.Error(), "addr")
	assert.Contains(t, err.Error(), "maxHeaderBytes")

	_, err = http2.New(
		http2.Config{},
		okHandler(),
		http2.WithTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}),
	)
	require.ErrorIs(t, err, http2.ErrInvalidConfig)
}

func TestNew_Config(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  http2.Config
		want http2.Config // the http.Server fields New should set
	}{
		{
			name: "defaults",
			want: http2.Config{
				Addr:              http2.DefaultAddr,
				ReadHeaderTimeout: http2.DefaultReadHeaderTimeout,
				ReadTimeout:       http2.DefaultReadTimeout,
				WriteTimeout:      http2.DefaultWriteTimeout,
				IdleTimeout:       http2.DefaultIdleTimeout,
			},
		},
		{
			name: "custom",
			cfg: http2.Config{
				Addr: "127.0.0.1:9000", ReadHeaderTimeout: time.Second, ReadTimeout: 2 * time.Second,
				WriteTimeout: 3 * time.Second, IdleTimeout: 4 * time.Second, MaxHeaderBytes: 512,
			},
			want: http2.Config{
				Addr: "127.0.0.1:9000", ReadHeaderTimeout: time.Second, ReadTimeout: 2 * time.Second,
				WriteTimeout: 3 * time.Second, IdleTimeout: 4 * time.Second, MaxHeaderBytes: 512,
			},
		},
		{
			// Negative values reach net/http, which disables the timeout; zero
			// would make ReadHeaderTimeout and IdleTimeout use ReadTimeout.
			name: "negative ReadHeaderTimeout",
			cfg:  http2.Config{ReadHeaderTimeout: -1},
			want: http2.Config{
				Addr: http2.DefaultAddr, ReadHeaderTimeout: -1, ReadTimeout: http2.DefaultReadTimeout,
				WriteTimeout: http2.DefaultWriteTimeout, IdleTimeout: http2.DefaultIdleTimeout,
			},
		},
		{
			name: "negative ReadTimeout",
			cfg:  http2.Config{ReadTimeout: -1},
			want: http2.Config{
				Addr: http2.DefaultAddr, ReadHeaderTimeout: http2.DefaultReadHeaderTimeout, ReadTimeout: -1,
				WriteTimeout: http2.DefaultWriteTimeout, IdleTimeout: http2.DefaultIdleTimeout,
			},
		},
		{
			name: "negative WriteTimeout",
			cfg:  http2.Config{WriteTimeout: -1},
			want: http2.Config{
				Addr: http2.DefaultAddr, ReadHeaderTimeout: http2.DefaultReadHeaderTimeout,
				ReadTimeout: http2.DefaultReadTimeout, WriteTimeout: -1, IdleTimeout: http2.DefaultIdleTimeout,
			},
		},
		{
			name: "negative IdleTimeout",
			cfg:  http2.Config{IdleTimeout: -1},
			want: http2.Config{
				Addr: http2.DefaultAddr, ReadHeaderTimeout: http2.DefaultReadHeaderTimeout,
				ReadTimeout: http2.DefaultReadTimeout, WriteTimeout: http2.DefaultWriteTimeout, IdleTimeout: -1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var got *http.Server

			_, err := http2.New(tt.cfg, okHandler(), http2.WithHTTPServer(func(s *http.Server) { got = s }))
			require.NoError(t, err)
			require.NotNil(t, got)

			assert.Equal(t, tt.want.Addr, got.Addr)
			assert.Equal(t, tt.want.ReadHeaderTimeout, got.ReadHeaderTimeout)
			assert.Equal(t, tt.want.ReadTimeout, got.ReadTimeout)
			assert.Equal(t, tt.want.WriteTimeout, got.WriteTimeout)
			assert.Equal(t, tt.want.IdleTimeout, got.IdleTimeout)
			assert.Equal(t, tt.want.MaxHeaderBytes, got.MaxHeaderBytes)
		})
	}
}

func TestServer_WithName(t *testing.T) {
	t.Parallel()

	srv := newServer(t, okHandler(), http2.WithName("admin"))
	assert.Equal(t, "admin", srv.Name())
}
