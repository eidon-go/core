package grpc_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/eidon-go/core/app"
	grpcsrv "github.com/eidon-go/core/contrib/grpc"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
)

func TestServer_Contract(t *testing.T) {
	t.Parallel()

	lifecycletest.Runner(t, func(t *testing.T) lifecycle.Runner {
		t.Helper()

		srv, _ := newServer(t)

		return srv
	})
}

func TestServer_Serves(t *testing.T) {
	t.Parallel()

	srv, ts := newServer(t)
	assert.Nil(t, srv.Addr(), "no address before Start")
	require.ErrorIs(t, srv.Healthcheck(t.Context()), grpcsrv.ErrNotServing)
	assert.Equal(t, grpcsrv.DefaultName, srv.Name())

	done := serve(t.Context(), t, srv)
	conn := dial(t, srv)

	close(ts.release)
	require.NoError(t, invoke(t.Context(), conn))
	require.NoError(t, srv.Healthcheck(t.Context()))

	require.NoError(t, srv.Shutdown(t.Context()))
	require.NoError(t, waitFor(t, done))
	require.ErrorIs(t, srv.Healthcheck(t.Context()), grpcsrv.ErrNotServing)
}

func TestServer_ShutdownDrainsInFlightRPCs(t *testing.T) {
	t.Parallel()

	srv, ts := newServer(t)
	done := serveObserved(t.Context(), t, srv, ts)

	inFlight := invokeAsync(t.Context(), dial(t, srv))
	waitFor(t, ts.entered)

	shutdown := make(chan error, 1)

	go func() { shutdown <- srv.Shutdown(t.Context()) }()

	// Shutdown has begun once the server stops reporting ready.
	require.Eventually(t, func() bool { return srv.Healthcheck(t.Context()) != nil }, testTimeout, time.Millisecond)
	close(ts.release)

	require.NoError(t, waitFor(t, inFlight))
	require.NoError(t, waitFor(t, shutdown))

	res := waitFor(t, done)
	require.NoError(t, res.err)
	assert.True(t, res.handlerDone, "Run returns once the drain is over")
}

func TestServer_ShutdownForceClosesWhenContextIsDone(t *testing.T) {
	t.Parallel()

	srv, ts := newServer(t)
	done := serve(t.Context(), t, srv)

	inFlight := invokeAsync(t.Context(), dial(t, srv))
	waitFor(t, ts.entered)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, srv.Shutdown(ctx), context.Canceled)
	assert.Equal(t, codes.Unavailable, status.Code(waitFor(t, inFlight)), "the in-flight RPC is cut off")
	require.NoError(t, waitFor(t, done))
}

func TestServer_CancelDrainsInFlightRPCs(t *testing.T) {
	t.Parallel()

	srv, ts := newServer(t)

	runCtx, cancelRun := context.WithCancel(t.Context())
	done := serveObserved(runCtx, t, srv, ts)

	inFlight := invokeAsync(t.Context(), dial(t, srv))
	waitFor(t, ts.entered)

	cancelRun()
	require.Eventually(t, func() bool { return srv.Healthcheck(t.Context()) != nil }, testTimeout, time.Millisecond)
	close(ts.release)

	require.NoError(t, waitFor(t, inFlight), "the RPC context is not cancelled with Run's ctx")

	res := waitFor(t, done)
	require.NoError(t, res.err)
	assert.True(t, res.handlerDone, "Run returns only after the in-flight RPC finished")
}

func TestServer_CancelDrainIsBoundedByShutdown(t *testing.T) {
	t.Parallel()

	srv, ts := newServer(t)

	runCtx, cancelRun := context.WithCancel(t.Context())
	done := serveObserved(runCtx, t, srv, ts)

	inFlight := invokeAsync(t.Context(), dial(t, srv))
	waitFor(t, ts.entered)

	// What app.App does: cancel Run's ctx, then call Shutdown with a deadline.
	cancelRun()

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, srv.Shutdown(ctx), context.DeadlineExceeded)

	shutdownReturned := time.Now()

	require.Error(t, waitFor(t, inFlight))

	res := waitFor(t, done)
	require.NoError(t, res.err)
	assert.Less(t, res.returnedTime.Sub(shutdownReturned), time.Second,
		"Run returns together with Shutdown")
}

// A handler that ignores its context keeps grpc-go's GracefulStop waiting even
// after Stop; Shutdown must still return at its deadline.
func TestServer_ShutdownReturnsWhenHandlerIgnoresContext(t *testing.T) {
	t.Parallel()

	srv, ts := newServer(t)
	t.Cleanup(func() { close(ts.release) }) // after Shutdown: let the handler go

	done := serve(t.Context(), t, srv)
	conn := dial(t, srv)

	inFlight := make(chan error, 1)

	go func() { inFlight <- conn.Invoke(t.Context(), methodStuck, &emptypb.Empty{}, &emptypb.Empty{}) }()

	waitFor(t, ts.entered)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	shutdown := make(chan error, 1)

	go func() { shutdown <- srv.Shutdown(ctx) }()

	require.ErrorIs(t, waitFor(t, shutdown), context.DeadlineExceeded)
	require.Error(t, waitFor(t, inFlight), "the client sees the connection closed")
	require.NoError(t, waitFor(t, done))

	expired, expire := context.WithCancel(t.Context())
	expire()
	require.NoError(t, srv.Shutdown(expired), "a later Shutdown returns at once")
}

func TestServer_Draining(t *testing.T) {
	t.Parallel()

	srv, _ := newServer(t)

	runCtx, cancelRun := context.WithCancel(t.Context())
	done := serve(runCtx, t, srv)

	select {
	case <-srv.Draining():
		t.Fatal("draining before the stop")
	default:
	}

	cancelRun()
	waitFor(t, srv.Draining())
	require.NoError(t, srv.Shutdown(t.Context()))
	require.NoError(t, waitFor(t, done))
}

// failingListener fails Accept with a permanent error.
type failingListener struct{ net.Listener }

func (l failingListener) Accept() (net.Conn, error) { return nil, errAccept }

var errAccept = errors.New("accept failed")

func TestServer_RunReturnsServeError(t *testing.T) {
	t.Parallel()

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv, _ := newServer(t, grpcsrv.WithListener(failingListener{ln}))
	err = waitFor(t, serve(t.Context(), t, srv))
	require.ErrorIs(t, err, errAccept)
	assert.Contains(t, err.Error(), "grpc: serve:")
	require.ErrorIs(t, srv.Healthcheck(t.Context()), grpcsrv.ErrNotServing)
	require.NoError(t, srv.Shutdown(t.Context()))
}

// Without WithHealth, a health service registered by hand is an ordinary
// service: interceptors apply to it.
func TestServer_NoHealthBypassWithoutWithHealth(t *testing.T) {
	t.Parallel()

	reject := func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
		return nil, status.Error(codes.Unauthenticated, "no token")
	}

	srv, _ := newServer(t, grpcsrv.WithUnaryInterceptors(reject))
	healthpb.RegisterHealthServer(srv, health.NewServer())
	serve(t.Context(), t, srv)

	_, err := healthpb.NewHealthClient(dial(t, srv)).Check(t.Context(), &healthpb.HealthCheckRequest{})
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestServer_StreamDrainIsBoundedByShutdown(t *testing.T) {
	t.Parallel()

	srv, ts := newServer(t)
	done := serve(t.Context(), t, srv)

	stream, err := dial(t, srv).NewStream(t.Context(), &grpc.StreamDesc{ServerStreams: true}, methodStream)
	require.NoError(t, err)
	require.NoError(t, stream.SendMsg(&emptypb.Empty{}))
	require.NoError(t, stream.CloseSend())
	require.NoError(t, stream.RecvMsg(&emptypb.Empty{}))
	waitFor(t, ts.entered)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, srv.Shutdown(ctx), context.DeadlineExceeded, "a stream that does not end holds the drain")
	require.Error(t, stream.RecvMsg(&emptypb.Empty{}))
	require.NoError(t, waitFor(t, done))
	// The force-close cancels the handler's context but does not wait for the
	// handler to return.
	waitFor(t, ts.exited)
}

// A finished stop is reported as clean even when the ctx of a later Shutdown
// has expired too: both are ready, and the finished stop must win.
func TestServer_FinishedStopWinsOverExpiredContext(t *testing.T) {
	t.Parallel()

	srv, _ := newServer(t)
	done := serve(t.Context(), t, srv)
	require.NoError(t, srv.Shutdown(t.Context()))
	require.NoError(t, waitFor(t, done))

	expired, expire := context.WithCancel(t.Context())
	expire()

	for range 300 {
		require.NoError(t, srv.Shutdown(expired))
	}
}

func TestServer_ConcurrentShutdownAndRun(t *testing.T) {
	t.Parallel()

	for range 50 {
		srv, _ := newServer(t)
		require.NoError(t, srv.Start(t.Context()))

		var wg sync.WaitGroup

		wg.Go(func() { assert.NoError(t, srv.Run(t.Context())) })
		wg.Go(func() { assert.NoError(t, srv.Shutdown(t.Context())) })
		wg.Wait()
	}
}

func TestServer_StateErrors(t *testing.T) {
	t.Parallel()

	t.Run("RunBeforeStart", func(t *testing.T) {
		t.Parallel()

		srv, _ := newServer(t)
		require.ErrorIs(t, srv.Run(t.Context()), grpcsrv.ErrNotStarted)
	})

	t.Run("StartTwice", func(t *testing.T) {
		t.Parallel()

		srv, _ := newServer(t)
		require.NoError(t, srv.Start(t.Context()))
		require.ErrorIs(t, srv.Start(t.Context()), grpcsrv.ErrAlreadyStarted)
	})

	t.Run("RunTwice", func(t *testing.T) {
		t.Parallel()

		srv, ts := newServer(t)
		done := serve(t.Context(), t, srv)

		// A served RPC proves the background Run is the first one.
		close(ts.release)
		require.NoError(t, invoke(t.Context(), dial(t, srv)))
		require.ErrorIs(t, srv.Run(t.Context()), grpcsrv.ErrAlreadyStarted)
		require.NoError(t, srv.Shutdown(t.Context()))
		require.NoError(t, waitFor(t, done))
	})

	t.Run("StartAfterShutdown", func(t *testing.T) {
		t.Parallel()

		srv, _ := newServer(t)
		require.NoError(t, srv.Shutdown(t.Context()))
		require.ErrorIs(t, srv.Start(t.Context()), grpcsrv.ErrClosed)
		require.NoError(t, srv.Run(t.Context()), "Run after Shutdown returns nil")
	})

	t.Run("ShutdownTwice", func(t *testing.T) {
		t.Parallel()

		srv, _ := newServer(t)
		require.NoError(t, srv.Start(t.Context()))
		require.NoError(t, srv.Shutdown(t.Context()))
		require.NoError(t, srv.Shutdown(t.Context()))
	})

	t.Run("RegisterAfterRun", func(t *testing.T) {
		t.Parallel()

		srv, _ := newServer(t)
		done := serve(t.Context(), t, srv)

		require.Eventually(t, func() bool { return srv.Healthcheck(t.Context()) == nil }, testTimeout, time.Millisecond)
		assert.PanicsWithValue(t, "grpc: RegisterService called after Run", func() {
			healthpb.RegisterHealthServer(srv, healthpb.UnimplementedHealthServer{})
		})
		require.NoError(t, srv.Shutdown(t.Context()))
		require.NoError(t, waitFor(t, done))
	})

	t.Run("RegisterTwice", func(t *testing.T) {
		t.Parallel()

		srv, ts := newServer(t)
		assert.PanicsWithValue(t, `grpc: service "test.Test" registered twice`, func() {
			srv.RegisterService(testServiceDesc(), ts)
		})
	})

	t.Run("RegisterWrongType", func(t *testing.T) {
		t.Parallel()

		srv, _ := newServer(t)
		assert.Panics(t, func() { srv.RegisterService(&healthpb.Health_ServiceDesc, "not a health server") })
	})
}

func TestServer_ShutdownAfterStartReleasesPort(t *testing.T) {
	t.Parallel()

	srv, _ := newServer(t)
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

	srv, _ := newServerWithConfig(t, grpcsrv.Config{Addr: busy.Addr().String()})
	err = srv.Start(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "grpc: listen on")
	assert.Nil(t, srv.Addr())
}

func TestServer_WithListener(t *testing.T) {
	t.Parallel()

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	counting := &countingListener{Listener: ln, done: make(chan struct{})}
	srv, ts := newServerWithConfig(t, grpcsrv.Config{Addr: "invalid:1"}, grpcsrv.WithListener(counting))
	done := serve(t.Context(), t, srv)
	assert.Equal(t, ln.Addr(), srv.Addr())

	close(ts.release)
	require.NoError(t, invoke(t.Context(), dial(t, srv)))
	require.NoError(t, srv.Shutdown(t.Context()))
	require.NoError(t, waitFor(t, done))
	assert.Equal(t, int32(1), counting.closes.Load(), "the listener is closed once")
}

func TestServer_TLS(t *testing.T) {
	t.Parallel()

	serverTLS, pool := selfSignedTLS(t)
	srv, ts := newServer(t, grpcsrv.WithTLSConfig(serverTLS))
	serve(t.Context(), t, srv)
	close(ts.release)

	creds := credentials.NewTLS(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12})
	require.NoError(t, invoke(t.Context(), dialWith(t, srv, creds)))

	err := invoke(t.Context(), dial(t, srv))
	assert.Equal(t, codes.Unavailable, status.Code(err), "plaintext clients are rejected")
}

func TestServer_Health(t *testing.T) {
	t.Parallel()

	var notReady atomic.Bool

	readiness := lifecycle.HealthcheckFunc(func(context.Context) error {
		if notReady.Load() {
			return app.ErrNotReady
		}

		return nil
	})

	reject := func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
		return nil, status.Error(codes.Unauthenticated, "no token")
	}

	srv, _ := newServer(t, grpcsrv.WithHealth(readiness), grpcsrv.WithUnaryInterceptors(reject))
	serve(t.Context(), t, srv)

	conn := dial(t, srv)
	client := healthpb.NewHealthClient(conn)

	check := func(service string) (healthpb.HealthCheckResponse_ServingStatus, error) {
		resp, err := client.Check(t.Context(), &healthpb.HealthCheckRequest{Service: service})

		return resp.GetStatus(), err
	}

	st, err := check("")
	require.NoError(t, err, "health RPCs bypass the interceptors")
	assert.Equal(t, healthpb.HealthCheckResponse_SERVING, st)

	st, err = check(testService)
	require.NoError(t, err)
	assert.Equal(t, healthpb.HealthCheckResponse_SERVING, st)

	_, err = check("unknown.Service")
	assert.Equal(t, codes.NotFound, status.Code(err))

	list, err := client.List(t.Context(), &healthpb.HealthListRequest{})
	require.NoError(t, err)
	assert.Len(t, list.GetStatuses(), 3, "the server, the test service and the health service")

	assert.Equal(t, codes.Unauthenticated, status.Code(invoke(t.Context(), conn)),
		"other RPCs go through the interceptors")

	notReady.Store(true)

	st, err = check("")
	require.NoError(t, err)
	assert.Equal(t, healthpb.HealthCheckResponse_NOT_SERVING, st)

	watch, err := client.Watch(t.Context(), &healthpb.HealthCheckRequest{})
	require.NoError(t, err)

	_, err = watch.Recv()
	assert.Equal(t, codes.Unimplemented, status.Code(err))
}

func TestServer_HealthFollowsServerWithoutReadiness(t *testing.T) {
	t.Parallel()

	srv, ts := newServer(t, grpcsrv.WithHealth(nil))
	serve(t.Context(), t, srv)

	conn := dial(t, srv)
	client := healthpb.NewHealthClient(conn)

	resp, err := client.Check(t.Context(), &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())

	// During the drain, an RPC on an open connection still reaches the server,
	// which reports NOT_SERVING.
	inFlight := invokeAsync(t.Context(), conn)
	waitFor(t, ts.entered)

	shutdown := make(chan error, 1)

	go func() { shutdown <- srv.Shutdown(t.Context()) }()

	require.Eventually(t, func() bool { return srv.Healthcheck(t.Context()) != nil }, testTimeout, time.Millisecond)

	resp, err = client.Check(t.Context(), &healthpb.HealthCheckRequest{})
	if err == nil { // the GOAWAY may already have reached the client
		assert.Equal(t, healthpb.HealthCheckResponse_NOT_SERVING, resp.GetStatus())
	}

	close(ts.release)
	require.NoError(t, waitFor(t, inFlight))
	require.NoError(t, waitFor(t, shutdown))
}

func TestServer_Interceptors(t *testing.T) {
	t.Parallel()

	var (
		mu    sync.Mutex
		order []string
	)

	record := func(name string) {
		mu.Lock()
		defer mu.Unlock()

		order = append(order, name)
	}

	unary := func(name string) grpc.UnaryServerInterceptor {
		return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			record(name)

			return h(ctx, req)
		}
	}

	stream := func(name string) grpc.StreamServerInterceptor {
		return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
			record(name)

			return h(srv, ss)
		}
	}

	srv, ts := newServer(t,
		grpcsrv.WithUnaryInterceptors(unary("u1"), nil, unary("u2")),
		grpcsrv.WithUnaryInterceptors(unary("u3")),
		grpcsrv.WithStreamInterceptors(stream("s1"), stream("s2")),
		grpcsrv.WithServerOptions(grpc.ChainUnaryInterceptor(unary("raw"))),
	)
	serve(t.Context(), t, srv)
	close(ts.release)

	conn := dial(t, srv)
	require.NoError(t, invoke(t.Context(), conn))

	st, err := conn.NewStream(t.Context(), &grpc.StreamDesc{ServerStreams: true}, methodStream)
	require.NoError(t, err)
	require.NoError(t, st.SendMsg(&emptypb.Empty{}))
	require.NoError(t, st.CloseSend())
	require.NoError(t, st.RecvMsg(&emptypb.Empty{}))

	mu.Lock()
	defer mu.Unlock()

	assert.Equal(t, []string{"u1", "u2", "u3", "raw", "s1", "s2"}, order)
}

func TestServer_Config(t *testing.T) {
	t.Parallel()

	srv, _ := newServerWithConfig(t, grpcsrv.Config{Addr: "127.0.0.1:0", MaxRecvMsgSize: 1024})
	serve(t.Context(), t, srv)

	conn := dial(t, srv)
	echo := func(size int) error {
		in := wrapperspb.Bytes(bytes.Repeat([]byte{'x'}, size))

		return conn.Invoke(t.Context(), methodEcho, in, new(wrapperspb.BytesValue))
	}

	require.NoError(t, echo(512))
	assert.Equal(t, codes.ResourceExhausted, status.Code(echo(2048)), "MaxRecvMsgSize applies")
}

func TestServer_Reflection(t *testing.T) {
	t.Parallel()

	srv, _ := newServer(t)
	reflection.Register(srv)

	assert.Contains(t, srv.GetServiceInfo(), testService)
	assert.Contains(t, srv.GetServiceInfo(), "grpc.reflection.v1.ServerReflection")
}

func TestServer_Tracing(t *testing.T) {
	t.Parallel()

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))

	srv, ts := newServer(t, grpcsrv.WithTracerProvider(tp), grpcsrv.WithHealth(nil))
	serve(t.Context(), t, srv)
	close(ts.release)

	conn := dial(t, srv)

	_, err := healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{})
	require.NoError(t, err)

	// The health RPC has ended on the server before this one starts.
	require.NoError(t, invoke(t.Context(), conn))
	require.Eventually(t, func() bool { return len(rec.Ended()) >= 1 }, testTimeout, time.Millisecond)

	ended := rec.Ended()
	require.Len(t, ended, 1, "health RPCs are not traced")
	assert.Equal(t, "test.Test/Wait", ended[0].Name())
}

// telemetry stands in for the Provider of the otel module.
type telemetry struct{ tp trace.TracerProvider }

func (t telemetry) TracerProvider() trace.TracerProvider { return t.tp }

func (telemetry) MeterProvider() metric.MeterProvider { return metricnoop.NewMeterProvider() }

func (telemetry) Propagators() propagation.TextMapPropagator { return propagation.TraceContext{} }

// With WithTelemetry alone, the server continues the trace of the call.
func TestServer_WithTelemetry(t *testing.T) {
	t.Parallel()

	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))

	srv, ts := newServer(t, grpcsrv.WithTelemetry(nil), grpcsrv.WithTelemetry(telemetry{tp: tp}))
	serve(t.Context(), t, srv)
	close(ts.release)

	ctx := metadata.AppendToOutgoingContext(t.Context(), "traceparent", "00-"+traceID+"-00f067aa0ba902b7-01")
	require.NoError(t, invoke(ctx, dial(t, srv)))

	require.Eventually(t, func() bool { return len(rec.Ended()) == 1 }, testTimeout, time.Millisecond)
	assert.Equal(t, traceID, rec.Ended()[0].SpanContext().TraceID().String(), "the incoming trace context is kept")
}

func TestServer_WithApp(t *testing.T) {
	t.Parallel()

	a := app.New(app.WithSignals(), app.WithShutdownTimeout(testTimeout))
	srv, ts := newServer(t, grpcsrv.WithHealth(a))
	a.Add(srv)

	ctx, stop := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() { done <- a.Run(ctx) }()

	// Start has run (the address is known) once the app reports ready.
	require.Eventually(t, func() bool { return a.Healthcheck(t.Context()) == nil }, testTimeout, time.Millisecond)

	conn := dial(t, srv)

	resp, err := healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())

	inFlight := invokeAsync(t.Context(), conn)
	waitFor(t, ts.entered)

	stop()
	require.Eventually(t, func() bool { return srv.Healthcheck(t.Context()) != nil }, testTimeout, time.Millisecond)

	close(ts.release)
	require.NoError(t, waitFor(t, inFlight), "the app drains in-flight RPCs on shutdown")
	require.NoError(t, waitFor(t, done))
}

func TestNew_Errors(t *testing.T) {
	t.Parallel()

	_, err := grpcsrv.New(grpcsrv.Config{
		Addr:              "no-port",
		MaxRecvMsgSize:    -1,
		MaxConnectionAge:  -time.Second,
		KeepaliveMinTime:  -time.Second,
		ConnectionTimeout: -time.Second,
	})
	require.ErrorIs(t, err, grpcsrv.ErrInvalidConfig)

	for _, field := range []string{"addr", "maxRecvMsgSize", "maxConnectionAge", "keepaliveMinTime", "connectionTimeout"} {
		assert.Contains(t, err.Error(), field)
	}

	_, err = grpcsrv.New(grpcsrv.Config{}, grpcsrv.WithTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	require.ErrorIs(t, err, grpcsrv.ErrInvalidConfig)

	_, err = grpcsrv.New(grpcsrv.Config{MaxConnectionAgeGrace: time.Second})
	require.ErrorIs(t, err, grpcsrv.ErrInvalidConfig)
	assert.Contains(t, err.Error(), "maxConnectionAgeGrace")
}

func TestServer_WithName(t *testing.T) {
	t.Parallel()

	srv, _ := newServer(t, grpcsrv.WithName("internal"))
	assert.Equal(t, "internal", srv.Name())
}

func TestServer_EveryConfigFieldAndOption(t *testing.T) {
	t.Parallel()

	cfg := grpcsrv.Config{
		Addr:                         "127.0.0.1:0",
		MaxRecvMsgSize:               1 << 20,
		MaxSendMsgSize:               1 << 20,
		MaxConcurrentStreams:         10,
		ConnectionTimeout:            time.Second,
		MaxConnectionIdle:            time.Minute,
		MaxConnectionAge:             time.Minute,
		MaxConnectionAgeGrace:        time.Second,
		KeepaliveTime:                time.Minute,
		KeepaliveTimeout:             time.Second,
		KeepaliveMinTime:             time.Second,
		KeepalivePermitWithoutStream: true,
	}

	srv, ts := newServerWithConfig(t, cfg,
		grpcsrv.WithMeterProvider(metricnoop.NewMeterProvider()),
		grpcsrv.WithPropagators(propagation.TraceContext{}),
		grpcsrv.WithOtelGRPCOptions(otelgrpc.WithMessageEvents(otelgrpc.ReceivedEvents)),
		nil,
	)
	serve(t.Context(), t, srv)
	close(ts.release)

	require.NoError(t, invoke(t.Context(), dial(t, srv)))
}
