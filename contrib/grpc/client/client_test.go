package client_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"sync"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"

	grpcsrv "github.com/eidon-go/core/contrib/grpc"
	"github.com/eidon-go/core/contrib/grpc/client"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
)

const (
	testTimeout = 10 * time.Second
	methodEcho  = "/test.Echo/Echo"
)

// echoServer answers Echo with its input and counts the calls.
type echoServer struct {
	calls atomic.Int32
}

func echoDesc() *grpc.ServiceDesc {
	return &grpc.ServiceDesc{
		ServiceName: "test.Echo",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Echo",
				Handler: func(
					srv any, ctx context.Context, dec func(any) error, intercept grpc.UnaryServerInterceptor,
				) (any, error) {
					in := new(wrapperspb.BytesValue)
					if err := dec(in); err != nil {
						return nil, err
					}

					handler := func(_ context.Context, req any) (any, error) {
						srv.(*echoServer).calls.Add(1) //nolint:forcetypeassert // registered with *echoServer

						return req, nil
					}
					if intercept == nil {
						return handler(ctx, in)
					}

					return intercept(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: methodEcho}, handler)
				},
			},
		},
	}
}

// startServer runs a contrib/grpc server with the echo service on a free
// loopback port until the test ends.
func startServer(t *testing.T, opts ...grpcsrv.Option) (*grpcsrv.Server, *echoServer) {
	t.Helper()

	srv, err := grpcsrv.New(grpcsrv.Config{Addr: "127.0.0.1:0"}, opts...)
	require.NoError(t, err)

	echo := &echoServer{}
	srv.RegisterService(echoDesc(), echo)
	require.NoError(t, srv.Start(t.Context()))

	done := make(chan error, 1)

	go func() { done <- srv.Run(context.Background()) }()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()

		assert.NoError(t, srv.Shutdown(ctx))
		assert.NoError(t, <-done)
	})

	return srv, echo
}

func newConn(t *testing.T, cfg client.Config, opts ...client.Option) *client.Conn {
	t.Helper()

	conn, err := client.New(cfg, opts...)
	require.NoError(t, err)

	t.Cleanup(func() { _ = conn.Shutdown(context.Background()) })

	return conn
}

func echo(ctx context.Context, conn grpc.ClientConnInterface, payload []byte) error {
	return conn.Invoke(ctx, methodEcho, wrapperspb.Bytes(payload), new(wrapperspb.BytesValue))
}

func plaintext(srv *grpcsrv.Server) client.Config {
	return client.Config{Target: srv.Addr().String(), Plaintext: true}
}

func TestConn_Contract(t *testing.T) {
	t.Parallel()

	lifecycletest.Resource(t, func(t *testing.T) lifecycle.Resource {
		t.Helper()

		return newConn(t, client.Config{Target: "127.0.0.1:1", Plaintext: true})
	})
}

func TestConn_Calls(t *testing.T) {
	t.Parallel()

	srv, server := startServer(t)
	conn := newConn(t, plaintext(srv))
	assert.Equal(t, client.DefaultName, conn.Name())

	require.NoError(t, conn.Start(t.Context()))
	require.NoError(t, echo(t.Context(), conn, []byte("hi")))
	assert.Equal(t, int32(1), server.calls.Load())
}

func TestConn_Shutdown(t *testing.T) {
	t.Parallel()

	srv, _ := startServer(t)
	conn := newConn(t, plaintext(srv))
	require.NoError(t, echo(t.Context(), conn, nil))

	require.NoError(t, conn.Shutdown(t.Context()))
	require.NoError(t, conn.Shutdown(t.Context()), "idempotent")
	assert.Equal(t, codes.Canceled, status.Code(echo(t.Context(), conn, nil)), "RPCs fail after Shutdown")
	require.ErrorIs(t, conn.Start(t.Context()), client.ErrClosed)
}

func TestConn_ShutdownAfterOwnClose(t *testing.T) {
	t.Parallel()

	conn := newConn(t, client.Config{Target: "127.0.0.1:1", Plaintext: true})
	require.NoError(t, conn.Close())
	require.NoError(t, conn.Shutdown(t.Context()))
}

func TestConn_ShutdownWithExpiredContext(t *testing.T) {
	t.Parallel()

	srv, _ := startServer(t)
	conn := newConn(t, plaintext(srv))
	require.NoError(t, echo(t.Context(), conn, nil))

	expired, expire := context.WithCancel(t.Context())
	expire()

	if err := conn.Shutdown(expired); err != nil { // the close may also win the race
		require.ErrorIs(t, err, context.Canceled)
	}

	require.NoError(t, conn.Shutdown(t.Context()), "a later Shutdown waits for the close")
	assert.Equal(t, codes.Canceled, status.Code(echo(t.Context(), conn, nil)))
}

func TestConn_TLS(t *testing.T) {
	t.Parallel()

	serverTLS, pool := selfSignedTLS(t)
	srv, _ := startServer(t, grpcsrv.WithTLSConfig(serverTLS))
	target := srv.Addr().String()

	trusted := newConn(t, client.Config{Target: target},
		client.WithTLSConfig(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}))
	require.NoError(t, echo(t.Context(), trusted, nil))

	systemRoots := newConn(t, client.Config{Target: target})
	assert.Equal(t, codes.Unavailable, status.Code(echo(t.Context(), systemRoots, nil)),
		"TLS by default: the self-signed certificate is not trusted")

	plain := newConn(t, client.Config{Target: target, Plaintext: true})
	assert.Equal(t, codes.Unavailable, status.Code(echo(t.Context(), plain, nil)))
}

func TestConn_Interceptors(t *testing.T) {
	t.Parallel()

	var (
		mu    sync.Mutex
		order []string
		seen  metadata.MD
	)

	record := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)

		mu.Lock()
		seen = md
		mu.Unlock()

		return h(ctx, req)
	}

	srv, _ := startServer(t, grpcsrv.WithUnaryInterceptors(record))

	unary := func(name string) grpc.UnaryClientInterceptor {
		return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn,
			invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
		) error {
			mu.Lock()

			order = append(order, name)
			mu.Unlock()

			return invoker(metadata.AppendToOutgoingContext(ctx, "x-"+name, "1"), method, req, reply, cc, opts...)
		}
	}

	conn := newConn(t, plaintext(srv),
		client.WithUnaryInterceptors(unary("a"), nil, unary("b")),
		client.WithUnaryInterceptors(unary("c")),
		client.WithStreamInterceptors(nil),
	)
	require.NoError(t, echo(t.Context(), conn, nil))

	mu.Lock()
	defer mu.Unlock()

	assert.Equal(t, []string{"a", "b", "c"}, order)
	assert.Equal(t, []string{"1"}, seen.Get("x-c"), "metadata from interceptors reaches the server")
}

func TestConn_TracePropagation(t *testing.T) {
	t.Parallel()

	clientSpans := tracetest.NewSpanRecorder()
	serverSpans := tracetest.NewSpanRecorder()
	propagators := propagation.TraceContext{}

	srv, _ := startServer(t,
		grpcsrv.WithTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(serverSpans))),
		grpcsrv.WithPropagators(propagators))
	conn := newConn(t, plaintext(srv),
		client.WithTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(clientSpans))),
		client.WithPropagators(propagators))

	require.NoError(t, echo(t.Context(), conn, nil))
	require.Eventually(t, func() bool {
		return len(clientSpans.Ended()) == 1 && len(serverSpans.Ended()) == 1
	}, testTimeout, time.Millisecond)

	clientSpan, serverSpan := clientSpans.Ended()[0], serverSpans.Ended()[0]
	assert.Equal(t, "test.Echo/Echo", clientSpan.Name())
	assert.Equal(t, trace.SpanKindClient, clientSpan.SpanKind())
	assert.Equal(t, clientSpan.SpanContext().TraceID(), serverSpan.SpanContext().TraceID(), "the trace continues")
	assert.Equal(t, clientSpan.SpanContext().SpanID(), serverSpan.Parent().SpanID())
}

// telemetry stands in for the Provider of the otel module.
type telemetry struct{ spans *tracetest.SpanRecorder }

func (t telemetry) TracerProvider() trace.TracerProvider {
	return sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(t.spans))
}

func (telemetry) MeterProvider() metric.MeterProvider { return metricnoop.NewMeterProvider() }

func (telemetry) Propagators() propagation.TextMapPropagator { return propagation.TraceContext{} }

func TestConn_TracePropagationWithTelemetry(t *testing.T) {
	t.Parallel()

	clientSpans := tracetest.NewSpanRecorder()
	serverSpans := tracetest.NewSpanRecorder()

	srv, _ := startServer(t, grpcsrv.WithTelemetry(telemetry{spans: serverSpans}))
	conn := newConn(t, plaintext(srv), client.WithTelemetry(telemetry{spans: clientSpans}))

	require.NoError(t, echo(t.Context(), conn, nil))
	require.Eventually(t, func() bool {
		return len(clientSpans.Ended()) == 1 && len(serverSpans.Ended()) == 1
	}, testTimeout, time.Millisecond)

	clientSpan, serverSpan := clientSpans.Ended()[0], serverSpans.Ended()[0]
	assert.Equal(t, clientSpan.SpanContext().TraceID(), serverSpan.SpanContext().TraceID(),
		"WithTelemetry sets the propagators along with the tracer provider")
	assert.Equal(t, clientSpan.SpanContext().SpanID(), serverSpan.Parent().SpanID())
}

func TestConn_LoadBalancing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		policy     string
		bothServed bool
	}{
		{policy: "", bothServed: false},
		{policy: client.PickFirst, bothServed: false},
		{policy: client.RoundRobin, bothServed: true},
	}

	for _, tt := range tests {
		t.Run("policy="+tt.policy, func(t *testing.T) {
			t.Parallel()

			srv1, echo1 := startServer(t)
			srv2, echo2 := startServer(t)

			res := manual.NewBuilderWithScheme("test")
			res.InitialState(resolver.State{Addresses: []resolver.Address{
				{Addr: srv1.Addr().String()}, {Addr: srv2.Addr().String()},
			}})

			conn := newConn(t,
				client.Config{Target: "test:///echo", Plaintext: true, LoadBalancingPolicy: tt.policy},
				client.WithDialOptions(grpc.WithResolvers(res)))

			bothServed := func() bool { return echo1.calls.Load() > 0 && echo2.calls.Load() > 0 }

			if tt.bothServed {
				// Round robin uses a backend once its connection is ready.
				require.Eventually(t, func() bool {
					return echo(t.Context(), conn, nil) == nil && bothServed()
				}, testTimeout, time.Millisecond)

				return
			}

			for range 20 {
				require.NoError(t, echo(t.Context(), conn, nil))
			}

			assert.False(t, bothServed(), "pick_first sends every RPC to one backend")
		})
	}
}

func TestConn_Config(t *testing.T) {
	t.Parallel()

	srv, _ := startServer(t)

	cfg := plaintext(srv)
	cfg.MaxRecvMsgSize = 1024
	cfg.MaxSendMsgSize = 2048
	cfg.MinConnectTimeout = time.Second
	cfg.IdleTimeout = time.Minute
	cfg.KeepaliveTime = time.Minute
	cfg.KeepaliveTimeout = time.Second
	conn := newConn(t, cfg)

	require.NoError(t, echo(t.Context(), conn, bytes.Repeat([]byte{'x'}, 512)))
	assert.Equal(t, codes.ResourceExhausted, status.Code(echo(t.Context(), conn, bytes.Repeat([]byte{'x'}, 1500))),
		"MaxRecvMsgSize: the echoed reply is too large")
	assert.Equal(t, codes.ResourceExhausted, status.Code(echo(t.Context(), conn, bytes.Repeat([]byte{'x'}, 4096))),
		"MaxSendMsgSize")

	noIdle := plaintext(srv)
	noIdle.IdleTimeout = -1
	require.NoError(t, echo(t.Context(), newConn(t, noIdle), nil))
}

func TestNew_Errors(t *testing.T) {
	t.Parallel()

	_, err := client.New(client.Config{
		LoadBalancingPolicy: "random",
		MinConnectTimeout:   -1,
		MaxRecvMsgSize:      -1,
		KeepaliveTimeout:    time.Second,
	})
	require.ErrorIs(t, err, client.ErrInvalidConfig)

	fields := []string{"target", "loadBalancingPolicy", "minConnectTimeout", "maxRecvMsgSize", "keepaliveTime"}
	for _, field := range fields {
		assert.Contains(t, err.Error(), field)
	}

	_, err = client.New(client.Config{Target: "localhost:1", Plaintext: true},
		client.WithTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	require.ErrorIs(t, err, client.ErrInvalidConfig)

	_, err = client.New(client.Config{Target: "localhost:1", Plaintext: true},
		client.WithDialOptions(grpc.WithDefaultServiceConfig("not json")))
	require.ErrorIs(t, err, client.ErrInvalidConfig, "grpc.NewClient errors are wrapped")
}

func TestConn_WithName(t *testing.T) {
	t.Parallel()

	conn := newConn(t, client.Config{Target: "localhost:1", Plaintext: true}, client.WithName("billing"))
	assert.Equal(t, "billing", conn.Name())
}

// selfSignedTLS returns a server TLS config for 127.0.0.1 and a client pool
// that trusts it.
func selfSignedTLS(t *testing.T) (*tls.Config, *x509.CertPool) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "grpc client test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	pool := x509.NewCertPool()
	pool.AddCert(cert)

	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key, Leaf: cert}},
		MinVersion:   tls.VersionTLS12,
	}, pool
}
