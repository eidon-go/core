package grpc_test

import (
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

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	grpcsrv "github.com/eidon-go/core/contrib/grpc"
)

const testTimeout = 10 * time.Second

// Methods of the hand-written test service.
const (
	testService  = "test.Test"
	methodWait   = "/" + testService + "/Wait"
	methodEcho   = "/" + testService + "/Echo"
	methodStuck  = "/" + testService + "/Stuck"
	methodStream = "/" + testService + "/Stream"
)

// testServer implements the test service: Wait blocks until release is
// closed (or the RPC is cancelled), Echo returns its input and Stream sends
// one message, then blocks like Wait.
type testServer struct {
	entered    chan struct{}
	release    chan struct{}
	once       sync.Once
	finished   atomic.Bool
	exited     chan struct{} // closed when the first handler returns
	exitedOnce sync.Once
}

func newTestServer() *testServer {
	return &testServer{entered: make(chan struct{}), release: make(chan struct{}), exited: make(chan struct{})}
}

func (s *testServer) wait(ctx context.Context) error {
	defer func() {
		s.finished.Store(true)
		s.exitedOnce.Do(func() { close(s.exited) })
	}()

	s.once.Do(func() { close(s.entered) })

	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// testServiceDesc describes the test service the way protoc-gen-go-grpc
// would, using well-known types as messages.
func testServiceDesc() *grpc.ServiceDesc {
	return &grpc.ServiceDesc{
		ServiceName: testService,
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: "Wait", Handler: unaryHandler(methodWait, waitRPC)},
			{MethodName: "Echo", Handler: unaryHandler(methodEcho, echoRPC)},
			{MethodName: "Stuck", Handler: unaryHandler(methodStuck, stuckRPC)},
		},
		Streams: []grpc.StreamDesc{{StreamName: "Stream", ServerStreams: true, Handler: streamRPC}},
	}
}

func waitRPC(ctx context.Context, s *testServer, _ *emptypb.Empty) (any, error) {
	return &emptypb.Empty{}, s.wait(ctx)
}

func echoRPC(_ context.Context, _ *testServer, in *wrapperspb.BytesValue) (any, error) {
	return in, nil
}

// stuckRPC ignores its context: it returns only once release is closed.
func stuckRPC(_ context.Context, s *testServer, _ *emptypb.Empty) (any, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.release

	return &emptypb.Empty{}, nil
}

// streamRPC receives one message, sends one, then blocks like Wait.
func streamRPC(srv any, stream grpc.ServerStream) error {
	if err := stream.RecvMsg(&emptypb.Empty{}); err != nil {
		return err
	}

	if err := stream.SendMsg(&emptypb.Empty{}); err != nil {
		return err
	}

	return srv.(*testServer).wait(stream.Context()) //nolint:forcetypeassert // registered with *testServer
}

func unaryHandler[In any, PIn interface{ *In }](
	method string,
	fn func(context.Context, *testServer, PIn) (any, error),
) func(any, context.Context, func(any) error, grpc.UnaryServerInterceptor) (any, error) {
	return func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		in := PIn(new(In))
		if err := dec(in); err != nil {
			return nil, err
		}

		ts := srv.(*testServer) //nolint:forcetypeassert // registered with *testServer
		if interceptor == nil {
			return fn(ctx, ts, in)
		}

		info := &grpc.UnaryServerInfo{Server: srv, FullMethod: method}

		return interceptor(ctx, in, info, func(ctx context.Context, req any) (any, error) {
			return fn(ctx, ts, req.(PIn)) //nolint:forcetypeassert // decoded above
		})
	}
}

// newServer builds a server on a free loopback port with the test service
// registered, and shuts it down when the test ends.
func newServer(t *testing.T, opts ...grpcsrv.Option) (*grpcsrv.Server, *testServer) {
	t.Helper()

	return newServerWithConfig(t, grpcsrv.Config{Addr: "127.0.0.1:0"}, opts...)
}

func newServerWithConfig(t *testing.T, cfg grpcsrv.Config, opts ...grpcsrv.Option) (*grpcsrv.Server, *testServer) {
	t.Helper()

	srv, err := grpcsrv.New(cfg, opts...)
	require.NoError(t, err)

	ts := newTestServer()
	srv.RegisterService(testServiceDesc(), ts)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()

		_ = srv.Shutdown(ctx)
	})

	return srv, ts
}

// serve starts srv and runs it in the background; the returned channel gets
// Run's result.
func serve(ctx context.Context, t *testing.T, srv *grpcsrv.Server) <-chan error {
	t.Helper()

	require.NoError(t, srv.Start(ctx))

	done := make(chan error, 1)

	go func() { done <- srv.Run(ctx) }()

	return done
}

// runResult is what Run returned, and whether the handler had finished every
// RPC by then.
type runResult struct {
	err          error
	handlerDone  bool
	returnedTime time.Time
}

// serveObserved is serve, recording the handler's state when Run returns.
func serveObserved(ctx context.Context, t *testing.T, srv *grpcsrv.Server, ts *testServer) <-chan runResult {
	t.Helper()

	require.NoError(t, srv.Start(ctx))

	done := make(chan runResult, 1)

	go func() {
		err := srv.Run(ctx)
		done <- runResult{err: err, handlerDone: ts.finished.Load(), returnedTime: time.Now()}
	}()

	return done
}

// dial connects to srv over plaintext.
func dial(t *testing.T, srv *grpcsrv.Server) *grpc.ClientConn {
	t.Helper()

	return dialWith(t, srv, insecure.NewCredentials())
}

func dialWith(t *testing.T, srv *grpcsrv.Server, creds credentials.TransportCredentials) *grpc.ClientConn {
	t.Helper()

	conn, err := grpc.NewClient(srv.Addr().String(), grpc.WithTransportCredentials(creds))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

// invoke calls Wait.
func invoke(ctx context.Context, conn *grpc.ClientConn) error {
	return conn.Invoke(ctx, methodWait, &emptypb.Empty{}, &emptypb.Empty{})
}

// invokeAsync calls Wait in the background.
func invokeAsync(ctx context.Context, conn *grpc.ClientConn) <-chan error {
	ch := make(chan error, 1)

	go func() { ch <- invoke(ctx, conn) }()

	return ch
}

// waitFor fails the test unless ch delivers within testTimeout.
func waitFor[T any](t *testing.T, ch <-chan T) T {
	t.Helper()

	select {
	case v := <-ch:
		return v
	case <-time.After(testTimeout):
		t.Fatal("timed out")

		var zero T

		return zero
	}
}

// selfSignedTLS returns a server TLS config for 127.0.0.1 and a client pool
// that trusts it.
func selfSignedTLS(t *testing.T) (*tls.Config, *x509.CertPool) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "grpc test"},
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

// countingListener counts Close calls and, like some real listeners, panics
// on the second one.
type countingListener struct {
	net.Listener

	closes atomic.Int32
	done   chan struct{}
}

func (l *countingListener) Close() error {
	l.closes.Add(1)
	close(l.done) // a second Close panics

	return l.Listener.Close()
}
