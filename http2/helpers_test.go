package http2_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/http2"
)

const testTimeout = 10 * time.Second

// newServer builds a server on a free loopback port and shuts it down when the
// test ends.
func newServer(t *testing.T, h http.Handler, opts ...http2.Option) *http2.Server {
	t.Helper()

	return newServerWithConfig(t, http2.Config{Addr: "127.0.0.1:0"}, h, opts...)
}

func newServerWithConfig(t *testing.T, cfg http2.Config, h http.Handler, opts ...http2.Option) *http2.Server {
	t.Helper()

	srv, err := http2.New(cfg, h, opts...)
	require.NoError(t, err)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()

		_ = srv.Shutdown(ctx)
	})

	return srv
}

// serve starts srv and runs it in the background; the returned channel gets
// Run's result.
func serve(ctx context.Context, t *testing.T, srv *http2.Server) <-chan error {
	t.Helper()

	require.NoError(t, srv.Start(ctx))

	done := make(chan error, 1)

	go func() { done <- srv.Run(ctx) }()

	return done
}

// runResult is what Run returned, and whether the handler had finished every
// request by then.
type runResult struct {
	err          error
	handlerDone  bool
	returnedTime time.Time
}

// serveObserved is serve, recording the handler's state when Run returns.
func serveObserved(ctx context.Context, t *testing.T, srv *http2.Server, h *blockingHandler) <-chan runResult {
	t.Helper()

	require.NoError(t, srv.Start(ctx))

	done := make(chan runResult, 1)

	go func() {
		err := srv.Run(ctx)
		done <- runResult{err: err, handlerDone: h.finished.Load(), returnedTime: time.Now()}
	}()

	return done
}

func baseURL(srv *http2.Server) string { return "http://" + srv.Addr().String() }

func newClient() *http.Client {
	return &http.Client{Timeout: testTimeout, Transport: &http.Transport{}}
}

type response struct {
	status int
	proto  int
	body   string
	err    error
}

func get(ctx context.Context, client *http.Client, url string) response {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return response{err: err}
	}

	resp, err := client.Do(req)
	if err != nil {
		return response{err: err}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)

	return response{status: resp.StatusCode, proto: resp.ProtoMajor, body: string(body), err: err}
}

func getAsync(ctx context.Context, client *http.Client, url string) <-chan response {
	ch := make(chan response, 1)

	go func() { ch <- get(ctx, client, url) }()

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

// blockingHandler blocks every request until release is closed and reports
// each request that entered it.
type blockingHandler struct {
	entered  chan struct{}
	release  chan struct{}
	once     sync.Once
	finished atomic.Bool
}

func newBlockingHandler() *blockingHandler {
	return &blockingHandler{entered: make(chan struct{}), release: make(chan struct{})}
}

func (h *blockingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer h.finished.Store(true)

	h.once.Do(func() { close(h.entered) })

	select {
	case <-h.release:
		_, _ = io.WriteString(w, "done")
	case <-r.Context().Done():
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
		Subject:      pkix.Name{CommonName: "http2 test"},
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
