package client_test

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/eidon-go/core/contrib/http2"
	"github.com/eidon-go/core/contrib/http2/client"
)

type response struct {
	status int
	proto  int
	body   string
}

func get(t *testing.T, c *http.Client, url string) response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)

	resp, err := c.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return response{status: resp.StatusCode, proto: resp.ProtoMajor, body: string(body)}
}

func echoProto() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.Proto)
	})
}

// transportOf builds a client and returns the *http.Transport New configured.
func transportOf(t *testing.T, cfg client.Config, opts ...client.Option) *http.Transport {
	t.Helper()

	var transport *http.Transport

	opts = append(opts, client.WithTransport(func(tr *http.Transport) { transport = tr }))

	_, err := client.New(cfg, opts...)
	require.NoError(t, err)
	require.NotNil(t, transport)

	return transport
}

func TestNew_Config(t *testing.T) {
	t.Parallel()

	t.Run("defaults", func(t *testing.T) {
		t.Parallel()

		tr := transportOf(t, client.Config{})
		assert.Equal(t, client.DefaultMaxIdleConns, tr.MaxIdleConns)
		assert.Equal(t, client.DefaultMaxIdleConnsPerHost, tr.MaxIdleConnsPerHost)
		assert.Zero(t, tr.MaxConnsPerHost)
		assert.Equal(t, client.DefaultTLSHandshakeTimeout, tr.TLSHandshakeTimeout)
		assert.Equal(t, client.DefaultResponseHeaderTimeout, tr.ResponseHeaderTimeout)
		assert.Equal(t, client.DefaultIdleConnTimeout, tr.IdleConnTimeout)
		assert.Equal(t, client.DefaultExpectContinueTimeout, tr.ExpectContinueTimeout)
		assert.True(t, tr.ForceAttemptHTTP2)
		assert.NotNil(t, tr.Proxy, "proxy from the environment")
		assert.Nil(t, tr.TLSClientConfig)
	})

	t.Run("custom", func(t *testing.T) {
		t.Parallel()

		tr := transportOf(t, client.Config{
			MaxIdleConns: 7, MaxIdleConnsPerHost: 3, MaxConnsPerHost: 5,
			TLSHandshakeTimeout: time.Second, ResponseHeaderTimeout: 2 * time.Second,
			IdleConnTimeout: 3 * time.Second,
		})
		assert.Equal(t, 7, tr.MaxIdleConns)
		assert.Equal(t, 3, tr.MaxIdleConnsPerHost)
		assert.Equal(t, 5, tr.MaxConnsPerHost)
		assert.Equal(t, time.Second, tr.TLSHandshakeTimeout)
		assert.Equal(t, 2*time.Second, tr.ResponseHeaderTimeout)
		assert.Equal(t, 3*time.Second, tr.IdleConnTimeout)
	})

	t.Run("negative disables", func(t *testing.T) {
		t.Parallel()

		tr := transportOf(t, client.Config{
			TLSHandshakeTimeout: -1, ResponseHeaderTimeout: -1, IdleConnTimeout: -1,
		})
		assert.Zero(t, tr.TLSHandshakeTimeout)
		assert.Zero(t, tr.ResponseHeaderTimeout)
		assert.Zero(t, tr.IdleConnTimeout)
	})

	t.Run("TLS config is cloned", func(t *testing.T) {
		t.Parallel()

		cfg := &tls.Config{MinVersion: tls.VersionTLS13}
		tr := transportOf(t, client.Config{}, client.WithTLSConfig(cfg))
		require.NotNil(t, tr.TLSClientConfig)
		assert.NotSame(t, cfg, tr.TLSClientConfig)
		assert.Equal(t, uint16(tls.VersionTLS13), tr.TLSClientConfig.MinVersion)
	})
}

func TestNew_InvalidConfig(t *testing.T) {
	t.Parallel()

	_, err := client.New(client.Config{MaxIdleConns: -1, MaxConnsPerHost: -2})
	require.ErrorIs(t, err, client.ErrInvalidConfig)
	assert.Contains(t, err.Error(), "maxIdleConns")
	assert.Contains(t, err.Error(), "maxConnsPerHost")
}

func TestClient_HTTP2OverTLS(t *testing.T) {
	t.Parallel()

	srv := httptest.NewUnstartedServer(echoProto())
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	tlsCfg := srv.Client().Transport.(*http.Transport).TLSClientConfig //nolint:forcetypeassert // httptest's client

	c, err := client.New(client.Config{}, client.WithTLSConfig(tlsCfg))
	require.NoError(t, err)

	resp := get(t, c, srv.URL)
	assert.Equal(t, 2, resp.proto)
	assert.Equal(t, "HTTP/2.0", resp.body)
}

func TestClient_UnencryptedHTTP2(t *testing.T) {
	t.Parallel()

	srv, err := http2.New(http2.Config{Addr: "127.0.0.1:0", UnencryptedHTTP2: true}, echoProto())
	require.NoError(t, err)
	require.NoError(t, srv.Start(t.Context()))

	go func() { _ = srv.Run(t.Context()) }()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_ = srv.Shutdown(ctx)
	})

	tests := []struct {
		name  string
		cfg   client.Config
		proto int
	}{
		{name: "default", cfg: client.Config{}, proto: 1},
		{name: "h2c", cfg: client.Config{UnencryptedHTTP2: true}, proto: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, err := client.New(tt.cfg)
			require.NoError(t, err)

			assert.Equal(t, tt.proto, get(t, c, "http://"+srv.Addr().String()).proto)
		})
	}
}

func TestClient_TracingAndPropagation(t *testing.T) {
	t.Parallel()

	var (
		mu          sync.Mutex
		traceparent string
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		traceparent = r.Header.Get("Traceparent")
		mu.Unlock()

		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))

	c, err := client.New(client.Config{},
		client.WithTracerProvider(tp), client.WithPropagators(propagation.TraceContext{}))
	require.NoError(t, err)

	assert.Equal(t, http.StatusNoContent, get(t, c, srv.URL).status)

	spans := rec.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, "GET", spans[0].Name())

	mu.Lock()
	defer mu.Unlock()

	assert.Contains(t, traceparent, spans[0].SpanContext().TraceID().String(), "the trace context is propagated")
}

// recordingTripper records the order in which wrappers see a request and tags
// the request with its name.
type recordingTripper struct {
	name   string
	next   http.RoundTripper
	mu     *sync.Mutex
	events *[]string
}

func (r recordingTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	*r.events = append(*r.events, r.name)
	r.mu.Unlock()

	req = req.Clone(req.Context())
	req.Header.Add("X-Wrapped", r.name)

	return r.next.RoundTrip(req)
}

func TestClient_RoundTripperOrder(t *testing.T) {
	t.Parallel()

	var (
		mu       sync.Mutex
		events   []string
		received []string
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		received = r.Header.Values("X-Wrapped")
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	wrapper := func(name string) func(http.RoundTripper) http.RoundTripper {
		return func(next http.RoundTripper) http.RoundTripper {
			return recordingTripper{name: name, next: next, mu: &mu, events: &events}
		}
	}

	c, err := client.New(client.Config{}, client.WithRoundTripper(wrapper("outer")),
		client.WithRoundTripper(wrapper("inner")))
	require.NoError(t, err)

	get(t, c, srv.URL)

	mu.Lock()
	defer mu.Unlock()

	assert.Equal(t, []string{"outer", "inner"}, events, "the first wrapper passed is the outermost")
	assert.Equal(t, []string{"outer", "inner"}, received)
}
