package nats_test

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/nats"
	"github.com/eidon-go/core/retry"
)

const (
	waitFor = 10 * time.Second
	tick    = 10 * time.Millisecond
)

// testServer is an embedded NATS server with JetStream.
type testServer struct {
	t    *testing.T
	mu   sync.Mutex
	opts *server.Options
	srv  *server.Server
}

func runServer(t *testing.T) *testServer {
	t.Helper()

	s := &testServer{t: t, opts: &server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	}}
	s.start()

	// Later starts (restart) use the same port.
	s.opts.Port = s.srv.Addr().(*net.TCPAddr).Port //nolint:forcetypeassert // a TCP listener

	t.Cleanup(s.stop)

	return s
}

func (s *testServer) start() {
	s.t.Helper()

	srv, err := server.NewServer(s.opts)
	require.NoError(s.t, err)

	go srv.Start()

	require.True(s.t, srv.ReadyForConnections(waitFor), "nats server not ready")

	s.mu.Lock()
	s.srv = srv
	s.mu.Unlock()
}

func (s *testServer) stop() {
	s.mu.Lock()
	srv := s.srv
	s.mu.Unlock()

	srv.Shutdown()
	srv.WaitForShutdown()
}

// restart stops the server and starts it again on the same port and store.
func (s *testServer) restart() {
	s.t.Helper()

	s.stop()
	s.start()
}

func (s *testServer) clientURL() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.srv.ClientURL()
}

// client returns a plain nats.go connection for the test's own plumbing.
func (s *testServer) client(t *testing.T) *natsgo.Conn {
	t.Helper()

	nc, err := natsgo.Connect(s.clientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	return nc
}

// jetStream returns a JetStream context of a plain client.
func (s *testServer) jetStream(t *testing.T) jetstream.JetStream {
	t.Helper()

	js, err := jetstream.New(s.client(t))
	require.NoError(t, err)

	return js
}

// createStream creates a stream capturing subjects.
func (s *testServer) createStream(t *testing.T, name string, subjects ...string) {
	t.Helper()

	_, err := s.jetStream(t).CreateStream(t.Context(), jetstream.StreamConfig{
		Name:     name,
		Subjects: subjects,
	})
	require.NoError(t, err)
}

func quietLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// fastRetry is a connect policy for tests.
func fastRetry() retry.Policy {
	return retry.Policy{
		MaxAttempts:  retry.Forever,
		InitialDelay: tick,
		MaxDelay:     50 * time.Millisecond,
	}
}

// newConnection returns a connection to url, not started; the test shuts
// it down.
func newConnection(t *testing.T, url string, opts ...nats.Option) *nats.Connection {
	t.Helper()

	opts = append([]nats.Option{
		nats.WithLogger(quietLogger()),
		nats.WithConnectRetry(fastRetry()),
	}, opts...)

	conn, err := nats.NewConnection(nats.Config{
		URL:           url,
		ReconnectWait: 50 * time.Millisecond,
	}, opts...)
	require.NoError(t, err)

	t.Cleanup(func() { _ = conn.Shutdown(context.Background()) })

	return conn
}

// startConnection returns a started connection to the server.
func startConnection(t *testing.T, s *testServer, opts ...nats.Option) *nats.Connection {
	t.Helper()

	conn := newConnection(t, s.clientURL(), opts...)
	require.NoError(t, conn.Start(t.Context()))

	return conn
}

func newPublisher(t *testing.T, conn *nats.Connection, opts ...nats.Option) *nats.Publisher {
	t.Helper()

	p, err := nats.NewPublisher(conn, append([]nats.Option{nats.WithLogger(quietLogger())}, opts...)...)
	require.NoError(t, err)

	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	return p
}

// runner is a Runner started in the background.
type runner struct {
	done chan error
}

// runInBackground runs r until the test ends.
func runInBackground(t *testing.T, r interface {
	Run(ctx context.Context) error
	Shutdown(ctx context.Context) error
},
) *runner {
	t.Helper()

	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	bg := &runner{done: make(chan error, 1)}

	go func() { bg.done <- r.Run(ctx) }()

	t.Cleanup(func() {
		cancel()

		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), waitFor)
		defer cancelShutdown()

		_ = r.Shutdown(shutdownCtx)
	})

	return bg
}

// wait returns Run's error, failing the test if it does not return in time.
func (r *runner) wait(t *testing.T) error {
	t.Helper()

	select {
	case err := <-r.done:
		return err
	case <-time.After(waitFor):
		t.Fatal("Run did not return")

		return nil
	}
}

// healthy waits until hc reports healthy.
func healthy(t *testing.T, hc interface {
	Healthcheck(ctx context.Context) error
},
) {
	t.Helper()

	require.Eventually(t, func() bool { return hc.Healthcheck(t.Context()) == nil }, waitFor, tick)
}

// received collects the messages a handler got.
type received struct {
	mu   sync.Mutex
	msgs []*nats.Message
	ch   chan *nats.Message
}

func newReceived() *received { return &received{ch: make(chan *nats.Message, 100)} }

func (r *received) Handle(_ context.Context, msg *nats.Message) error {
	r.mu.Lock()
	r.msgs = append(r.msgs, msg)
	r.mu.Unlock()

	r.ch <- msg

	return nil
}

// next returns the next message, failing the test if none comes.
func (r *received) next(t *testing.T) *nats.Message {
	t.Helper()

	select {
	case msg := <-r.ch:
		return msg
	case <-time.After(waitFor):
		t.Fatal("no message received")

		return nil
	}
}

// none checks that no message comes within d.
func (r *received) none(t *testing.T, d time.Duration) {
	t.Helper()

	select {
	case msg := <-r.ch:
		t.Fatalf("unexpected message on %q: %s", msg.Subject, msg.Data)
	case <-time.After(d):
	}
}
