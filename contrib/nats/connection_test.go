package nats_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/nats"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
)

func TestConnection_Contract(t *testing.T) {
	t.Parallel()

	s := runServer(t)

	lifecycletest.Resource(t, func(t *testing.T) lifecycle.Resource {
		t.Helper()

		return newConnection(t, s.clientURL())
	})
}

func TestConnection_StartHealthcheckShutdown(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	conn := newConnection(t, s.clientURL(), nats.WithName("orders-nats"))

	assert.Equal(t, "orders-nats", conn.Name())
	require.ErrorIs(t, conn.Healthcheck(t.Context()), nats.ErrNotStarted)
	assert.Nil(t, conn.Conn())

	_, err := conn.JetStream()
	require.ErrorIs(t, err, nats.ErrNotStarted)

	require.NoError(t, conn.Start(t.Context()))
	require.NoError(t, conn.Healthcheck(t.Context()))
	require.NotNil(t, conn.Conn())
	assert.Equal(t, "orders-nats", conn.Conn().Opts.Name, "ConnectionName defaults to the component name")

	js, err := conn.JetStream()
	require.NoError(t, err)

	_, err = js.AccountInfo(t.Context())
	require.NoError(t, err)

	require.ErrorIs(t, conn.Start(t.Context()), nats.ErrAlreadyStarted)

	nc := conn.Conn()
	require.NoError(t, conn.Shutdown(t.Context()))
	assert.True(t, nc.IsClosed())
	require.ErrorIs(t, conn.Healthcheck(t.Context()), nats.ErrClosed)
	require.ErrorIs(t, conn.Start(t.Context()), nats.ErrClosed)
	require.NoError(t, conn.Shutdown(t.Context()), "idempotent")
}

func TestConnection_StartRetriesUntilServerIsUp(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	url := s.clientURL()
	s.stop()

	conn := newConnection(t, url)

	started := make(chan error, 1)

	go func() { started <- conn.Start(t.Context()) }()

	select {
	case err := <-started:
		t.Fatalf("Start returned without a server: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	s.start()

	select {
	case err := <-started:
		require.NoError(t, err)
	case <-time.After(waitFor):
		t.Fatal("Start did not connect")
	}

	healthy(t, conn)
}

func TestConnection_StartFailsWhenCtxEnds(t *testing.T) {
	t.Parallel()

	conn := newConnection(t, "nats://"+unusedAddr(t))

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	err := conn.Start(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, conn.Healthcheck(t.Context()), nats.ErrNotStarted)
	require.ErrorIs(t, conn.Start(t.Context()), nats.ErrAlreadyStarted)
}

func TestConnection_ShutdownEndsStart(t *testing.T) {
	t.Parallel()

	conn := newConnection(t, "nats://"+unusedAddr(t))

	started := make(chan error, 1)

	go func() { started <- conn.Start(context.WithoutCancel(t.Context())) }()

	require.NoError(t, conn.Shutdown(t.Context()))

	select {
	case err := <-started:
		require.ErrorIs(t, err, nats.ErrClosed)
	case <-time.After(waitFor):
		t.Fatal("Shutdown did not end Start")
	}
}

func TestConnection_Reconnects(t *testing.T) {
	t.Parallel()

	s := runServer(t)

	disconnected := make(chan struct{}, 10)
	conn := startConnection(t, s, nats.WithConnOptions(natsgo.DisconnectErrHandler(func(*natsgo.Conn, error) {
		disconnected <- struct{}{}
	})))

	s.stop()

	select {
	case <-disconnected:
	case <-time.After(waitFor):
		t.Fatal("disconnect handler of WithConnOptions not called")
	}

	require.ErrorIs(t, conn.Healthcheck(t.Context()), nats.ErrNotConnected)

	s.start()
	healthy(t, conn)
}

func TestConnection_ShutdownDrainsAndFlushes(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	conn := startConnection(t, s)

	nc := s.client(t)
	sub, err := nc.SubscribeSync("flush.>")
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	for range 100 {
		require.NoError(t, conn.Conn().Publish("flush.x", []byte("x")))
	}

	require.NoError(t, conn.Shutdown(t.Context()))

	for range 100 {
		_, err := sub.NextMsg(waitFor)
		require.NoError(t, err)
	}
}

func TestConnection_ShutdownDeadlineClosesAtOnce(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	conn := startConnection(t, s)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, conn.Shutdown(ctx), context.Canceled)
	// Closed in the background, without a drain: Shutdown does not wait for
	// a close that may flush to a peer.
	require.Eventually(t, conn.Conn().IsClosed, waitFor, tick)
}

func TestConnection_ShutdownWithoutClosedCallback(t *testing.T) {
	t.Parallel()

	s := runServer(t)

	// The option suppresses the closed handler; Shutdown must still see the
	// end of the drain.
	conn := startConnection(t, s, nats.WithConnOptions(natsgo.NoCallbacksAfterClientClose()))

	ctx, cancel := context.WithTimeout(t.Context(), waitFor)
	defer cancel()

	require.NoError(t, conn.Shutdown(ctx))
	assert.True(t, conn.Conn().IsClosed())
}

func TestConnection_InvalidSeedFile(t *testing.T) {
	t.Parallel()

	s := runServer(t)

	conn, err := nats.NewConnection(nats.Config{
		URL:          s.clientURL(),
		NKeySeedFile: t.TempDir() + "/missing.nk",
	}, nats.WithLogger(quietLogger()), nats.WithConnectRetry(fastRetry()))
	require.NoError(t, err)

	shutdownLater(t, conn)

	err = conn.Start(t.Context())
	require.ErrorContains(t, err, "nkeySeedFile")
}

func TestConnection_InvalidConnOption(t *testing.T) {
	t.Parallel()

	conn := newConnection(t, "nats://127.0.0.1:4222", nats.WithConnOptions(func(*natsgo.Options) error {
		return errors.New("bad option")
	}))

	require.ErrorIs(t, conn.Start(t.Context()), nats.ErrInvalidConfig)
}

func TestConnection_Credentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  func(url string) nats.Config
	}{
		{"user", func(url string) nats.Config {
			return nats.Config{URL: url, User: "app", Password: "secret"}
		}},
		{"token", func(url string) nats.Config {
			return nats.Config{URL: url, Token: "t0ken"}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := runAuthServer(t, tt.name)

			conn, err := nats.NewConnection(tt.cfg(srv.clientURL()), nats.WithLogger(quietLogger()))
			require.NoError(t, err)

			shutdownLater(t, conn)

			require.NoError(t, conn.Start(t.Context()))
		})
	}
}

// runAuthServer runs a server that requires the credentials of
// TestConnection_Credentials.
func runAuthServer(t *testing.T, kind string) *testServer {
	t.Helper()

	s := &testServer{t: t, opts: &server.Options{
		Host:   "127.0.0.1",
		Port:   -1,
		NoLog:  true,
		NoSigs: true,
	}}

	switch kind {
	case "user":
		s.opts.Username, s.opts.Password = "app", "secret"
	case "token":
		s.opts.Authorization = "t0ken"
	}

	s.start()
	t.Cleanup(s.stop)

	return s
}

// unusedAddr returns a local address nothing listens on.
func unusedAddr(t *testing.T) string {
	t.Helper()

	var lc net.ListenConfig

	l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := l.Addr().String()
	require.NoError(t, l.Close())

	return addr
}
