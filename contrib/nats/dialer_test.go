package nats_test

import (
	"context"
	"net"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/nats"
)

// blockingCloseDialer dials TCP connections whose Close blocks until the
// test ends, like a TLS connection sending close_notify to a stalled peer.
type blockingCloseDialer struct {
	release chan struct{}
	dialed  chan struct{}
}

func (d *blockingCloseDialer) Dial(network, address string) (net.Conn, error) {
	var nd net.Dialer

	conn, err := nd.DialContext(context.Background(), network, address)
	if err != nil {
		return nil, err
	}

	select {
	case d.dialed <- struct{}{}:
	default:
	}

	return &blockingCloseConn{Conn: conn, release: d.release}, nil
}

type blockingCloseConn struct {
	net.Conn

	release chan struct{}
}

func (c *blockingCloseConn) Close() error {
	<-c.release

	return c.Conn.Close()
}

func TestConnection_ShutdownDoesNotWaitForSocketClose(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	dialer := &blockingCloseDialer{release: make(chan struct{}), dialed: make(chan struct{}, 1)}

	t.Cleanup(func() { close(dialer.release) })

	conn := newConnection(t, s.clientURL(), nats.WithConnOptions(natsgo.SetCustomDialer(dialer)))
	require.NoError(t, conn.Start(t.Context()))
	recv(t, dialer.dialed)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	shutdown := make(chan error, 1)
	began := time.Now()

	go func() { shutdown <- conn.Shutdown(ctx) }()

	require.ErrorIs(t, recv(t, shutdown), context.Canceled)
	assert.Less(t, time.Since(began), time.Second, "Shutdown waited for the socket's Close")
}
