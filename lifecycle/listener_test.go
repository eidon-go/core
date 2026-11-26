package lifecycle_test

import (
	"errors"
	"net"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/lifecycle"
)

// countingListener counts Close calls and fails every one after the first.
type countingListener struct {
	net.Listener

	closes atomic.Int32
}

var errClosedTwice = errors.New("closed twice")

func (l *countingListener) Close() error {
	if l.closes.Add(1) > 1 {
		return errClosedTwice
	}

	return l.Listener.Close()
}

func TestListenerOnce(t *testing.T) {
	t.Parallel()

	ln, err := lifecycle.ListenTCP(t.Context(), "127.0.0.1:0")
	require.NoError(t, err)

	inner := &countingListener{Listener: ln}
	once := lifecycle.ListenerOnce(inner)

	require.NoError(t, once.Close())
	require.NoError(t, once.Close())
	assert.Equal(t, int32(1), inner.closes.Load())
	assert.Same(t, once, lifecycle.ListenerOnce(once))
}

func TestListenTCP(t *testing.T) {
	t.Parallel()

	ln, err := lifecycle.ListenTCP(t.Context(), "127.0.0.1:0")
	require.NoError(t, err)

	require.NoError(t, ln.Close())
	require.NoError(t, ln.Close(), "a second Close is harmless")

	_, err = lifecycle.ListenTCP(t.Context(), "127.0.0.1:-1")
	require.Error(t, err)
}
