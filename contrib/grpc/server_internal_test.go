package grpc

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// The listener closed by drain before grpc-go has marked the server as
// stopping (Shutdown with an expired ctx) makes Serve fail on Accept; during
// the stop that is not an error. Reproduced deterministically: the stop has
// begun, the listener is closed and the grpc.Server is not yet stopping.
func TestServe_ListenerClosedByDrain(t *testing.T) {
	t.Parallel()

	s, err := New(Config{Addr: "127.0.0.1:0"})
	require.NoError(t, err)
	require.NoError(t, s.Start(t.Context()))
	require.NoError(t, s.Shutdown(t.Context())) // begins the stop, closes the listener

	s.srv = grpc.NewServer() // a server that GracefulStop has not reached yet

	require.NoError(t, s.serve())
}

func TestServe_ListenerClosedBeforeStop(t *testing.T) {
	t.Parallel()

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, ln.Close())

	s, err := New(Config{Addr: "127.0.0.1:0"}, WithListener(ln))
	require.NoError(t, err)
	require.NoError(t, s.Start(t.Context()))

	require.ErrorIs(t, s.serve(), net.ErrClosed, "a listener that fails outside a stop is an error")
	require.NoError(t, s.Shutdown(t.Context()))
}
