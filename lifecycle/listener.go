package lifecycle

import (
	"context"
	"net"
	"sync"
)

// ListenTCP opens a TCP listener on addr within ctx (Server's Listen); the
// listener is wrapped with ListenerOnce.
func ListenTCP(ctx context.Context, addr string) (net.Listener, error) {
	var lc net.ListenConfig

	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, err //nolint:wrapcheck // the caller adds the address and its prefix
	}

	return ListenerOnce(ln), nil
}

// ListenerOnce returns ln with a Close that closes it once and returns the
// result of that close to every call. A server's Serve closes the listener
// it is given, and its Close closes it again in case Serve never ran; not
// every net.Listener tolerates a second Close. A listener that ListenerOnce
// returned is returned as is.
func ListenerOnce(ln net.Listener) net.Listener {
	if once, ok := ln.(*onceListener); ok {
		return once
	}

	return &onceListener{Listener: ln} //nolint:exhaustruct_v5 // once state starts at zero
}

type onceListener struct {
	net.Listener

	once sync.Once
	err  error
}

func (l *onceListener) Close() error {
	l.once.Do(func() { l.err = l.Listener.Close() })

	return l.err
}
