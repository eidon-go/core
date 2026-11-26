package nats

import (
	"errors"
	"net"
	"sync"

	natsgo "github.com/nats-io/nats.go"
)

// errInterrupted fails the dials of a Connection whose sockets were
// interrupted: it is shutting down and must not reconnect.
var errInterrupted = errors.New("nats: connection shut down")

// sockets dials the TCP connections of a Connection and can interrupt them.
// nats.go writes to the socket holding its connection mutex, which Drain,
// Close and every state query wait for: against a peer that stops reading,
// only closing the socket ends such a write before nats.go's write timeout.
type sockets struct {
	mu          sync.Mutex
	current     net.Conn
	interrupted bool
}

// dialer wraps the dialer nats.go would use with o: the CustomDialer set by
// WithConnOptions, or a net.Dialer like nats.go's default one.
func (s *sockets) dialer(o *natsgo.Options) natsgo.CustomDialer {
	base := o.CustomDialer
	if base == nil {
		d := &net.Dialer{Timeout: o.Timeout} //nolint:exhaustruct_v5 // Go defaults
		if o.Dialer != nil {
			copied := *o.Dialer
			d = &copied
		}

		if o.AllowMultipathTCP != nil {
			d.SetMultipathTCP(*o.AllowMultipathTCP)
		}

		base = d
	}

	return &trackingDialer{sockets: s, base: base}
}

// track records conn as the current socket; it reports false (and closes
// conn) once the sockets are interrupted.
func (s *sockets) track(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.interrupted {
		_ = conn.Close()

		return false
	}

	s.current = conn

	return true
}

// interrupt closes the current socket — a write blocked on it fails at
// once — and fails every later dial.
func (s *sockets) interrupt() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.interrupted = true

	if s.current != nil {
		_ = s.current.Close()
	}
}

// trackingDialer is the CustomDialer of a Connection.
type trackingDialer struct {
	sockets *sockets
	base    natsgo.CustomDialer
}

// Dial dials with the wrapped dialer and tracks the socket.
func (d *trackingDialer) Dial(network, address string) (net.Conn, error) {
	conn, err := d.base.Dial(network, address)
	if err != nil {
		return nil, err //nolint:wrapcheck // nats.go reports the dial error
	}

	if !d.sockets.track(conn) {
		return nil, errInterrupted
	}

	return conn, nil
}

// SkipTLSHandshake keeps the choice of a wrapped dialer that skips the TLS
// handshake (nats.go asks the CustomDialer).
func (d *trackingDialer) SkipTLSHandshake() bool {
	skip, ok := d.base.(interface{ SkipTLSHandshake() bool })

	return ok && skip.SkipTLSHandshake()
}
