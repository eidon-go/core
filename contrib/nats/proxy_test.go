package nats_test

import (
	"io"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

// stallingProxy forwards TCP connections to a NATS server; once stalled it
// stops reading from the clients, like a server that hangs, so that their
// writes block when the socket buffers are full.
type stallingProxy struct {
	listener net.Listener
	target   string
	stalled  chan struct{}
	once     sync.Once
	closing  chan struct{}

	mu    sync.Mutex
	conns []net.Conn
}

func newStallingProxy(t *testing.T, serverURL string) *stallingProxy {
	t.Helper()

	u, err := url.Parse(serverURL)
	require.NoError(t, err)

	var lc net.ListenConfig

	l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	p := &stallingProxy{
		listener: l,
		target:   u.Host,
		stalled:  make(chan struct{}),
		closing:  make(chan struct{}),
	}

	go p.accept()

	t.Cleanup(p.close)

	return p
}

func (p *stallingProxy) url() string { return "nats://" + p.listener.Addr().String() }

// stall stops reading from the clients.
func (p *stallingProxy) stall() { p.once.Do(func() { close(p.stalled) }) }

func (p *stallingProxy) accept() {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}

		server, err := net.Dial("tcp", p.target) //nolint:noctx // test proxy
		if err != nil {
			_ = client.Close()

			continue
		}

		p.mu.Lock()
		p.conns = append(p.conns, client, server)
		p.mu.Unlock()

		go func() { _, _ = io.Copy(client, server) }()
		go p.forward(server, client)
	}
}

// forward copies from the client to the server until the proxy stalls.
func (p *stallingProxy) forward(server, client net.Conn) {
	buf := make([]byte, 32*1024)

	for {
		select {
		case <-p.stalled:
			<-p.closing

			return
		default:
		}

		n, err := client.Read(buf)
		if err != nil {
			return
		}

		if _, err := server.Write(buf[:n]); err != nil {
			return
		}
	}
}

func (p *stallingProxy) close() {
	close(p.closing)
	_ = p.listener.Close()

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, c := range p.conns {
		_ = c.Close()
	}
}

// mutexHeld reports whether nats.go's connection mutex stays held — by a
// write blocked on a peer that does not read — for longer than a moment.
func mutexHeld(nc *natsgo.Conn) bool {
	free := make(chan struct{})

	go func() {
		_, _ = nc.Buffered()

		close(free)
	}()

	select {
	case <-free:
		return false
	case <-time.After(100 * time.Millisecond):
		return true
	}
}
