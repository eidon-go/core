package rabbitmq

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/retry"
)

// Connection is one AMQP connection that publishers and consumers share; it
// keeps itself connected. It is a Starter, Resource, Healthchecker and Namer:
//
//   - Start dials the broker, retried with the WithConnectRetry policy, and
//     starts a background loop that redials whenever the connection is lost
//     — with the same delays, until Shutdown.
//   - Shutdown stops the loop and closes the connection; the channels of
//     its publishers and consumers close with it. The close handshake is
//     bounded by Shutdown's deadline (without one, by Config.DialTimeout);
//     then the socket is closed.
//   - Healthcheck reports whether the connection is up (cached state).
//
// While the connection is down, Publish and the consumers wait for it (see
// Publisher.Publish and Consumer). Healthcheck does not report a connection
// the broker has blocked (a resource alarm) — the block is logged — and on
// such a connection opening a channel waits until the alarm clears.
//
// A Connection whose Start failed stays unusable: a second Start returns
// ErrAlreadyStarted, and its users wait for a connection until their ctx is
// done. app.App shuts the service down after a failed Start.
//
// Register a Connection before the publishers and consumers that use it, so
// that it is shut down after them.
type Connection struct {
	name         string
	logger       *slog.Logger
	addr         string
	amqpCfg      amqp.Config
	connect      retry.Policy
	reconnect    retry.Policy
	dial         dialFunc
	closeTimeout time.Duration // bounds a close without a deadline of its own

	mu      sync.Mutex
	started bool
	closed  bool
	conn    amqpConn      // nil while down
	ready   chan struct{} // closed while conn is set; replaced when it is lost

	loops sync.WaitGroup // the reconnect loop

	closing context.Context //nolint:containedctx // releaser.Context(): the close signal of the loop

	releaser *lifecycle.Releaser
}

// NewConnection validates cfg and builds a connection; it does no network
// I/O (Start dials).
func NewConnection(cfg Config, opts ...Option) (*Connection, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	o := newOptions(opts)

	var tlsConfig *tls.Config
	if o.tlsConfig != nil {
		tlsConfig = o.tlsConfig
	} else if cfg.TLS {
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12} //nolint:exhaustruct_v5 // Go defaults
	}

	cfg = cfg.withDefaults(o.name, tlsConfig != nil)

	dial := o.dial
	if dial == nil {
		timeout := cfg.DialTimeout
		dial = func(ctx context.Context, addr string, ac amqp.Config) (amqpConn, error) {
			return dialAMQP(ctx, addr, ac, timeout)
		}
	}

	c := &Connection{ //nolint:exhaustruct_v5 // state starts at zero
		name:         o.name,
		logger:       o.logger,
		addr:         cfg.address(tlsConfig != nil),
		amqpCfg:      cfg.amqpConfig(tlsConfig),
		connect:      o.connectPolicy(),
		reconnect:    o.reconnectPolicy(),
		dial:         dial,
		closeTimeout: cfg.DialTimeout,
		ready:        make(chan struct{}),
	}
	c.releaser = lifecycle.ShutdownOnce(c.close)
	c.closing = c.releaser.Context()

	return c, nil
}

// Name returns the component name (WithName). Implements lifecycle.Namer.
func (c *Connection) Name() string { return c.name }

// Start dials the broker until it answers, retried with the WithConnectRetry
// policy, then starts the reconnect loop. Shutdown ends the retries with
// ErrClosed. It returns ErrAlreadyStarted when called twice. Implements
// lifecycle.Starter.
func (c *Connection) Start(ctx context.Context) error {
	c.mu.Lock()

	switch {
	case c.closed:
		c.mu.Unlock()

		return wrap(ErrClosed)
	case c.started:
		c.mu.Unlock()

		return wrap(ErrAlreadyStarted)
	}

	c.started = true
	c.mu.Unlock()

	var conn amqpConn

	err := c.releaser.Do(ctx, func(ctx context.Context) error {
		var err error
		if conn, err = retry.DoValue(ctx, c.connect, c.dialOnce); err != nil {
			return fmt.Errorf("connect: %w", err)
		}

		return nil
	})
	if err != nil {
		return wrap(err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		c.discard(conn)

		return wrap(ErrClosed)
	}

	c.set(conn)
	c.loops.Go(func() { c.watch(conn) })
	c.logger.InfoContext(ctx, "rabbitmq connected", slog.String("addr", c.addr))

	return nil
}

// Shutdown stops the reconnect loop and closes the connection. The close
// handshake is bounded by ctx's deadline — then the socket is closed and
// Shutdown returns ctx.Err() — and, without a deadline, by
// Config.DialTimeout. A redial in progress (its TCP connect honors the stop;
// its handshakes are bounded by Config.DialTimeout) is waited for until ctx
// is done. Safe without Start and after a failed Start. Idempotent.
// Implements lifecycle.Resource.
func (c *Connection) Shutdown(ctx context.Context) error {
	return c.releaser.Shutdown(ctx) //nolint:wrapcheck // the caller's context error or nil
}

// Healthcheck returns nil while the connection is up, ErrNotConnected while
// it is redialed, ErrNotStarted before Start and ErrClosed after Shutdown.
// Implements lifecycle.Healthchecker.
func (c *Connection) Healthcheck(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch {
	case c.closed:
		return wrap(ErrClosed)
	case !c.started:
		return wrap(ErrNotStarted)
	case c.conn == nil:
		return ErrNotConnected
	default:
		return nil
	}
}

// dialOnce is one attempt of a (re)dial.
func (c *Connection) dialOnce(ctx context.Context) (amqpConn, error) {
	return c.dial(ctx, c.addr, c.amqpCfg)
}

// set makes conn the current connection and wakes up the waiters. Called
// with mu held.
func (c *Connection) set(conn amqpConn) {
	c.conn = conn
	close(c.ready)
}

// lost marks conn as lost, if it is still the current one; the waiters wait
// for the next connection. It is called by the loop when amqp091 reports
// the close, and by channel when opening a channel shows it first.
func (c *Connection) lost(conn amqpConn) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == conn && conn != nil {
		c.conn = nil
		c.ready = make(chan struct{})
	}
}

// watch waits for the loss of conn, then redials until Shutdown, and so on.
func (c *Connection) watch(conn amqpConn) {
	for {
		closed := conn.notifyClose(make(chan *amqp.Error, 1))
		blocked := conn.notifyBlocked(make(chan amqp.Blocking, 1))

		if !c.waitLost(closed, blocked) {
			return
		}

		c.lost(conn)

		next, err := retry.DoValue(c.closing, c.reconnect, c.dialOnce)
		if err != nil {
			return // Shutdown
		}

		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			c.discard(next)

			return
		}

		c.set(next)
		c.mu.Unlock()

		c.logger.InfoContext(c.closing, "rabbitmq reconnected", slog.String("addr", c.addr))

		conn = next
	}
}

// waitLost waits until the connection is closed (true) or Shutdown begins
// (false), logging the flow control of the broker meanwhile.
func (c *Connection) waitLost(closed <-chan *amqp.Error, blocked <-chan amqp.Blocking) bool {
	for {
		select {
		case <-c.closing.Done():
			return false
		case blocking, ok := <-blocked:
			if !ok {
				blocked = nil

				continue
			}

			if blocking.Active {
				c.logger.WarnContext(c.closing, "rabbitmq connection blocked by the broker",
					slog.String("reason", blocking.Reason))
			} else {
				c.logger.InfoContext(c.closing, "rabbitmq connection unblocked")
			}
		case err := <-closed:
			if c.closing.Err() != nil {
				return false
			}

			attrs := []any{slog.String("addr", c.addr)}
			if err != nil {
				attrs = append(attrs, slog.Any("error", err))
			}

			c.logger.WarnContext(c.closing, "rabbitmq connection lost, reconnecting", attrs...)

			return true
		}
	}
}

// channel opens a channel on the current connection. While the connection
// is down it waits for the next one until ctx is done; it returns
// ErrNotStarted before Start and ErrClosed after Shutdown.
func (c *Connection) channel(ctx context.Context) (amqpChannel, error) {
	for {
		c.mu.Lock()
		conn, ready, started, closed := c.conn, c.ready, c.started, c.closed
		c.mu.Unlock()

		switch {
		case closed:
			return nil, wrap(ErrClosed)
		case !started:
			return nil, wrap(ErrNotStarted)
		}

		if conn == nil {
			select {
			case <-ready:
				continue
			case <-c.closing.Done():
				return nil, wrap(ErrClosed)
			case <-ctx.Done():
				return nil, errors.Join(ErrNotConnected, ctx.Err())
			}
		}

		ch, err := conn.channel()
		if err == nil {
			return ch, nil
		}

		if !conn.isClosed() {
			return nil, err
		}

		// Lost before the loop has noticed: wait for the next connection.
		c.lost(conn)
	}
}

// close stops the loop and closes the connection within ctx; run once by the
// Releaser. A loop that is still redialing when ctx is done discards the
// connection it gets (it sees closed).
func (c *Connection) close(ctx context.Context) error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()

	loopDone := make(chan struct{})

	go func() {
		c.loops.Wait()
		close(loopDone)
	}()

	select {
	case <-loopDone:
	case <-ctx.Done():
	}

	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()

	if conn == nil {
		return ctx.Err() //nolint:wrapcheck // the caller's own context error, or nil
	}

	// The deadline bounds the close handshake on the socket, but amqp091
	// does more after it (notifying listeners has timeouts of its own): wait
	// for the close only until ctx is done.
	closed := make(chan error, 1)
	deadline := c.closeDeadline(ctx)

	go func() { closed <- conn.closeDeadline(deadline) }()

	var err error

	select {
	case err = <-closed:
	case <-ctx.Done():
	}

	switch {
	case ctx.Err() != nil:
		return ctx.Err() //nolint:wrapcheck // the caller's own context error
	case err != nil && !errors.Is(err, amqp.ErrClosed):
		return fmt.Errorf("rabbitmq: close: %w", err)
	default:
		return nil
	}
}

// closeDeadline returns the deadline of a close within ctx: now when ctx is
// done (force), its deadline, or Config.DialTimeout from now.
func (c *Connection) closeDeadline(ctx context.Context) time.Time {
	if ctx.Err() != nil {
		return time.Now()
	}

	if deadline, ok := ctx.Deadline(); ok {
		return deadline
	}

	return time.Now().Add(c.closeTimeout)
}

// discard closes a connection that is no longer wanted, in the background.
func (c *Connection) discard(conn amqpConn) {
	go func() { _ = conn.closeDeadline(time.Now().Add(c.closeTimeout)) }()
}

// Compile-time checks.
var (
	_ lifecycle.Starter       = (*Connection)(nil)
	_ lifecycle.Resource      = (*Connection)(nil)
	_ lifecycle.Healthchecker = (*Connection)(nil)
	_ lifecycle.Namer         = (*Connection)(nil)
)
