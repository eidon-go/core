package nats

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/retry"
)

// Connection is one NATS client connection that publishers, subscribers and
// consumers share. It is a Starter, Resource, Healthchecker and Namer:
//
//   - Start connects, retried with the WithConnectRetry policy. Once
//     connected, nats.go keeps the connection up by itself: it reconnects
//     (Config.ReconnectWait) until Shutdown, re-creates the subscriptions
//     and buffers what is published meanwhile (Config.ReconnectBufferSize).
//   - Shutdown drains the connection — the subscriptions left on it are
//     drained, what was published is flushed — and closes it. The drain is
//     bounded by Shutdown's deadline and by Config.DrainTimeout; then the
//     connection is closed at once.
//   - Healthcheck reports whether the client is connected (cached state).
//
// A Connection whose Start failed stays unusable: a second Start returns
// ErrAlreadyStarted, and its users get ErrNotStarted. app.App shuts the
// service down after a failed Start.
//
// Register a Connection before the publishers, subscribers and consumers
// that use it, so that it is shut down after them.
type Connection struct {
	name      string
	logger    *slog.Logger
	cfg       Config
	tlsConfig *tls.Config
	extra     []natsgo.Option
	connect   retry.Policy

	mu      sync.Mutex
	started bool
	closed  bool
	nc      *natsgo.Conn
	ncDone  chan struct{} // closed when nc is closed

	closing       context.Context //nolint:containedctx // the close signal of Start
	cancelClosing context.CancelFunc

	releaser *lifecycle.Releaser
}

// NewConnection validates cfg and builds a connection; it does no I/O
// (Start connects and reads Config.NKeySeedFile).
func NewConnection(cfg Config, opts ...Option) (*Connection, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	o := newOptions(opts)

	var tlsConfig *tls.Config
	if o.tlsConfig != nil {
		tlsConfig = o.tlsConfig.Clone()
	} else if cfg.TLS {
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12} //nolint:exhaustruct_v5 // Go defaults
	}

	closing, cancel := context.WithCancel(context.Background())

	c := &Connection{ //nolint:exhaustruct_v5 // state starts at zero
		name:          o.name,
		logger:        o.logger,
		cfg:           cfg.withDefaults(o.name),
		tlsConfig:     tlsConfig,
		extra:         o.connOptions,
		connect:       o.connectPolicy(),
		closing:       closing,
		cancelClosing: cancel,
	}
	c.releaser = lifecycle.ShutdownOnce(c.close)

	return c, nil
}

// Name returns the component name (WithName). Implements lifecycle.Namer.
func (c *Connection) Name() string { return c.name }

// Start connects, retried with the WithConnectRetry policy until ctx is
// done; Shutdown ends the retries with ErrClosed. An unreadable
// Config.NKeySeedFile fails at once. It returns ErrAlreadyStarted when
// called twice. Implements lifecycle.Starter.
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

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stop := context.AfterFunc(c.closing, cancel) //nolint:contextcheck // the close signal, not a parent context
	defer stop()

	connected, err := retry.DoValue(ctx, c.connect, c.dial)
	if err != nil {
		if c.closing.Err() != nil {
			return wrap(ErrClosed)
		}

		return fmt.Errorf("nats: connect: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		connected.nc.Close()

		return wrap(ErrClosed)
	}

	c.nc, c.ncDone = connected.nc, connected.done
	c.logger.InfoContext(ctx, "nats connected", slog.String("url", connected.nc.ConnectedUrlRedacted()))

	return nil
}

// Shutdown drains the connection and closes it. The drain is bounded by
// ctx — then the connection is closed at once and Shutdown returns
// ctx.Err() — and by Config.DrainTimeout. A connect in
// progress is abandoned. Safe without Start and after a failed Start.
// Idempotent. Implements lifecycle.Resource.
func (c *Connection) Shutdown(ctx context.Context) error {
	return c.releaser.Shutdown(ctx) //nolint:wrapcheck // the caller's context error or nil
}

// Healthcheck returns nil while the client is connected, ErrNotConnected
// while it is reconnecting (or draining), ErrNotStarted before Start has
// connected and ErrClosed after Shutdown. Implements lifecycle.Healthchecker.
func (c *Connection) Healthcheck(context.Context) error {
	nc, err := c.conn()
	if err != nil {
		return wrap(err)
	}

	if status := nc.Status(); status != natsgo.CONNECTED {
		return fmt.Errorf("%w: %s", ErrNotConnected, status)
	}

	return nil
}

// Conn returns the nats.go connection, or nil before Start has connected.
// Use it for what this module does not cover (key-value and object stores,
// stream management, statistics). Do not close or drain it: Shutdown does.
func (c *Connection) Conn() *natsgo.Conn {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.nc
}

// JetStream returns a JetStream context on the connection (no I/O), for
// what this module does not cover: key-value and object stores, stream
// management. It returns ErrNotStarted before Start has connected and
// ErrClosed after Shutdown.
func (c *Connection) JetStream(opts ...jetstream.JetStreamOpt) (jetstream.JetStream, error) {
	nc, err := c.conn()
	if err != nil {
		return nil, wrap(err)
	}

	js, err := jetstream.New(nc, opts...)
	if err != nil {
		return nil, wrap(err)
	}

	return js, nil
}

// conn returns the connection for a user of it.
func (c *Connection) conn() (*natsgo.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch {
	case c.closed:
		return nil, ErrClosed
	case c.nc == nil:
		return nil, ErrNotStarted
	default:
		return c.nc, nil
	}
}

// link is a connection and the channel closed when it is closed.
type link struct {
	nc   *natsgo.Conn
	done chan struct{}
}

// dial connects once. nats.go's connect takes no context: when ctx ends
// first, the connect is abandoned and closed once it returns.
func (c *Connection) dial(ctx context.Context) (link, error) {
	opts, done, err := c.options() //nolint:contextcheck // nats.go calls the handlers without a context
	if err != nil {
		return link{}, retry.Permanent(err) //nolint:wrapcheck // err carries the module prefix
	}

	type result struct {
		nc  *natsgo.Conn
		err error
	}

	results := make(chan result, 1)

	go func() {
		nc, err := opts.Connect()
		results <- result{nc, err}
	}()

	select {
	case r := <-results:
		if r.err != nil {
			return link{}, r.err
		}

		return link{nc: r.nc, done: done}, nil
	case <-ctx.Done():
		go func() {
			if r := <-results; r.nc != nil {
				r.nc.Close()
			}
		}()

		return link{}, ctx.Err() //nolint:wrapcheck // wrapped by Start
	}
}

// options builds the nats.go options of one connect: Config, TLS, the NKey
// seed, WithConnOptions, then the Connection's handlers (which log, then
// call the ones set by WithConnOptions). done is closed when the connection
// is closed.
func (c *Connection) options() (natsgo.Options, chan struct{}, error) {
	o := natsgo.GetDefaultOptions()

	for s := range strings.SplitSeq(c.cfg.URL, ",") {
		o.Servers = append(o.Servers, strings.TrimSpace(s))
	}

	opts := c.cfg.connOptions()

	if c.tlsConfig != nil {
		opts = append(opts, natsgo.Secure(c.tlsConfig.Clone()))
	}

	if c.cfg.NKeySeedFile != "" {
		nkey, err := natsgo.NkeyOptionFromSeed(c.cfg.NKeySeedFile)
		if err != nil {
			return o, nil, fmt.Errorf("nats: read nkeySeedFile: %w", err)
		}

		opts = append(opts, nkey)
	}

	for _, opt := range append(opts, c.extra...) {
		if opt == nil {
			continue
		}

		if err := opt(&o); err != nil {
			return o, nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
		}
	}

	done := make(chan struct{})
	c.handlers(&o, done)

	return o, done, nil
}

// handlers wraps the connection callbacks of o with the Connection's: they
// log, and the closed handler closes done.
func (c *Connection) handlers(o *natsgo.Options, done chan struct{}) {
	ctx := context.Background()
	disconnected, reconnected, closed, async, lameDuck :=
		o.DisconnectedErrCB, o.ReconnectedCB, o.ClosedCB, o.AsyncErrorCB, o.LameDuckModeHandler

	o.DisconnectedErrCB = func(nc *natsgo.Conn, err error) {
		if !nc.IsClosed() {
			c.logger.WarnContext(ctx, "nats disconnected, reconnecting", slog.Any("error", err))
		}

		if disconnected != nil {
			disconnected(nc, err)
		}
	}

	o.ReconnectedCB = func(nc *natsgo.Conn) {
		c.logger.InfoContext(ctx, "nats reconnected", slog.String("url", nc.ConnectedUrlRedacted()))

		if reconnected != nil {
			reconnected(nc)
		}
	}

	o.ClosedCB = func(nc *natsgo.Conn) {
		close(done)

		if closed != nil {
			closed(nc)
		}
	}

	o.AsyncErrorCB = func(nc *natsgo.Conn, sub *natsgo.Subscription, err error) {
		attrs := []any{slog.Any("error", err)}
		if sub != nil {
			attrs = append(attrs, slog.String("subject", sub.Subject))
		}

		c.logger.ErrorContext(ctx, "nats async error", attrs...)

		if async != nil {
			async(nc, sub, err)
		}
	}

	o.LameDuckModeHandler = func(nc *natsgo.Conn) {
		c.logger.WarnContext(ctx, "nats server entered lame duck mode, will reconnect elsewhere",
			slog.String("url", nc.ConnectedUrlRedacted()))

		if lameDuck != nil {
			lameDuck(nc)
		}
	}
}

// close is the release of Shutdown.
func (c *Connection) close(ctx context.Context) error {
	c.cancelClosing()

	c.mu.Lock()
	c.closed = true
	nc, done := c.nc, c.ncDone
	c.mu.Unlock()

	if nc == nil {
		return nil
	}

	// No time to drain: close at once.
	if err := ctx.Err(); err != nil {
		nc.Close()
		c.logger.WarnContext(ctx, "nats closed without a drain: the shutdown deadline has passed")

		return err //nolint:wrapcheck // the caller's own context error
	}

	// Drain fails on a connection that is reconnecting or closed: what was
	// buffered is lost then.
	if err := nc.Drain(); err != nil {
		nc.Close()
		c.logger.InfoContext(ctx, "nats closed", slog.String("drain", err.Error()))

		return nil
	}

	select {
	case <-done:
		c.logger.InfoContext(ctx, "nats closed")

		return nil
	case <-ctx.Done():
		nc.Close()
		c.logger.WarnContext(ctx, "nats drain cut off by the shutdown deadline")

		return ctx.Err() //nolint:wrapcheck // the caller's own context error
	}
}

// Compile-time checks.
var (
	_ lifecycle.Starter       = (*Connection)(nil)
	_ lifecycle.Resource      = (*Connection)(nil)
	_ lifecycle.Healthchecker = (*Connection)(nil)
	_ lifecycle.Namer         = (*Connection)(nil)
)
