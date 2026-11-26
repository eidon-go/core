package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/trace"

	"github.com/eidon-go/core/lifecycle"
)

// Publisher publishes messages on a channel of its Connection in confirm
// mode: Publish returns once the broker has taken responsibility for every
// message (persisted it, for persistent messages on durable queues). It is a
// Resource and Namer; it has no goroutines of its own, and opens its
// channel on the first Publish.
//
// Publish is safe for concurrent use; all Publish calls share the channel.
// Messages that the broker cannot route (no queue bound for the routing key)
// are confirmed and dropped by it: bind the queues, or give the exchange an
// "alternate-exchange". A channel error — publishing to an exchange that
// does not exist, say — makes the broker close the channel: every Publish
// waiting on it then fails with ErrNotConfirmed and the broker's reason, and
// the next Publish opens a new channel.
type Publisher struct {
	conn   *Connection
	name   string
	logger *slog.Logger
	tel    *telemetry

	open chan struct{} // a token: held while a channel is being opened

	mu       sync.Mutex
	ch       *confirmChannel // nil until the first Publish
	closed   bool
	inflight sync.WaitGroup

	abort       context.Context //nolint:containedctx // cancelled when Shutdown gives up waiting
	cancelAbort context.CancelFunc

	releaser *lifecycle.Releaser
}

// NewPublisher builds a publisher on conn; it does no network I/O.
func NewPublisher(conn *Connection, opts ...Option) (*Publisher, error) {
	if conn == nil {
		return nil, fmt.Errorf("%w: connection is nil", ErrInvalidConfig)
	}

	o := newOptions(opts)
	abort, cancel := context.WithCancel(context.Background())

	p := &Publisher{ //nolint:exhaustruct_v5 // state starts at zero
		conn:        conn,
		name:        o.name,
		logger:      o.logger,
		tel:         newTelemetry(&o),
		open:        make(chan struct{}, 1),
		abort:       abort,
		cancelAbort: cancel,
	}
	p.releaser = lifecycle.ShutdownOnce(p.shutdown)

	return p, nil
}

// Name returns the component name (WithName). Implements lifecycle.Namer.
func (p *Publisher) Name() string { return p.name }

// Publish sends msgs, in order, on one channel, and waits until the broker
// has confirmed every one; it returns the first error. A message that fails
// to be sent ends the sending: the ones after it are not sent — not on
// another channel either, which would break the order — and the ones before
// it are still waited for.
//
// While the connection is down Publish waits for it, until ctx is done; the
// error then wraps ErrNotConnected and ctx.Err(). A message the broker
// refuses fails with ErrNacked. One whose channel closed before its confirm
// — a lost connection, a channel error, or Shutdown giving up waiting —
// fails with ErrNotConfirmed (joined with the broker's *amqp.Error, if it
// gave one): it may have been delivered, so a retry may duplicate it. When
// ctx is done while waiting for a confirm, the error wraps ctx.Err() and the
// message may still be delivered. It returns ErrClosed after Shutdown and
// ErrNotStarted before the connection is started.
//
// On a connection the broker has blocked (a memory or disk alarm) Publish
// may wait past ctx: amqp091 opens channels without a context — on the first
// Publish, and the first after a reconnect — and writes to the socket
// without a deadline, which blocks once the broker stops reading. It
// returns when the alarm clears. Only the wait for a confirm honors ctx
// there. Separate connections keep the consumers out of this (see the
// package documentation).
func (p *Publisher) Publish(ctx context.Context, msgs ...*Message) error {
	for i, msg := range msgs {
		if msg == nil {
			return fmt.Errorf("%w: msgs[%d] is nil", ErrInvalidConfig, i)
		}
	}

	if !p.enter() {
		return wrap(ErrClosed)
	}
	defer p.inflight.Done()

	callerCtx := ctx

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stop := context.AfterFunc(p.abort, cancel) //nolint:contextcheck // the abort signal, not a parent context
	defer stop()

	type pending struct {
		msg  *Message
		span trace.Span
		conf confirmation
	}

	var (
		pinned   *confirmChannel // every message goes on the channel of the first one
		waiting  = make([]pending, 0, len(msgs))
		firstErr error
	)

	for _, msg := range msgs {
		spanCtx, span, headers := p.tel.startPublish(ctx, msg)

		ch, conf, err := p.send(spanCtx, msg, headers, pinned)
		if err != nil {
			err = p.aborted(callerCtx, err, wrap(ErrClosed))
			p.tel.endPublish(spanCtx, span, msg, err)
			firstErr = err

			break
		}

		pinned = ch

		waiting = append(waiting, pending{msg: msg, span: span, conf: conf})
	}

	for _, w := range waiting {
		spanCtx := trace.ContextWithSpan(ctx, w.span)
		err := p.aborted(callerCtx, awaitConfirm(spanCtx, pinned, w.conf), ErrNotConfirmed)
		p.tel.endPublish(spanCtx, w.span, w.msg, err)

		if firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// Shutdown refuses new messages and waits for the confirms of the messages
// being published until ctx is done; then the Publish calls still waiting
// fail with ErrNotConfirmed and Shutdown returns ctx.Err(). It closes the
// channel, waiting for that until ctx is done too (the close goes on in the
// background). The Connection is not closed. Safe without any Publish.
// Idempotent. Implements lifecycle.Resource.
func (p *Publisher) Shutdown(ctx context.Context) error {
	return p.releaser.Shutdown(ctx) //nolint:wrapcheck // the caller's context error or nil
}

// enter registers a Publish; false after Shutdown.
func (p *Publisher) enter() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return false
	}

	p.inflight.Add(1)

	return true
}

// aborted replaces err by abortErr when err comes from Shutdown giving up
// waiting — the abort cancelled the Publish, not the caller's ctx.
func (p *Publisher) aborted(callerCtx context.Context, err, abortErr error) error {
	if err != nil && p.abort.Err() != nil && callerCtx.Err() == nil && errors.Is(err, context.Canceled) {
		return abortErr
	}

	return err
}

// send publishes msg on pinned or, without one, on the confirm channel — a
// new one if the current one closed before the message was sent.
func (p *Publisher) send(
	ctx context.Context, msg *Message, headers amqp.Table, pinned *confirmChannel,
) (*confirmChannel, confirmation, error) {
	pub := msg.publishing(headers)

	for {
		ch := pinned
		if ch == nil {
			var err error
			if ch, err = p.channel(ctx); err != nil {
				return nil, nil, err
			}
		}

		conf, err := ch.publish(ctx, msg.Exchange, msg.RoutingKey, pub)
		if err == nil {
			return ch, conf, nil
		}

		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, nil, fmt.Errorf("rabbitmq: publish: %w", ctxErr)
		}

		closed := errors.Is(err, amqp.ErrClosed) || ch.isClosed()

		switch {
		case closed && pinned != nil:
			return nil, nil, fmt.Errorf("rabbitmq: publish to %q: channel closed: %w",
				destination(msg.Exchange), ch.closeErr())
		case !closed:
			return nil, nil, fmt.Errorf("rabbitmq: publish to %q: %w", destination(msg.Exchange), err)
		}

		// The channel closed before the message was sent: not delivered.
		p.drop(ch)
	}
}

// channel returns the confirm channel, opening one if there is none or it
// closed. One caller opens it at a time; the others wait with their ctx.
func (p *Publisher) channel(ctx context.Context) (*confirmChannel, error) {
	if ch := p.current(); ch != nil {
		return ch, nil
	}

	select {
	case p.open <- struct{}{}:
	case <-ctx.Done():
		return nil, errors.Join(ErrNotConnected, ctx.Err())
	}

	defer func() { <-p.open }()

	if ch := p.current(); ch != nil {
		return ch, nil
	}

	raw, err := p.conn.channel(ctx)
	if err != nil {
		return nil, err
	}

	if err := raw.confirm(); err != nil {
		_ = raw.close()

		return nil, fmt.Errorf("rabbitmq: enable publisher confirms: %w", err)
	}

	ch := newConfirmChannel(raw)

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed && p.abort.Err() != nil {
		go func() { _ = ch.close() }()

		return nil, wrap(ErrClosed)
	}

	p.ch = ch

	return ch, nil
}

// current returns the confirm channel if it is open.
func (p *Publisher) current() *confirmChannel {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.ch != nil && !p.ch.isClosed() {
		return p.ch
	}

	return nil
}

// drop forgets ch, if it is still the confirm channel, and closes it.
func (p *Publisher) drop(ch *confirmChannel) {
	p.mu.Lock()
	if p.ch == ch {
		p.ch = nil
	}
	p.mu.Unlock()

	_ = ch.close()
}

// shutdown is the release of the Releaser.
func (p *Publisher) shutdown(ctx context.Context) error {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()

	done := make(chan struct{})

	go func() {
		p.inflight.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}

	// Waiting Publish calls return now (ErrNotConfirmed).
	p.cancelAbort()

	p.mu.Lock()
	ch := p.ch
	p.ch = nil
	p.mu.Unlock()

	if ch != nil {
		// channel.close is a call without a context: bound the wait for it.
		closed := make(chan error, 1)

		go func() { closed <- ch.close() }()

		select {
		case err := <-closed:
			if err != nil && !errors.Is(err, amqp.ErrClosed) {
				p.logger.WarnContext(ctx, "rabbitmq publisher channel close failed", slog.Any("error", err))
			}
		case <-ctx.Done():
		}
	}

	return ctx.Err() //nolint:wrapcheck // the caller's own context error, or nil
}

// awaitConfirm waits for the broker's confirm of a message sent on ch.
func awaitConfirm(ctx context.Context, ch *confirmChannel, conf confirmation) error {
	acked, err := conf.wait(ctx)

	switch {
	case err != nil:
		return fmt.Errorf("rabbitmq: wait for confirm: %w", err)
	case acked:
		return nil
	case ch.isClosed():
		return ch.closeErr()
	default:
		return ErrNacked
	}
}

// confirmChannel is a channel in confirm mode that keeps the reason the
// broker closed it with.
type confirmChannel struct {
	amqpChannel

	closes chan *amqp.Error

	mu     sync.Mutex
	known  bool        // the close was seen
	reason *amqp.Error // nil for a close without a broker error
}

func newConfirmChannel(ch amqpChannel) *confirmChannel {
	return &confirmChannel{ //nolint:exhaustruct_v5 // state starts at zero
		amqpChannel: ch,
		closes:      ch.notifyClose(make(chan *amqp.Error, 1)),
	}
}

// closeErr returns ErrNotConfirmed, joined with the broker's reason for
// closing the channel if it gave one. amqp091 hands the reason to the close
// listeners before it releases the confirms, so it is known by then.
func (c *confirmChannel) closeErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.known {
		select {
		case reason, ok := <-c.closes:
			c.known = true

			if ok {
				c.reason = reason
			}
		default:
		}
	}

	if c.reason != nil {
		return fmt.Errorf("%w: channel closed by the broker: %w", ErrNotConfirmed, c.reason)
	}

	return ErrNotConfirmed
}

// Compile-time checks.
var (
	_ lifecycle.Resource = (*Publisher)(nil)
	_ lifecycle.Namer    = (*Publisher)(nil)
)
