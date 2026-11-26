package nats

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"

	"github.com/eidon-go/core/lifecycle"
)

// Publisher publishes messages on a Connection: core NATS publishes
// (Publish), JetStream publishes acknowledged by the stream
// (PublishJetStream) and requests (Request). It is a Resource and Namer;
// it has no goroutines and no Start — the Connection connects.
//
// Shutdown refuses new calls (ErrClosed), waits for the calls in flight and
// flushes what was published to the server; all of it bounded by its ctx.
//
// Register it after its Connection and before the subscribers and
// consumers whose handlers publish with it, so that it is shut down after
// them (their handlers may publish while they drain) and before the
// Connection.
type Publisher struct {
	conn   *Connection
	name   string
	logger *slog.Logger
	tel    *telemetry

	mu       sync.Mutex
	closed   bool
	inflight sync.WaitGroup
	js       jetstream.JetStream
	jsConn   *natsgo.Conn // the connection js was made on

	releaser *lifecycle.Releaser
}

// NewPublisher builds a publisher on conn; it does no I/O.
func NewPublisher(conn *Connection, opts ...Option) (*Publisher, error) {
	if conn == nil {
		return nil, fmt.Errorf("%w: connection is nil", ErrInvalidConfig)
	}

	o := newOptions(opts)

	p := &Publisher{ //nolint:exhaustruct_v5 // state starts at zero
		conn:   conn,
		name:   o.name,
		logger: o.logger,
		tel:    newTelemetry(&o),
	}
	p.releaser = lifecycle.ShutdownOnce(p.shutdown)

	return p, nil
}

// Name returns the component name (WithName). Implements lifecycle.Namer.
func (p *Publisher) Name() string { return p.name }

// Publish publishes msgs with core NATS, in order: at most once, to the
// subscribers listening at the time — nothing is stored. It returns once
// the messages are handed to the client, which sends them in the
// background (Shutdown flushes them); while the client reconnects they are
// buffered (Config.ReconnectBufferSize). It stops at the first error.
// It returns an error wrapping ErrInvalidConfig for an invalid message
// (none is published then), ErrNotStarted before the Connection is started
// and ErrClosed after Shutdown.
func (p *Publisher) Publish(ctx context.Context, msgs ...*Message) error {
	for _, msg := range msgs {
		if err := validateMessage(msg); err != nil {
			return err
		}
	}

	nc, err := p.enter()
	if err != nil {
		return err
	}
	defer p.inflight.Done()

	for _, msg := range msgs {
		if err := ctx.Err(); err != nil {
			return err //nolint:wrapcheck // the caller's own context error
		}

		spanCtx, span, header := p.tel.startPublish(ctx, opPublish, msg)
		err := nc.PublishMsg(msg.natsMsg(header))
		p.tel.endPublish(spanCtx, span, opPublish, err)

		if err != nil {
			return fmt.Errorf("nats: publish to %q: %w", msg.Subject, err)
		}
	}

	return nil
}

// PublishJetStream publishes msg to the JetStream stream that captures its
// subject and returns once the stream has stored it, with the stream's
// acknowledgement. opts are passed to nats.go: jetstream.WithMsgID (the
// stream drops a duplicate ID within its duplicate window — the way to
// retry a publish safely), jetstream.WithExpectStream,
// jetstream.WithExpectLastSequence and the like. ctx bounds the wait for
// the acknowledgement; without a deadline nats.go applies its default
// timeout (5s). It returns jetstream.ErrNoStreamResponse when no stream
// captures the subject, ErrNotStarted before the Connection is started and
// ErrClosed after Shutdown.
func (p *Publisher) PublishJetStream(
	ctx context.Context, msg *Message, opts ...jetstream.PublishOpt,
) (*jetstream.PubAck, error) {
	if err := validateMessage(msg); err != nil {
		return nil, err
	}

	js, err := p.jetStream()
	if err != nil {
		return nil, err
	}
	defer p.inflight.Done()

	spanCtx, span, header := p.tel.startPublish(ctx, opPublish, msg)

	ack, err := js.PublishMsg(spanCtx, msg.natsMsg(header), opts...)
	if err == nil {
		span.SetAttributes(attribute.String("messaging.nats.stream", ack.Stream),
			semconv.MessagingMessageID(fmt.Sprintf("%s:%d", ack.Stream, ack.Sequence)))
	}

	p.tel.endPublish(spanCtx, span, opPublish, err)

	if err != nil {
		return nil, fmt.Errorf("nats: publish to stream, subject %q: %w", msg.Subject, err)
	}

	return ack, nil
}

// Request publishes msg as a request and returns the first response (a
// Subscriber's handler answers with Message.Respond). msg.Reply is
// replaced by an inbox of the client. ctx bounds the wait; without a
// deadline it waits until a response comes or ctx is cancelled. It returns
// ErrNoResponders at once when nobody listens on the subject, ErrNotStarted
// before the Connection is started and ErrClosed after Shutdown.
func (p *Publisher) Request(ctx context.Context, msg *Message) (*Message, error) {
	if err := validateMessage(msg); err != nil {
		return nil, err
	}

	nc, err := p.enter()
	if err != nil {
		return nil, err
	}
	defer p.inflight.Done()

	spanCtx, span, header := p.tel.startPublish(ctx, opRequest, msg)
	req := msg.natsMsg(header)
	req.Reply = ""

	resp, err := nc.RequestMsgWithContext(spanCtx, req)
	p.tel.endPublish(spanCtx, span, opRequest, err)

	if err != nil {
		return nil, fmt.Errorf("nats: request to %q: %w", msg.Subject, err)
	}

	return messageFromNATS(resp), nil
}

// Shutdown refuses new calls, waits for the ones in flight and flushes the
// connection, until ctx is done; then it returns ctx.Err(). Without a
// deadline the flush is bounded by the Connection's Config.DrainTimeout.
// The Connection is not closed. Safe without Start. Idempotent. Implements
// lifecycle.Resource.
func (p *Publisher) Shutdown(ctx context.Context) error {
	return p.releaser.Shutdown(ctx) //nolint:wrapcheck // the caller's context error or nil
}

// enter registers a call in flight and returns the connection, or the
// reason to refuse it.
func (p *Publisher) enter() (*natsgo.Conn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, wrap(ErrClosed)
	}

	nc, err := p.conn.conn()
	if err != nil {
		return nil, wrap(err)
	}

	p.inflight.Add(1)

	return nc, nil
}

// jetStream is enter for JetStream: it returns the JetStream context of the
// connection, made once.
func (p *Publisher) jetStream() (jetstream.JetStream, error) {
	nc, err := p.enter()
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.jsConn != nc {
		js, err := jetstream.New(nc)
		if err != nil {
			p.inflight.Done()

			return nil, wrap(err)
		}

		p.js, p.jsConn = js, nc
	}

	return p.js, nil
}

// shutdown is the release of Shutdown.
func (p *Publisher) shutdown(ctx context.Context) error {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()

	idle := make(chan struct{})

	go func() {
		p.inflight.Wait()
		close(idle)
	}()

	select {
	case <-idle:
	case <-ctx.Done():
		return ctx.Err() //nolint:wrapcheck // the caller's own context error
	}

	nc, err := p.conn.conn()
	if err != nil {
		return nil //nolint:nilerr // nothing to flush: never started, or the Connection is closed
	}

	if _, ok := ctx.Deadline(); ok {
		err = nc.FlushWithContext(ctx)
	} else {
		err = nc.FlushTimeout(p.conn.cfg.DrainTimeout)
	}

	if ctx.Err() != nil {
		return ctx.Err() //nolint:wrapcheck // the caller's own context error
	}

	if err != nil {
		p.logger.WarnContext(ctx, "nats publisher flush failed", slog.Any("error", err))
	}

	return nil
}

// Compile-time checks.
var (
	_ lifecycle.Resource = (*Publisher)(nil)
	_ lifecycle.Namer    = (*Publisher)(nil)
)
