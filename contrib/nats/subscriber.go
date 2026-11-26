package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"

	natsgo "github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"

	"github.com/eidon-go/core/lifecycle"
)

// SubscriberConfig describes a core NATS subscription.
type SubscriberConfig struct {
	// Subject to subscribe to; wildcards allowed ("orders.*", "orders.>").
	// Required.
	Subject string `json:"subject" yaml:"subject"`

	// Queue is the queue group: the subscribers of one group share the
	// messages, each message goes to one of them. Empty: every subscriber
	// gets every message.
	Queue string `json:"queue" yaml:"queue"`

	// Concurrency is the number of messages handled at once. Above 1 the
	// order of the messages is lost. Default: 1.
	Concurrency int `json:"concurrency" yaml:"concurrency"`

	// PendingMessages and PendingBytes limit the messages the client keeps
	// for the handlers; beyond them it drops messages (a slow consumer,
	// logged). Negative: no limit. Defaults: nats.go's, 512Ki messages and
	// 64 MiB. The client also holds at most the connection's sync queue
	// length of messages — 64Ki unless WithConnOptions(nats.SyncQueueLen(n))
	// raises it — so that is the effective message limit above it.
	PendingMessages int `json:"pendingMessages" yaml:"pendingMessages"`
	PendingBytes    int `json:"pendingBytes"    yaml:"pendingBytes"`
}

// Validate reports every invalid field at once. Each error wraps
// ErrInvalidConfig.
func (c SubscriberConfig) Validate() error {
	var errs []error

	if c.Subject == "" {
		errs = append(errs, fmt.Errorf("%w: subject must not be empty", ErrInvalidConfig))
	} else if strings.ContainsAny(c.Subject, " \t\r\n") {
		errs = append(errs, fmt.Errorf("%w: subject %q contains white space", ErrInvalidConfig, c.Subject))
	}

	if strings.ContainsAny(c.Queue, " \t\r\n") {
		errs = append(errs, fmt.Errorf("%w: queue %q contains white space", ErrInvalidConfig, c.Queue))
	}

	if c.Concurrency < 0 {
		errs = append(errs, fmt.Errorf("%w: concurrency must not be negative", ErrInvalidConfig))
	}

	return errors.Join(errs...)
}

// Subscriber is a core NATS subscriber: every message on its subject goes
// to a Handler, with Concurrency workers. Core NATS delivers at most once —
// nothing is stored or redelivered; use a Consumer (JetStream) for
// messages that must not be lost. A handler answers a request with
// Message.Respond. It is a Runner, Healthchecker and Namer, built on
// lifecycle.Worker:
//
//   - Run subscribes and handles messages until the stop — ctx cancelled or
//     Shutdown called. The subscription is then drained: the server sends
//     no more, the messages the client already holds are handled, the
//     messages in flight finish on a context that only Shutdown's deadline
//     cancels — and Run returns nil. While the client reconnects the
//     subscription waits; nats.go re-creates it.
//   - Shutdown bounds that stop; at its deadline the handlers' context is
//     cancelled and the messages not handled yet are lost.
//   - Healthcheck reports whether Run is subscribed.
//
// A message whose handler fails goes to the ErrorHandler (WithErrorHandler,
// default DefaultErrorHandler): retry it in place (WithRedeliveryPolicy),
// drop it (Requeue and Reject: core NATS cannot redeliver) or stop.
type Subscriber struct {
	conn        *Connection
	cfg         SubscriberConfig
	name        string
	logger      *slog.Logger
	tel         *telemetry
	proc        *processor
	concurrency int

	runner *runner
}

// NewSubscriber validates cfg and builds a subscriber of conn for handler;
// it does no I/O.
func NewSubscriber(conn *Connection, cfg SubscriberConfig, handler Handler, opts ...Option) (*Subscriber, error) {
	if conn == nil {
		return nil, fmt.Errorf("%w: connection is nil", ErrInvalidConfig)
	}

	if lifecycle.IsNil(handler) {
		return nil, fmt.Errorf("%w: handler is nil", ErrInvalidConfig)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	o := newOptions(opts)
	tel := newTelemetry(&o)

	sub := &subscription{name: cfg.Subject} //nolint:exhaustruct_v5 // attrs below
	if cfg.Queue != "" {
		sub.attrs = []attribute.KeyValue{semconv.MessagingConsumerGroupName(cfg.Queue)}
	}

	s := &Subscriber{ //nolint:exhaustruct_v5 // worker is set below, state starts at zero
		conn:   conn,
		cfg:    cfg,
		name:   o.name,
		logger: o.logger,
		tel:    tel,
		proc: &processor{
			handler:      handler,
			errorHandler: o.errorHandler,
			redelivery:   o.redelivery(),
			logger:       o.logger,
			tel:          tel,
			sub:          sub,
		},
		concurrency: max(cfg.Concurrency, 1),
	}
	s.runner = newRunner(s.loop, s.logger, wrap(ErrNotRunning))

	return s, nil
}

// Name returns the component name (WithName). Implements lifecycle.Namer.
func (s *Subscriber) Name() string { return s.name }

// Run subscribes and handles messages until the subscriber is stopped —
// ctx cancelled or Shutdown called — and the messages held and in flight
// are handled, then returns nil. It returns an error wrapping ErrStopped
// when the ErrorHandler stops the subscriber, ErrNotStarted when the
// Connection is not started, ErrClosed when it is shut down while Run
// runs, ErrAlreadyStarted when called twice, and nil at once after
// Shutdown. Implements lifecycle.Runner.
func (s *Subscriber) Run(ctx context.Context) error {
	return s.runner.run(ctx)
}

// Shutdown stops Run, if it runs, waiting for the messages held and in
// flight until ctx is done; then the handlers' context is cancelled and
// Shutdown returns ctx.Err(). The Connection is not closed. Safe without
// Run. Idempotent. Implements lifecycle.Resource.
func (s *Subscriber) Shutdown(ctx context.Context) error {
	return s.runner.shutdown(ctx)
}

// Healthcheck returns nil while Run is subscribed and ErrNotRunning
// otherwise. A subscription waiting for the client to reconnect counts as
// subscribed: the Connection's Healthcheck reports that. Implements
// lifecycle.Healthchecker.
func (s *Subscriber) Healthcheck(ctx context.Context) error {
	return s.runner.healthcheck(ctx)
}

// loop is the worker's loop.
func (s *Subscriber) loop(stop, work context.Context) error {
	nc, err := s.conn.conn()
	if err != nil {
		return wrap(err)
	}

	sub, err := nc.QueueSubscribeSync(s.cfg.Subject, s.cfg.Queue)
	if err != nil {
		if errors.Is(err, natsgo.ErrConnectionClosed) {
			return wrap(ErrClosed)
		}

		return fmt.Errorf("nats: subscribe to %q: %w", s.cfg.Subject, err)
	}

	defer func() { _ = sub.Unsubscribe() }()

	if s.cfg.PendingMessages != 0 || s.cfg.PendingBytes != 0 {
		msgs, bytes := natsgo.DefaultSubPendingMsgsLimit, natsgo.DefaultSubPendingBytesLimit
		if s.cfg.PendingMessages != 0 {
			msgs = s.cfg.PendingMessages
		}

		if s.cfg.PendingBytes != 0 {
			bytes = s.cfg.PendingBytes
		}

		if err := sub.SetPendingLimits(msgs, bytes); err != nil {
			return fmt.Errorf("nats: set pending limits: %w", err)
		}
	}

	defer s.runner.markSubscribed()()

	s.logger.InfoContext(stop, "nats subscribed",
		slog.String("subject", s.cfg.Subject), slog.String("queue", s.cfg.Queue))

	return s.consume(stop, work, nc, sub)
}

// consume runs the workers until the stop or a stopping error; on the stop
// the subscription is drained, on an error it is dropped.
func (s *Subscriber) consume(stop, work context.Context, nc *natsgo.Conn, sub *natsgo.Subscription) error {
	end, cancelEnd := context.WithCancel(stop)
	defer cancelEnd()

	var (
		wg      sync.WaitGroup
		errOnce sync.Once
		stopErr error
		failed  atomic.Bool
	)

	for range s.concurrency {
		wg.Go(func() {
			if err := s.work(stop, end, work, nc, sub); err != nil {
				errOnce.Do(func() { stopErr = err })
				failed.Store(true)
			}

			cancelEnd()
		})
	}

	<-end.Done()

	// The server stops sending; on a stop the messages the client holds are
	// still handled, after a failure they are dropped.
	if failed.Load() || stop.Err() == nil {
		_ = sub.Unsubscribe()
	} else if err := sub.Drain(); err != nil && !errors.Is(err, natsgo.ErrConnectionClosed) {
		s.logger.WarnContext(work, "nats subscription drain failed", slog.Any("error", err))

		_ = sub.Unsubscribe()
	}

	wg.Wait()

	return stopErr
}

// work is one worker: it handles messages until the end — after the stop,
// until the subscription is drained. It returns an error to stop the
// subscriber, or when the subscription ended under it.
func (s *Subscriber) work(stop, end, work context.Context, nc *natsgo.Conn, sub *natsgo.Subscription) error {
	for {
		// Until the end the wait for a message ends with it; after the stop
		// the messages left are taken until the subscription is drained.
		ctx := end
		if stop.Err() != nil {
			ctx = work
		}

		raw, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			if again, err := s.receiveFailed(stop, end, work, nc, err); !again {
				return err
			}

			continue
		}

		if err := s.handle(end, work, nc, raw); err != nil {
			return err
		}
	}
}

// receiveFailed decides what a worker does after a failed wait for a
// message: wait again (true), or end with the error — nil when the
// subscription has ended with the stop, ErrClosed when it has ended under a
// running subscriber because the Connection was shut down or closed.
func (s *Subscriber) receiveFailed(stop, end, work context.Context, nc *natsgo.Conn, err error) (bool, error) {
	ended := errors.Is(err, natsgo.ErrBadSubscription) || errors.Is(err, natsgo.ErrConnectionClosed)

	switch {
	case errors.Is(err, natsgo.ErrSlowConsumer):
		return true, nil // logged by the Connection's async error handler
	case ended && end.Err() == nil:
		return false, s.endedUnder(nc, err)
	case ended, work.Err() != nil:
		return false, nil //nolint:nilerr // the subscription has ended with the stop: not an error
	case end.Err() != nil:
		// The wait ended with the end: after the stop drain what is left,
		// after a stopping error take nothing more.
		return stop.Err() != nil, nil //nolint:nilerr // the end is not an error
	default:
		return false, fmt.Errorf("nats: receive on %q: %w", s.cfg.Subject, err)
	}
}

// endedUnder returns the error of a subscription that ended under a running
// subscriber: ErrClosed when the Connection was shut down (its drain removes
// the subscription) or closed.
func (s *Subscriber) endedUnder(nc *natsgo.Conn, err error) error {
	if _, connErr := s.conn.conn(); connErr != nil || nc.IsClosed() || nc.IsDraining() {
		return wrap(ErrClosed)
	}

	return fmt.Errorf("nats: subscription to %q ended: %w", s.cfg.Subject, err)
}

// handle handles one message. It returns an error only to stop the
// subscriber.
func (s *Subscriber) handle(end, work context.Context, nc *natsgo.Conn, raw *natsgo.Msg) error {
	msg := messageFromNATS(raw)
	msg.respond = s.responder(nc)

	attrs := []slog.Attr{slog.String("subject", raw.Subject), slog.String("queue", s.cfg.Queue)}

	out, err := s.proc.process(end, work, msg, attrs)
	if out == outcomeStop {
		return fmt.Errorf("%w: subject %q: %w", ErrStopped, s.cfg.Subject, err)
	}

	return nil
}

// responder returns the function Message.Respond calls.
func (s *Subscriber) responder(nc *natsgo.Conn) func(ctx context.Context, resp *Message) error {
	return func(ctx context.Context, resp *Message) error {
		spanCtx, span, header := s.tel.startPublish(ctx, opRespond, resp)
		err := nc.PublishMsg(resp.natsMsg(header))
		s.tel.endPublish(spanCtx, span, opRespond, err)

		return wrap(err)
	}
}

// Compile-time checks.
var (
	_ lifecycle.Runner        = (*Subscriber)(nil)
	_ lifecycle.Healthchecker = (*Subscriber)(nil)
	_ lifecycle.Namer         = (*Subscriber)(nil)
)
