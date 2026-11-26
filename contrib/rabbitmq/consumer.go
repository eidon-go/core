package rabbitmq

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/trace"

	"github.com/eidon-go/core/consume"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/retry"
)

// maxPrefetch is the largest prefetch count basic.qos carries.
const maxPrefetch = 65535

// ConsumerConfig describes what a Consumer consumes.
type ConsumerConfig struct {
	// Queue to consume from. Required.
	Queue string `json:"queue" yaml:"queue"`

	// Topology is declared before every subscription — at the start and
	// after every reconnect — or only checked (WithTopologyMode). Empty: the
	// queue must exist.
	Topology Topology `json:"topology" yaml:"topology"`

	// Concurrency is the number of messages handled at the same time, each
	// by its own worker goroutine. Above 1, messages are no longer handled
	// in queue order. Default: 1.
	Concurrency int `json:"concurrency" yaml:"concurrency"`

	// Prefetch is the number of messages the broker delivers ahead of the
	// acks (basic.qos). Default: Concurrency. A larger value hides network
	// latency; on stop the prefetched messages are requeued. At most 65535
	// (an AMQP short), the default included.
	Prefetch int `json:"prefetch" yaml:"prefetch"`

	// Exclusive asks to be the queue's only consumer.
	Exclusive bool `json:"exclusive" yaml:"exclusive"`

	// Tag is the consumer tag prefix shown in the management UI; a random
	// suffix makes it unique per subscription. Default: the component name.
	Tag string `json:"tag" yaml:"tag"`

	// Args are the optional arguments of basic.consume ("x-priority", …).
	Args map[string]any `json:"args" yaml:"args"`
}

// Validate reports every invalid field at once. Each error wraps
// ErrInvalidConfig.
func (c ConsumerConfig) Validate() error {
	var errs []error

	if strings.TrimSpace(c.Queue) == "" {
		errs = append(errs, fmt.Errorf("%w: queue must not be empty", ErrInvalidConfig))
	}

	if c.Concurrency < 0 {
		errs = append(errs, fmt.Errorf("%w: concurrency must not be negative", ErrInvalidConfig))
	}

	if c.Prefetch < 0 {
		errs = append(errs, fmt.Errorf("%w: prefetch must not be negative", ErrInvalidConfig))
	}

	// basic.qos carries the prefetch as a 16-bit count: a larger one fails
	// on every subscription, so reject it here, the default included.
	if prefetch := cmp.Or(c.Prefetch, c.Concurrency); prefetch > maxPrefetch {
		errs = append(errs, fmt.Errorf("%w: prefetch %d (or concurrency, its default) is above %d",
			ErrInvalidConfig, prefetch, maxPrefetch))
	}

	if err := c.Topology.Validate(); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

// Consumer consumes a queue and hands every message to a Handler; a handled
// message is acked. Delivery is at least once — a message whose ack is lost
// comes again — so handlers must be idempotent.
//
// It is a Runner, Healthchecker and Namer; it has no Start: its Connection
// connects.
//
//   - Run subscribes — declares the Topology, sets the prefetch, consumes —
//     and handles messages. When the connection or the channel is lost, Run
//     waits for the connection and subscribes again. When it is stopped,
//     it cancels the subscription (the broker stops delivering), the
//     messages in flight finish on a context that only Shutdown's deadline
//     cancels and are acked, the channel is closed — the broker requeues
//     the prefetched messages — and Run returns nil.
//   - Shutdown bounds that stop; at its deadline the handlers' context is
//     cancelled, and the unacked messages are requeued when the channel
//     closes.
//   - Healthcheck reports whether Run is subscribed.
//
// A message whose handler fails goes to the ErrorHandler given to
// NewConsumer: retry it in place (WithRedeliveryPolicy), requeue it, reject
// it to the dead-letter exchange, or stop the consumer.
type Consumer struct {
	conn        *Connection
	cfg         ConsumerConfig
	name        string
	logger      *slog.Logger
	redelivery  retry.Policy
	proc        consume.Processor[*Message]
	tel         *telemetry
	concurrency int
	prefetch    int
	tagPrefix   string
	// topologyMode is WithTopologyMode.
	topologyMode TopologyMode

	worker     *lifecycle.Worker
	subscribed atomic.Bool

	// paused is set while an ErrorActionPause holds the consumer
	// unsubscribed; resume wakes the loop (Resume).
	paused atomic.Bool
	resume chan struct{}
}

// NewConsumer validates cfg and builds a consumer of conn for handler; it
// does no network I/O. onError decides what happens to a message whose
// handler failed. There is no default, because every choice has a cost:
// DeadLetter keeps the consumer going and moves the message to the queue's
// dead-letter exchange, StopOnFailure stops the consumer — and with app.App
// the whole service, which then meets the same message again after a
// restart.
func NewConsumer(conn *Connection, cfg ConsumerConfig, handler Handler, onError ErrorHandler,
	opts ...Option,
) (*Consumer, error) {
	if err := checkArgs(conn, handler, onError); err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	cfg.Topology = cfg.Topology.clone()
	cfg.Args = maps.Clone(cfg.Args)

	concurrency := max(cfg.Concurrency, 1)

	prefetch := cfg.Prefetch
	if prefetch == 0 {
		prefetch = concurrency
	}

	o := newOptions(opts)

	tagPrefix := cfg.Tag
	if tagPrefix == "" {
		tagPrefix = o.name
	}

	c := &Consumer{ //nolint:exhaustruct_v5 // proc and worker are set below, state starts at zero
		conn:         conn,
		cfg:          cfg,
		name:         o.name,
		logger:       o.logger,
		redelivery:   o.redelivery(),
		tel:          newTelemetry(&o),
		concurrency:  concurrency,
		prefetch:     prefetch,
		tagPrefix:    tagPrefix,
		topologyMode: o.topologyMode,
		resume:       make(chan struct{}, 1),
	}
	c.proc = consume.Processor[*Message]{
		Handle:  handler.Handle,
		Observe: c.observe,
		OnError: func(ctx context.Context, msg *Message, err error) consume.Action {
			return onError(ctx, msg, err).consume()
		},
		Redelivery: c.redelivery,
		Transient:  o.transient(),
		Exhausted:  ErrRetriesExhausted,
		Logger:     o.logger,
		Name:       "rabbitmq",
	}
	c.worker = lifecycle.NewWorker(lifecycle.WorkerFuncs{Run: c.loop, Logger: c.logger})

	return c, nil
}

// Name returns the component name (WithName). Implements lifecycle.Namer.
func (c *Consumer) Name() string { return c.name }

// checkArgs validates the arguments of NewConsumer other than its config.
func checkArgs(conn *Connection, handler Handler, onError ErrorHandler) error {
	switch {
	case conn == nil:
		return fmt.Errorf("%w: connection is nil", ErrInvalidConfig)
	case lifecycle.IsNil(handler):
		return fmt.Errorf("%w: handler is nil", ErrInvalidConfig)
	case onError == nil:
		return fmt.Errorf("%w: error handler is nil", ErrInvalidConfig)
	default:
		return nil
	}
}

// Run consumes until the consumer is stopped — ctx cancelled or Shutdown
// called — and the messages in flight are handled, then returns nil. It
// returns an error wrapping ErrStopped when the ErrorHandler stops the
// consumer, ErrNotStarted when the Connection is not started, ErrClosed when
// it is shut down while Run consumes, ErrAlreadyStarted when called twice,
// and nil at once after Shutdown. Implements lifecycle.Runner.
func (c *Consumer) Run(ctx context.Context) error {
	err := c.worker.Run(ctx)
	if errors.Is(err, ErrStopped) || errors.Is(err, ErrClosed) || errors.Is(err, ErrNotStarted) ||
		errors.Is(err, ErrTopology) {
		return err //nolint:wrapcheck // the loop's errors carry the module prefix
	}

	return wrap(err)
}

// Shutdown stops Run, if it runs, waiting for the messages in flight until
// ctx is done; then the handlers' context is cancelled and Shutdown returns
// ctx.Err(). The channel is closed by Run once the handlers have returned —
// or, for a handler that ignores its context, with the Connection — and the
// broker requeues the unacked messages. The Connection is not closed. Safe
// without Run. Idempotent. Implements lifecycle.Resource.
func (c *Consumer) Shutdown(ctx context.Context) error {
	// Closing the channel here would race with the loop's basic.cancel:
	// amqp091 does not serialize synchronous calls on one channel.
	return wrap(c.worker.Shutdown(ctx))
}

// Healthcheck returns nil while Run is subscribed, ErrNotConnected while it
// waits for the connection or subscribes again, ErrPaused while an
// ErrorActionPause holds it unsubscribed, and ErrNotRunning when Run is not
// running. Implements lifecycle.Healthchecker.
func (c *Consumer) Healthcheck(ctx context.Context) error {
	if err := c.worker.Healthcheck(ctx); err != nil {
		return wrap(err)
	}

	if c.paused.Load() {
		return ErrPaused
	}

	if !c.subscribed.Load() {
		return ErrNotConnected
	}

	return nil
}

// Resume subscribes again a consumer that an ErrorActionPause paused.
// Without a pause it does nothing.
func (c *Consumer) Resume() {
	if !c.paused.Load() {
		return
	}

	select {
	case c.resume <- struct{}{}:
	default: // already signalled
	}
}

// loop is the worker's loop: it subscribes again after every lost channel
// until the stop.
func (c *Consumer) loop(stop, work context.Context) error {
	failures := 0

	for {
		subscribed, err := c.session(stop, work)

		switch {
		case errors.Is(err, ErrStopped):
			return err
		case stop.Err() != nil:
			return nil //nolint:nilerr // the stop is not an error
		case errors.Is(err, errPaused):
			if !c.waitResume(stop, err) {
				return nil
			}

			failures = 0

			continue
		case errors.Is(err, ErrClosed), errors.Is(err, ErrNotStarted), errors.Is(err, ErrTopology):
			return err
		case subscribed:
			failures = 0

			c.logger.WarnContext(
				stop,
				"rabbitmq subscription lost, subscribing again",
				slog.String("queue", c.cfg.Queue),
			)

			continue
		}

		failures++
		c.logger.ErrorContext(stop, "rabbitmq subscribe failed",
			slog.String("queue", c.cfg.Queue), slog.Int("attempt", failures), slog.Any("error", err))

		if !sleep(stop, resubscribePolicy.Delay(failures)) {
			return nil
		}
	}
}

// errPaused ends a session on an ErrorActionPause.
var errPaused = errors.New("rabbitmq: consumer paused")

// waitResume holds the consumer unsubscribed until Resume (true) or the
// stop (false).
func (c *Consumer) waitResume(stop context.Context, cause error) bool {
	select { // a Resume of an earlier pause does not end this one
	case <-c.resume:
	default:
	}

	c.paused.Store(true)
	defer c.paused.Store(false)

	c.logger.ErrorContext(stop, "rabbitmq consumer paused, waiting for Resume",
		slog.String("queue", c.cfg.Queue), slog.Any("error", cause))

	select {
	case <-c.resume:
		c.logger.InfoContext(stop, "rabbitmq consumer resumed", slog.String("queue", c.cfg.Queue))

		return true
	case <-stop.Done():
		return false
	}
}

// session subscribes on a new channel and handles its deliveries until the
// stop, the loss of the channel or a stopping handler error. It reports
// whether the subscription was made.
func (c *Consumer) session(stop, work context.Context) (bool, error) {
	ch, err := c.conn.channel(stop)
	if err != nil {
		return false, err
	}

	// Closing the channel requeues every message not acked.
	defer func() { _ = ch.close() }()

	tag := c.tagPrefix + "-" + randomSuffix()

	deliveries, err := c.subscribe(ch, tag)
	if err != nil {
		return false, err
	}

	c.subscribed.Store(true)
	defer c.subscribed.Store(false)

	c.logger.InfoContext(stop, "rabbitmq consumer subscribed",
		slog.String("queue", c.cfg.Queue), slog.String("tag", tag))

	return true, c.consume(stop, work, ch, tag, deliveries)
}

// subscribe declares the topology, sets the prefetch and consumes.
func (c *Consumer) subscribe(ch amqpChannel, tag string) (<-chan amqp.Delivery, error) {
	if err := c.cfg.Topology.apply(ch, c.topologyMode); err != nil {
		return nil, err
	}

	if err := ch.qos(c.prefetch); err != nil {
		return nil, fmt.Errorf("rabbitmq: set prefetch: %w", err)
	}

	deliveries, err := ch.consume(c.cfg.Queue, tag, c.cfg.Exclusive, toTable(c.cfg.Args, 0))
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: consume %q: %w", c.cfg.Queue, err)
	}

	return deliveries, nil
}

// consume runs the workers on deliveries until the stop, the end of
// deliveries (the channel closed) or a stopping handler error; then it
// cancels the subscription and waits for the messages in flight.
func (c *Consumer) consume(
	stop, work context.Context,
	ch amqpChannel,
	tag string,
	deliveries <-chan amqp.Delivery,
) error {
	end, cancelEnd := context.WithCancel(stop)
	defer cancelEnd()

	var (
		wg      sync.WaitGroup
		errOnce sync.Once
		stopErr error
	)

	for range c.concurrency {
		wg.Go(func() {
			if err := c.work(end, work, deliveries); err != nil {
				errOnce.Do(func() { stopErr = err })
			}

			cancelEnd()
		})
	}

	<-end.Done()

	// The broker stops delivering; the deliveries it sent meanwhile are
	// requeued when the channel closes.
	if !ch.isClosed() {
		if err := ch.cancel(tag); err != nil && !errors.Is(err, amqp.ErrClosed) {
			c.logger.WarnContext(work, "rabbitmq consumer cancel failed", slog.Any("error", err))
		}
	}

	wg.Wait()

	return stopErr
}

// work is one worker: it handles deliveries until end is done or deliveries
// is closed (the channel closed). It returns an error only to stop the
// consumer.
func (c *Consumer) work(end, work context.Context, deliveries <-chan amqp.Delivery) error {
	for {
		select {
		case <-end.Done():
			return nil
		case d, ok := <-deliveries:
			// A delivery taken together with the end is not handled: it is
			// requeued when the channel closes.
			if !ok || end.Err() != nil {
				return nil //nolint:nilerr // the end is not an error
			}

			if err := c.handle(end, work, &d); err != nil {
				return err
			}
		}
	}
}

// handle handles one delivery, with the attempts the ErrorHandler and the
// redelivery policy allow (see consume.Processor), and acks, requeues or
// rejects it. A stop during a wait leaves it unacked (requeued), and so does
// an attempt cut off by Shutdown's deadline: the ErrorHandler is not asked
// about it, since the message did not fail — it would otherwise be rejected
// (dead-lettered) or stop the consumer. It returns an error only to stop the
// consumer.
func (c *Consumer) handle(stop, work context.Context, d *amqp.Delivery) error {
	msg := messageFromDelivery(d)
	c.tel.delivered(work, c.cfg.Queue, msg)

	// Logged with every failure; never the body or the headers, which may
	// carry personal data or secrets.
	attrs := []slog.Attr{
		slog.String("queue", c.cfg.Queue), slog.String("exchange", d.Exchange),
		slog.String("routing_key", d.RoutingKey), slog.String("message_id", d.MessageId),
		slog.Uint64("delivery_tag", d.DeliveryTag), slog.Bool("redelivered", d.Redelivered),
	}

	res := c.proc.Process(stop, work, msg, consume.Delivery{Attrs: attrs}) //nolint:exhaustruct_v5 // no deadline

	switch res.Outcome {
	case consume.Handled:
		c.settle(work, d, actionAck)
	case consume.Requeued:
		if c.waitRequeue(stop, work, d, res.Err) {
			c.settle(work, d, ErrorActionRequeue)
		} // else stopped: requeued when the channel closes
	case consume.Rejected:
		c.settle(work, d, ErrorActionReject)
	case consume.Stopped:
		return fmt.Errorf("%w: queue %q: %w", ErrStopped, c.cfg.Queue, res.Err)
	case consume.Paused: // unacked: requeued when the channel closes
		return fmt.Errorf("%w: queue %q: %w", errPaused, c.cfg.Queue, res.Err)
	case consume.Abandoned, consume.Yielded: // unacked: requeued when the channel closes
	}

	return nil
}

// actionAck is the settlement of a handled message.
const actionAck ErrorAction = -1

// settle acks (actionAck), requeues or rejects a delivery. A failure is
// logged: the channel is gone, and the broker requeues the message.
func (c *Consumer) settle(ctx context.Context, d *amqp.Delivery, action ErrorAction) {
	var (
		err  error
		what string
	)

	switch action { //nolint:exhaustive // the other actions do not settle
	case ErrorActionRequeue:
		err, what = d.Nack(false, true), "requeue"
	case ErrorActionReject:
		err, what = d.Nack(false, false), "reject"
	default:
		err, what = d.Ack(false), "ack"
	}

	if err != nil {
		c.logger.WarnContext(ctx, "rabbitmq "+what+" failed, the message will be redelivered",
			slog.String("queue", c.cfg.Queue), slog.Uint64("delivery_tag", d.DeliveryTag), slog.Any("error", err))
	}
}

// waitRequeue waits, before a requeue, for the redelivery delay of the
// message's delivery number, so that the broker does not hand a failing
// message straight back in a hot loop. The worker holds the message
// meanwhile. It reports false if the stop began first.
func (c *Consumer) waitRequeue(stop, work context.Context, d *amqp.Delivery, err error) bool {
	n := deliveryNumber(d)
	delay := c.redelivery.Delay(n)

	if c.redelivery.OnRetry != nil {
		c.redelivery.OnRetry(work, retry.Attempt{Number: n, Err: err, Delay: delay})
	}

	return sleep(stop, delay) && stop.Err() == nil
}

// deliveryNumber is which delivery of its message d is, counting from 1. A
// quorum queue counts the earlier ones in the x-delivery-count header; other
// queues only flag a redelivery, which then counts as the second.
func deliveryNumber(d *amqp.Delivery) int {
	switch n := d.Headers[headerDeliveryCount].(type) {
	case int64:
		return int(n) + 1
	case int32:
		return int(n) + 1
	case int:
		return n + 1
	}

	if d.Redelivered {
		return 2 //nolint:mnd // the second delivery at least
	}

	return 1
}

// headerDeliveryCount is the header in which a quorum queue counts the
// earlier deliveries of a message.
const headerDeliveryCount = "x-delivery-count"

// observe runs one attempt in the message's process span.
func (c *Consumer) observe(work context.Context, msg *Message, call func(context.Context) error) error {
	ctx, span := c.tel.startProcess(work, c.cfg.Queue, msg)
	start := time.Now()

	err := call(trace.ContextWithSpan(ctx, span))
	c.tel.endProcess(ctx, span, c.cfg.Queue, msg, start, err)

	return err
}

// consume maps the action to the processor's; an unknown one stops.
func (a ErrorAction) consume() consume.Action {
	switch a { //nolint:exhaustive // actionAck is internal
	case ErrorActionRetry:
		return consume.Retry
	case ErrorActionRequeue:
		return consume.Requeue
	case ErrorActionReject:
		return consume.Reject
	case ErrorActionStop:
		return consume.Stop
	case ErrorActionPause:
		return consume.Pause
	default:
		return consume.Stop
	}
}

// randomSuffix makes consumer tags unique.
func randomSuffix() string {
	b := make([]byte, 6) //nolint:mnd // 12 hex digits
	_, _ = rand.Read(b)

	return hex.EncodeToString(b)
}

// sleep waits for d unless ctx is done first; it reports whether it waited.
func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// Compile-time checks.
var (
	_ lifecycle.Runner        = (*Consumer)(nil)
	_ lifecycle.Healthchecker = (*Consumer)(nil)
	_ lifecycle.Namer         = (*Consumer)(nil)
)
