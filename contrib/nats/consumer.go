package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"

	"github.com/eidon-go/core/consume"
	"github.com/eidon-go/core/lifecycle"
)

// ConsumerConfig describes a durable JetStream pull consumer. The Consumer
// creates it on its stream when it is missing (WithTopology for other
// modes); WithConsumerConfig sets what is not here.
type ConsumerConfig struct {
	// Stream is the name of the stream to consume. It must exist, unless
	// WithStream creates it. Required.
	Stream string `json:"stream" yaml:"stream"`

	// Durable is the name of the durable consumer: the instances of a
	// service that use the same name share its messages, each message goes
	// to one of them. Required.
	Durable string `json:"durable" yaml:"durable"`

	// FilterSubjects limit the consumer to the messages of these subjects
	// (wildcards allowed). Empty: every message of the stream.
	FilterSubjects []string `json:"filterSubjects" yaml:"filterSubjects"`

	// AckWait is how long the server waits for the ack of a delivered
	// message before it delivers it again. While the Consumer handles a
	// message — its attempts and the redelivery delays between them — it
	// tells the server every AckWait/2 that the message is in progress, so
	// AckWait only matters for an instance that died or lost its
	// connection. A BackOff set with WithConsumerConfig replaces it per
	// delivery; the in-progress interval is then half its shortest entry.
	// Default: the server's, 30s.
	AckWait time.Duration `json:"ackWait" yaml:"ackWait"`

	// MaxDeliver is how many times a message is delivered at most; then the
	// server gives up on it (a MAX_DELIVERIES advisory). Negative: no limit.
	// Default: the server's, no limit.
	MaxDeliver int `json:"maxDeliver" yaml:"maxDeliver"`

	// MaxAckPending is how many delivered messages may wait for their ack;
	// the server delivers no more until some are acked. It bounds the
	// in-flight messages of every instance together, so keep it at least
	// (Concurrency + Prefetch) × instances. Negative: no limit. Default:
	// the server's, 1000.
	MaxAckPending int `json:"maxAckPending" yaml:"maxAckPending"`

	// Concurrency is the number of messages handled at once. Above 1 the
	// order of the messages is lost. Default: 1.
	Concurrency int `json:"concurrency" yaml:"concurrency"`

	// Prefetch is the number of messages the client pulls ahead and holds
	// for the handlers. Default: Concurrency.
	Prefetch int `json:"prefetch" yaml:"prefetch"`

	// MaxProcessingTime bounds the handling of one message — its attempts
	// and the redelivery delays between them. Past it the handler's context
	// is cancelled, the consumer stops telling the server the message is in
	// progress, and the message is naked (or, if the handler ignores its
	// context, delivered again once its AckWait expires), so a hung handler
	// cannot hold a message forever: MaxDeliver still applies. Default: 10 ×
	// AckWait (30s when unset). Negative: no limit.
	MaxProcessingTime time.Duration `json:"maxProcessingTime" yaml:"maxProcessingTime"`
}

// Validate reports every invalid field at once. Each error wraps
// ErrInvalidConfig.
func (c ConsumerConfig) Validate() error {
	var errs []error

	errs = append(errs, validateName("stream", c.Stream)...)
	errs = append(errs, validateName("durable", c.Durable)...)

	for i, s := range c.FilterSubjects {
		if s == "" || strings.ContainsAny(s, " \t\r\n") {
			errs = append(errs, fmt.Errorf("%w: filterSubjects[%d] %q is not a subject", ErrInvalidConfig, i, s))
		}
	}

	if c.AckWait < 0 {
		errs = append(errs, fmt.Errorf("%w: ackWait must not be negative", ErrInvalidConfig))
	}

	if c.Concurrency < 0 {
		errs = append(errs, fmt.Errorf("%w: concurrency must not be negative", ErrInvalidConfig))
	}

	if c.Prefetch < 0 {
		errs = append(errs, fmt.Errorf("%w: prefetch must not be negative", ErrInvalidConfig))
	}

	return errors.Join(errs...)
}

// defaultProcessingFactor × the ack deadline is the default
// MaxProcessingTime.
const defaultProcessingFactor = 10

// processingLimit returns the bound of one message's handling, 0 for none.
func (c ConsumerConfig) processingLimit(ackDeadline time.Duration) time.Duration {
	switch {
	case c.MaxProcessingTime < 0:
		return 0
	case c.MaxProcessingTime == 0:
		return defaultProcessingFactor * ackDeadline
	default:
		return c.MaxProcessingTime
	}
}

// validateName checks a stream or consumer name: required, and none of the
// characters JetStream forbids in names.
func validateName(field, name string) []error {
	switch {
	case name == "":
		return []error{fmt.Errorf("%w: %s must not be empty", ErrInvalidConfig, field)}
	case strings.ContainsAny(name, " \t\r\n.*>/\\"):
		return []error{
			fmt.Errorf("%w: %s %q contains a character not allowed in a name", ErrInvalidConfig, field, name),
		}
	default:
		return nil
	}
}

// jetStreamConfig builds the consumer configuration.
func (c ConsumerConfig) jetStreamConfig(changes []func(*jetstream.ConsumerConfig)) jetstream.ConsumerConfig {
	cfg := jetstream.ConsumerConfig{ //nolint:exhaustruct_v5 // the server's defaults
		Durable:       c.Durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       c.AckWait,
		MaxDeliver:    c.MaxDeliver,
		MaxAckPending: c.MaxAckPending,
	}

	if c.MaxDeliver < 0 {
		cfg.MaxDeliver = -1
	}

	if c.MaxAckPending < 0 {
		cfg.MaxAckPending = -1
	}

	if len(c.FilterSubjects) == 1 {
		cfg.FilterSubject = c.FilterSubjects[0]
	} else if len(c.FilterSubjects) > 1 {
		cfg.FilterSubjects = append([]string(nil), c.FilterSubjects...)
	}

	for _, change := range changes {
		change(&cfg)
	}

	return cfg
}

// Consumer is a JetStream consumer: every message of its durable consumer
// goes to a Handler, with Concurrency workers; a handled message is acked,
// a failed one is retried, redelivered later, terminated or stops the
// consumer, as the ErrorHandler given to NewConsumer decides (Discard,
// StopOnFailure).
// Delivery is at least once: a handler must cope with a message it has
// seen. It is a Runner, Healthchecker and Namer, built on lifecycle.Worker:
//
//   - Run creates the stream (WithStream) and the durable consumer, or
//     updates them, then pulls and handles messages until the stop — ctx
//     cancelled or Shutdown called. The client then pulls no more, the
//     messages in flight finish on a context that only Shutdown's deadline
//     cancels and are acked, the prefetched ones are naked (delivered again
//     at once, to another instance) — and Run returns nil. If the
//     subscription fails (the consumer was deleted, the stream is gone), Run
//     subscribes again with a backoff. While the client reconnects the
//     consumer waits: nats.go resumes the pulls after a short outage, Run
//     subscribes again after a longer one (missed heartbeats).
//   - Shutdown bounds that stop; at its deadline the handlers' context is
//     cancelled and the messages not acked are naked.
//   - Healthcheck reports whether Run is subscribed.
//
// Register it after the Connection and the Publisher its handler uses.
type Consumer struct {
	conn     *Connection
	cfg      ConsumerConfig
	jsConfig jetstream.ConsumerConfig
	stream   *jetstream.StreamConfig
	topology TopologyMode
	name     string
	logger   *slog.Logger
	proc     *processor
	workers  int
	prefetch int
	// progressEvery is the interval of the in-progress acks while a
	// message is handled: half its AckWait.
	progressEvery time.Duration
	// maxProcessing bounds the handling of one message; 0 for no bound.
	maxProcessing time.Duration

	// paused is set while an ErrorActionPause holds the consumer
	// unsubscribed; resume wakes the loop (Resume).
	paused atomic.Bool
	resume chan struct{}

	runner *runner
}

// NewConsumer validates cfg and builds a consumer of conn for handler; it
// does no I/O. onError decides what happens to a message whose handler
// failed. There is no default, because every choice has a cost: Discard keeps
// the consumer going and terminates the message, StopOnFailure stops the
// consumer — and with app.App the whole service, which then meets the same
// message again after a restart.
func NewConsumer(
	conn *Connection,
	cfg ConsumerConfig,
	handler Handler,
	onError ErrorHandler,
	opts ...Option,
) (*Consumer, error) {
	if err := checkArgs(conn, handler, onError); err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	o := newOptions(opts)

	if o.stream != nil && o.stream.Name != cfg.Stream {
		return nil, fmt.Errorf("%w: WithStream names stream %q, the consumer consumes %q",
			ErrInvalidConfig, o.stream.Name, cfg.Stream)
	}

	cfg.FilterSubjects = append([]string(nil), cfg.FilterSubjects...)
	workers := max(cfg.Concurrency, 1)

	prefetch := cfg.Prefetch
	if prefetch == 0 {
		prefetch = workers
	}

	jsConfig := cfg.jetStreamConfig(o.consumerConfig)

	c := &Consumer{ //nolint:exhaustruct_v5 // worker is set below, state starts at zero
		conn:     conn,
		cfg:      cfg,
		jsConfig: jsConfig,
		stream:   o.stream,
		topology: o.topology,
		name:     o.name,
		logger:   o.logger,
		proc: newProcessor(handler, onError, &o, newTelemetry(&o), &subscription{
			name: cfg.Stream,
			attrs: []attribute.KeyValue{
				semconv.MessagingConsumerGroupName(cfg.Durable),
				semconv.MessagingDestinationSubscriptionName(cfg.Durable),
			},
		}),
		workers:       workers,
		prefetch:      prefetch,
		progressEvery: ackDeadline(jsConfig) / 2, //nolint:mnd // half of it
		maxProcessing: cfg.processingLimit(ackDeadline(jsConfig)),
		resume:        make(chan struct{}, 1),
	}
	c.runner = newRunner(c.loop, c.logger, ErrNotConnected)

	return c, nil
}

// Name returns the component name (WithName). Implements lifecycle.Namer.
func (c *Consumer) Name() string { return c.name }

// Run consumes until the consumer is stopped — ctx cancelled or Shutdown
// called — and the messages in flight are handled, then returns nil. It
// returns an error wrapping ErrStopped when the ErrorHandler stops the
// consumer, ErrNotStarted when the Connection is not started, ErrClosed
// when it is shut down while Run consumes, ErrAlreadyStarted when called
// twice, and nil at once after Shutdown. Implements lifecycle.Runner.
func (c *Consumer) Run(ctx context.Context) error {
	return c.runner.run(ctx)
}

// Shutdown stops Run, if it runs, waiting for the messages in flight until
// ctx is done; then the handlers' context is cancelled, the messages not
// acked are naked, and Shutdown returns ctx.Err(). The Connection is not
// closed. Safe without Run. Idempotent. Implements lifecycle.Resource.
func (c *Consumer) Shutdown(ctx context.Context) error {
	return c.runner.shutdown(ctx)
}

// Healthcheck returns nil while Run is subscribed, ErrNotConnected while it
// creates its consumer or subscribes again, ErrPaused while an
// ErrorActionPause holds it unsubscribed, and ErrNotRunning when Run is not
// running. Implements lifecycle.Healthchecker.
func (c *Consumer) Healthcheck(ctx context.Context) error {
	if c.paused.Load() {
		return ErrPaused
	}

	return c.runner.healthcheck(ctx)
}

// Resume subscribes again a consumer that an ErrorActionPause paused; the
// paused message is delivered again. Without a pause it does nothing.
func (c *Consumer) Resume() {
	if !c.paused.Load() {
		return
	}

	select {
	case c.resume <- struct{}{}:
	default: // already signalled
	}
}

// stableSession is how long a subscription must have lasted for its loss
// to be followed by an immediate resubscription; a shorter one counts as a
// failure, so that a subscription that keeps ending at once is retried with
// a backoff, not in a hot loop.
const stableSession = 5 * time.Second

// defaultAckWait is the server's AckWait when the consumer sets none.
const defaultAckWait = 30 * time.Second

// ackDeadline returns the shortest time the server gives a delivery to be
// acked: the smallest BackOff entry when BackOff is set (it replaces
// AckWait, per delivery), AckWait otherwise.
func ackDeadline(cfg jetstream.ConsumerConfig) time.Duration {
	if len(cfg.BackOff) > 0 && slices.Min(cfg.BackOff) > 0 {
		return slices.Min(cfg.BackOff)
	}

	if cfg.AckWait > 0 {
		return cfg.AckWait
	}

	return defaultAckWait
}

// loop is the worker's loop: it subscribes again after every failed or lost
// subscription until the stop.
func (c *Consumer) loop(stop, work context.Context) error {
	failures := 0

	for {
		began := time.Now()
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
		case subscribed && time.Since(began) >= stableSession:
			failures = 0

			c.logger.WarnContext(stop, "nats consumer subscription lost, subscribing again",
				slog.String("stream", c.cfg.Stream), slog.String("durable", c.cfg.Durable), slog.Any("error", err))

			continue
		}

		failures++
		c.logger.ErrorContext(stop, "nats consumer subscribe failed",
			slog.String("stream", c.cfg.Stream), slog.String("durable", c.cfg.Durable),
			slog.Int("attempt", failures), slog.Any("error", err))

		if !sleep(stop, resubscribePolicy.Delay(failures)) {
			return nil
		}
	}
}

// errPaused ends a session on an ErrorActionPause.
var errPaused = errors.New("nats: consumer paused")

// waitResume holds the consumer unsubscribed until Resume (true) or the
// stop (false).
func (c *Consumer) waitResume(stop context.Context, cause error) bool {
	select { // a Resume of an earlier pause does not end this one
	case <-c.resume:
	default:
	}

	c.paused.Store(true)
	defer c.paused.Store(false)

	c.logger.ErrorContext(stop, "nats consumer paused, waiting for Resume",
		slog.String("stream", c.cfg.Stream), slog.String("durable", c.cfg.Durable), slog.Any("error", cause))

	select {
	case <-c.resume:
		c.logger.InfoContext(stop, "nats consumer resumed",
			slog.String("stream", c.cfg.Stream), slog.String("durable", c.cfg.Durable))

		return true
	case <-stop.Done():
		return false
	}
}

// session creates the stream and the consumer, then handles its messages
// until the stop, the end of the subscription or a stopping handler error.
// It reports whether the subscription was made.
func (c *Consumer) session(stop, work context.Context) (bool, error) {
	nc, err := c.conn.conn()
	if err != nil {
		return false, wrap(err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return false, wrap(err)
	}

	if err := c.ensureStream(stop, js); err != nil {
		return false, err
	}

	cons, err := c.ensureConsumer(stop, js)
	if err != nil {
		return false, err
	}

	iter, err := cons.Messages(jetstream.PullMaxMessages(c.prefetch))
	if err != nil {
		return false, closedOr(fmt.Errorf("nats: consume %q: %w", c.cfg.Durable, err))
	}

	defer iter.Stop()

	defer c.runner.markSubscribed()()

	c.logger.InfoContext(stop, "nats consumer subscribed",
		slog.String("stream", c.cfg.Stream), slog.String("durable", c.cfg.Durable))

	return true, c.consume(stop, work, iter)
}

// closedOr returns ErrClosed for an error caused by a closed connection,
// err otherwise.
func closedOr(err error) error {
	if errors.Is(err, natsgo.ErrConnectionClosed) || errors.Is(err, jetstream.ErrConnectionClosed) {
		return wrap(ErrClosed)
	}

	return err
}

// consume runs the fetcher and the workers until the stop, the end of the
// subscription or a stopping handler error; then the client stops pulling,
// the prefetched messages are naked and the messages in flight finish.
func (c *Consumer) consume(stop, work context.Context, iter jetstream.MessagesContext) error {
	end, cancelEnd := context.WithCancel(stop)
	defer cancelEnd()

	deliveries := make(chan jetstream.Msg)
	fetched := make(chan error, 1)

	go func() {
		err := c.fetch(end, iter, deliveries)
		if err != nil {
			cancelEnd()
		}

		fetched <- err
	}()

	var (
		wg      sync.WaitGroup
		errOnce sync.Once
		stopErr error
	)

	for range c.workers {
		wg.Go(func() {
			if err := c.work(end, work, deliveries); err != nil {
				errOnce.Do(func() { stopErr = err })
				cancelEnd()
			}
		})
	}

	<-end.Done()

	// The client pulls no more; the fetcher hands the messages it holds
	// back (nak) until Next reports the end.
	iter.Drain()
	wg.Wait()

	var fetchErr error

	select {
	case fetchErr = <-fetched:
	case <-work.Done():
		// Cut off: the fetcher is abandoned; it naks what it still gets
		// and ends once the iterator closes.
	}

	if stopErr != nil {
		return stopErr
	}

	return fetchErr
}

// fetch takes the messages from the iterator and hands them to the
// workers; once end is done it naks them. It returns when the iterator
// ends: nil after the stop, the error of the subscription otherwise.
func (c *Consumer) fetch(end context.Context, iter jetstream.MessagesContext, deliveries chan<- jetstream.Msg) error {
	defer close(deliveries)

	for {
		jsMsg, err := iter.Next()
		if err != nil {
			switch {
			case end.Err() != nil:
				return nil
			case errors.Is(err, jetstream.ErrConnectionClosed), errors.Is(err, natsgo.ErrConnectionClosed):
				return wrap(ErrClosed)
			default:
				// The Connection's Shutdown drains the connection, which ends
				// the iterator with a plain ErrMsgIteratorClosed.
				if _, connErr := c.conn.conn(); connErr != nil {
					return wrap(ErrClosed)
				}

				return fmt.Errorf("nats: consume %q: %w", c.cfg.Durable, err)
			}
		}

		if end.Err() != nil {
			c.settle(end, jsMsg, "nak", jsMsg.Nak())

			continue
		}

		select {
		case deliveries <- jsMsg:
		case <-end.Done():
			c.settle(end, jsMsg, "nak", jsMsg.Nak())
		}
	}
}

// work is one worker: it handles messages until end is done or the fetcher
// ends. It returns an error only to stop the consumer.
func (c *Consumer) work(end, work context.Context, deliveries <-chan jetstream.Msg) error {
	for {
		select {
		case <-end.Done():
			return nil
		case jsMsg, ok := <-deliveries:
			if !ok {
				return nil
			}

			// A message taken together with the end is not handled.
			if end.Err() != nil {
				c.settle(work, jsMsg, "nak", jsMsg.Nak())

				return nil //nolint:nilerr // the end is not an error
			}

			if err := c.handle(end, work, jsMsg); err != nil {
				return err
			}
		}
	}
}

// handle handles one message and settles it. It returns an error only to
// stop the consumer.
func (c *Consumer) handle(end, work context.Context, jsMsg jetstream.Msg) error {
	msg := messageFromJetStream(jsMsg)

	attrs := []slog.Attr{
		slog.String("stream", c.cfg.Stream), slog.String("durable", c.cfg.Durable),
		slog.String("subject", msg.Subject),
	}

	delivered := uint64(1)
	if meta := msg.Metadata; meta != nil {
		delivered = meta.NumDelivered
		attrs = append(attrs, slog.Uint64("stream_sequence", meta.Sequence.Stream),
			slog.Uint64("delivered", meta.NumDelivered))
	}

	msgCtx, cancel := c.processingContext(work)
	defer cancel()

	stopProgress := c.keepInProgress(msgCtx, jsMsg)
	res := c.proc.process(end, msgCtx, msg, attrs)

	stopProgress()

	if res.Outcome == consume.Abandoned && msgCtx.Err() != nil && work.Err() == nil {
		c.logger.LogAttrs(work, slog.LevelWarn, "nats message processing exceeded MaxProcessingTime, nak",
			append(attrs, slog.Duration("max_processing_time", c.maxProcessing))...)
	}

	return c.settleOutcome(work, jsMsg, res, delivered)
}

// settleOutcome settles a handled message according to res. It returns an
// error only to stop (ErrStopped) or pause (errPaused) the consumer.
func (c *Consumer) settleOutcome(
	work context.Context, jsMsg jetstream.Msg, res consume.Result, delivered uint64,
) error {
	switch res.Outcome {
	case consume.Handled:
		c.settle(work, jsMsg, "ack", jsMsg.Ack())
	case consume.Requeued:
		delay := c.proc.redelivery.Delay(int(min(delivered, math.MaxInt32)))
		c.settle(work, jsMsg, "nak", jsMsg.NakWithDelay(delay))
	case consume.Rejected:
		c.settle(work, jsMsg, "term", jsMsg.Term())
	case consume.Abandoned, consume.Yielded:
		c.settle(work, jsMsg, "nak", jsMsg.Nak())
	case consume.Stopped:
		c.settle(work, jsMsg, "nak", jsMsg.Nak())

		return fmt.Errorf("%w: stream %q, consumer %q: %w", ErrStopped, c.cfg.Stream, c.cfg.Durable, res.Err)
	case consume.Paused:
		c.settle(work, jsMsg, "nak", jsMsg.Nak())

		return fmt.Errorf("%w: %w", errPaused, res.Err)
	}

	return nil
}

// processingContext derives the context of one message's handling from
// work, bounded by MaxProcessingTime.
func (c *Consumer) processingContext(work context.Context) (context.Context, context.CancelFunc) {
	if c.maxProcessing <= 0 {
		return context.WithCancel(work)
	}

	return context.WithTimeout(work, c.maxProcessing)
}

// keepInProgress tells the server every AckWait/2 that the message is in
// progress, so that its AckWait does not expire while it is handled —
// attempts and redelivery delays included — until the returned function is
// called (it waits for the last one) or work is cancelled.
func (c *Consumer) keepInProgress(work context.Context, jsMsg jetstream.Msg) func() {
	done := make(chan struct{})

	var wg sync.WaitGroup

	wg.Go(func() {
		ticker := time.NewTicker(c.progressEvery)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-work.Done():
				return
			case <-ticker.C:
				if err := jsMsg.InProgress(); err != nil {
					c.settle(work, jsMsg, "in-progress", err)
				}
			}
		}
	})

	return func() {
		close(done)
		wg.Wait()
	}
}

// settle logs a failed ack, nak or term: the server delivers the message
// again once its AckWait expires.
func (c *Consumer) settle(ctx context.Context, jsMsg jetstream.Msg, what string, err error) {
	if err == nil || errors.Is(err, jetstream.ErrMsgAlreadyAckd) {
		return
	}

	c.logger.WarnContext(ctx, "nats "+what+" failed, the message will be delivered again",
		slog.String("stream", c.cfg.Stream), slog.String("durable", c.cfg.Durable),
		slog.String("subject", jsMsg.Subject()), slog.Any("error", err))
}

// Compile-time checks.
var (
	_ lifecycle.Runner        = (*Consumer)(nil)
	_ lifecycle.Healthchecker = (*Consumer)(nil)
	_ lifecycle.Namer         = (*Consumer)(nil)
)
