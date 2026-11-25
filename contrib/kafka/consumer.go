package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/retry"
)

// fetchErrorPolicy spaces out polls that return only errors (a broker that is
// down), so that the loop does not spin; reset by the next records.
var fetchErrorPolicy = retry.Policy{ //nolint:gochecknoglobals,exhaustruct_v5 // read-only; only Delay is used
	InitialDelay: fetchErrorInitialDelay,
	Multiplier:   backoffMultiplier,
	MaxDelay:     fetchErrorMaxDelay,
	Jitter:       backoffJitter,
}

// Consumer is a consumer group member that hands every message to a Handler,
// one at a time in partition order, and commits what was handled: delivery
// is at least once, so handlers must be idempotent.
//
// It is a Starter, Runner, Healthchecker and Namer:
//
//   - Start creates the franz-go client — the member joins the group — and
//     pings the cluster with retry.
//   - Run polls and handles messages. When it is stopped it takes no new
//     message: the one in flight finishes on a context that only Shutdown's
//     deadline cancels, the handled messages are committed, Run returns nil.
//   - Shutdown bounds that stop, then leaves the group and closes the client.
//   - Healthcheck reports whether Run is consuming.
//
// A message whose handler fails goes to the ErrorHandler (WithErrorHandler,
// default DefaultErrorHandler): retry it in place (WithRedeliveryPolicy),
// skip it, or stop the consumer. Offsets are committed every few seconds,
// when partitions are revoked and when the consumer stops; a message is
// committed only once it and every earlier message of its partition are
// handled or skipped.
type Consumer struct {
	cfg          ConsumerConfig
	handler      Handler
	name         string
	logger       *slog.Logger
	connect      retry.Policy
	redelivery   retry.Policy
	errorHandler ErrorHandler
	tracer       *kotel.Tracer
	maxPoll      int

	lazy     *lazyClient
	worker   *lifecycle.Worker
	releaser *lifecycle.Releaser
}

// NewConsumer validates cfg and builds a consumer for handler; it does no
// network I/O (the group is joined in Start).
func NewConsumer(cfg ConsumerConfig, handler Handler, opts ...Option) (*Consumer, error) {
	if lifecycle.IsNil(handler) {
		return nil, fmt.Errorf("%w: handler is nil", ErrInvalidConfig)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	cfg.Config = cfg.clone()
	cfg.Topics = slices.Clone(cfg.Topics)
	offset, _ := cfg.Offset.kgo() // validated

	maxPoll := cfg.MaxPollRecords
	if maxPoll == 0 {
		maxPoll = DefaultMaxPollRecords
	}

	o := newOptions(opts)
	tracer := o.tracer(cfg.ClientID, cfg.Group)

	c := &Consumer{ //nolint:exhaustruct_v5 // lazy, worker and releaser are set below
		cfg:          cfg,
		handler:      handler,
		name:         o.name,
		logger:       o.logger,
		connect:      o.connectPolicy(),
		redelivery:   o.redelivery(),
		errorHandler: o.errorHandler,
		tracer:       tracer,
		maxPoll:      maxPoll,
	}

	kgoOpts := o.clientOpts(cfg.Config, tracer,
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.ConsumeResetOffset(offset),
		// Commit only what was handled, every few seconds and on revoke.
		kgo.AutoCommitMarks(),
		// Rebalances wait for the polled records to be handled, so that no
		// offset is committed for a partition this member no longer owns.
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsRevoked(c.onRevoked),
	)
	// A loop cut off by Shutdown's deadline may still hold a poll: the
	// rebalance is allowed so that leaving the group does not wait for it.
	c.lazy = newLazyClient(kgoOpts, (*kgo.Client).CloseAllowingRebalance)
	c.worker = lifecycle.NewWorker(lifecycle.WorkerFuncs{Run: c.loop, Logger: c.logger})
	c.releaser = lifecycle.CloseOnce(c.lazy.close)

	return c, nil
}

// Name returns the component name (WithName). Implements lifecycle.Namer.
func (c *Consumer) Name() string { return c.name }

// Start creates the client, which joins the group, and pings the cluster
// until it answers, retried with the WithConnectRetry policy; Shutdown ends
// the retries. It returns ErrClosed after Shutdown and ErrAlreadyStarted when
// called twice. Implements lifecycle.Starter.
func (c *Consumer) Start(ctx context.Context) error {
	return c.lazy.start(ctx, c.connect)
}

// Run consumes until the consumer is stopped — ctx cancelled or Shutdown
// called — and the message in flight is handled, then commits and returns
// nil. It returns an error wrapping ErrStopped when the ErrorHandler stops
// the consumer, an error wrapping kgo.ErrMaxDecompress when a partition
// cannot be fetched any more, ErrNotStarted without Start, ErrAlreadyStarted
// when called twice, and nil at once after Shutdown. Implements
// lifecycle.Runner.
func (c *Consumer) Run(ctx context.Context) error {
	if c.lazy.get() == nil && !c.lazy.isClosed() {
		return wrap(ErrNotStarted)
	}

	err := c.worker.Run(ctx)
	if errors.Is(err, ErrStopped) || errors.Is(err, kgo.ErrMaxDecompress) {
		return err //nolint:wrapcheck // the loop's errors carry the module prefix
	}

	return wrap(err)
}

// Shutdown stops Run, if it runs, waiting for the message in flight and the
// final commit until ctx is done; then it leaves the group and closes the
// client. When ctx is done first, the handler's context is cancelled, the
// close goes on in the background and Shutdown returns ctx.Err(). It is safe
// without Start or Run. Idempotent. Implements lifecycle.Resource.
func (c *Consumer) Shutdown(ctx context.Context) error {
	stopErr := c.worker.Shutdown(ctx)
	closeErr := c.releaser.Shutdown(ctx)

	if stopErr != nil {
		return wrap(stopErr)
	}

	return wrap(closeErr)
}

// Healthcheck returns nil while Run consumes, ErrNotRunning otherwise.
// Implements lifecycle.Healthchecker.
func (c *Consumer) Healthcheck(ctx context.Context) error {
	return wrap(c.worker.Healthcheck(ctx))
}

// onRevoked commits the handled messages of the partitions being revoked
// (and of the others: commits are per group member) before another member
// takes them over.
func (c *Consumer) onRevoked(ctx context.Context, client *kgo.Client, _ map[string][]int32) {
	if err := client.CommitMarkedOffsets(ctx); err != nil {
		c.logger.WarnContext(ctx, "kafka commit on revoke failed", slog.Any("error", err))
	}
}

// loop is the worker's loop: it polls with stop and handles with work.
func (c *Consumer) loop(stop, work context.Context) error {
	client := c.lazy.get()
	errStreak := 0

	for {
		fetches := client.PollRecords(stop, c.maxPoll)

		if fetches.IsClientClosed() {
			client.AllowRebalance()

			return nil
		}

		if stop.Err() != nil {
			// Records polled together with the stop are not handled and not
			// committed: they come again after a restart from the committed
			// offset (or, with no commit yet, from the Offset of the config —
			// with OffsetNewest they are not read again).
			client.AllowRebalance()
			c.commit(work, client)

			return nil //nolint:nilerr // the stop is not an error
		}

		if err := fatalFetchError(fetches); err != nil {
			client.AllowRebalance()
			c.commit(work, client)

			return err
		}

		if c.logFetchErrors(stop, fetches) && fetches.NumRecords() == 0 {
			client.AllowRebalance()

			errStreak++
			if !sleep(stop, fetchErrorPolicy.Delay(errStreak)) {
				c.commit(work, client)

				return nil
			}

			continue
		}

		errStreak = 0

		err := c.handleFetches(stop, work, client, fetches)

		client.AllowRebalance()

		if err != nil || stop.Err() != nil {
			c.commit(work, client)

			return err
		}
	}
}

// logFetchErrors logs the errors of a poll and reports whether there were
// any; context errors of the stop are not logged.
func (c *Consumer) logFetchErrors(ctx context.Context, fetches kgo.Fetches) bool {
	found := false

	fetches.EachError(func(topic string, partition int32, err error) {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}

		found = true

		c.logger.ErrorContext(ctx, "kafka fetch failed",
			slog.String("topic", topic), slog.Int("partition", int(partition)), slog.Any("error", err))
	})

	return found
}

// fatalFetchError returns the first error after which franz-go stops
// fetching a partition for good: a batch larger than the decompression
// bound (kgo.ErrDecompressTooLarge) is never skipped, so the partition would
// stall while the consumer looks healthy.
func fatalFetchError(fetches kgo.Fetches) error {
	var fatal error

	fetches.EachError(func(topic string, partition int32, err error) {
		if fatal == nil && errors.Is(err, kgo.ErrMaxDecompress) {
			fatal = fmt.Errorf("kafka: fetch %s[%d]: %w", topic, partition, err)
		}
	})

	return fatal
}

// handleFetches handles the polled records in order until one stops the
// consumer or the stop begins.
func (c *Consumer) handleFetches(stop, work context.Context, client *kgo.Client, fetches kgo.Fetches) error {
	for iter := fetches.RecordIter(); !iter.Done(); {
		if stop.Err() != nil {
			return nil //nolint:nilerr // the stop is not an error
		}

		if err := c.handleRecord(stop, work, client, iter.Next()); err != nil {
			return err
		}
	}

	return nil
}

// handleRecord handles one record, with the attempts the ErrorHandler and
// the redelivery policy allow, and marks it for commit once it is handled
// or skipped. A stop during a redelivery delay leaves it unmarked.
func (c *Consumer) handleRecord(stop, work context.Context, client *kgo.Client, r *kgo.Record) error {
	msg := messageFromRecord(r)

	for attempt := 1; ; attempt++ {
		err := c.handleOnce(work, r, msg)
		if err == nil {
			client.MarkCommitRecords(r)

			return nil
		}

		exhausted := c.redelivery.MaxAttempts >= 0 && attempt >= max(c.redelivery.MaxAttempts, 1)
		if exhausted {
			err = errors.Join(ErrRetriesExhausted, err)
		}

		action := c.decide(work, msg, err)
		c.logFailure(work, r, attempt, action, err)

		switch {
		case action == ErrorActionSkip:
			client.MarkCommitRecords(r)

			return nil
		case action == ErrorActionRetry && !exhausted:
			if !c.waitRedelivery(stop, work, attempt, err) {
				return nil
			}
		default:
			return fmt.Errorf("%w: %s[%d] offset %d: %w", ErrStopped, r.Topic, r.Partition, r.Offset, err)
		}
	}
}

func (c *Consumer) logFailure(ctx context.Context, r *kgo.Record, attempt int, action ErrorAction, err error) {
	c.logger.ErrorContext(ctx, "kafka message handling failed",
		slog.String("topic", r.Topic), slog.Int("partition", int(r.Partition)),
		slog.Int64("offset", r.Offset), slog.Int("attempt", attempt),
		slog.String("action", action.String()), slog.Any("error", err))
}

// waitRedelivery waits for the redelivery delay after the failed attempt,
// unless the stop begins first; it reports whether a new attempt may start.
func (c *Consumer) waitRedelivery(stop, work context.Context, attempt int, err error) bool {
	delay := c.redelivery.Delay(attempt)
	if c.redelivery.OnRetry != nil {
		c.redelivery.OnRetry(work, retry.Attempt{Number: attempt, Err: err, Delay: delay})
	}

	// The stop may have begun just as the delay ended: no new attempt then.
	return sleep(stop, delay) && stop.Err() == nil
}

// handleOnce calls the handler in the record's process span; a panic is an
// error.
func (c *Consumer) handleOnce(work context.Context, r *kgo.Record, msg *Message) (err error) {
	_, span := c.tracer.WithProcessSpan(r)
	defer span.End()

	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("kafka: handler panicked: %v", p) //nolint:err113 // carries the panic value
		}

		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
	}()

	return c.handler.Handle(trace.ContextWithSpan(work, span), msg) //nolint:wrapcheck // goes to the ErrorHandler as is
}

// decide asks the ErrorHandler; a panic in it stops the consumer.
func (c *Consumer) decide(ctx context.Context, msg *Message, err error) ErrorAction {
	action := ErrorActionStop

	func() {
		defer func() {
			if p := recover(); p != nil {
				c.logger.ErrorContext(ctx, "kafka error handler panicked", slog.Any("panic", p))
			}
		}()

		action = c.errorHandler(ctx, msg, err)
	}()

	return action
}

// commit commits the handled messages with the work context; a failure is
// logged — the messages come again after the restart.
func (c *Consumer) commit(ctx context.Context, client *kgo.Client) {
	if err := client.CommitMarkedOffsets(ctx); err != nil {
		c.logger.WarnContext(ctx, "kafka commit failed", slog.Any("error", err))
	}
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
	_ lifecycle.Starter       = (*Consumer)(nil)
	_ lifecycle.Runner        = (*Consumer)(nil)
	_ lifecycle.Healthchecker = (*Consumer)(nil)
	_ lifecycle.Namer         = (*Consumer)(nil)
)
