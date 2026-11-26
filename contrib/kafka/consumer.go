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

	"github.com/eidon-go/core/consume"
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
// A message whose handler fails goes to the ErrorHandler given to
// NewConsumer: retry it in place (WithRedeliveryPolicy), skip it, or stop
// the consumer. Offsets are committed every few seconds,
// when partitions are revoked and when the consumer stops; a message is
// committed only once it and every earlier message of its partition are
// handled or skipped.
type Consumer struct {
	cfg     ConsumerConfig
	name    string
	logger  *slog.Logger
	connect retry.Policy
	proc    consume.Processor[record]
	tracer  *kotel.Tracer
	maxPoll int

	// budget and resume belong to the loop's goroutine: the time one poll's
	// records may take, and where a record cut off by it resumes.
	budget time.Duration
	resume resumePoint

	lazy     *lazyClient
	worker   *lifecycle.Worker
	releaser *lifecycle.Releaser
}

// NewConsumer validates cfg and builds a consumer for handler; it does no
// network I/O (the group is joined in Start). onError decides what happens to
// a message whose handler failed. There is no default, because every choice
// has a cost: DeadLetter keeps the consumer going and the message in a
// dead-letter topic, StopOnFailure stops the consumer — and with app.App the
// whole service, which then fails again on the same message after a restart.
func NewConsumer(cfg ConsumerConfig, handler Handler, onError ErrorHandler, opts ...Option) (*Consumer, error) {
	if lifecycle.IsNil(handler) {
		return nil, fmt.Errorf("%w: handler is nil", ErrInvalidConfig)
	}

	if onError == nil {
		return nil, fmt.Errorf("%w: error handler is nil", ErrInvalidConfig)
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

	c := &Consumer{ //nolint:exhaustruct_v5 // proc, lazy, worker and releaser are set below
		cfg:     cfg,
		name:    o.name,
		logger:  o.logger,
		connect: o.connectPolicy(),
		tracer:  tracer,
		maxPoll: maxPoll,
	}
	c.proc = newProcessor(handler, onError, &o, c.observe)

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
	c.budget = pollBudget(client)
	errStreak := 0

	for {
		fetches := client.PollRecords(stop, c.maxPoll)
		deadline := time.Now().Add(c.budget)

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

		err := c.handleFetches(stop, work, client, fetches, deadline)

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

// defaultRebalanceTimeout is franz-go's kgo.RebalanceTimeout.
const defaultRebalanceTimeout = time.Minute

// pollBudget is the time the records of one poll may take: three quarters
// of the group's rebalance timeout, since a rebalance waits for them
// (BlockRebalanceOnPoll) and a member that does not rejoin within the
// timeout is kicked out of the group.
func pollBudget(client *kgo.Client) time.Duration {
	timeout, _ := client.OptValue(kgo.RebalanceTimeout).(time.Duration)
	if timeout <= 0 {
		timeout = defaultRebalanceTimeout
	}

	return timeout * 3 / 4 //nolint:mnd // three quarters
}

// errYield ends the handling of a poll early: its budget ran out.
var errYield = errors.New("kafka: poll budget exhausted")

// resumePoint is the record whose retries a yield cut off, and its last
// attempt: when the record comes again its attempts go on from there
// instead of starting over.
type resumePoint struct {
	topic     string
	partition int32
	offset    int64
	attempt   int
}

// handleFetches handles the polled records in order until one stops the
// consumer, the stop begins or the poll's budget runs out; then the records
// not handled are fetched again (see yield).
func (c *Consumer) handleFetches(
	stop, work context.Context, client *kgo.Client, fetches kgo.Fetches, deadline time.Time,
) error {
	for iter := fetches.RecordIter(); !iter.Done(); {
		if stop.Err() != nil {
			return nil //nolint:nilerr // the stop is not an error
		}

		r := iter.Next()

		err := errYield
		if time.Now().Before(deadline) {
			err = c.handleRecord(stop, work, client, r, deadline)
		}

		if errors.Is(err, errYield) {
			c.yield(work, client, r, iter)

			return nil
		}

		if err != nil {
			return err
		}
	}

	return nil
}

// yield rewinds every partition of the poll to its first record not handled
// — r and the ones iter still holds — so that the next polls fetch them
// again, after the rebalance waiting for this poll has run. Nothing past
// them is marked, so no commit skips them.
func (c *Consumer) yield(ctx context.Context, client *kgo.Client, r *kgo.Record, iter *kgo.FetchesRecordIter) {
	rewind := map[string]map[int32]kgo.EpochOffset{}
	add := func(r *kgo.Record) {
		if rewind[r.Topic] == nil {
			rewind[r.Topic] = map[int32]kgo.EpochOffset{}
		}

		if _, ok := rewind[r.Topic][r.Partition]; !ok { // the first is the earliest
			rewind[r.Topic][r.Partition] = kgo.EpochOffset{Epoch: r.LeaderEpoch, Offset: r.Offset}
		}
	}

	for add(r); !iter.Done(); {
		add(iter.Next())
	}

	client.SetOffsets(rewind)

	c.logger.WarnContext(ctx, "kafka poll ran out of its budget, the rest is fetched again",
		slog.Duration("budget", c.budget), slog.String("topic", r.Topic),
		slog.Int("partition", int(r.Partition)), slog.Int64("offset", r.Offset))
}

// handleRecord handles one record, with the attempts the ErrorHandler and
// the redelivery policy allow (see consume.Processor), and marks it for
// commit once it is handled or skipped. A stop during a wait leaves it
// unmarked; a wait past the poll's deadline returns errYield, and the
// attempts go on when the record comes again.
func (c *Consumer) handleRecord(
	stop, work context.Context, client *kgo.Client, r *kgo.Record, deadline time.Time,
) error {
	res := c.proc.Process(stop, work, record{r: r, msg: messageFromRecord(r)}, consume.Delivery{
		Attrs: []slog.Attr{
			slog.String("topic", r.Topic), slog.Int("partition", int(r.Partition)), slog.Int64("offset", r.Offset),
		},
		Attempts: c.resumeAttempt(r),
		Deadline: deadline,
	})

	switch res.Outcome {
	case consume.Handled, consume.Rejected:
		client.MarkCommitRecords(r)
	case consume.Yielded:
		c.resume = resumePoint{topic: r.Topic, partition: r.Partition, offset: r.Offset, attempt: res.Attempts}

		return errYield
	case consume.Abandoned: // the stop: unmarked, it comes again after the restart
	case consume.Stopped, consume.Requeued: // kafka has no requeue
		return fmt.Errorf("%w: %s[%d] offset %d: %w", ErrStopped, r.Topic, r.Partition, r.Offset, res.Err)
	}

	return nil
}

// resumeAttempt returns the attempts r already had before a yield cut them
// off, 0 for none. A resume point of r's partition is used up either way: a
// record at another offset means r's partition moved on.
func (c *Consumer) resumeAttempt(r *kgo.Record) int {
	p := c.resume
	if p.topic != r.Topic || p.partition != r.Partition {
		return 0
	}

	c.resume = resumePoint{} //nolint:exhaustruct_v5 // cleared

	if p.offset != r.Offset {
		return 0
	}

	return p.attempt
}

// observe runs one attempt in the record's process span.
func (c *Consumer) observe(work context.Context, rec record, call func(context.Context) error) error {
	_, span := c.tracer.WithProcessSpan(rec.r)
	defer span.End()

	err := call(trace.ContextWithSpan(work, span))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
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
