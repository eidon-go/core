package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

// TopologyMode is how a Consumer treats the stream (WithStream) and its
// durable consumer on the server when it subscribes.
type TopologyMode int

const (
	// TopologyCreateIfMissing (the default) creates the stream and the
	// consumer when they do not exist and leaves existing ones as they are
	// (a consumer whose configuration differs is logged at Warn). Instances
	// of two versions in a rolling update do not overwrite each other's
	// configuration; change an existing one with TopologyCreateOrUpdate, or
	// with the nats CLI, Terraform or NACK.
	TopologyCreateIfMissing TopologyMode = iota

	// TopologyCreateOrUpdate creates them, or updates them to this
	// instance's configuration, on every subscription.
	TopologyCreateOrUpdate

	// TopologyVerify creates and changes nothing: the stream and the
	// consumer must exist — managed outside the service — and the
	// consumer's AckPolicy, FilterSubjects, MaxDeliver and AckWait must
	// match the ConsumerConfig (those it sets); otherwise Run fails with an
	// error wrapping ErrTopology. The service needs no rights to manage
	// streams and consumers.
	TopologyVerify
)

// String returns the mode's name.
func (m TopologyMode) String() string {
	switch m {
	case TopologyCreateIfMissing:
		return "create-if-missing"
	case TopologyCreateOrUpdate:
		return "create-or-update"
	case TopologyVerify:
		return "verify"
	default:
		return fmt.Sprintf("topology(%d)", int(m))
	}
}

// WithTopology sets how the Consumer treats the stream and its consumer on
// the server. Default: TopologyCreateIfMissing. Consumer only.
func WithTopology(mode TopologyMode) Option {
	return func(o *options) { o.topology = mode }
}

// ensureStream makes sure the stream of WithStream exists, as the mode
// says; without WithStream the stream is not touched.
func (c *Consumer) ensureStream(ctx context.Context, js jetstream.JetStream) error {
	if c.stream == nil {
		return nil
	}

	var err error

	switch c.topology { //nolint:exhaustive // TopologyCreateIfMissing (and an unknown mode) is the default
	case TopologyCreateOrUpdate:
		_, err = js.CreateOrUpdateStream(ctx, *c.stream)
	case TopologyVerify:
		_, err = js.Stream(ctx, c.stream.Name)
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return fmt.Errorf("%w: stream %q does not exist", ErrTopology, c.stream.Name)
		}
	default:
		_, err = js.Stream(ctx, c.stream.Name)
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			_, err = js.CreateStream(ctx, *c.stream)
			if errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
				err = nil // created by another instance meanwhile
			}
		}
	}

	if err != nil {
		return closedOr(fmt.Errorf("nats: stream %q: %w", c.stream.Name, err))
	}

	return nil
}

// ensureConsumer returns the durable consumer, created or checked as the
// mode says.
//
//nolint:ireturn // jetstream's consumer interface
func (c *Consumer) ensureConsumer(ctx context.Context, js jetstream.JetStream) (jetstream.Consumer, error) {
	var (
		cons jetstream.Consumer
		err  error
	)

	switch c.topology { //nolint:exhaustive // TopologyCreateIfMissing (and an unknown mode) is the default
	case TopologyCreateOrUpdate:
		cons, err = js.CreateOrUpdateConsumer(ctx, c.cfg.Stream, c.jsConfig)
	case TopologyVerify:
		cons, err = js.Consumer(ctx, c.cfg.Stream, c.cfg.Durable)
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			return nil, fmt.Errorf("%w: consumer %q on stream %q does not exist",
				ErrTopology, c.cfg.Durable, c.cfg.Stream)
		}

		if err == nil {
			if diffs := consumerDiffs(cons.CachedInfo().Config, c.jsConfig); len(diffs) > 0 {
				return nil, fmt.Errorf("%w: consumer %q on stream %q: %s",
					ErrTopology, c.cfg.Durable, c.cfg.Stream, strings.Join(diffs, ", "))
			}
		}
	default:
		cons, err = c.consumerIfMissing(ctx, js)
	}

	if err != nil {
		return nil, closedOr(fmt.Errorf("nats: consumer %q on stream %q: %w", c.cfg.Durable, c.cfg.Stream, err))
	}

	return cons, nil
}

// consumerIfMissing returns the existing consumer — logging a configuration
// that differs — or creates it.
//
//nolint:ireturn // jetstream's consumer interface
func (c *Consumer) consumerIfMissing(ctx context.Context, js jetstream.JetStream) (jetstream.Consumer, error) {
	cons, err := js.Consumer(ctx, c.cfg.Stream, c.cfg.Durable)
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		cons, err = js.CreateConsumer(ctx, c.cfg.Stream, c.jsConfig)
		if errors.Is(err, jetstream.ErrConsumerExists) {
			cons, err = js.Consumer(ctx, c.cfg.Stream, c.cfg.Durable) // created by another instance meanwhile
		}

		return cons, err //nolint:wrapcheck // wrapped by ensureConsumer
	}

	if err != nil {
		return nil, err //nolint:wrapcheck // wrapped by ensureConsumer
	}

	if diffs := consumerDiffs(cons.CachedInfo().Config, c.jsConfig); len(diffs) > 0 {
		c.logger.WarnContext(ctx, "nats consumer exists with another configuration, left as it is",
			slog.String("stream", c.cfg.Stream), slog.String("durable", c.cfg.Durable),
			slog.String("differences", strings.Join(diffs, ", ")))
	}

	return cons, nil
}

// consumerDiffs lists how have differs from want in the fields a
// ConsumerConfig sets: the ack policy and filter subjects always, AckWait and
// MaxDeliver when want sets them (else the server's defaults apply).
func consumerDiffs(have, want jetstream.ConsumerConfig) []string {
	var diffs []string

	if have.AckPolicy != want.AckPolicy {
		diffs = append(diffs, fmt.Sprintf("ack policy %s, want %s", have.AckPolicy, want.AckPolicy))
	}

	if haveF, wantF := filters(have), filters(want); !slices.Equal(haveF, wantF) {
		diffs = append(diffs, fmt.Sprintf("filter subjects %v, want %v", haveF, wantF))
	}

	return append(diffs, deliveryDiffs(have, want)...)
}

// deliveryDiffs describes how the delivery settings of have differ from the
// ones want sets.
func deliveryDiffs(have, want jetstream.ConsumerConfig) []string {
	var diffs []string

	if want.AckWait > 0 && have.AckWait != want.AckWait {
		diffs = append(diffs, fmt.Sprintf("ack wait %s, want %s", have.AckWait, want.AckWait))
	}

	if want.MaxDeliver != 0 && have.MaxDeliver != want.MaxDeliver {
		diffs = append(diffs, fmt.Sprintf("max deliver %d, want %d", have.MaxDeliver, want.MaxDeliver))
	}

	if want.MaxAckPending != 0 && have.MaxAckPending != want.MaxAckPending {
		diffs = append(diffs, fmt.Sprintf("max ack pending %d, want %d", have.MaxAckPending, want.MaxAckPending))
	}

	if len(want.BackOff) > 0 && !slices.Equal(have.BackOff, want.BackOff) {
		diffs = append(diffs, fmt.Sprintf("backoff %v, want %v", have.BackOff, want.BackOff))
	}

	return diffs
}

// filters returns the filter subjects of cfg, sorted.
func filters(cfg jetstream.ConsumerConfig) []string {
	out := slices.Clone(cfg.FilterSubjects)
	if cfg.FilterSubject != "" {
		out = append(out, cfg.FilterSubject)
	}

	slices.Sort(out)

	return out
}
