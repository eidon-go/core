package kafka

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"unicode/utf8"

	"github.com/eidon-go/core/retry"
)

// Headers DeadLetter adds to a dead-lettered message.
const (
	HeaderDLTOriginalTopic     = "dlt-original-topic"
	HeaderDLTOriginalPartition = "dlt-original-partition"
	HeaderDLTOriginalOffset    = "dlt-original-offset"
	// HeaderDLTOriginalTimestamp is the original message's timestamp in
	// Unix milliseconds. The dead letter itself gets the time it is
	// published, so the retention of the dead-letter topic counts from then.
	HeaderDLTOriginalTimestamp = "dlt-original-timestamp"
	// HeaderDLTError is the handler's error, cut to MaxDLTErrorLength bytes.
	HeaderDLTError = "dlt-error"
)

// MaxDLTErrorLength bounds the HeaderDLTError value: a wrapped error (a
// query, a panic's stack) could otherwise push the dead letter over the
// topic's max.message.bytes, and a dead letter that cannot be published
// stops the consumer.
const MaxDLTErrorLength = 1024

// base10 formats the partition and offset headers.
const base10 = 10

// StopOnFailure is an ErrorHandler that retries an error (see
// WithRedeliveryPolicy) and stops the consumer once the attempts are
// exhausted or for an error marked with retry.Permanent: no message is ever
// skipped. Run then returns an error, so with app.App the whole service
// stops — and after a restart it meets the same message again. Prefer
// DeadLetter in production unless a stuck partition must stop everything.
func StopOnFailure(_ context.Context, _ *Message, err error) ErrorAction {
	if final(err) {
		return ErrorActionStop
	}

	return ErrorActionRetry
}

// DeadLetter returns an ErrorHandler that retries an error like
// StopOnFailure, but once the attempts are exhausted — or at once for
// an error marked with retry.Permanent (a message that cannot be decoded) —
// publishes the message to topic and skips it. The dead letter keeps the
// key, value and headers and adds HeaderDLTOriginalTopic,
// HeaderDLTOriginalPartition, HeaderDLTOriginalOffset,
// HeaderDLTOriginalTimestamp and HeaderDLTError; its own timestamp is the
// time of publishing. If the publish fails — the dead-letter topic is a
// dependency that is down, not a fault of the message — the failure is
// logged (slog.Default) and the message's partition is paused
// (ErrorActionPause): the message is not lost, Healthcheck reports ErrPaused
// and the service keeps running. OnDLTFailure chooses another action.
func DeadLetter(publisher Publisher, topic string, opts ...DeadLetterOption) ErrorHandler {
	o := deadLetterOptions{onFailure: ErrorActionPause}

	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}

	return func(ctx context.Context, msg *Message, err error) ErrorAction {
		if !final(err) {
			return ErrorActionRetry
		}

		headers := make([]Header, 0, len(msg.Headers)+5) //nolint:mnd // the five headers below
		headers = append(headers, msg.Headers...)
		headers = append(headers,
			Header{Key: HeaderDLTOriginalTopic, Value: []byte(msg.Topic)},
			Header{Key: HeaderDLTOriginalPartition, Value: strconv.AppendInt(nil, int64(msg.Partition), base10)},
			Header{Key: HeaderDLTOriginalOffset, Value: strconv.AppendInt(nil, msg.Offset, base10)},
			Header{Key: HeaderDLTOriginalTimestamp, Value: strconv.AppendInt(nil, msg.Timestamp.UnixMilli(), base10)},
			Header{Key: HeaderDLTError, Value: []byte(truncate(err.Error(), MaxDLTErrorLength))},
		)

		dead := &Message{ //nolint:exhaustruct_v5 // partition, offset and timestamp are set on publishing
			Topic:   topic,
			Key:     msg.Key,
			Value:   msg.Value,
			Headers: headers,
		}

		if pubErr := publisher.Publish(ctx, dead); pubErr != nil {
			slog.Default().ErrorContext(ctx, "kafka dead letter publish failed",
				slog.String("topic", msg.Topic), slog.Int("partition", int(msg.Partition)),
				slog.Int64("offset", msg.Offset), slog.String("dlt", topic), slog.Any("error", pubErr))

			return o.onFailure
		}

		return ErrorActionSkip
	}
}

// DeadLetterOption configures DeadLetter.
type DeadLetterOption func(*deadLetterOptions)

type deadLetterOptions struct {
	onFailure ErrorAction
}

// OnDLTFailure sets the action DeadLetter returns when publishing the dead
// letter fails: ErrorActionPause (default) or ErrorActionStop, which stops
// the consumer (and, under app.App, the service) as before.
func OnDLTFailure(action ErrorAction) DeadLetterOption {
	return func(o *deadLetterOptions) { o.onFailure = action }
}

// truncate cuts s to at most limit bytes on a rune boundary, marking the cut
// with "...".
func truncate(s string, limit int) string {
	const ellipsis = "..."

	if len(s) <= limit {
		return s
	}

	cut := limit - len(ellipsis)
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}

	return s[:cut] + ellipsis
}

// final reports whether err allows no more attempts.
func final(err error) bool {
	return errors.Is(err, ErrRetriesExhausted) || errors.Is(err, retry.ErrPermanent)
}
