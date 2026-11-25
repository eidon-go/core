package kafka

import (
	"context"
	"errors"
	"strconv"

	"github.com/eidon-go/core/retry"
)

// Headers DeadLetter adds to a dead-lettered message.
const (
	HeaderDLTOriginalTopic     = "dlt-original-topic"
	HeaderDLTOriginalPartition = "dlt-original-partition"
	HeaderDLTOriginalOffset    = "dlt-original-offset"
	HeaderDLTError             = "dlt-error"
)

// base10 formats the partition and offset headers.
const base10 = 10

// DefaultErrorHandler is the ErrorHandler of a Consumer without
// WithErrorHandler. It retries an error (see WithRedeliveryPolicy) and stops
// the consumer once the attempts are exhausted or for an error marked with
// retry.Permanent: no message is ever dropped silently. Use DeadLetter, or
// an ErrorHandler of your own, to go on past a message that cannot be
// handled.
func DefaultErrorHandler(_ context.Context, _ *Message, err error) ErrorAction {
	if final(err) {
		return ErrorActionStop
	}

	return ErrorActionRetry
}

// DeadLetter returns an ErrorHandler that retries an error like
// DefaultErrorHandler, but once the attempts are exhausted — or at once for
// an error marked with retry.Permanent (a message that cannot be decoded) —
// publishes the message to topic and skips it. The dead letter keeps the
// key, value and headers and adds HeaderDLTOriginalTopic,
// HeaderDLTOriginalPartition, HeaderDLTOriginalOffset and HeaderDLTError. If
// the publish fails, the consumer stops (the message is not lost).
func DeadLetter(publisher Publisher, topic string) ErrorHandler {
	return func(ctx context.Context, msg *Message, err error) ErrorAction {
		if !final(err) {
			return ErrorActionRetry
		}

		headers := make([]Header, 0, len(msg.Headers)+4) //nolint:mnd // the four headers below
		headers = append(headers, msg.Headers...)
		headers = append(headers,
			Header{Key: HeaderDLTOriginalTopic, Value: []byte(msg.Topic)},
			Header{Key: HeaderDLTOriginalPartition, Value: strconv.AppendInt(nil, int64(msg.Partition), base10)},
			Header{Key: HeaderDLTOriginalOffset, Value: strconv.AppendInt(nil, msg.Offset, base10)},
			Header{Key: HeaderDLTError, Value: []byte(err.Error())},
		)

		dead := &Message{ //nolint:exhaustruct_v5 // partition and offset are set by the broker
			Topic:     topic,
			Key:       msg.Key,
			Value:     msg.Value,
			Headers:   headers,
			Timestamp: msg.Timestamp,
		}

		if publisher.Publish(ctx, dead) != nil {
			return ErrorActionStop
		}

		return ErrorActionSkip
	}
}

// final reports whether err allows no more attempts.
func final(err error) bool {
	return errors.Is(err, ErrRetriesExhausted) || errors.Is(err, retry.ErrPermanent)
}
