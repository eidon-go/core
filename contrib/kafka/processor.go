package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/eidon-go/core/consume"
)

// record is what the consumer's processor handles: the message and the
// record it comes from, for the process span.
type record struct {
	r   *kgo.Record
	msg *Message
}

// newProcessor builds the processor that runs handler on every record, with
// the attempts onError and the redelivery policy allow.
func newProcessor(
	handler Handler, onError ErrorHandler, o *options,
	observe func(ctx context.Context, rec record, call func(context.Context) error) error,
) consume.Processor[record] {
	return consume.Processor[record]{
		Handle:  func(ctx context.Context, rec record) error { return handler.Handle(ctx, rec.msg) },
		Observe: observe,
		OnError: func(ctx context.Context, rec record, err error) consume.Action {
			return onError(ctx, rec.msg, err).consume()
		},
		Redelivery: o.redelivery(),
		Transient:  o.transient(),
		Exhausted:  ErrRetriesExhausted,
		Logger:     o.logger,
		Name:       "kafka",
	}
}

// consume maps the action to the processor's: Skip is Reject (the consumer
// commits the message and goes on); an unknown action stops.
func (a ErrorAction) consume() consume.Action {
	switch a {
	case ErrorActionRetry:
		return consume.Retry
	case ErrorActionSkip:
		return consume.Reject
	case ErrorActionStop:
		return consume.Stop
	default:
		return consume.Stop
	}
}
