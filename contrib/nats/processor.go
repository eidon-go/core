package nats

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/eidon-go/core/consume"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/retry"
)

// checkArgs validates the arguments shared by NewSubscriber and NewConsumer.
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

// processor runs the handler of a Subscriber or a Consumer on one message,
// with the attempts the ErrorHandler and the redelivery policy allow (see
// consume.Processor), in the message's process span.
type processor struct {
	proc       consume.Processor[*Message]
	redelivery retry.Policy
	tel        *telemetry
	sub        *subscription
}

func newProcessor(handler Handler, onError ErrorHandler, o *options, tel *telemetry, sub *subscription) *processor {
	p := &processor{redelivery: o.redelivery(), tel: tel, sub: sub} //nolint:exhaustruct_v5 // proc below
	p.proc = consume.Processor[*Message]{
		Handle:  handler.Handle,
		Observe: p.observe,
		OnError: func(ctx context.Context, msg *Message, err error) consume.Action {
			return onError(ctx, msg, err).consume()
		},
		Redelivery: p.redelivery,
		Transient:  o.transient(),
		Exhausted:  ErrRetriesExhausted,
		Logger:     o.logger,
		Name:       "nats",
	}

	return p
}

// process handles msg (see consume.Processor.Process); logAttrs locate it in
// the logs.
func (p *processor) process(stop, work context.Context, msg *Message, logAttrs []slog.Attr) consume.Result {
	p.tel.delivered(work, p.sub)

	return p.proc.Process(stop, work, msg, consume.Delivery{Attrs: logAttrs}) //nolint:exhaustruct_v5 // no deadline
}

// observe runs one attempt in the message's process span.
func (p *processor) observe(work context.Context, msg *Message, call func(context.Context) error) error {
	ctx, span := p.tel.startProcess(work, p.sub, msg)
	start := time.Now()

	err := call(trace.ContextWithSpan(ctx, span))
	p.tel.endProcess(ctx, span, p.sub, start, err)

	return err
}

// consume maps the action to the processor's; an unknown one stops.
func (a ErrorAction) consume() consume.Action {
	switch a {
	case ErrorActionRetry:
		return consume.Retry
	case ErrorActionRequeue:
		return consume.Requeue
	case ErrorActionReject:
		return consume.Reject
	case ErrorActionStop:
		return consume.Stop
	default:
		return consume.Stop
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
