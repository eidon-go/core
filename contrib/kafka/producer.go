package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/retry"
)

// Producer publishes messages synchronously. Register it before the
// consumers that publish while they drain: it is a plain Resource, so
// app.App shuts it down after every Run has returned.
//
// It is a Publisher, Starter, Resource, Healthchecker and Namer.
type Producer struct {
	name    string
	connect retry.Policy
	lazy    *lazyClient
	flusher *lifecycle.Releaser // flushes the buffered records, once
	closer  *lifecycle.Releaser // closes the client, once
}

// NewProducer validates cfg and prepares the producer; it does no network
// I/O: the franz-go client is created in Start.
func NewProducer(cfg Config, opts ...Option) (*Producer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	cfg = cfg.clone()
	o := newOptions(opts)
	lazy := newLazyClient(o.clientOpts(cfg, o.tracer(cfg.ClientID, "")), (*kgo.Client).Close)

	return &Producer{
		name:    o.name,
		connect: o.connectPolicy(),
		lazy:    lazy,
		flusher: lifecycle.ShutdownOnce(func(ctx context.Context) error {
			if client := lazy.get(); client != nil {
				return client.Flush(ctx)
			}

			return nil
		}),
		closer: lifecycle.CloseOnce(lazy.close),
	}, nil
}

// Publish sends msgs and waits until every one is acknowledged; it returns
// the first error. Each message needs a Topic. The trace context of ctx goes
// into the record headers. It returns ErrNotStarted before Start.
//
// Cancelling ctx ends the wait for records that have not been sent yet; a
// record already in flight to the broker (the producer is idempotent) is
// waited for until the broker answers or the client's delivery timeout
// passes, so Publish may return after ctx is done. An error does not mean
// that the broker rejected the record: it may have been written, and a
// retry may duplicate it. Implements Publisher.
func (p *Producer) Publish(ctx context.Context, msgs ...*Message) error {
	client := p.lazy.get()
	if client == nil {
		return fmt.Errorf("kafka: publish: %w", ErrNotStarted)
	}

	records := make([]*kgo.Record, 0, len(msgs))

	for i, m := range msgs {
		if m == nil {
			return fmt.Errorf("kafka: publish: message %d is nil", i) //nolint:err113 // programming error
		}

		records = append(records, m.record(ctx))
	}

	if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("kafka: publish: %w", err)
	}

	return nil
}

// Produce publishes one message with the given topic, key and value; see
// Publish.
func (p *Producer) Produce(ctx context.Context, topic string, key, value []byte) error {
	return p.Publish(ctx, &Message{Topic: topic, Key: key, Value: value}) //nolint:exhaustruct_v5 // a plain message
}

// Kgo returns the franz-go client, nil before Start. It is owned by
// Producer: do not close it, call Shutdown.
func (p *Producer) Kgo() *kgo.Client { return p.lazy.get() }

// Name returns the component name (WithName). Implements lifecycle.Namer.
func (p *Producer) Name() string { return p.name }

// Start creates the client and pings the cluster until it answers, retried
// with the WithConnectRetry policy; Shutdown ends the retries. It returns
// ErrClosed after Shutdown and ErrAlreadyStarted when called twice.
// Implements lifecycle.Starter.
func (p *Producer) Start(ctx context.Context) error {
	return p.lazy.start(ctx, p.connect)
}

// Shutdown waits until the buffered records are delivered (Flush), until ctx
// is done, then closes the client: records still buffered then fail with an
// error of their Publish. When ctx is done before the close has finished, it
// returns ctx.Err() and the close goes on in the background; a later
// Shutdown waits for it. Idempotent, safe without Start. Implements
// lifecycle.Resource.
func (p *Producer) Shutdown(ctx context.Context) error {
	flushErr := p.flusher.Shutdown(ctx)
	closeErr := p.closer.Shutdown(ctx)

	if flushErr != nil {
		return fmt.Errorf("kafka: flush: %w", flushErr)
	}

	return wrap(closeErr)
}

// Healthcheck pings the cluster within ctx; ErrNotStarted before Start,
// ErrClosed after Shutdown. Implements lifecycle.Healthchecker.
func (p *Producer) Healthcheck(ctx context.Context) error {
	return p.lazy.ping(ctx)
}

// JSONProducer publishes values of type T as JSON to one topic, with a
// "content-type: application/json" header.
type JSONProducer[T any] struct {
	topic     string
	publisher Publisher
}

// NewJSONProducer returns a JSONProducer publishing to topic through
// publisher (typically a *Producer).
func NewJSONProducer[T any](publisher Publisher, topic string) *JSONProducer[T] {
	return &JSONProducer[T]{topic: topic, publisher: publisher}
}

// Produce encodes value as JSON and publishes it with key (no key if empty).
func (p *JSONProducer[T]) Produce(ctx context.Context, key string, value T) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("kafka: encode %T: %w", value, err)
	}

	msg := &Message{ //nolint:exhaustruct_v5 // a plain message
		Topic:   p.topic,
		Value:   data,
		Headers: []Header{{Key: "content-type", Value: []byte("application/json")}},
	}
	if key != "" {
		msg.Key = []byte(key)
	}

	return p.publisher.Publish(ctx, msg) //nolint:wrapcheck // the Publisher's error, as Publish documents
}

// Compile-time checks.
var (
	_ Publisher               = (*Producer)(nil)
	_ lifecycle.Starter       = (*Producer)(nil)
	_ lifecycle.Resource      = (*Producer)(nil)
	_ lifecycle.Healthchecker = (*Producer)(nil)
	_ lifecycle.Namer         = (*Producer)(nil)
)
