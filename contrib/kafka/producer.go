package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

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
	name     string
	logger   *slog.Logger
	connect  retry.Policy
	client   *kgo.Client
	releaser *lifecycle.Releaser
}

// NewProducer validates cfg and builds the producer; it does no network I/O —
// franz-go connects on first use, and Start pings.
func NewProducer(cfg Config, opts ...Option) (*Producer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	o := newOptions(opts)

	client, err := kgo.NewClient(o.clientOpts(cfg, o.tracer(cfg.ClientID, ""))...)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}

	p := &Producer{
		name:     o.name,
		logger:   o.logger,
		connect:  o.connectPolicy(),
		client:   client,
		releaser: nil,
	}
	p.releaser = lifecycle.ShutdownOnce(p.release)

	return p, nil
}

// Publish sends msgs and waits until every one is acknowledged or ctx is
// done; it returns the first error. Each message needs a Topic. The trace
// context of ctx goes into the record headers. Implements Publisher.
func (p *Producer) Publish(ctx context.Context, msgs ...*Message) error {
	records := make([]*kgo.Record, 0, len(msgs))

	for i, m := range msgs {
		if m == nil {
			return fmt.Errorf("kafka: publish: message %d is nil", i) //nolint:err113 // programming error
		}

		records = append(records, m.record(ctx))
	}

	if err := p.client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("kafka: publish: %w", err)
	}

	return nil
}

// Produce publishes one message with the given topic, key and value; see
// Publish.
func (p *Producer) Produce(ctx context.Context, topic string, key, value []byte) error {
	return p.Publish(ctx, &Message{Topic: topic, Key: key, Value: value}) //nolint:exhaustruct_v5 // a plain message
}

// Kgo returns the franz-go client. It is owned by Producer: do not close it,
// call Shutdown.
func (p *Producer) Kgo() *kgo.Client { return p.client }

// Name returns the component name (WithName). Implements lifecycle.Namer.
func (p *Producer) Name() string { return p.name }

// Start pings the cluster until it answers, retried with the WithConnectRetry
// policy. It returns ErrClosed after Shutdown. Implements lifecycle.Starter.
func (p *Producer) Start(ctx context.Context) error {
	return startPing(ctx, p.connect, p.client, p.releaser)
}

// Shutdown waits until the buffered records are delivered (Flush), until ctx
// is done, then closes the client: records still buffered then fail with an
// error of their Publish. Idempotent. Implements lifecycle.Resource.
func (p *Producer) Shutdown(ctx context.Context) error {
	return wrap(p.releaser.Shutdown(ctx))
}

// Healthcheck pings the cluster within ctx. Implements lifecycle.Healthchecker.
func (p *Producer) Healthcheck(ctx context.Context) error {
	return healthPing(ctx, p.client, p.releaser)
}

// release flushes until ctx is done, then closes the client; the close is
// not waited for past ctx.
func (p *Producer) release(ctx context.Context) error {
	flushErr := p.client.Flush(ctx)

	closed := make(chan struct{})

	go func() {
		defer close(closed)

		p.client.Close()
	}()

	select {
	case <-closed:
	case <-ctx.Done():
		return errors.Join(flushErr, ctx.Err())
	}

	if flushErr != nil {
		return fmt.Errorf("flush: %w", flushErr)
	}

	return nil
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
