package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/eidon-go/core/contrib/kafka"
	"github.com/eidon-go/core/retry"
)

// Example wires a producer and a consumer that dead-letters the messages it
// cannot handle. With app.App, register the producer before the consumer:
//
//	a.Add(producer) // a Resource: shut down after the consumer has drained
//	a.Add(consumer) // a Runner
func Example() {
	// An in-memory cluster stands in for a real one.
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, "orders", "orders.dlt"))
	if err != nil {
		fmt.Println(err)

		return
	}
	defer cluster.Close()

	cfg := kafka.Config{Brokers: cluster.ListenAddrs()}
	ctx := context.Background()

	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		fmt.Println(err)

		return
	}

	handled := make(chan struct{})
	handler := kafka.HandlerFunc(func(_ context.Context, msg *kafka.Message) error {
		defer close(handled)

		if len(msg.Value) == 0 {
			return retry.Permanent(errors.New("empty order")) // not worth retrying
		}

		fmt.Printf("order %s\n", msg.Value)

		return nil
	})

	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Config: cfg,
		Group:  "billing",
		Topics: []string{"orders"},
		Offset: kafka.OffsetOldest,
	}, handler, kafka.WithErrorHandler(kafka.DeadLetter(producer, "orders.dlt")))
	if err != nil {
		fmt.Println(err)

		return
	}

	err = errors.Join(
		producer.Start(ctx),
		consumer.Start(ctx),
		producer.Produce(ctx, "orders", []byte("customer-1"), []byte("42")),
	)
	if err != nil {
		fmt.Println(err)

		return
	}

	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)

	go func() { done <- consumer.Run(runCtx) }()

	<-handled
	stop() // as app.App does on a signal

	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	fmt.Println(<-done, consumer.Shutdown(shutdownCtx), producer.Shutdown(shutdownCtx))

	// Output:
	// order 42
	// <nil> <nil> <nil>
}

func ExampleJSONProducer() {
	type Order struct {
		ID int `json:"id"`
	}

	publisher := &memoryPublisher{} // a *kafka.Producer in production

	orders := kafka.NewJSONProducer[Order](publisher, "orders")
	if err := orders.Produce(context.Background(), "customer-1", Order{ID: 42}); err != nil {
		fmt.Println(err)

		return
	}

	m := publisher.msgs[0]
	fmt.Println(m.Topic, string(m.Key), string(m.Value))

	// Output:
	// orders customer-1 {"id":42}
}

// memoryPublisher keeps what it is given.
type memoryPublisher struct{ msgs []*kafka.Message }

func (p *memoryPublisher) Publish(_ context.Context, msgs ...*kafka.Message) error {
	p.msgs = append(p.msgs, msgs...)

	return nil
}
