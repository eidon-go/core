package rabbitmq_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/eidon-go/core/contrib/rabbitmq"
	"github.com/eidon-go/core/retry"
)

type orderCreated struct {
	ID string `json:"id"`
}

// Example wires a publisher and a consumer, each on its own connection, so
// that a publisher the broker throttles does not hold up the consumer's
// acks. The consumer's handler publishes too. The constructors do no I/O;
// with app.App, register the components in this order:
//
//	a.Add(publishConn) // Starters: dial in Start, closed last
//	a.Add(consumeConn)
//	a.Add(publisher) // a Resource: shut down after the consumer has drained
//	a.Add(consumer)  // a Runner: stopped first
func Example() {
	cfg := rabbitmq.Config{Host: "localhost", User: "app", Password: "secret"}

	publishConn, err := rabbitmq.NewConnection(cfg, rabbitmq.WithName("rabbitmq-publish"))
	if err != nil {
		fmt.Println(err)

		return
	}

	consumeConn, err := rabbitmq.NewConnection(cfg, rabbitmq.WithName("rabbitmq-consume"))
	if err != nil {
		fmt.Println(err)

		return
	}

	publisher, err := rabbitmq.NewPublisher(publishConn, rabbitmq.WithName("orders-publisher"))
	if err != nil {
		fmt.Println(err)

		return
	}

	handler := rabbitmq.JSON(func(ctx context.Context, order orderCreated, _ *rabbitmq.Message) error {
		msg, err := rabbitmq.JSONMessage("billing", "invoice.requested", order)
		if err != nil {
			return err
		}

		return publisher.Publish(ctx, msg)
	})

	consumer, err := rabbitmq.NewConsumer(consumeConn, rabbitmq.ConsumerConfig{
		Queue:       "billing.orders",
		Concurrency: 4,
		Topology: rabbitmq.Topology{
			Exchanges: []rabbitmq.Exchange{{Name: "orders", Kind: rabbitmq.ExchangeTopic, Durable: true}},
			Queues:    []rabbitmq.Queue{{Name: "billing.orders", Durable: true}},
			Bindings: []rabbitmq.Binding{
				{Queue: "billing.orders", Exchange: "orders", RoutingKey: "order.created"},
			},
		},
	}, handler, rabbitmq.WithName("billing-consumer"))
	if err != nil {
		fmt.Println(err)

		return
	}

	fmt.Println(publishConn.Name(), consumeConn.Name(), publisher.Name(), consumer.Name())
	fmt.Println(errors.Is(publishConn.Healthcheck(context.Background()), rabbitmq.ErrNotStarted))
	// Output:
	// rabbitmq-publish rabbitmq-consume orders-publisher billing-consumer
	// true
}

// ExampleDeadLetter shows what DeadLetter decides: a failed message is
// retried; once the retries are exhausted — or at once for an error marked
// with retry.Permanent — it is rejected, and the broker moves it to the
// dead-letter exchange of the queue (declared here with
// "x-dead-letter-exchange").
func ExampleDeadLetter() {
	topology := rabbitmq.Topology{
		Exchanges: []rabbitmq.Exchange{{Name: "orders.dlx", Kind: rabbitmq.ExchangeFanout, Durable: true}},
		Queues: []rabbitmq.Queue{
			{Name: "orders.dead", Durable: true},
			{Name: "orders", Durable: true, Args: map[string]any{
				"x-queue-type":           "quorum",
				"x-dead-letter-exchange": "orders.dlx",
			}},
		},
		Bindings: []rabbitmq.Binding{{Queue: "orders.dead", Exchange: "orders.dlx"}},
	}
	cfg := rabbitmq.ConsumerConfig{Queue: "orders", Topology: topology}
	fmt.Println(cfg.Validate())

	// Passed to NewConsumer as rabbitmq.WithErrorHandler(rabbitmq.DeadLetter).
	ctx, msg, failed := context.Background(), &rabbitmq.Message{}, errors.New("billing is down")
	fmt.Println(rabbitmq.DeadLetter(ctx, msg, failed))
	fmt.Println(rabbitmq.DeadLetter(ctx, msg, errors.Join(rabbitmq.ErrRetriesExhausted, failed)))
	fmt.Println(rabbitmq.DeadLetter(ctx, msg, retry.Permanent(errors.New("bad order"))))
	// Output:
	// <nil>
	// retry
	// reject
	// reject
}

// ExampleJSON decodes the body of every message into a typed value. A body
// that does not decode is a permanent error: retrying cannot help.
func ExampleJSON() {
	handler := rabbitmq.JSON(func(_ context.Context, order orderCreated, msg *rabbitmq.Message) error {
		fmt.Printf("order %s via %s\n", order.ID, msg.RoutingKey)

		return nil
	})

	ctx := context.Background()
	good := &rabbitmq.Message{RoutingKey: "order.created", Body: []byte(`{"id":"42"}`)}
	bad := &rabbitmq.Message{RoutingKey: "order.created", Body: []byte(`{`)}

	fmt.Println(handler.Handle(ctx, good))
	fmt.Println(errors.Is(handler.Handle(ctx, bad), retry.ErrPermanent))
	// Output:
	// order 42 via order.created
	// <nil>
	// true
}

// ExampleJSONMessage builds a JSON message to publish; set a message ID so
// that consumers can drop the duplicates of at-least-once delivery.
func ExampleJSONMessage() {
	msg, err := rabbitmq.JSONMessage("orders", "order.created", orderCreated{ID: "42"})
	if err != nil {
		fmt.Println(err)

		return
	}

	msg.MessageID = "order-42"

	fmt.Println(msg.Exchange, msg.RoutingKey, msg.ContentType, string(msg.Body))
	// Output:
	// orders order.created application/json {"id":"42"}
}
