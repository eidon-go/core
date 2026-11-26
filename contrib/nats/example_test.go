package nats_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/eidon-go/core/contrib/nats"
	"github.com/eidon-go/core/retry"
)

type orderCreated struct {
	ID string `json:"id"`
}

// Example wires a JetStream consumer whose handler publishes, and a
// request-reply service, on one connection. The constructors do no I/O;
// with app.App, register the components in this order:
//
//	a.Add(conn)                // a Starter: connects in Start, drained and closed last
//	a.Add(publisher)           // a Resource: shut down after the runners have drained
//	a.Add(consumer, responder) // Runners: stopped first
func Example() {
	conn, err := nats.NewConnection(nats.Config{URL: "nats://localhost:4222", CredsFile: "/etc/nats/app.creds"})
	if err != nil {
		fmt.Println(err)

		return
	}

	publisher, err := nats.NewPublisher(conn, nats.WithName("orders-publisher"))
	if err != nil {
		fmt.Println(err)

		return
	}

	handler := nats.JSON(func(ctx context.Context, order orderCreated, _ *nats.Message) error {
		msg, err := nats.JSONMessage("invoices.requested", order)
		if err != nil {
			return err
		}

		// Stored by an INVOICES stream on "invoices.>", which must exist;
		// the message ID makes a retried publish safe.
		_, err = publisher.PublishJetStream(ctx, msg, jetstream.WithMsgID("invoice-"+order.ID))

		return err
	})

	consumer, err := nats.NewConsumer(conn, nats.ConsumerConfig{
		Stream:         "ORDERS",
		Durable:        "billing",
		FilterSubjects: []string{"orders.created"},
		Concurrency:    4,
	}, handler,
		nats.WithName("billing-consumer"),
		nats.WithStream(jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}}),
		nats.WithErrorHandler(nats.Discard),
	)
	if err != nil {
		fmt.Println(err)

		return
	}

	responder, err := nats.NewSubscriber(conn, nats.SubscriberConfig{Subject: "billing.status", Queue: "billing"},
		nats.HandlerFunc(func(ctx context.Context, msg *nats.Message) error {
			return msg.Respond(ctx, &nats.Message{Data: []byte("ok")})
		}), nats.WithName("billing-status"))
	if err != nil {
		fmt.Println(err)

		return
	}

	fmt.Println(conn.Name(), publisher.Name(), consumer.Name(), responder.Name())
	fmt.Println(errors.Is(conn.Healthcheck(context.Background()), nats.ErrNotStarted))
	// Output:
	// nats orders-publisher billing-consumer billing-status
	// true
}

// ExampleDiscard shows what Discard decides: a failed message is retried;
// once the retries are exhausted — or at once for an error marked with
// retry.Permanent — it is rejected: a Consumer terminates it, a Subscriber
// drops it.
func ExampleDiscard() {
	ctx, msg, failed := context.Background(), &nats.Message{}, errors.New("billing is down")

	fmt.Println(nats.Discard(ctx, msg, failed))
	fmt.Println(nats.Discard(ctx, msg, errors.Join(nats.ErrRetriesExhausted, failed)))
	fmt.Println(nats.Discard(ctx, msg, retry.Permanent(errors.New("bad order"))))
	fmt.Println(nats.DefaultErrorHandler(ctx, msg, errors.Join(nats.ErrRetriesExhausted, failed)))
	// Output:
	// retry
	// reject
	// reject
	// stop
}

// ExampleJSON decodes the data of every message into a typed value. Data
// that does not decode is a permanent error: retrying cannot help.
func ExampleJSON() {
	handler := nats.JSON(func(_ context.Context, order orderCreated, msg *nats.Message) error {
		fmt.Printf("order %s via %s\n", order.ID, msg.Subject)

		return nil
	})

	ctx := context.Background()
	good := &nats.Message{Subject: "orders.created", Data: []byte(`{"id":"42"}`)}
	bad := &nats.Message{Subject: "orders.created", Data: []byte(`{`)}

	fmt.Println(handler.Handle(ctx, good))
	fmt.Println(errors.Is(handler.Handle(ctx, bad), retry.ErrPermanent))
	// Output:
	// order 42 via orders.created
	// <nil>
	// true
}

// ExampleJSONMessage builds a JSON message to publish.
func ExampleJSONMessage() {
	msg, err := nats.JSONMessage("orders.created", orderCreated{ID: "42"})
	if err != nil {
		fmt.Println(err)

		return
	}

	fmt.Println(msg.Subject, msg.Header.Get("Content-Type"), string(msg.Data))
	// Output:
	// orders.created application/json {"id":"42"}
}
