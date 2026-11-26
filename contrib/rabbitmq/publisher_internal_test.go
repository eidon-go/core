package rabbitmq

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
)

func newTestPublisher(t *testing.T, conn *Connection, opts ...Option) *Publisher {
	t.Helper()

	pub, err := NewPublisher(conn, append([]Option{WithLogger(discard)}, opts...)...)
	require.NoError(t, err)

	t.Cleanup(func() { _ = pub.Shutdown(context.Background()) })

	return pub
}

func TestPublisher_Lifecycle(t *testing.T) {
	t.Parallel()

	lifecycletest.Resource(t, func(t *testing.T) lifecycle.Resource {
		t.Helper()

		return newTestPublisher(t, newTestConnection(t, newFakeBroker(), true))
	})
}

func TestPublisher_Publish(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	recorder := tracetest.NewSpanRecorder()
	pub := newTestPublisher(t, newTestConnection(t, broker, true),
		WithTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))),
		WithPropagators(propagation.TraceContext{}))

	headers := map[string]any{"tenant": "acme"}
	first, err := JSONMessage("orders", "order.created", map[string]int{"id": 1})
	require.NoError(t, err)

	first.Headers = headers
	second := &Message{
		RoutingKey: "jobs",
		Body:       []byte("b"),
		Transient:  true,
		MessageID:  "m-2",
	}
	require.NoError(t, pub.Publish(t.Context(), first, second))

	got := broker.publishedMessages()
	require.Len(t, got, 2)
	assert.JSONEq(t, `{"id":1}`, string(got[0].Body))
	assert.Equal(t, "application/"+"json", got[0].ContentType)
	assert.Equal(t, amqp.Persistent, got[0].DeliveryMode)
	assert.Equal(t, "acme", got[0].Headers["tenant"])
	assert.Contains(t, got[0].Headers, "traceparent")
	assert.NotContains(t, headers, "traceparent", "the caller's headers are not changed")
	assert.Equal(t, amqp.Transient, got[1].DeliveryMode)
	assert.Equal(t, "m-2", got[1].MessageId)
	assert.Equal(t, []string{"order.created", "jobs"}, broker.routingKeys)

	spans := recorder.Ended()
	require.Len(t, spans, 2)
	assert.Equal(t, "publish orders", spans[0].Name())
	assert.Equal(t, "publish amq.default", spans[1].Name())
}

func TestPublisher_Nacked(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	broker.nackPublish = true
	pub := newTestPublisher(t, newTestConnection(t, broker, true))

	err := pub.Publish(t.Context(), &Message{RoutingKey: "q"})
	require.ErrorIs(t, err, ErrNacked)
}

func TestPublisher_NotConfirmedWhenTheConnectionIsLost(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	broker.holdConfirms = true
	pub := newTestPublisher(t, newTestConnection(t, broker, true))

	got := make(chan error, 1)

	go func() { got <- pub.Publish(t.Context(), &Message{RoutingKey: "q"}) }()

	require.Eventually(t, func() bool { return broker.heldCount() == 1 }, 5*time.Second, time.Millisecond)
	broker.drop()
	require.ErrorIs(t, <-got, ErrNotConfirmed)

	// The next Publish goes out on a channel of the new connection.
	broker.releaseConfirms(true)
	require.NoError(t, pub.Publish(t.Context(), &Message{RoutingKey: "q"}))
}

func TestPublisher_WaitsForTheConnection(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	pub := newTestPublisher(t, newTestConnection(t, broker, true))

	broker.setDialErr(errBroker)
	broker.drop()

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	err := pub.Publish(ctx, &Message{RoutingKey: "q"})
	require.ErrorIs(t, err, ErrNotConnected)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	got := make(chan error, 1)

	go func() { got <- pub.Publish(t.Context(), &Message{RoutingKey: "q"}) }()

	broker.setDialErr(nil)
	require.NoError(t, <-got)
	assert.Len(t, broker.publishedMessages(), 1)
}

func TestPublisher_ShutdownWaitsForConfirms(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	broker.holdConfirms = true
	pub := newTestPublisher(t, newTestConnection(t, broker, true))

	published := make(chan error, 1)

	go func() { published <- pub.Publish(t.Context(), &Message{RoutingKey: "q"}) }()

	require.Eventually(t, func() bool { return broker.heldCount() == 1 }, 5*time.Second, time.Millisecond)

	shut := make(chan error, 1)

	go func() { shut <- pub.Shutdown(t.Context()) }()

	// New messages are refused while Shutdown waits.
	require.Eventually(t, func() bool {
		pub.mu.Lock()
		defer pub.mu.Unlock()

		return pub.closed
	}, 5*time.Second, time.Millisecond)
	require.ErrorIs(
		t,
		pub.Publish(t.Context(), &Message{RoutingKey: "q"}),
		ErrClosed,
	)

	select {
	case err := <-shut:
		t.Fatalf("Shutdown returned %v before the confirm", err)
	case <-time.After(10 * time.Millisecond):
	}

	broker.releaseConfirms(true)
	require.NoError(t, <-published)
	require.NoError(t, <-shut)
	require.ErrorIs(
		t,
		pub.Publish(t.Context(), &Message{RoutingKey: "q"}),
		ErrClosed,
	)
}

func TestPublisher_ShutdownDeadlineFailsTheWaitingPublish(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	broker.holdConfirms = true
	pub := newTestPublisher(t, newTestConnection(t, broker, true))

	published := make(chan error, 1)

	go func() { published <- pub.Publish(t.Context(), &Message{RoutingKey: "q"}) }()

	require.Eventually(t, func() bool { return broker.heldCount() == 1 }, 5*time.Second, time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, pub.Shutdown(ctx), context.DeadlineExceeded)

	err := <-published
	require.ErrorIs(t, err, ErrNotConfirmed, "the caller's ctx was not cancelled: %v", err)
	require.NotErrorIs(t, err, context.Canceled)
}

// channel.close has no context: Shutdown must not wait for a broker that
// does not answer it past its deadline.
func TestPublisher_ShutdownIsBoundedWhenTheCloseHangs(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	pub := newTestPublisher(t, newTestConnection(t, broker, true))
	require.NoError(t, pub.Publish(t.Context(), &Message{RoutingKey: "q"}))

	unblock := make(chan struct{})
	defer close(unblock)

	broker.mu.Lock()
	broker.blockClose = unblock
	broker.mu.Unlock()

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()

	require.ErrorIs(t, pub.Shutdown(ctx), context.DeadlineExceeded)
	assert.Less(t, time.Since(start), time.Second)
}

// A channel error (an unknown exchange) closes the shared channel: the
// Publish calls waiting on it fail with ErrNotConfirmed and the broker's
// reason; the next Publish gets a new channel.
func TestPublisher_ChannelErrorCarriesTheReason(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	pub := newTestPublisher(t, newTestConnection(t, broker, true))

	err := pub.Publish(t.Context(), &Message{Exchange: missingExchange, RoutingKey: "q"})
	require.ErrorIs(t, err, ErrNotConfirmed)

	var amqpErr *amqp.Error
	require.ErrorAs(t, err, &amqpErr)
	assert.Equal(t, amqp.NotFound, amqpErr.Code)

	require.NoError(t, pub.Publish(t.Context(), &Message{RoutingKey: "q"}))
}

// Messages of one Publish go on one channel: when it dies in the middle of a
// batch, the rest is not resent on a new channel (that would break the
// order), and the error says so.
func TestPublisher_BatchStaysOnOneChannel(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	broker.dropOnPublish = 2
	pub := newTestPublisher(t, newTestConnection(t, broker, true))

	err := pub.Publish(t.Context(),
		&Message{RoutingKey: "q", Body: []byte("1")},
		&Message{RoutingKey: "q", Body: []byte("2")},
		&Message{RoutingKey: "q", Body: []byte("3")})
	require.ErrorIs(t, err, ErrNotConfirmed)
	require.ErrorContains(t, err, "channel closed")

	got := broker.publishedMessages()
	require.Len(t, got, 1, "nothing after the failed message is sent")
	assert.Equal(t, "1", string(got[0].Body))

	require.NoError(t, pub.Publish(t.Context(), &Message{RoutingKey: "q", Body: []byte("4")}))
}

func TestPublisher_States(t *testing.T) {
	t.Parallel()

	_, err := NewPublisher(nil)
	require.ErrorIs(t, err, ErrInvalidConfig)

	conn := newTestConnection(t, newFakeBroker(), false)
	pub := newTestPublisher(t, conn, WithName("orders-publisher"))
	assert.Equal(t, "orders-publisher", pub.Name())

	require.ErrorIs(t, pub.Publish(t.Context(), &Message{}), ErrNotStarted)
	require.ErrorIs(t, pub.Publish(t.Context(), nil), ErrInvalidConfig)

	require.NoError(t, pub.Shutdown(t.Context()))
	require.ErrorIs(t, pub.Publish(t.Context(), &Message{}), ErrClosed)
}
