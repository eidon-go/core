package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/eidon-go/core/consume"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
	"github.com/eidon-go/core/retry"
)

var errHandler = errors.New("handler failed")

// fastRedelivery retries quickly, attempts times.
func fastRedelivery(attempts int) Option {
	return WithRedeliveryPolicy(
		retry.Policy{MaxAttempts: attempts, InitialDelay: time.Millisecond},
	)
}

func newTestConsumer(t *testing.T, conn *Connection, cfg ConsumerConfig, h Handler, opts ...Option) *Consumer {
	t.Helper()

	return newTestConsumerOnError(t, conn, cfg, h, StopOnFailure, opts...)
}

func newTestConsumerOnError(
	t *testing.T, conn *Connection, cfg ConsumerConfig, h Handler, onError ErrorHandler, opts ...Option,
) *Consumer {
	t.Helper()

	c, err := NewConsumer(conn, cfg, h, onError, append([]Option{WithLogger(discard)}, opts...)...)
	require.NoError(t, err)

	t.Cleanup(func() { _ = c.Shutdown(cleanupContext(t)) })

	return c
}

// run runs c in the background and returns Run's result channel.
func run(ctx context.Context, c *Consumer) <-chan error {
	done := make(chan error, 1)

	go func() { done <- c.Run(ctx) }()

	return done
}

// recordingHandler records the bodies it handled and fails as fail says.
type recordingHandler struct {
	mu      sync.Mutex
	bodies  []string
	calls   atomic.Int32
	failFor func(call int32) error
}

func (h *recordingHandler) Handle(_ context.Context, msg *Message) error {
	call := h.calls.Add(1)
	if h.failFor != nil {
		if err := h.failFor(call); err != nil {
			return err
		}
	}

	h.mu.Lock()
	h.bodies = append(h.bodies, string(msg.Body))
	h.mu.Unlock()

	return nil
}

func (h *recordingHandler) handled() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	return append([]string(nil), h.bodies...)
}

var ordersConfig = ConsumerConfig{ //nolint:gochecknoglobals // a read-only test value
	Queue: "orders",
	Topology: Topology{
		Exchanges: []Exchange{
			{Name: "events", Kind: ExchangeTopic, Durable: true},
		},
		Queues: []Queue{
			{Name: "orders", Durable: true},
		},
		Bindings: []Binding{
			{Queue: "orders", Exchange: "events", RoutingKey: "order.*"},
		},
	},
}

func body(s string) amqp.Delivery {
	return amqp.Delivery{
		Body:       []byte(s),
		Exchange:   "events",
		RoutingKey: "order.created",
	}
}

func TestConsumer_Lifecycle(t *testing.T) {
	t.Parallel()

	lifecycletest.Runner(t, func(t *testing.T) lifecycle.Runner {
		t.Helper()

		conn := newTestConnection(t, newFakeBroker(), true)

		return newTestConsumer(t, conn, ordersConfig, &recordingHandler{})
	})
}

func TestConsumer_HandlesAndAcks(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	h := &recordingHandler{}
	c := newTestConsumer(t, newTestConnection(t, broker, true), ordersConfig, h)

	ctx, cancel := context.WithCancel(t.Context())
	done := run(ctx, c)

	broker.deliver(t, body("a"))
	broker.deliver(t, body("b"))

	require.Eventually(t, func() bool { return len(h.handled()) == 2 }, 5*time.Second, time.Millisecond)
	require.NoError(t, c.Healthcheck(t.Context()))

	cancel()
	require.NoError(t, receive(t, done))

	acked, requeued, rejected := broker.snapshot()
	assert.Len(t, acked, 2)
	assert.Empty(t, requeued)
	assert.Empty(t, rejected)
	assert.Equal(t, []string{"a", "b"}, h.handled())
	assert.Equal(t, []string{"exchange events", "queue orders", "bind orders events"}, broker.declared)
	assert.Equal(t, []int{1}, broker.prefetch)
}

func TestConsumer_RetriesInPlace(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	h := &recordingHandler{failFor: func(call int32) error {
		if call < 3 {
			return errHandler
		}

		return nil
	}}

	var retries atomic.Int32

	policy := retry.Policy{
		MaxAttempts:  5,
		InitialDelay: time.Millisecond,
		OnRetry:      func(context.Context, retry.Attempt) { retries.Add(1) },
	}
	c := newTestConsumer(t, newTestConnection(t, broker, true), ordersConfig, h, WithRedeliveryPolicy(policy))

	ctx, cancel := context.WithCancel(t.Context())
	done := run(ctx, c)

	broker.deliver(t, body("a"))
	require.Eventually(t, func() bool { return len(h.handled()) == 1 }, 5*time.Second, time.Millisecond)

	cancel()
	require.NoError(t, receive(t, done))

	acked, requeued, _ := broker.snapshot()
	assert.Len(t, acked, 1)
	assert.Empty(t, requeued)
	assert.EqualValues(t, 3, h.calls.Load())
	assert.EqualValues(t, 2, retries.Load())
}

func TestConsumer_StopsWhenRetriesAreExhausted(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	h := &recordingHandler{failFor: func(int32) error { return errHandler }}
	c := newTestConsumer(t, newTestConnection(t, broker, true), ordersConfig, h, fastRedelivery(3))

	done := run(t.Context(), c)

	broker.deliver(t, body("a"))

	err := receive(t, done)
	require.ErrorIs(t, err, ErrStopped)
	require.ErrorIs(t, err, errHandler)
	require.ErrorIs(t, err, ErrRetriesExhausted)
	assert.EqualValues(t, 3, h.calls.Load())

	// Not acked: requeued when the channel closed.
	acked, requeued, rejected := broker.snapshot()
	assert.Empty(t, acked)
	assert.Len(t, requeued, 1)
	assert.Empty(t, rejected)
}

func TestConsumer_ErrorActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		handler      ErrorHandler
		handlerErr   error
		wantCalls    int32
		wantRequeued int
		wantRejected int
	}{
		{
			name:         "dead letter after the retries",
			handler:      DeadLetter,
			handlerErr:   errHandler,
			wantCalls:    2,
			wantRejected: 1,
		},
		{
			name:         "dead letter at once for a permanent error",
			handler:      DeadLetter,
			handlerErr:   retry.Permanent(errHandler),
			wantCalls:    1,
			wantRejected: 1,
		},
		{
			name: "requeue",
			handler: func(context.Context, *Message, error) ErrorAction {
				return ErrorActionRequeue
			},
			handlerErr:   errHandler,
			wantCalls:    1,
			wantRequeued: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			broker := newFakeBroker()
			h := &recordingHandler{
				failFor: func(int32) error { return tt.handlerErr },
			}
			c := newTestConsumerOnError(t, newTestConnection(t, broker, true), ordersConfig, h,
				tt.handler, fastRedelivery(2))

			ctx, cancel := context.WithCancel(t.Context())
			done := run(ctx, c)

			broker.deliver(t, body("a"))
			require.Eventually(t, func() bool {
				_, requeued, rejected := broker.snapshot()

				return len(requeued)+len(rejected) == 1
			}, 5*time.Second, time.Millisecond)

			cancel()
			require.NoError(t, receive(t, done))

			acked, requeued, rejected := broker.snapshot()
			assert.Empty(t, acked)
			assert.Len(t, requeued, tt.wantRequeued)
			assert.Len(t, rejected, tt.wantRejected)
			assert.Equal(t, tt.wantCalls, h.calls.Load())
		})
	}
}

func requeueAlways(context.Context, *Message, error) ErrorAction { return ErrorActionRequeue }

// A transient error of a dependency is waited out in place: the
// ErrorHandler is not asked and the message is acked once handled.
func TestConsumer_TransientErrorIsWaitedOut(t *testing.T) {
	t.Parallel()

	var asked atomic.Int32

	broker := newFakeBroker()
	h := &recordingHandler{failFor: func(call int32) error {
		if call <= 3 {
			return consume.Transient(errHandler)
		}

		return nil
	}}
	c := newTestConsumerOnError(t, newTestConnection(t, broker, true), ordersConfig, h,
		func(context.Context, *Message, error) ErrorAction {
			asked.Add(1)

			return ErrorActionReject
		},
		WithTransientPolicy(retry.Policy{MaxAttempts: retry.Forever, InitialDelay: time.Millisecond}))

	ctx, cancel := context.WithCancel(t.Context())
	done := run(ctx, c)

	broker.deliver(t, body("a"))
	require.Eventually(t, func() bool {
		acked, _, _ := broker.snapshot()

		return len(acked) == 1
	}, 5*time.Second, time.Millisecond)

	cancel()
	require.NoError(t, receive(t, done))

	assert.EqualValues(t, 4, h.calls.Load())
	assert.Zero(t, asked.Load())
}

// A requeue waits the redelivery delay first, so a failing message does not
// come straight back from the broker in a hot loop.
func TestConsumer_RequeueWaitsForTheDelay(t *testing.T) {
	t.Parallel()

	const delay = 200 * time.Millisecond

	broker := newFakeBroker()
	h := &recordingHandler{failFor: func(int32) error { return errHandler }}
	c := newTestConsumerOnError(t, newTestConnection(t, broker, true), ordersConfig, h, requeueAlways,
		WithRedeliveryPolicy(retry.Policy{MaxAttempts: 1, InitialDelay: delay}))

	ctx, cancel := context.WithCancel(t.Context())
	done := run(ctx, c)

	start := time.Now()

	broker.deliver(t, body("a"))
	require.Eventually(t, func() bool {
		_, requeued, _ := broker.snapshot()

		return len(requeued) == 1
	}, 5*time.Second, time.Millisecond)
	assert.GreaterOrEqual(t, time.Since(start), delay)

	cancel()
	require.NoError(t, receive(t, done))
}

// A stop during the wait before a requeue ends it at once; the message is
// left unacked, so the broker requeues it when the channel closes.
func TestConsumer_StopDuringRequeueDelay(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	h := &recordingHandler{failFor: func(int32) error { return errHandler }}
	c := newTestConsumerOnError(t, newTestConnection(t, broker, true), ordersConfig, h, requeueAlways,
		WithRedeliveryPolicy(retry.Policy{MaxAttempts: 1, InitialDelay: time.Hour}))

	ctx, cancel := context.WithCancel(t.Context())
	done := run(ctx, c)

	broker.deliver(t, body("a"))
	require.Eventually(t, func() bool { return h.calls.Load() == 1 }, 5*time.Second, time.Millisecond)

	cancel()
	require.NoError(t, receive(t, done))

	acked, requeued, rejected := broker.snapshot()
	assert.Empty(t, acked)
	assert.Len(t, requeued, 1)
	assert.Empty(t, rejected)
}

func TestDeliveryNumber(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		d    amqp.Delivery
		want int
	}{
		{name: "first delivery", d: amqp.Delivery{}, want: 1},
		{name: "redelivered, uncounted", d: amqp.Delivery{Redelivered: true}, want: 2},
		{
			name: "quorum queue count",
			d:    amqp.Delivery{Redelivered: true, Headers: amqp.Table{headerDeliveryCount: int64(4)}},
			want: 5,
		},
		{name: "int32 count", d: amqp.Delivery{Headers: amqp.Table{headerDeliveryCount: int32(1)}}, want: 2},
		{name: "foreign type ignored", d: amqp.Delivery{Headers: amqp.Table{headerDeliveryCount: "x"}}, want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, deliveryNumber(&tt.d))
		})
	}
}

func TestConsumer_PanicsAreErrors(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	h := HandlerFunc(func(context.Context, *Message) error { panic("boom") })
	c := newTestConsumerOnError(t, newTestConnection(t, broker, true), ordersConfig, h,
		func(context.Context, *Message, error) ErrorAction { panic("again") }, fastRedelivery(1))

	done := run(t.Context(), c)

	broker.deliver(t, body("a"))

	err := receive(t, done)
	require.ErrorIs(t, err, ErrStopped)
	assert.Contains(t, err.Error(), "handler panicked: boom")
}

// The stop lets the message in flight finish on a live context, acks it,
// and requeues what was delivered but not handled.
func TestConsumer_DrainsOnStop(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	entered := make(chan struct{})
	release := make(chan struct{})

	var handlerCtxDone atomic.Bool

	h := HandlerFunc(func(ctx context.Context, msg *Message) error {
		if string(msg.Body) == "slow" {
			close(entered)
			<-release

			handlerCtxDone.Store(ctx.Err() != nil)
		}

		return nil
	})
	c := newTestConsumer(t, newTestConnection(t, broker, true), ordersConfig, h)

	ctx, cancel := context.WithCancel(t.Context())
	done := run(ctx, c)

	broker.deliver(t, body("slow"))
	<-entered
	broker.deliver(t, body("prefetched"))

	cancel()

	select {
	case err := <-done:
		t.Fatalf("Run returned %v before the message in flight finished", err)
	case <-time.After(10 * time.Millisecond):
	}

	require.ErrorIs(t, c.Healthcheck(t.Context()), ErrNotRunning)

	close(release)
	require.NoError(t, receive(t, done))
	assert.False(t, handlerCtxDone.Load(), "the handler's context lives through the stop")

	acked, requeued, _ := broker.snapshot()
	assert.Len(t, acked, 1)
	assert.Len(t, requeued, 1)
}

// Shutdown's deadline cancels the handler's context; the cut-off attempt is
// not a failure: the ErrorHandler is not asked — DeadLetter would reject the
// message on its last attempt — and the unacked message is requeued.
func TestConsumer_ShutdownDeadlineCutsOffTheHandler(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	entered := make(chan struct{})
	h := HandlerFunc(func(ctx context.Context, _ *Message) error {
		close(entered)
		<-ctx.Done()

		return ctx.Err()
	})

	var asked atomic.Bool

	errorHandler := func(ctx context.Context, msg *Message, err error) ErrorAction {
		asked.Store(true)

		return DeadLetter(ctx, msg, err)
	}
	c := newTestConsumerOnError(t, newTestConnection(t, broker, true), ordersConfig, h,
		errorHandler, fastRedelivery(1))

	done := run(t.Context(), c)

	broker.deliver(t, body("a"))
	<-entered

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, c.Shutdown(ctx), context.DeadlineExceeded)
	require.NoError(t, receive(t, done))

	require.Eventually(t, func() bool {
		_, requeued, _ := broker.snapshot()

		return len(requeued) == 1
	}, 5*time.Second, time.Millisecond)

	acked, _, rejected := broker.snapshot()
	assert.Empty(t, acked)
	assert.Empty(t, rejected, "a cut-off message is not dead-lettered")
	assert.False(t, asked.Load(), "the ErrorHandler is not asked about a cut-off attempt")
}

func TestConsumer_SubscribesAgainAfterAReconnect(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	h := &recordingHandler{}
	c := newTestConsumer(t, newTestConnection(t, broker, true), ordersConfig, h)

	ctx, cancel := context.WithCancel(t.Context())
	done := run(ctx, c)

	broker.deliver(t, body("before"))
	require.Eventually(t, func() bool { return len(h.handled()) == 1 }, 5*time.Second, time.Millisecond)

	broker.setDialErr(errBroker)
	broker.drop()

	require.Eventually(t, func() bool {
		return errors.Is(c.Healthcheck(t.Context()), ErrNotConnected)
	}, 5*time.Second, time.Millisecond)

	broker.setDialErr(nil)
	broker.deliver(t, body("after"))
	require.Eventually(t, func() bool { return len(h.handled()) == 2 }, 5*time.Second, time.Millisecond)
	require.NoError(t, c.Healthcheck(t.Context()))

	cancel()
	require.NoError(t, receive(t, done))

	assert.Equal(t, []string{"before", "after"}, h.handled())
	assert.Len(t, broker.declared, 6, "the topology is declared on every subscription")
}

func TestConsumer_PauseAndResume(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()

	var fixed atomic.Bool

	h := &recordingHandler{
		failFor: func(int32) error {
			if fixed.Load() {
				return nil
			}

			return errHandler
		},
	}
	pause := func(context.Context, *Message, error) ErrorAction { return ErrorActionPause }
	c := newTestConsumerOnError(t, newTestConnection(t, broker, true), ordersConfig, h, pause, fastRedelivery(2))

	ctx, cancel := context.WithCancel(t.Context())
	done := run(ctx, c)

	broker.deliver(t, body("a"))
	require.Eventually(t, func() bool { return errors.Is(c.Healthcheck(t.Context()), ErrPaused) },
		5*time.Second, time.Millisecond)

	require.Eventually(t, func() bool {
		_, requeued, _ := broker.snapshot()

		return len(requeued) == 1
	}, 5*time.Second, time.Millisecond, "unacked, requeued with the channel after the redelivery delay")

	_, _, rejected := broker.snapshot()
	assert.Empty(t, rejected)

	fixed.Store(true)
	c.Resume()

	require.Eventually(t, func() bool { return c.Healthcheck(t.Context()) == nil }, 5*time.Second, time.Millisecond)
	broker.deliver(t, body("b"))
	require.Eventually(t, func() bool { return len(h.handled()) == 1 }, 5*time.Second, time.Millisecond)

	cancel()
	require.NoError(t, receive(t, done))
	assert.Equal(t, []string{"b"}, h.handled())
}

// With WithPauseFor the paused message is held unacked for the pause, then
// requeued, and the consumer subscribes again without Resume.
func TestConsumer_PauseEndsByItself(t *testing.T) {
	t.Parallel()

	const pauseFor = 300 * time.Millisecond

	broker := newFakeBroker()
	h := &recordingHandler{failFor: func(int32) error { return errHandler }}
	pause := func(context.Context, *Message, error) ErrorAction { return ErrorActionPause }
	c := newTestConsumerOnError(t, newTestConnection(t, broker, true), ordersConfig, h, pause,
		fastRedelivery(2), WithPauseFor(pauseFor))

	ctx, cancel := context.WithCancel(t.Context())
	done := run(ctx, c)

	began := time.Now()

	broker.deliver(t, body("a"))
	require.Eventually(t, func() bool { return errors.Is(c.Healthcheck(t.Context()), ErrPaused) },
		5*time.Second, time.Millisecond)

	require.Eventually(t, func() bool {
		_, requeued, _ := broker.snapshot()

		return len(requeued) == 1
	}, 5*time.Second, time.Millisecond)
	assert.GreaterOrEqual(t, time.Since(began), pauseFor, "held for the pause before the requeue")

	require.Eventually(t, func() bool { return c.Healthcheck(t.Context()) == nil }, 5*time.Second, time.Millisecond,
		"subscribed again without Resume")

	cancel()
	require.NoError(t, receive(t, done))
}

func TestConsumer_RetriesAFailedSubscription(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	broker.declareErr = errBroker
	h := &recordingHandler{}
	c := newTestConsumer(t, newTestConnection(t, broker, true), ordersConfig, h)

	ctx, cancel := context.WithCancel(t.Context())
	done := run(ctx, c)

	require.Never(t, func() bool { return c.Healthcheck(t.Context()) == nil }, 20*time.Millisecond, time.Millisecond)

	broker.mu.Lock()
	broker.declareErr = nil
	broker.mu.Unlock()

	broker.deliver(t, body("a"))
	require.Eventually(t, func() bool { return len(h.handled()) == 1 }, 5*time.Second, time.Millisecond)

	cancel()
	require.NoError(t, receive(t, done))
}

func TestConsumer_Concurrency(t *testing.T) {
	t.Parallel()

	const workers = 3

	broker := newFakeBroker()
	barrier := make(chan struct{})

	var inFlight atomic.Int32

	h := HandlerFunc(func(context.Context, *Message) error {
		if inFlight.Add(1) == workers {
			close(barrier)
		}

		<-barrier

		return nil
	})

	cfg := ordersConfig
	cfg.Concurrency = workers
	c := newTestConsumer(t, newTestConnection(t, broker, true), cfg, h)

	ctx, cancel := context.WithCancel(t.Context())
	done := run(ctx, c)

	for range workers {
		broker.deliver(t, body("x"))
	}

	require.Eventually(t, func() bool {
		acked, _, _ := broker.snapshot()

		return len(acked) == workers
	}, 5*time.Second, time.Millisecond)

	cancel()
	require.NoError(t, receive(t, done))
	assert.Equal(t, []int{workers}, broker.prefetch, "prefetch defaults to the concurrency")
}

func TestConsumer_ConnectionStates(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	conn := newTestConnection(t, broker, false)
	c := newTestConsumer(t, conn, ordersConfig, &recordingHandler{})
	require.ErrorIs(t, c.Run(t.Context()), ErrNotStarted)

	conn2 := newTestConnection(t, broker, true)
	c2 := newTestConsumer(t, conn2, ordersConfig, &recordingHandler{})
	done := run(t.Context(), c2)

	broker.consumer(t)
	require.NoError(t, conn2.Shutdown(t.Context()))
	require.ErrorIs(t, receive(t, done), ErrClosed)
}

func TestConsumer_TracesTheProcessAsAChildOfThePublish(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	recorder := tracetest.NewSpanRecorder()
	opts := []Option{
		WithTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))),
		WithPropagators(propagation.TraceContext{}),
	}
	conn := newTestConnection(t, broker, true)
	pub := newTestPublisher(t, conn, opts...)

	var handlerSpan atomic.Bool

	h := HandlerFunc(func(context.Context, *Message) error {
		handlerSpan.Store(true)

		return nil
	})
	c := newTestConsumer(t, conn, ordersConfig, h, opts...)

	ctx, cancel := context.WithCancel(t.Context())
	done := run(ctx, c)

	require.NoError(
		t,
		pub.Publish(t.Context(), &Message{Exchange: "events", RoutingKey: "order.created"}),
	)

	sent := broker.publishedMessages()[0]
	broker.deliver(
		t,
		amqp.Delivery{Headers: sent.Headers, Exchange: "events", RoutingKey: "order.created"},
	)
	require.Eventually(t, func() bool { return len(recorder.Ended()) == 2 }, 5*time.Second, time.Millisecond)

	cancel()
	require.NoError(t, receive(t, done))

	spans := recorder.Ended()
	publish, process := spans[0], spans[1]
	assert.Equal(t, "publish events", publish.Name())
	assert.Equal(t, "process orders", process.Name())
	assert.Equal(t, publish.SpanContext().TraceID(), process.Parent().TraceID())
	assert.Equal(t, publish.SpanContext().SpanID(), process.Parent().SpanID())
}

func TestJSON(t *testing.T) {
	t.Parallel()

	type order struct{ ID int }

	var got order

	h := JSON(func(_ context.Context, v order, _ *Message) error {
		got = v

		return nil
	})

	require.NoError(t, h.Handle(t.Context(), &Message{Body: []byte(`{"ID":7}`)}))
	assert.Equal(t, 7, got.ID)

	err := h.Handle(t.Context(), &Message{Body: []byte(`{`)})
	require.ErrorIs(t, err, retry.ErrPermanent)

	_, err = JSONMessage("x", "y", func() {})
	require.Error(t, err)
}

func TestNewConsumer_Invalid(t *testing.T) {
	t.Parallel()

	conn := newTestConnection(t, newFakeBroker(), false)
	h := &recordingHandler{}
	_, err := NewConsumer(nil, ordersConfig, h, DeadLetter)
	require.ErrorIs(t, err, ErrInvalidConfig)

	_, err = NewConsumer(conn, ordersConfig, nil, DeadLetter)
	require.ErrorIs(t, err, ErrInvalidConfig)

	var nilHandler HandlerFunc

	_, err = NewConsumer(conn, ordersConfig, nilHandler, DeadLetter)
	require.ErrorIs(t, err, ErrInvalidConfig)

	_, err = NewConsumer(conn, ordersConfig, h, nil)
	require.ErrorIs(t, err, ErrInvalidConfig, "the error strategy is required")

	_, err = NewConsumer(conn, ConsumerConfig{
		Concurrency: -1,
		Prefetch:    -1,
		Topology:    Topology{Exchanges: []Exchange{{}}, Queues: []Queue{{}}, Bindings: []Binding{{}}},
	}, h, DeadLetter)
	require.ErrorIs(t, err, ErrInvalidConfig)

	for _, want := range []string{
		"queue must not be empty", "concurrency", "prefetch", "exchanges[0]: name", "exchanges[0]: kind",
		"queues[0]: name", "bindings[0]: queue", "bindings[0]: exchange",
	} {
		assert.Contains(t, err.Error(), want)
	}
}

// basic.qos carries a 16-bit prefetch: a larger one, or a default taken from
// a larger concurrency, is a configuration error, not a failure at every
// subscription.
func TestConsumerConfig_PrefetchLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     ConsumerConfig
		wantErr bool
	}{
		{name: "largest", cfg: ConsumerConfig{Queue: "q", Prefetch: maxPrefetch}},
		{name: "too large", cfg: ConsumerConfig{Queue: "q", Prefetch: maxPrefetch + 1}, wantErr: true},
		{
			name:    "default from concurrency",
			cfg:     ConsumerConfig{Queue: "q", Concurrency: maxPrefetch + 1},
			wantErr: true,
		},
		{
			name: "explicit prefetch below a large concurrency",
			cfg:  ConsumerConfig{Queue: "q", Concurrency: maxPrefetch + 1, Prefetch: 10},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.Validate()
			if tt.wantErr {
				require.ErrorIs(t, err, ErrInvalidConfig)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestErrorAction_String(t *testing.T) {
	t.Parallel()

	for action, want := range map[ErrorAction]string{
		ErrorActionRetry: "retry", ErrorActionRequeue: "requeue", ErrorActionReject: "reject",
		ErrorActionStop: "stop", ErrorActionPause: "pause", ErrorAction(42): "unknown",
	} {
		assert.Equal(t, want, action.String())
	}
}

// TopologyVerify checks the exchanges and queues with passive declares and
// declares nothing.
func TestConsumer_TopologyVerify(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	h := &recordingHandler{}
	c := newTestConsumer(t, newTestConnection(t, broker, true), ordersConfig, h, WithTopologyMode(TopologyVerify))

	ctx, cancel := context.WithCancel(t.Context())
	done := run(ctx, c)

	require.Eventually(t, func() bool { return c.Healthcheck(t.Context()) == nil }, 5*time.Second, time.Millisecond)

	broker.deliver(t, body("a"))
	require.Eventually(t, func() bool { return len(h.handled()) == 1 }, 5*time.Second, time.Millisecond)

	cancel()
	require.NoError(t, receive(t, done))

	broker.mu.Lock()
	defer broker.mu.Unlock()

	assert.Equal(t, []string{"exchange events", "queue orders"}, broker.checked)
	assert.Empty(t, broker.declared, "nothing is declared")
}

func TestConsumer_TopologyVerifyFailsOnAMissingQueue(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	broker.missing = map[string]bool{"queue orders": true}
	c := newTestConsumer(t, newTestConnection(t, broker, true), ordersConfig, &recordingHandler{},
		WithTopologyMode(TopologyVerify))

	err := receive(t, run(t.Context(), c))
	require.ErrorIs(t, err, ErrTopology)
	require.ErrorContains(t, err, `queue "orders" does not exist`)
}

func TestTopologyMode_String(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{"declare", "verify", "topology(9)"},
		[]string{TopologyDeclare.String(), TopologyVerify.String(), TopologyMode(9).String()})
}
