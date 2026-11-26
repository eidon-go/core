package nats_test

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/nats"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
	"github.com/eidon-go/core/retry"
)

func ordersConsumer() nats.ConsumerConfig {
	return nats.ConsumerConfig{Stream: "ORDERS", Durable: "billing"}
}

func newConsumer(
	t *testing.T, conn *nats.Connection, cfg nats.ConsumerConfig, h nats.Handler, opts ...nats.Option,
) *nats.Consumer {
	t.Helper()

	return newConsumerOnError(t, conn, cfg, h, nats.StopOnFailure, opts...)
}

func newConsumerOnError(
	t *testing.T, conn *nats.Connection, cfg nats.ConsumerConfig, h nats.Handler, onError nats.ErrorHandler,
	opts ...nats.Option,
) *nats.Consumer {
	t.Helper()

	opts = append([]nats.Option{
		nats.WithLogger(quietLogger()),
		nats.WithRedeliveryPolicy(retry.Policy{MaxAttempts: 3, InitialDelay: time.Millisecond}),
	}, opts...)

	c, err := nats.NewConsumer(conn, cfg, h, onError, opts...)
	require.NoError(t, err)

	return c
}

// publishJS publishes to a stream with a plain client.
func publishJS(t *testing.T, s *testServer, subject string, data string) {
	t.Helper()

	_, err := s.jetStream(t).Publish(t.Context(), subject, []byte(data))
	require.NoError(t, err)
}

// consumerInfo returns the state of the durable consumer.
func consumerInfo(t *testing.T, s *testServer, stream, durable string) *jetstream.ConsumerInfo {
	t.Helper()

	cons, err := s.jetStream(t).Consumer(t.Context(), stream, durable)
	require.NoError(t, err)

	info, err := cons.Info(t.Context())
	require.NoError(t, err)

	return info
}

// settled waits until the consumer has no message waiting for its ack.
func settled(t *testing.T, s *testServer, stream, durable string) {
	t.Helper()

	require.Eventually(t, func() bool {
		return consumerInfo(t, s, stream, durable).NumAckPending == 0
	}, waitFor, tick)
}

func TestConsumer_Contract(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	s.createStream(t, "ORDERS", "orders.>")

	lifecycletest.Runner(t, func(t *testing.T) lifecycle.Runner {
		t.Helper()

		return newConsumer(t, startConnection(t, s), ordersConsumer(), newReceived())
	})
}

func TestConsumer_ConsumesAndAcks(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	s.createStream(t, "ORDERS", "orders.>")
	conn := startConnection(t, s)
	got := newReceived()

	cfg := ordersConsumer()
	cfg.FilterSubjects = []string{"orders.created"}
	cfg.AckWait = 5 * time.Second
	c := newConsumer(t, conn, cfg, got, nats.WithName("billing"))

	assert.Equal(t, "billing", c.Name())
	require.ErrorIs(t, c.Healthcheck(t.Context()), nats.ErrNotRunning)

	runInBackground(t, c)
	healthy(t, c)

	publishJS(t, s, "orders.paid", "skip")
	publishJS(t, s, "orders.created", "1")

	msg := got.next(t)
	assert.Equal(t, "orders.created", msg.Subject)
	assert.Equal(t, "1", string(msg.Data))
	require.NotNil(t, msg.Metadata)
	assert.Equal(t, "ORDERS", msg.Metadata.Stream)
	assert.Equal(t, "billing", msg.Metadata.Consumer)
	assert.Equal(t, uint64(1), msg.Metadata.NumDelivered)
	assert.Empty(t, msg.Reply, "the ack subject is not exposed")
	require.ErrorIs(t, msg.Respond(t.Context(), &nats.Message{}), nats.ErrNoReply)

	settled(t, s, "ORDERS", "billing")

	info := consumerInfo(t, s, "ORDERS", "billing")
	assert.Equal(t, "orders.created", info.Config.FilterSubject)
	assert.Equal(t, 5*time.Second, info.Config.AckWait)
	assert.Equal(t, jetstream.AckExplicitPolicy, info.Config.AckPolicy)

	got.none(t, 100*time.Millisecond)
}

func TestConsumer_WithStreamCreatesIt(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	conn := startConnection(t, s)
	got := newReceived()

	c := newConsumer(t, conn, nats.ConsumerConfig{
		Stream: "EVENTS", Durable: "audit", FilterSubjects: []string{"events.a", "events.b"},
	}, got,
		nats.WithStream(jetstream.StreamConfig{Name: "EVENTS", Subjects: []string{"events.>"}}),
		nats.WithConsumerConfig(func(cfg *jetstream.ConsumerConfig) { cfg.Description = "audit trail" }),
	)
	runInBackground(t, c)
	healthy(t, c)

	publishJS(t, s, "events.a", "a")
	publishJS(t, s, "events.c", "c")
	publishJS(t, s, "events.b", "b")

	assert.Equal(t, "a", string(got.next(t).Data))
	assert.Equal(t, "b", string(got.next(t).Data))
	settled(t, s, "EVENTS", "audit")

	info := consumerInfo(t, s, "EVENTS", "audit")
	assert.Equal(t, "audit trail", info.Config.Description)
	assert.Equal(t, []string{"events.a", "events.b"}, info.Config.FilterSubjects)
}

func TestConsumer_WaitsForTheStream(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	conn := startConnection(t, s)
	got := newReceived()
	c := newConsumer(t, conn, ordersConsumer(), got)
	runInBackground(t, c)

	require.Eventually(t, func() bool {
		return errors.Is(c.Healthcheck(t.Context()), nats.ErrNotConnected)
	}, waitFor, tick)

	s.createStream(t, "ORDERS", "orders.>")
	healthy(t, c)

	publishJS(t, s, "orders.x", "x")
	got.next(t)
}

func TestConsumer_RetryInPlace(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	s.createStream(t, "ORDERS", "orders.>")
	conn := startConnection(t, s)

	var attempts atomic.Int32

	got := newReceived()
	c := newConsumer(t, conn, ordersConsumer(), nats.HandlerFunc(func(ctx context.Context, msg *nats.Message) error {
		if attempts.Add(1) < 3 {
			return errors.New("transient")
		}

		return got.Handle(ctx, msg)
	}))
	runInBackground(t, c)
	healthy(t, c)

	publishJS(t, s, "orders.x", "x")

	msg := got.next(t)
	assert.Equal(t, uint64(1), msg.Metadata.NumDelivered, "retried in place, not redelivered")
	settled(t, s, "ORDERS", "billing")
}

func TestConsumer_Requeue(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	s.createStream(t, "ORDERS", "orders.>")
	conn := startConnection(t, s)
	got := newReceived()

	c := newConsumerOnError(
		t,
		conn,
		ordersConsumer(),
		nats.HandlerFunc(func(ctx context.Context, msg *nats.Message) error {
			if msg.Metadata.NumDelivered == 1 {
				return errors.New("later")
			}

			return got.Handle(ctx, msg)
		}),
		func(context.Context, *nats.Message, error) nats.ErrorAction {
			return nats.ErrorActionRequeue
		},
	)
	runInBackground(t, c)
	healthy(t, c)

	publishJS(t, s, "orders.x", "x")

	assert.Equal(t, uint64(2), got.next(t).Metadata.NumDelivered)
	settled(t, s, "ORDERS", "billing")
}

func TestConsumer_DiscardTerminates(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	s.createStream(t, "ORDERS", "orders.>")
	conn := startConnection(t, s)

	var attempts atomic.Int32

	got := newReceived()
	c := newConsumerOnError(
		t,
		conn,
		ordersConsumer(),
		nats.JSON(func(ctx context.Context, _ map[string]any, msg *nats.Message) error {
			return got.Handle(ctx, msg)
		}),
		func(ctx context.Context, msg *nats.Message, err error) nats.ErrorAction {
			attempts.Add(1)

			return nats.Discard(ctx, msg, err)
		},
	)
	runInBackground(t, c)
	healthy(t, c)

	publishJS(t, s, "orders.bad", "not json")
	publishJS(t, s, "orders.good", `{"ok":true}`)

	assert.Equal(t, "orders.good", got.next(t).Subject)
	settled(t, s, "ORDERS", "billing")
	assert.Equal(t, int32(1), attempts.Load(), "a permanent error is not retried")
	got.none(t, 100*time.Millisecond)
}

func TestConsumer_StopNaksAndReturns(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	s.createStream(t, "ORDERS", "orders.>")
	conn := startConnection(t, s)

	errBoom := errors.New("boom")
	c := newConsumer(t, conn, ordersConsumer(), nats.HandlerFunc(func(context.Context, *nats.Message) error {
		return errBoom
	}))
	bg := runInBackground(t, c)
	healthy(t, c)

	publishJS(t, s, "orders.x", "x")

	err := bg.wait(t)
	require.ErrorIs(t, err, nats.ErrStopped)
	require.ErrorIs(t, err, errBoom)

	// The naked message goes to the next instance at once.
	got := newReceived()
	next := newConsumer(t, startConnection(t, s), ordersConsumer(), got)
	runInBackground(t, next)

	assert.Equal(t, uint64(2), got.next(t).Metadata.NumDelivered)
}

func TestConsumer_StopFinishesInFlightAndNaksPrefetched(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	s.createStream(t, "ORDERS", "orders.>")
	conn := startConnection(t, s)

	inHandler := make(chan struct{}, 10)
	release := make(chan struct{})

	var handled atomic.Int32

	cfg := ordersConsumer()
	cfg.Prefetch = 5
	cfg.AckWait = time.Minute // a message not naked would not come back in the test
	c := newConsumer(t, conn, cfg, nats.HandlerFunc(func(ctx context.Context, _ *nats.Message) error {
		inHandler <- struct{}{}

		<-release
		handled.Add(1)

		return ctx.Err()
	}))

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() { done <- c.Run(ctx) }()

	healthy(t, c)

	for i := range 5 {
		publishJS(t, s, "orders.x", strconv.Itoa(i))
	}

	recv(t, inHandler)
	cancel()
	close(release)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(waitFor):
		t.Fatal("Run did not return")
	}

	assert.Equal(t, int32(1), handled.Load(), "only the message in flight is handled")

	// The handled one is acked, the others come back at once.
	got := newReceived()
	next := newConsumer(t, startConnection(t, s), cfg, got)
	runInBackground(t, next)

	seen := map[string]bool{}
	for range 4 {
		seen[string(got.next(t).Data)] = true
	}

	assert.Len(t, seen, 4)
	assert.False(t, seen["0"], "the first message was acked")
}

func TestConsumer_SurvivesServerRestart(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	s.createStream(t, "ORDERS", "orders.>")
	conn := startConnection(t, s)
	got := newReceived()
	c := newConsumer(t, conn, ordersConsumer(), got)
	runInBackground(t, c)
	healthy(t, c)

	s.restart()
	healthy(t, conn)

	publishJS(t, s, "orders.after", "x")
	assert.Equal(t, "orders.after", got.next(t).Subject)
}

func TestConsumer_ResubscribesWhenConsumerDeleted(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	s.createStream(t, "ORDERS", "orders.>")
	conn := startConnection(t, s)
	got := newReceived()
	c := newConsumer(t, conn, ordersConsumer(), got)
	runInBackground(t, c)
	healthy(t, c)

	require.NoError(t, s.jetStream(t).DeleteConsumer(t.Context(), "ORDERS", "billing"))

	require.Eventually(t, func() bool {
		_, err := s.jetStream(t).Consumer(t.Context(), "ORDERS", "billing")

		return err == nil
	}, waitFor, tick, "the consumer is created again")

	publishJS(t, s, "orders.x", "x")
	got.next(t)
}

func TestConsumer_ConnectionClosedWhileRunning(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	s.createStream(t, "ORDERS", "orders.>")
	conn := startConnection(t, s)
	c := newConsumer(t, conn, ordersConsumer(), newReceived())
	bg := runInBackground(t, c)
	healthy(t, c)

	conn.Conn().Close()

	require.ErrorIs(t, bg.wait(t), nats.ErrClosed)
}

func TestConsumer_NotStarted(t *testing.T) {
	t.Parallel()

	conn := newConnection(t, "nats://127.0.0.1:4222")
	c := newConsumer(t, conn, ordersConsumer(), newReceived())

	require.ErrorIs(t, c.Run(t.Context()), nats.ErrNotStarted)
}

func TestConsumer_ConnectionShutdownWhileRunning(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	s.createStream(t, "ORDERS", "orders.>")
	conn := startConnection(t, s)
	c := newConsumer(t, conn, ordersConsumer(), newReceived())
	bg := runInBackground(t, c)
	healthy(t, c)

	require.NoError(t, conn.Shutdown(t.Context()))
	require.ErrorIs(t, bg.wait(t), nats.ErrClosed)
}

func TestConsumer_KeepsLongHandlingInProgress(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	s.createStream(t, "ORDERS", "orders.>")
	conn := startConnection(t, s)

	var calls atomic.Int32

	got := newReceived()
	cfg := ordersConsumer()
	cfg.AckWait = 300 * time.Millisecond
	c := newConsumer(t, conn, cfg, nats.HandlerFunc(func(ctx context.Context, msg *nats.Message) error {
		calls.Add(1)

		// Longer than AckWait: the in-progress acks keep the server from
		// delivering the message again meanwhile.
		select {
		case <-time.After(4 * cfg.AckWait):
		case <-ctx.Done():
		}

		return got.Handle(ctx, msg)
	}))
	runInBackground(t, c)
	healthy(t, c)

	publishJS(t, s, "orders.x", "x")

	assert.Equal(t, uint64(1), got.next(t).Metadata.NumDelivered)
	settled(t, s, "ORDERS", "billing")
	assert.Equal(t, int32(1), calls.Load())
}

func TestConsumer_BackOffKeepsHandlingInProgress(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	s.createStream(t, "ORDERS", "orders.>")
	conn := startConnection(t, s)

	var calls atomic.Int32

	got := newReceived()
	backOff := 300 * time.Millisecond

	// BackOff replaces AckWait (30s here): the in-progress acks must follow
	// it, or the server delivers the message again mid-handling.
	c := newConsumer(t, conn, ordersConsumer(), nats.HandlerFunc(func(ctx context.Context, msg *nats.Message) error {
		calls.Add(1)

		select {
		case <-time.After(4 * backOff):
		case <-ctx.Done():
		}

		return got.Handle(ctx, msg)
	}), nats.WithConsumerConfig(func(cfg *jetstream.ConsumerConfig) {
		cfg.BackOff = []time.Duration{backOff, time.Second}
		cfg.MaxDeliver = 3
	}))
	runInBackground(t, c)
	healthy(t, c)

	publishJS(t, s, "orders.x", "x")

	assert.Equal(t, uint64(1), got.next(t).Metadata.NumDelivered)
	settled(t, s, "ORDERS", "billing")
	assert.Equal(t, int32(1), calls.Load())
}

func TestConsumer_RequeueWaitsForTheDelay(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	s.createStream(t, "ORDERS", "orders.>")
	conn := startConnection(t, s)

	const delay = 300 * time.Millisecond

	deliveries := make(chan time.Time, 2)
	c := newConsumerOnError(
		t,
		conn,
		ordersConsumer(),
		nats.HandlerFunc(func(_ context.Context, msg *nats.Message) error {
			deliveries <- time.Now()

			if msg.Metadata.NumDelivered == 1 {
				return errors.New("later")
			}

			return nil
		}),
		func(context.Context, *nats.Message, error) nats.ErrorAction {
			return nats.ErrorActionRequeue
		},
		nats.WithRedeliveryPolicy(retry.Policy{MaxAttempts: 1, InitialDelay: delay}),
	)
	runInBackground(t, c)
	healthy(t, c)

	publishJS(t, s, "orders.x", "x")

	first, second := recv(t, deliveries), recv(t, deliveries)
	assert.GreaterOrEqual(t, second.Sub(first), delay-50*time.Millisecond)
}

// MaxProcessingTime cancels a handler that takes too long; the message is
// naked and delivered again.
func TestConsumer_MaxProcessingTimeNaks(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	s.createStream(t, "ORDERS", "orders.>")
	conn := startConnection(t, s)

	cfg := ordersConsumer()
	cfg.AckWait = time.Minute // only the nak brings the message back in the test
	cfg.MaxProcessingTime = 100 * time.Millisecond

	got := newReceived()
	c := newConsumer(t, conn, cfg, nats.HandlerFunc(func(ctx context.Context, msg *nats.Message) error {
		if msg.Metadata.NumDelivered == 1 {
			<-ctx.Done() // hangs until the limit

			return ctx.Err()
		}

		return got.Handle(ctx, msg)
	}))
	runInBackground(t, c)
	healthy(t, c)

	publishJS(t, s, "orders.x", "x")

	assert.Equal(t, uint64(2), got.next(t).Metadata.NumDelivered)
	settled(t, s, "ORDERS", "billing")
}

// A handler that ignores its context still cannot hold the message: past
// MaxProcessingTime the consumer stops reporting it in progress, so the
// server delivers it again once AckWait expires.
func TestConsumer_MaxProcessingTimeStopsInProgress(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	s.createStream(t, "ORDERS", "orders.>")
	conn := startConnection(t, s)

	cfg := ordersConsumer()
	cfg.AckWait = 300 * time.Millisecond
	cfg.MaxProcessingTime = 100 * time.Millisecond
	cfg.Concurrency = 2

	release := make(chan struct{})

	t.Cleanup(func() { close(release) })

	got := newReceived()
	c := newConsumer(t, conn, cfg, nats.HandlerFunc(func(ctx context.Context, msg *nats.Message) error {
		if msg.Metadata.NumDelivered == 1 {
			<-release // hung: ignores ctx

			return nil
		}

		return got.Handle(ctx, msg)
	}))
	runInBackground(t, c)
	healthy(t, c)

	publishJS(t, s, "orders.x", "x")

	assert.Equal(t, uint64(2), got.next(t).Metadata.NumDelivered, "redelivered to the other worker")
}

func TestConsumerConfig_MaxProcessingTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ackWait time.Duration
		limit   time.Duration
		want    time.Duration
	}{
		{name: "default: 10 × the server's AckWait", want: 300 * time.Second},
		{name: "default: 10 × AckWait", ackWait: 2 * time.Second, want: 20 * time.Second},
		{name: "explicit", ackWait: 2 * time.Second, limit: time.Second, want: time.Second},
		{name: "negative: no limit", limit: -1, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := ordersConsumer()
			cfg.AckWait = tt.ackWait
			cfg.MaxProcessingTime = tt.limit

			require.NoError(t, cfg.Validate())
			assert.Equal(t, tt.want, nats.ProcessingLimit(cfg))
		})
	}
}

func TestConsumer_ShutdownDeadlineNaksInFlight(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	s.createStream(t, "ORDERS", "orders.>")
	conn := startConnection(t, s)
	inHandler := make(chan struct{})

	cfg := ordersConsumer()
	cfg.AckWait = time.Minute // only a nak brings the message back in the test
	c := newConsumer(t, conn, cfg, nats.HandlerFunc(func(ctx context.Context, _ *nats.Message) error {
		close(inHandler)
		<-ctx.Done()

		return ctx.Err()
	}))
	bg := runInBackground(t, c)
	healthy(t, c)

	publishJS(t, s, "orders.x", "x")
	recv(t, inHandler)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, c.Shutdown(ctx), context.DeadlineExceeded)
	require.NoError(t, bg.wait(t))

	got := newReceived()
	next := newConsumer(t, startConnection(t, s), cfg, got)
	runInBackground(t, next)

	assert.Equal(t, uint64(2), got.next(t).Metadata.NumDelivered)
}

func TestNewConsumer_Invalid(t *testing.T) {
	t.Parallel()

	conn := newConnection(t, "nats://127.0.0.1:4222")

	_, err := nats.NewConsumer(nil, ordersConsumer(), newReceived(), nats.Discard)
	require.ErrorIs(t, err, nats.ErrInvalidConfig)

	_, err = nats.NewConsumer(conn, ordersConsumer(), nil, nats.Discard)
	require.ErrorIs(t, err, nats.ErrInvalidConfig)

	_, err = nats.NewConsumer(conn, ordersConsumer(), newReceived(), nil)
	require.ErrorIs(t, err, nats.ErrInvalidConfig, "the error strategy is required")

	_, err = nats.NewConsumer(conn, nats.ConsumerConfig{}, newReceived(), nats.Discard)
	require.ErrorIs(t, err, nats.ErrInvalidConfig)

	_, err = nats.NewConsumer(conn, ordersConsumer(), newReceived(), nats.Discard,
		nats.WithStream(jetstream.StreamConfig{Name: "OTHER"}))
	require.ErrorIs(t, err, nats.ErrInvalidConfig)
}

func TestConsumerConfig_Validate(t *testing.T) {
	t.Parallel()

	valid := ordersConsumer()

	tests := []struct {
		name   string
		change func(*nats.ConsumerConfig)
		want   string
	}{
		{"valid", func(*nats.ConsumerConfig) {}, ""},
		{"no stream", func(c *nats.ConsumerConfig) { c.Stream = "" }, "stream must not be empty"},
		{"dot in stream", func(c *nats.ConsumerConfig) { c.Stream = "a.b" }, "stream"},
		{"no durable", func(c *nats.ConsumerConfig) { c.Durable = "" }, "durable must not be empty"},
		{"wildcard in durable", func(c *nats.ConsumerConfig) { c.Durable = "a*" }, "durable"},
		{"empty filter", func(c *nats.ConsumerConfig) { c.FilterSubjects = []string{""} }, "filterSubjects[0]"},
		{"negative ackWait", func(c *nats.ConsumerConfig) { c.AckWait = -time.Second }, "ackWait"},
		{"negative concurrency", func(c *nats.ConsumerConfig) { c.Concurrency = -1 }, "concurrency"},
		{"negative prefetch", func(c *nats.ConsumerConfig) { c.Prefetch = -1 }, "prefetch"},
		{"unlimited", func(c *nats.ConsumerConfig) { c.MaxDeliver, c.MaxAckPending = -1, -1 }, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := valid
			tt.change(&cfg)

			err := cfg.Validate()
			if tt.want == "" {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, nats.ErrInvalidConfig)
			require.ErrorContains(t, err, tt.want)
		})
	}
}
