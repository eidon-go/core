package nats_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/nats"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
	"github.com/eidon-go/core/retry"
)

func newSubscriber(
	t *testing.T, conn *nats.Connection, cfg nats.SubscriberConfig, h nats.Handler, opts ...nats.Option,
) *nats.Subscriber {
	t.Helper()

	opts = append([]nats.Option{
		nats.WithLogger(quietLogger()),
		nats.WithRedeliveryPolicy(retry.Policy{MaxAttempts: 3, InitialDelay: time.Millisecond}),
	}, opts...)

	s, err := nats.NewSubscriber(conn, cfg, h, opts...)
	require.NoError(t, err)

	return s
}

func TestSubscriber_Contract(t *testing.T) {
	t.Parallel()

	s := runServer(t)

	lifecycletest.Runner(t, func(t *testing.T) lifecycle.Runner {
		t.Helper()

		conn := startConnection(t, s)

		return newSubscriber(t, conn, nats.SubscriberConfig{Subject: "contract.>"}, newReceived())
	})
}

func TestSubscriber_Receives(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	conn := startConnection(t, s)
	got := newReceived()
	sub := newSubscriber(t, conn, nats.SubscriberConfig{Subject: "orders.*"}, got, nats.WithName("orders"))

	assert.Equal(t, "orders", sub.Name())
	require.ErrorIs(t, sub.Healthcheck(t.Context()), nats.ErrNotRunning)

	runInBackground(t, sub)
	healthy(t, sub)

	pub := newPublisher(t, conn)
	msg, err := nats.JSONMessage("orders.created", map[string]int{"id": 7})
	require.NoError(t, err)
	msg.Header.Set("X-Tenant", "acme")

	require.NoError(t, pub.Publish(t.Context(), msg, &nats.Message{Subject: "orders.paid", Data: []byte("2")}))

	first := got.next(t)
	assert.Equal(t, "orders.created", first.Subject)
	assert.JSONEq(t, `{"id":7}`, string(first.Data))
	assert.Equal(t, "acme", first.Header.Get("X-Tenant"))
	assert.Equal(t, nats.ContentTypeJSON, first.Header.Get("Content-Type"))
	assert.Nil(t, first.Metadata)

	assert.Equal(t, "orders.paid", got.next(t).Subject)
}

func TestSubscriber_QueueGroupSharesMessages(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	conn := startConnection(t, s)
	got := newReceived()
	cfg := nats.SubscriberConfig{Subject: "jobs", Queue: "workers"}

	for range 2 {
		sub := newSubscriber(t, conn, cfg, got)
		runInBackground(t, sub)
		healthy(t, sub)
	}

	pub := newPublisher(t, conn)

	for i := range 10 {
		require.NoError(t, pub.Publish(t.Context(), &nats.Message{Subject: "jobs", Data: fmt.Appendf(nil, "%d", i)}))
	}

	for range 10 {
		got.next(t)
	}

	got.none(t, 100*time.Millisecond)
}

func TestSubscriber_RequestRespond(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	conn := startConnection(t, s)

	sub := newSubscriber(t, conn, nats.SubscriberConfig{Subject: "echo"},
		nats.HandlerFunc(func(ctx context.Context, msg *nats.Message) error {
			return msg.Respond(ctx, &nats.Message{Data: append([]byte("re: "), msg.Data...)})
		}))
	runInBackground(t, sub)
	healthy(t, sub)

	pub := newPublisher(t, conn)

	ctx, cancel := context.WithTimeout(t.Context(), waitFor)
	defer cancel()

	resp, err := pub.Request(ctx, &nats.Message{Subject: "echo", Data: []byte("hi")})
	require.NoError(t, err)
	assert.Equal(t, "re: hi", string(resp.Data))

	require.ErrorIs(t, resp.Respond(ctx, &nats.Message{}), nats.ErrNoReply, "a response cannot be responded to")

	_, err = pub.Request(ctx, &nats.Message{Subject: "nobody.listens"})
	require.ErrorIs(t, err, nats.ErrNoResponders)
}

func TestSubscriber_RespondWithoutReply(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	conn := startConnection(t, s)
	errs := make(chan error, 1)

	sub := newSubscriber(t, conn, nats.SubscriberConfig{Subject: "noreply"},
		nats.HandlerFunc(func(ctx context.Context, msg *nats.Message) error {
			errs <- msg.Respond(ctx, &nats.Message{})

			return nil
		}))
	runInBackground(t, sub)
	healthy(t, sub)

	require.NoError(t, newPublisher(t, conn).Publish(t.Context(), &nats.Message{Subject: "noreply"}))
	require.ErrorIs(t, <-errs, nats.ErrNoReply)
}

func TestSubscriber_RetryThenStop(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	conn := startConnection(t, s)

	var attempts atomic.Int32

	errBoom := errors.New("boom")
	sub := newSubscriber(t, conn, nats.SubscriberConfig{Subject: "fail"},
		nats.HandlerFunc(func(context.Context, *nats.Message) error {
			attempts.Add(1)

			return errBoom
		}))
	bg := runInBackground(t, sub)
	healthy(t, sub)

	require.NoError(t, newPublisher(t, conn).Publish(t.Context(), &nats.Message{Subject: "fail"}))

	err := bg.wait(t)
	require.ErrorIs(t, err, nats.ErrStopped)
	require.ErrorIs(t, err, errBoom)
	require.ErrorIs(t, err, nats.ErrRetriesExhausted)
	assert.Equal(t, int32(3), attempts.Load())
}

func TestSubscriber_DiscardGoesOn(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	conn := startConnection(t, s)
	got := newReceived()

	sub := newSubscriber(t, conn, nats.SubscriberConfig{Subject: "mixed"},
		nats.JSON(func(ctx context.Context, _ struct{ N int }, msg *nats.Message) error {
			return got.Handle(ctx, msg)
		}), nats.WithErrorHandler(nats.Discard))
	runInBackground(t, sub)
	healthy(t, sub)

	pub := newPublisher(t, conn)
	require.NoError(t, pub.Publish(t.Context(),
		&nats.Message{Subject: "mixed", Data: []byte("not json")},
		&nats.Message{Subject: "mixed", Data: []byte(`{"N":1}`)},
	))

	assert.JSONEq(t, `{"N":1}`, string(got.next(t).Data))
	require.NoError(t, sub.Healthcheck(t.Context()))
}

func TestSubscriber_StopDrainsHeldMessages(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	conn := startConnection(t, s)

	release := make(chan struct{})
	inHandler := make(chan struct{}, 5)

	var handled atomic.Int32

	sub := newSubscriber(t, conn, nats.SubscriberConfig{Subject: "drain"},
		nats.HandlerFunc(func(ctx context.Context, _ *nats.Message) error {
			inHandler <- struct{}{}

			<-release
			handled.Add(1)

			return ctx.Err()
		}))

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() { done <- sub.Run(ctx) }()

	healthy(t, sub)

	pub := newPublisher(t, conn)
	for range 5 {
		require.NoError(t, pub.Publish(t.Context(), &nats.Message{Subject: "drain"}))
	}

	// The server delivers to a connection before it answers its ping: after
	// the flush the first message is in the handler, the others are held by
	// the client. The stop handles all of them.
	require.NoError(t, conn.Conn().Flush())
	<-inHandler

	cancel()
	close(release)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(waitFor):
		t.Fatal("Run did not return")
	}

	assert.Equal(t, int32(5), handled.Load())
	require.NoError(t, sub.Shutdown(t.Context()))
}

func TestSubscriber_ShutdownDeadlineCutsHandlerOff(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	conn := startConnection(t, s)
	inHandler := make(chan struct{})

	sub := newSubscriber(t, conn, nats.SubscriberConfig{Subject: "slow"},
		nats.HandlerFunc(func(ctx context.Context, _ *nats.Message) error {
			close(inHandler)
			<-ctx.Done()

			return ctx.Err()
		}))
	bg := runInBackground(t, sub)
	healthy(t, sub)

	require.NoError(t, newPublisher(t, conn).Publish(t.Context(), &nats.Message{Subject: "slow"}))
	<-inHandler

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, sub.Shutdown(ctx), context.DeadlineExceeded)
	require.NoError(t, bg.wait(t))
}

func TestSubscriber_ConnectionClosedWhileRunning(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	conn := startConnection(t, s)
	sub := newSubscriber(t, conn, nats.SubscriberConfig{Subject: "x"}, newReceived())
	bg := runInBackground(t, sub)
	healthy(t, sub)

	conn.Conn().Close()

	require.ErrorIs(t, bg.wait(t), nats.ErrClosed)
}

func TestSubscriber_NotStarted(t *testing.T) {
	t.Parallel()

	conn := newConnection(t, "nats://127.0.0.1:4222")
	sub := newSubscriber(t, conn, nats.SubscriberConfig{Subject: "x"}, newReceived())

	require.ErrorIs(t, sub.Run(t.Context()), nats.ErrNotStarted)
}

func TestSubscriber_ConnectionShutdownWhileRunning(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	conn := startConnection(t, s)
	sub := newSubscriber(t, conn, nats.SubscriberConfig{Subject: "x"}, newReceived())
	bg := runInBackground(t, sub)
	healthy(t, sub)

	require.NoError(t, conn.Shutdown(t.Context()))
	require.ErrorIs(t, bg.wait(t), nats.ErrClosed)
}

func TestSubscriber_ResubscribesAfterReconnect(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	conn := startConnection(t, s)
	got := newReceived()
	sub := newSubscriber(t, conn, nats.SubscriberConfig{Subject: "after.restart"}, got)
	runInBackground(t, sub)
	healthy(t, sub)

	s.restart()
	healthy(t, conn)

	nc := s.client(t)
	require.Eventually(t, func() bool {
		_ = nc.Publish("after.restart", []byte("x"))

		select {
		case <-got.ch:
			return true
		default:
			return false
		}
	}, waitFor, 50*time.Millisecond)
}

func TestNewSubscriber_Invalid(t *testing.T) {
	t.Parallel()

	conn := newConnection(t, "nats://127.0.0.1:4222")
	valid := nats.SubscriberConfig{Subject: "x"}

	_, err := nats.NewSubscriber(nil, valid, newReceived())
	require.ErrorIs(t, err, nats.ErrInvalidConfig)

	_, err = nats.NewSubscriber(conn, valid, nil)
	require.ErrorIs(t, err, nats.ErrInvalidConfig)

	_, err = nats.NewSubscriber(conn, nats.SubscriberConfig{}, newReceived())
	require.ErrorIs(t, err, nats.ErrInvalidConfig)

	var nilHandler nats.HandlerFunc

	_, err = nats.NewSubscriber(conn, valid, nilHandler)
	require.ErrorIs(t, err, nats.ErrInvalidConfig)
}

func TestSubscriberConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  nats.SubscriberConfig
		want string
	}{
		{"valid", nats.SubscriberConfig{Subject: "a.>", Queue: "q", Concurrency: 2}, ""},
		{"empty subject", nats.SubscriberConfig{}, "subject must not be empty"},
		{"space in subject", nats.SubscriberConfig{Subject: "a b"}, "white space"},
		{"space in queue", nats.SubscriberConfig{Subject: "a", Queue: "q q"}, "queue"},
		{"negative concurrency", nats.SubscriberConfig{Subject: "a", Concurrency: -1}, "concurrency"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.Validate()
			if tt.want == "" {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, nats.ErrInvalidConfig)
			require.ErrorContains(t, err, tt.want)
		})
	}
}
