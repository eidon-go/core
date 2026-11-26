package nats_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/nats"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
)

func TestPublisher_Contract(t *testing.T) {
	t.Parallel()

	s := runServer(t)

	lifecycletest.Resource(t, func(t *testing.T) lifecycle.Resource {
		t.Helper()

		return newPublisher(t, startConnection(t, s))
	})
}

func TestPublisher_PublishJetStream(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	s.createStream(t, "PAYMENTS", "payments.>")
	pub := newPublisher(t, startConnection(t, s), nats.WithName("payments-pub"))

	assert.Equal(t, "payments-pub", pub.Name())

	msg := &nats.Message{Subject: "payments.created", Data: []byte("1")}

	ack, err := pub.PublishJetStream(t.Context(), msg, jetstream.WithMsgID("payment-1"))
	require.NoError(t, err)
	assert.Equal(t, "PAYMENTS", ack.Stream)
	assert.Equal(t, uint64(1), ack.Sequence)
	assert.False(t, ack.Duplicate)

	ack, err = pub.PublishJetStream(t.Context(), msg, jetstream.WithMsgID("payment-1"))
	require.NoError(t, err)
	assert.True(t, ack.Duplicate, "the same message ID is dropped by the stream")

	_, err = pub.PublishJetStream(t.Context(), msg, jetstream.WithExpectLastSequence(7))
	require.Error(t, err, "the expectation fails")

	_, err = pub.PublishJetStream(t.Context(), &nats.Message{Subject: "nostream"})
	require.ErrorIs(t, err, jetstream.ErrNoStreamResponse)

	stream, err := s.jetStream(t).Stream(t.Context(), "PAYMENTS")
	require.NoError(t, err)

	stored, err := stream.GetMsg(t.Context(), 1)
	require.NoError(t, err)
	assert.Equal(t, "1", string(stored.Data))
}

func TestPublisher_InvalidMessage(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	pub := newPublisher(t, startConnection(t, s))

	require.ErrorIs(t, pub.Publish(t.Context(), nil), nats.ErrInvalidConfig)
	require.ErrorIs(t, pub.Publish(t.Context(), &nats.Message{}), nats.ErrInvalidConfig)

	_, err := pub.PublishJetStream(t.Context(), nil)
	require.ErrorIs(t, err, nats.ErrInvalidConfig)

	_, err = pub.Request(t.Context(), &nats.Message{})
	require.ErrorIs(t, err, nats.ErrInvalidConfig)
}

func TestPublisher_NotStartedAndClosed(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	conn := newConnection(t, s.clientURL())
	pub := newPublisher(t, conn)
	msg := &nats.Message{Subject: "x"}

	require.ErrorIs(t, pub.Publish(t.Context(), msg), nats.ErrNotStarted)

	_, err := pub.PublishJetStream(t.Context(), msg)
	require.ErrorIs(t, err, nats.ErrNotStarted)

	_, err = pub.Request(t.Context(), msg)
	require.ErrorIs(t, err, nats.ErrNotStarted)

	require.NoError(t, conn.Start(t.Context()))
	require.NoError(t, pub.Publish(t.Context(), msg))

	require.NoError(t, pub.Shutdown(t.Context()))
	require.ErrorIs(t, pub.Publish(t.Context(), msg), nats.ErrClosed)

	_, err = pub.PublishJetStream(t.Context(), msg)
	require.ErrorIs(t, err, nats.ErrClosed)
}

func TestPublisher_ConnectionClosed(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	conn := startConnection(t, s)
	pub := newPublisher(t, conn)

	require.NoError(t, conn.Shutdown(t.Context()))
	require.ErrorIs(t, pub.Publish(t.Context(), &nats.Message{Subject: "x"}), nats.ErrClosed)
	require.NoError(t, pub.Shutdown(t.Context()), "nothing to flush")
}

func TestPublisher_CancelledContext(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	pub := newPublisher(t, startConnection(t, s))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, pub.Publish(ctx, &nats.Message{Subject: "x"}), context.Canceled)
}

func TestPublisher_ShutdownWaitsForInFlight(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	conn := startConnection(t, s)
	pub := newPublisher(t, conn)

	// A request with no response in sight is a call in flight.
	nc := s.client(t)
	responder, err := nc.SubscribeSync("slow.service")
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	reqDone := make(chan error, 1)

	go func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), waitFor)
		defer cancel()

		_, err := pub.Request(ctx, &nats.Message{Subject: "slow.service"})
		reqDone <- err
	}()

	_, err = responder.NextMsg(waitFor)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, pub.Shutdown(ctx), context.DeadlineExceeded)
	require.NoError(t, conn.Shutdown(t.Context()))
	require.Error(t, recv(t, reqDone))
}
