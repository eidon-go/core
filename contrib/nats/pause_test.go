package nats_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/nats"
)

func TestConsumer_PauseEndsByItself(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	s.createStream(t, "ORDERS", "orders.>")
	conn := startConnection(t, s)
	got := newReceived()

	var fixed atomic.Bool

	c := newConsumerOnError(
		t,
		conn,
		ordersConsumer(),
		nats.HandlerFunc(func(ctx context.Context, msg *nats.Message) error {
			if !fixed.Load() {
				return errors.New("database schema is behind")
			}

			return got.Handle(ctx, msg)
		}),
		func(context.Context, *nats.Message, error) nats.ErrorAction { return nats.ErrorActionPause },
		nats.WithPauseFor(300*time.Millisecond),
	)
	runInBackground(t, c)
	healthy(t, c)

	publishJS(t, s, "orders.x", "x")

	require.Eventually(t, func() bool { return errors.Is(c.Healthcheck(t.Context()), nats.ErrPaused) },
		5*time.Second, 10*time.Millisecond)

	fixed.Store(true) // no Resume: the pause ends by itself

	assert.Equal(t, uint64(2), got.next(t).Metadata.NumDelivered, "delivered again after the pause")
	healthy(t, c)
	settled(t, s, "ORDERS", "billing")
}
