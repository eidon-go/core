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

func TestPauser_Lengths(t *testing.T) {
	t.Parallel()

	t.Run("without WithPauseFor a pause has no length", func(t *testing.T) {
		t.Parallel()

		assert.Zero(t, nats.NewPauser(0).Begin(time.Now()))
	})

	t.Run("doubles after a quick automatic end, up to MaxPause", func(t *testing.T) {
		t.Parallel()

		p := nats.NewPauser(5 * time.Minute)
		now := time.Now()

		got := make([]time.Duration, 0, 4)

		for range 4 {
			length := p.Begin(now)
			got = append(got, length)
			now = now.Add(length)
			p.End(now, true)
			now = now.Add(time.Second) // paused again soon after
		}

		assert.Equal(t, []time.Duration{5 * time.Minute, 10 * time.Minute, nats.MaxPause, nats.MaxPause}, got)
	})

	t.Run("begins once while active", func(t *testing.T) {
		t.Parallel()

		p := nats.NewPauser(time.Second)
		now := time.Now()

		assert.Equal(t, time.Second, p.Begin(now))
		p.End(now.Add(time.Second), true)

		now = now.Add(1500 * time.Millisecond)
		assert.Equal(t, 2*time.Second, p.Begin(now))
		assert.Equal(t, 2*time.Second, p.Begin(now), "a second handler joins the same pause")
	})

	t.Run("resets after a manual resume or a long run", func(t *testing.T) {
		t.Parallel()

		p := nats.NewPauser(time.Second)
		now := time.Now()

		p.Begin(now)
		p.End(now, false)
		assert.Equal(t, time.Second, p.Begin(now), "Resume does not count")

		p.End(now, true)
		assert.Equal(t, time.Second, p.Begin(now.Add(time.Minute)), "ran longer than the pause")
	})

	t.Run("a base above MaxPause is kept", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, time.Hour, nats.NewPauser(time.Hour).Begin(time.Now()))
	})
}

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
