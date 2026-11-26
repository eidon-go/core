package nats_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/nats"
)

// existingConsumer creates the "billing" consumer of ORDERS with an AckWait
// of one minute, as another version of the service (or Terraform) would.
func existingConsumer(t *testing.T, s *testServer) {
	t.Helper()

	_, err := s.jetStream(t).CreateConsumer(t.Context(), "ORDERS", jetstream.ConsumerConfig{
		Durable:   "billing",
		AckPolicy: jetstream.AckExplicitPolicy,
		AckWait:   time.Minute,
	})
	require.NoError(t, err)
}

func ackWaitOnServer(t *testing.T, s *testServer) time.Duration {
	t.Helper()

	cons, err := s.jetStream(t).Consumer(t.Context(), "ORDERS", "billing")
	require.NoError(t, err)

	return cons.CachedInfo().Config.AckWait
}

func TestConsumer_Topology(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		opts        []nats.Option
		wantAckWait time.Duration
	}{
		{name: "create if missing leaves it", wantAckWait: time.Minute},
		{
			name:        "create or update updates it",
			opts:        []nats.Option{nats.WithTopology(nats.TopologyCreateOrUpdate)},
			wantAckWait: 2 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := runServer(t)
			s.createStream(t, "ORDERS", "orders.>")
			existingConsumer(t, s)

			cfg := ordersConsumer()
			cfg.AckWait = 2 * time.Minute

			c := newConsumer(t, startConnection(t, s), cfg, newReceived(), tt.opts...)
			runInBackground(t, c)
			healthy(t, c)

			assert.Equal(t, tt.wantAckWait, ackWaitOnServer(t, s))
		})
	}
}

func TestConsumer_TopologyCreatesWhatIsMissing(t *testing.T) {
	t.Parallel()

	s := runServer(t)
	stream := jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}}

	c := newConsumer(t, startConnection(t, s), ordersConsumer(), newReceived(), nats.WithStream(stream))
	runInBackground(t, c)
	healthy(t, c)

	assert.Equal(t, 30*time.Second, ackWaitOnServer(t, s), "the server's default")
}

func TestConsumer_TopologyVerify(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setup         func(t *testing.T, s *testServer)
		ackWait       time.Duration
		maxAckPending int
		opts          []nats.Option
		wantErr       string
	}{
		{name: "missing consumer", setup: func(*testing.T, *testServer) {}, wantErr: "does not exist"},
		{name: "different ack wait", setup: existingConsumer, ackWait: 2 * time.Minute, wantErr: "ack wait 1m0s"},
		{
			name: "different max ack pending", setup: existingConsumer, ackWait: time.Minute,
			maxAckPending: 7, wantErr: "max ack pending",
		},
		{
			name: "different backoff", setup: existingConsumer, ackWait: time.Minute,
			opts: []nats.Option{nats.WithConsumerConfig(func(cc *jetstream.ConsumerConfig) {
				cc.BackOff = []time.Duration{time.Second, 5 * time.Second}
			})},
			wantErr: "backoff",
		},
		{name: "matching", setup: existingConsumer, ackWait: time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := runServer(t)
			s.createStream(t, "ORDERS", "orders.>")
			tt.setup(t, s)

			cfg := ordersConsumer()
			cfg.AckWait = tt.ackWait
			cfg.MaxAckPending = tt.maxAckPending

			opts := append([]nats.Option{nats.WithTopology(nats.TopologyVerify)}, tt.opts...)
			c := newConsumer(t, startConnection(t, s), cfg, newReceived(), opts...)

			if tt.wantErr == "" {
				runInBackground(t, c)
				healthy(t, c)

				return
			}

			ctx, cancel := context.WithTimeout(t.Context(), waitFor)
			defer cancel()

			err := c.Run(ctx)
			require.ErrorIs(t, err, nats.ErrTopology)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestTopologyMode_String(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{"create-if-missing", "create-or-update", "verify", "topology(9)"}, []string{
		nats.TopologyCreateIfMissing.String(), nats.TopologyCreateOrUpdate.String(),
		nats.TopologyVerify.String(), nats.TopologyMode(9).String(),
	})
}
