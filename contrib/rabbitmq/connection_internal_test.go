package rabbitmq

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
)

func TestConnection_Lifecycle(t *testing.T) {
	t.Parallel()

	lifecycletest.Resource(t, func(t *testing.T) lifecycle.Resource {
		t.Helper()

		return newTestConnection(t, newFakeBroker(), false)
	})
}

func TestConnection_StartRetriesUntilTheBrokerAnswers(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	broker.failDials = 3
	conn := newTestConnection(t, broker, false)

	require.NoError(t, conn.Start(t.Context()))
	require.NoError(t, conn.Healthcheck(t.Context()))
	assert.Equal(t, 4, broker.dialCount())
}

func TestConnection_StartFailsWhenTheRetriesAreExhausted(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	broker.setDialErr(errBroker)

	policy := fastRetry()
	policy.MaxAttempts = 2
	conn := newTestConnection(t, broker, false, WithConnectRetry(policy))

	err := conn.Start(t.Context())
	require.ErrorIs(t, err, errBroker)
	assert.Equal(t, 2, broker.dialCount())
}

func TestConnection_ShutdownEndsStartRetries(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	broker.setDialErr(errBroker)
	conn := newTestConnection(t, broker, false)

	started := make(chan error, 1)

	go func() { started <- conn.Start(t.Context()) }()

	require.Eventually(t, func() bool { return broker.dialCount() > 1 }, 5*time.Second, time.Millisecond)
	require.NoError(t, conn.Shutdown(t.Context()))
	require.ErrorIs(t, <-started, ErrClosed)
	require.ErrorIs(t, conn.Start(t.Context()), ErrClosed)
}

func TestConnection_StartTwice(t *testing.T) {
	t.Parallel()

	conn := newTestConnection(t, newFakeBroker(), true)

	require.ErrorIs(t, conn.Start(t.Context()), ErrAlreadyStarted)
}

func TestConnection_Healthcheck(t *testing.T) {
	t.Parallel()

	conn := newTestConnection(t, newFakeBroker(), false)

	require.ErrorIs(t, conn.Healthcheck(t.Context()), ErrNotStarted)
	require.NoError(t, conn.Start(t.Context()))
	require.NoError(t, conn.Healthcheck(t.Context()))
	require.NoError(t, conn.Shutdown(t.Context()))
	require.ErrorIs(t, conn.Healthcheck(t.Context()), ErrClosed)
}

func TestConnection_Reconnects(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	conn := newTestConnection(t, broker, true)

	broker.setDialErr(errBroker)
	broker.drop()

	require.Eventually(t, func() bool {
		return conn.Healthcheck(t.Context()) != nil
	}, 5*time.Second, time.Millisecond)
	require.ErrorIs(t, conn.Healthcheck(t.Context()), ErrNotConnected)

	dials := broker.dialCount()
	broker.setDialErr(nil)

	require.Eventually(t, func() bool {
		return conn.Healthcheck(t.Context()) == nil
	}, 5*time.Second, time.Millisecond)
	assert.Greater(t, broker.dialCount(), dials)

	// The new connection is usable.
	ch, err := conn.channel(t.Context())
	require.NoError(t, err)
	require.NoError(t, ch.close())
}

func TestConnection_ChannelWaitsForTheConnection(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	conn := newTestConnection(t, broker, true)

	broker.setDialErr(errBroker)
	broker.drop()

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	_, err := conn.channel(ctx)
	require.ErrorIs(t, err, ErrNotConnected)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	got := make(chan error, 1)

	go func() {
		ch, err := conn.channel(t.Context())
		if err == nil {
			_ = ch.close()
		}

		got <- err
	}()

	broker.setDialErr(nil)
	require.NoError(t, <-got)
}

func TestConnection_ChannelStates(t *testing.T) {
	t.Parallel()

	conn := newTestConnection(t, newFakeBroker(), false)

	_, err := conn.channel(t.Context())
	require.ErrorIs(t, err, ErrNotStarted)

	require.NoError(t, conn.Start(t.Context()))
	require.NoError(t, conn.Shutdown(t.Context()))

	_, err = conn.channel(t.Context())
	require.ErrorIs(t, err, ErrClosed)
}

func TestConnection_ShutdownWakesUpWaiters(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	conn := newTestConnection(t, broker, true)

	broker.setDialErr(errBroker)
	broker.drop()

	got := make(chan error, 1)

	go func() {
		_, err := conn.channel(t.Context())
		got <- err
	}()

	require.NoError(t, conn.Shutdown(t.Context()))
	require.ErrorIs(t, <-got, ErrClosed)
}

func TestNewConnection_InvalidConfig(t *testing.T) {
	t.Parallel()

	_, err := NewConnection(Config{})
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestConnection_Name(t *testing.T) {
	t.Parallel()

	conn := newTestConnection(t, newFakeBroker(), false)
	assert.Equal(t, DefaultName, conn.Name())

	named := newTestConnection(t, newFakeBroker(), false, WithName("rabbitmq-publish"))
	assert.Equal(t, "rabbitmq-publish", named.Name())
	assert.Equal(t, "rabbitmq-publish", named.amqpCfg.Properties["connection_name"])
}
