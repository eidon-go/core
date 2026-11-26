package rabbitmq

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The reason of a close may come late: closeErr waits for it until its ctx
// is done, however long that is, and a caller whose ctx ends first is not
// held by the one that waits.
func TestConfirmChannel_CloseErrWaitsForTheReason(t *testing.T) {
	t.Parallel()

	closes := make(chan *amqp.Error, 1)
	c := newCloseState(nil, closes)

	patient := make(chan error, 1)

	go func() { patient <- c.closeErr(t.Context()) }()

	// A caller with a short deadline returns at it, while the other one
	// holds the listener.
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := c.closeErr(ctx)

	assert.Less(t, time.Since(start), time.Second)
	require.ErrorIs(t, err, ErrNotConfirmed)

	var amqpErr *amqp.Error
	require.NotErrorAs(t, err, &amqpErr, "no reason yet")

	// Later than any fixed bound would have waited: the delay is what is
	// tested, not a synchronization.
	time.Sleep(1100 * time.Millisecond)

	closes <- &amqp.Error{Code: amqp.NotFound, Reason: "late"}

	close(closes)

	err = receive(t, patient)
	require.ErrorAs(t, err, &amqpErr)
	assert.Equal(t, "late", amqpErr.Reason)

	// Every later caller gets the same reason at once.
	require.ErrorAs(t, c.closeErr(t.Context()), &amqpErr)
}

// A close without a reason (by the client) ends the wait with
// ErrNotConfirmed alone.
func TestConfirmChannel_CloseErrWithoutReason(t *testing.T) {
	t.Parallel()

	closes := make(chan *amqp.Error, 1)
	close(closes)

	c := newCloseState(nil, closes)

	err := c.closeErr(t.Context())
	require.ErrorIs(t, err, ErrNotConfirmed)

	var amqpErr *amqp.Error
	assert.NotErrorAs(t, err, &amqpErr)
	require.ErrorIs(t, c.closeErr(t.Context()), ErrNotConfirmed)
}
