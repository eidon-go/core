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

	// The patient caller holds the listener before the impatient one starts.
	require.Eventually(t, func() bool { return len(c.reader) == 1 }, testWait, time.Millisecond)

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

// A send that gave up waiting for the reason (its ctx ended) gets it added
// by Publish once it is known, read before or still in the listener.
func TestWithCloseReason(t *testing.T) {
	t.Parallel()

	closes := make(chan *amqp.Error, 1)
	c := newCloseState(nil, closes)

	expired, cancel := context.WithCancel(t.Context())
	cancel()

	err := c.closeErr(expired)
	assert.Equal(t, ErrNotConfirmed, err, "no reason yet")
	assert.Equal(t, err, withCloseReason(err, c), "still none")

	closes <- &amqp.Error{Code: amqp.NotFound, Reason: "late"}

	close(closes)

	var amqpErr *amqp.Error

	got := withCloseReason(err, c)
	require.ErrorIs(t, got, ErrNotConfirmed)
	require.ErrorAs(t, got, &amqpErr)
	assert.Equal(t, "late", amqpErr.Reason)

	// Other errors, and ones that already carry a reason, are left alone.
	assert.Equal(t, ErrNacked, withCloseReason(ErrNacked, c))
	assert.Equal(t, got, withCloseReason(got, c))
	assert.Equal(t, err, withCloseReason(err, nil))
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
