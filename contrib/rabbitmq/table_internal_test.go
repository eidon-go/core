package rabbitmq

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// amqp091 takes nested tables only as amqp.Table: nested map[string]any
// values, also inside arrays, are converted; the input is not changed.
func TestToTable(t *testing.T) {
	t.Parallel()

	in := map[string]any{
		"tenant":   "acme",
		"metadata": map[string]any{"region": "eu", "tags": []any{map[string]any{"k": "v"}, int32(1)}},
		"when":     time.Unix(0, 0),
	}

	out := toTable(in, 1)
	require.NoError(t, out.Validate())

	nested, ok := out["metadata"].(amqp.Table)
	require.True(t, ok, "a nested map becomes an amqp.Table")

	tags, ok := nested["tags"].([]any)
	require.True(t, ok)
	assert.IsType(t, amqp.Table{}, tags[0], "maps inside arrays too")

	assert.IsType(t, map[string]any{}, in["metadata"], "the input is not changed")
	assert.Nil(t, toTable(nil, 0))
	assert.NotNil(t, toTable(nil, 1))
}

func TestPublisher_Headers(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	pub := newTestPublisher(t, newTestConnection(t, broker, true))

	nested := &Message{RoutingKey: "q", Headers: map[string]any{"metadata": map[string]any{"tenant": "acme"}}}
	require.NoError(t, pub.Publish(t.Context(), nested))

	got := broker.publishedMessages()
	require.Len(t, got, 1)
	assert.IsType(t, amqp.Table{}, got[0].Headers["metadata"])

	// A type AMQP cannot carry fails before anything is sent.
	err := pub.Publish(t.Context(), &Message{RoutingKey: "q", Headers: map[string]any{"n": uint64(1)}})
	require.ErrorIs(t, err, ErrInvalidConfig)
	assert.Len(t, broker.publishedMessages(), 1)
}
