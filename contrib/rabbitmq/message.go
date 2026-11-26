package rabbitmq

import (
	"context"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Message is an AMQP message: what a Publisher sends and what a Handler
// receives.
type Message struct {
	// Exchange to publish to; empty is the default exchange, which routes
	// to the queue named by RoutingKey. Set by the broker on a consumed
	// message.
	Exchange string

	// RoutingKey routes the message to the queues bound to Exchange.
	RoutingKey string

	// Body is the payload.
	Body []byte

	// Headers are the application headers. The value types AMQP tables
	// allow: string, []byte, bool, int, int8, int16, int32, int64, uint8,
	// uint16, uint32, float32, float64, time.Time, amqp.Decimal, nil, and
	// nested map[string]any (or amqp.Table) and []any of these — not uint
	// or uint64. Publish fails on another type. The trace context is added
	// to them when publishing (the caller's map is not changed). On a
	// consumed message nested tables are amqp.Table.
	Headers map[string]any

	// ContentType and ContentEncoding describe Body ("application/json").
	ContentType     string
	ContentEncoding string

	// Transient messages are not written to disk by durable queues. The
	// zero value publishes persistent messages.
	Transient bool

	// Priority is 0–9 (queues declared with "x-max-priority").
	Priority uint8

	// CorrelationID, ReplyTo, MessageID, Type, UserID and AppID are the AMQP
	// properties of the same names. UserID, if set, must be the user of the
	// connection.
	CorrelationID string
	ReplyTo       string
	MessageID     string
	Type          string
	UserID        string
	AppID         string

	// Expiration is the per-message TTL in milliseconds, as a decimal
	// string ("60000").
	Expiration string

	// Timestamp is the AMQP timestamp property (second precision).
	Timestamp time.Time

	// Redelivered is set on a consumed message that was delivered before
	// (requeued, or not acked by a consumer that stopped). Ignored when
	// publishing.
	Redelivered bool

	// ConsumerTag is the tag of the consumer that received the message.
	// Ignored when publishing.
	ConsumerTag string
}

const deliveryModePersistent = amqp.Persistent

// publishing converts msg for amqp091, with headers (the trace context
// added) instead of msg.Headers.
func (m *Message) publishing(headers amqp.Table) amqp.Publishing {
	mode := amqp.Persistent
	if m.Transient {
		mode = amqp.Transient
	}

	return amqp.Publishing{
		Headers:         headers,
		ContentType:     m.ContentType,
		ContentEncoding: m.ContentEncoding,
		DeliveryMode:    mode,
		Priority:        m.Priority,
		CorrelationId:   m.CorrelationID,
		ReplyTo:         m.ReplyTo,
		Expiration:      m.Expiration,
		MessageId:       m.MessageID,
		Timestamp:       m.Timestamp,
		Type:            m.Type,
		UserId:          m.UserID,
		AppId:           m.AppID,
		Body:            m.Body,
	}
}

func messageFromDelivery(d *amqp.Delivery) *Message {
	return &Message{
		Exchange:        d.Exchange,
		RoutingKey:      d.RoutingKey,
		Body:            d.Body,
		Headers:         d.Headers,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		Transient:       d.DeliveryMode != deliveryModePersistent,
		Priority:        d.Priority,
		CorrelationID:   d.CorrelationId,
		ReplyTo:         d.ReplyTo,
		MessageID:       d.MessageId,
		Type:            d.Type,
		UserID:          d.UserId,
		AppID:           d.AppId,
		Expiration:      d.Expiration,
		Timestamp:       d.Timestamp,
		Redelivered:     d.Redelivered,
		ConsumerTag:     d.ConsumerTag,
	}
}

// Handler handles the messages of a Consumer. It returns nil once the
// message is handled (it is then acked); an error goes to the Consumer's
// ErrorHandler. With ConsumerConfig.Concurrency above 1 it is called
// concurrently. ctx is the consumer's work context: it is not cancelled when
// the consumer stops — the message in flight finishes — only at the deadline
// of its Shutdown. It carries the trace span of the message. A retry in
// place (ErrorActionRetry) passes the same *Message again, with the changes
// the handler made to it.
type Handler interface {
	Handle(ctx context.Context, msg *Message) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, msg *Message) error

// Handle calls f.
func (f HandlerFunc) Handle(ctx context.Context, msg *Message) error { return f(ctx, msg) }

// ErrorAction is what the Consumer does with a message whose handler failed;
// an ErrorHandler returns it.
type ErrorAction int

const (
	// ErrorActionRetry handles the message again after the delay of the
	// redelivery policy (WithRedeliveryPolicy), in the same worker, which
	// holds the message meanwhile. A stop during the delay ends the wait;
	// the message is requeued. Once the policy allows no more attempts, the
	// ErrorHandler is called again with ErrRetriesExhausted joined to the
	// error; Retry is then treated as Stop.
	ErrorActionRetry ErrorAction = iota

	// ErrorActionRequeue returns the message to its queue (basic.nack with
	// requeue), from where it is delivered again, to this or another
	// consumer, with Redelivered set. The worker first waits the redelivery
	// policy's delay for the message's delivery number (a quorum queue's
	// x-delivery-count; otherwise 1, or 2 once redelivered), so a failing
	// message does not come straight back in a hot loop; a stop during the
	// wait leaves it to be requeued when the channel closes. Bound it with a
	// quorum queue's "x-delivery-limit" and a dead-letter exchange.
	ErrorActionRequeue

	// ErrorActionReject rejects the message (basic.nack without requeue):
	// the broker moves it to the queue's dead-letter exchange
	// ("x-dead-letter-exchange") — or drops it, if the queue has none.
	ErrorActionReject

	// ErrorActionStop stops the consumer: Run returns an error wrapping
	// ErrStopped and the handler's error, and app.App shuts the service
	// down. The message is not acked; the broker requeues it when the
	// channel closes. Use it when going on would lose data.
	ErrorActionStop

	// ErrorActionPause pauses the consumer: it cancels its subscription
	// (the message is not acked and the broker requeues it when the channel
	// closes), Healthcheck reports ErrPaused, and the service keeps running.
	// Consumer.Resume (or a restart) subscribes again; meanwhile the message
	// may go to another consumer of the queue.
	ErrorActionPause
)

// String returns the action's name.
func (a ErrorAction) String() string {
	switch a { //nolint:exhaustive // actionAck is internal
	case ErrorActionRetry:
		return "retry"
	case ErrorActionRequeue:
		return "requeue"
	case ErrorActionReject:
		return "reject"
	case ErrorActionStop:
		return "stop"
	case ErrorActionPause:
		return "pause"
	default:
		return "unknown"
	}
}

// ErrorHandler decides what to do with a message whose handler returned err
// (with ErrRetriesExhausted joined once the redelivery policy allows no
// more attempts). ctx is the work context of the message. An unknown action
// or a panic stops the consumer.
type ErrorHandler func(ctx context.Context, msg *Message, err error) ErrorAction
