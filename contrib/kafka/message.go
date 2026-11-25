package kafka

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Message is a Kafka record: what a Handler receives and what a Publisher
// sends.
type Message struct {
	// Topic is the record's topic. Required when publishing.
	Topic string

	// Partition and Offset locate a consumed record; they are ignored when
	// publishing (the partitioner picks the partition, by Key).
	Partition int32
	Offset    int64

	// Key is the record key: records with the same key go to the same
	// partition and keep their order.
	Key []byte

	// Value is the record payload.
	Value []byte

	// Headers are the record headers, in order; keys may repeat.
	Headers []Header

	// Timestamp is the record timestamp. Zero when publishing means the
	// time of the publish.
	Timestamp time.Time
}

// Header is a record header.
type Header struct {
	Key   string
	Value []byte
}

// Handler handles the messages of a Consumer, one at a time, in partition
// order. It returns nil once the message is handled (it is then committed);
// an error goes to the Consumer's ErrorHandler. ctx is the consumer's work
// context: it is not cancelled when the consumer stops — the message in
// flight finishes — only at the deadline of its Shutdown. It carries the
// trace span of the message.
type Handler interface {
	Handle(ctx context.Context, msg *Message) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, msg *Message) error

// Handle calls f.
func (f HandlerFunc) Handle(ctx context.Context, msg *Message) error { return f(ctx, msg) }

// Publisher publishes messages; *Producer implements it. Depend on it rather
// than on *Producer to swap in a fake in tests.
type Publisher interface {
	// Publish sends msgs and waits until every one is acknowledged or ctx is
	// done. It returns the first error.
	Publish(ctx context.Context, msgs ...*Message) error
}

// ErrorAction is what the Consumer does with a message whose handler failed;
// an ErrorHandler returns it.
type ErrorAction int

const (
	// ErrorActionRetry handles the message again after the delay of the
	// redelivery policy (WithRedeliveryPolicy). The consumer blocks on it,
	// which keeps the partition's order. A stop during the delay ends the
	// wait; the message is not committed and is redelivered after a restart
	// or rebalance. Once the policy allows no more attempts, the
	// ErrorHandler is called again with ErrRetriesExhausted joined to the
	// error; Retry is then treated as Stop.
	ErrorActionRetry ErrorAction = iota

	// ErrorActionSkip commits the message as handled and goes on — after
	// the ErrorHandler has saved it elsewhere (a dead-letter topic, see
	// DeadLetter), or for messages that may be dropped.
	ErrorActionSkip

	// ErrorActionStop stops the consumer: Run returns an error wrapping
	// ErrStopped and the handler's error, and app.App shuts the service down.
	// The message is not committed (the ones handled before it are), so it
	// is redelivered once the service is back. Use it when going on would
	// lose or reorder data.
	ErrorActionStop
)

// String returns the action's name.
func (a ErrorAction) String() string {
	switch a {
	case ErrorActionRetry:
		return "retry"
	case ErrorActionSkip:
		return "skip"
	case ErrorActionStop:
		return "stop"
	default:
		return "unknown"
	}
}

// ErrorHandler decides what to do with a message whose handler returned err
// (with ErrRetriesExhausted joined once the redelivery policy allows no
// more attempts). ctx is the work context of the message. An unknown action
// is treated as Stop.
type ErrorHandler func(ctx context.Context, msg *Message, err error) ErrorAction

// messageFromRecord copies the record's fields into a Message. The slices
// are franz-go's own, not reused after the record is returned.
func messageFromRecord(r *kgo.Record) *Message {
	msg := &Message{
		Topic:     r.Topic,
		Partition: r.Partition,
		Offset:    r.Offset,
		Key:       r.Key,
		Value:     r.Value,
		Headers:   nil,
		Timestamp: r.Timestamp,
	}

	if len(r.Headers) > 0 {
		msg.Headers = make([]Header, len(r.Headers))
		for i, h := range r.Headers {
			msg.Headers[i] = Header{Key: h.Key, Value: h.Value}
		}
	}

	return msg
}

// record builds the record to produce for msg.
func (m *Message) record(ctx context.Context) *kgo.Record {
	r := &kgo.Record{ //nolint:exhaustruct_v5 // the rest is set by the client
		Topic:     m.Topic,
		Key:       m.Key,
		Value:     m.Value,
		Timestamp: m.Timestamp,
		Context:   ctx,
	}

	if len(m.Headers) > 0 {
		r.Headers = make([]kgo.RecordHeader, len(m.Headers))
		for i, h := range m.Headers {
			r.Headers[i] = kgo.RecordHeader{Key: h.Key, Value: h.Value}
		}
	}

	return r
}
