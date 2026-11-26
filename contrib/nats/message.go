package nats

import (
	"context"
	"fmt"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Message is a NATS message: what a Publisher sends and what a Handler
// receives.
type Message struct {
	// Subject to publish to; required. On a received message, the subject it
	// was published to.
	Subject string

	// Reply is the subject to respond to. When publishing, it asks the
	// receiver to respond there (Publisher.Request sets its own). On a
	// message from a Subscriber, Respond answers it; on a message from a
	// Consumer it is empty.
	Reply string

	// Data is the payload.
	Data []byte

	// Header holds the message headers. The trace context is added to them
	// when publishing (the caller's map is not changed).
	Header natsgo.Header

	// Metadata is set on a message from a Consumer: its stream, its stream
	// and consumer sequences, how many times it was delivered. Nil on a
	// message from a Subscriber; ignored when publishing.
	Metadata *jetstream.MsgMetadata

	// respond publishes a response to Reply; set by a Subscriber.
	respond func(ctx context.Context, resp *Message) error
}

// Respond publishes resp to the reply subject of m — the answer to a
// request (Publisher.Request on the other side). resp.Subject is set to
// m.Reply. It returns ErrNoReply when m has no reply subject or did not
// come from a Subscriber.
func (m *Message) Respond(ctx context.Context, resp *Message) error {
	if m.respond == nil || m.Reply == "" {
		return ErrNoReply
	}

	if resp == nil {
		return fmt.Errorf("%w: response is nil", ErrInvalidConfig)
	}

	resp.Subject = m.Reply

	return m.respond(ctx, resp)
}

// natsMsg converts m for nats.go, with header (the trace context added)
// instead of m.Header.
func (m *Message) natsMsg(header natsgo.Header) *natsgo.Msg {
	return &natsgo.Msg{ //nolint:exhaustruct_v5 // Sub is for received messages
		Subject: m.Subject,
		Reply:   m.Reply,
		Data:    m.Data,
		Header:  header,
	}
}

func messageFromNATS(msg *natsgo.Msg) *Message {
	return &Message{ //nolint:exhaustruct_v5 // Metadata and respond are set by the caller
		Subject: msg.Subject,
		Reply:   msg.Reply,
		Data:    msg.Data,
		Header:  msg.Header,
	}
}

// messageFromJetStream converts a message of a JetStream consumer. Its reply
// subject is the ack subject, which the Consumer settles: it is not exposed.
func messageFromJetStream(msg jetstream.Msg) *Message {
	meta, err := msg.Metadata()
	if err != nil {
		meta = nil
	}

	return &Message{ //nolint:exhaustruct_v5 // no reply subject
		Subject:  msg.Subject(),
		Data:     msg.Data(),
		Header:   msg.Headers(),
		Metadata: meta,
	}
}

// validateMessage checks a message to publish.
func validateMessage(m *Message) error {
	switch {
	case m == nil:
		return fmt.Errorf("%w: message is nil", ErrInvalidConfig)
	case m.Subject == "":
		return fmt.Errorf("%w: message subject is empty", ErrInvalidConfig)
	default:
		return nil
	}
}

// Handler handles the messages of a Subscriber or a Consumer. It returns
// nil once the message is handled (a Consumer then acks it); an error goes
// to the ErrorHandler. With Concurrency above 1 it is called concurrently.
// ctx is the work context: it is not cancelled when the component stops —
// the message in flight finishes — only at the deadline of its Shutdown. It
// carries the trace span of the message. A retry in place
// (ErrorActionRetry) passes the same *Message again, with the changes the
// handler made to it.
type Handler interface {
	Handle(ctx context.Context, msg *Message) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, msg *Message) error

// Handle calls f.
func (f HandlerFunc) Handle(ctx context.Context, msg *Message) error { return f(ctx, msg) }

// ErrorAction is what is done with a message whose handler failed; an
// ErrorHandler returns it.
type ErrorAction int

const (
	// ErrorActionRetry handles the message again after the delay of the
	// redelivery policy (WithRedeliveryPolicy), in the same worker, which
	// holds the message meanwhile (a Consumer keeps telling the server it is
	// in progress, so that its AckWait does not expire). A stop during the
	// delay ends the wait; a Consumer then naks the message. Once the policy
	// allows no more attempts, the ErrorHandler is called again with
	// ErrRetriesExhausted joined to the error; Retry is then treated as
	// Stop.
	ErrorActionRetry ErrorAction = iota

	// ErrorActionRequeue gives the message back for a later delivery. A
	// Consumer naks it with the delay the redelivery policy gives for its
	// delivery count: the server delivers it again — to this or another
	// instance — after that delay, until the consumer's MaxDeliver. A
	// Subscriber cannot: core NATS has no redelivery, and the message is
	// dropped (logged).
	ErrorActionRequeue

	// ErrorActionReject gives up on the message. A Consumer terminates it
	// (AckTerm): it is never delivered again, and the server publishes a
	// MSG_TERMINATED advisory ($JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.
	// <stream>.<consumer>) that a dead-letter flow can capture. A Subscriber
	// drops it (logged).
	ErrorActionReject

	// ErrorActionStop stops the component: Run returns an error wrapping
	// ErrStopped and the handler's error, and app.App shuts the service
	// down. A Consumer naks the message, so that it is delivered again —
	// to another instance — at once. Use it when going on would lose data.
	ErrorActionStop
)

// String returns the action's name.
func (a ErrorAction) String() string {
	switch a {
	case ErrorActionRetry:
		return "retry"
	case ErrorActionRequeue:
		return "requeue"
	case ErrorActionReject:
		return "reject"
	case ErrorActionStop:
		return "stop"
	default:
		return "unknown"
	}
}

// ErrorHandler decides what to do with a message whose handler returned err
// (with ErrRetriesExhausted joined once the redelivery policy allows no
// more attempts). ctx is the work context of the message. An unknown action
// or a panic stops the component.
type ErrorHandler func(ctx context.Context, msg *Message, err error) ErrorAction
