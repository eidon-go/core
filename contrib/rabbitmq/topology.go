package rabbitmq

import (
	"errors"
	"fmt"
	"maps"
	"strings"
)

// Exchange kinds built into RabbitMQ.
const (
	ExchangeDirect  = "direct"
	ExchangeFanout  = "fanout"
	ExchangeTopic   = "topic"
	ExchangeHeaders = "headers"
)

// Exchange declares an exchange (exchange.declare). Declaring an existing
// exchange with other settings fails with a channel error (406
// PRECONDITION_FAILED).
type Exchange struct {
	// Name of the exchange. Required.
	Name string `json:"name" yaml:"name"`

	// Kind is the exchange type: ExchangeDirect, ExchangeFanout,
	// ExchangeTopic, ExchangeHeaders or a plugin's type. Required.
	Kind string `json:"kind" yaml:"kind"`

	// Durable exchanges survive a broker restart.
	Durable bool `json:"durable" yaml:"durable"`

	// AutoDelete exchanges are deleted when their last binding is removed.
	AutoDelete bool `json:"autoDelete" yaml:"autoDelete"`

	// Internal exchanges take messages from other exchanges only.
	Internal bool `json:"internal" yaml:"internal"`

	// Args are the optional arguments ("alternate-exchange", …).
	Args map[string]any `json:"args" yaml:"args"`
}

// Queue declares a queue (queue.declare).
type Queue struct {
	// Name of the queue. Required: server-named queues are not supported,
	// since a consumer subscribes again to the same name after a reconnect.
	Name string `json:"name" yaml:"name"`

	// Durable queues survive a broker restart (their persistent messages
	// too).
	Durable bool `json:"durable" yaml:"durable"`

	// AutoDelete queues are deleted when their last consumer unsubscribes.
	AutoDelete bool `json:"autoDelete" yaml:"autoDelete"`

	// Exclusive queues belong to the declaring connection and are deleted
	// with it.
	Exclusive bool `json:"exclusive" yaml:"exclusive"`

	// Args are the optional arguments: "x-queue-type" ("quorum"),
	// "x-dead-letter-exchange", "x-message-ttl", …
	Args map[string]any `json:"args" yaml:"args"`
}

// Binding binds a queue to an exchange (queue.bind).
type Binding struct {
	// Queue and Exchange to bind. Required.
	Queue    string `json:"queue"    yaml:"queue"`
	Exchange string `json:"exchange" yaml:"exchange"`

	// RoutingKey is the binding key; its meaning depends on the exchange
	// kind (a pattern for topic exchanges, ignored by fanout ones).
	RoutingKey string `json:"routingKey" yaml:"routingKey"`

	// Args are the optional arguments (the match headers of a headers
	// exchange).
	Args map[string]any `json:"args" yaml:"args"`
}

// Topology is declared, in the order exchanges, queues, bindings, every time
// a consumer subscribes — at the start and after every reconnect — so that
// what the broker lost (a restarted broker without durable entities) is
// there again.
type Topology struct {
	Exchanges []Exchange `json:"exchanges" yaml:"exchanges"`
	Queues    []Queue    `json:"queues"    yaml:"queues"`
	Bindings  []Binding  `json:"bindings"  yaml:"bindings"`
}

// Validate reports every invalid declaration at once. Each error wraps
// ErrInvalidConfig.
func (t Topology) Validate() error {
	var errs []error

	for idx, e := range t.Exchanges {
		if strings.TrimSpace(e.Name) == "" {
			errs = append(errs, fmt.Errorf("%w: exchanges[%d]: name must not be empty", ErrInvalidConfig, idx))
		}

		if strings.TrimSpace(e.Kind) == "" {
			errs = append(errs, fmt.Errorf("%w: exchanges[%d]: kind must not be empty", ErrInvalidConfig, idx))
		}
	}

	for idx, q := range t.Queues {
		if strings.TrimSpace(q.Name) == "" {
			errs = append(errs, fmt.Errorf("%w: queues[%d]: name must not be empty", ErrInvalidConfig, idx))
		}
	}

	for idx, b := range t.Bindings {
		if strings.TrimSpace(b.Queue) == "" {
			errs = append(errs, fmt.Errorf("%w: bindings[%d]: queue must not be empty", ErrInvalidConfig, idx))
		}

		if strings.TrimSpace(b.Exchange) == "" {
			errs = append(errs, fmt.Errorf("%w: bindings[%d]: exchange must not be empty", ErrInvalidConfig, idx))
		}
	}

	return errors.Join(errs...)
}

// declare declares the topology on ch.
func (t Topology) declare(ch amqpChannel) error {
	for _, e := range t.Exchanges {
		if err := ch.exchangeDeclare(e); err != nil {
			return fmt.Errorf("rabbitmq: declare exchange %q: %w", e.Name, err)
		}
	}

	for _, q := range t.Queues {
		if err := ch.queueDeclare(q); err != nil {
			return fmt.Errorf("rabbitmq: declare queue %q: %w", q.Name, err)
		}
	}

	for _, b := range t.Bindings {
		if err := ch.queueBind(b); err != nil {
			return fmt.Errorf("rabbitmq: bind queue %q to exchange %q: %w", b.Queue, b.Exchange, err)
		}
	}

	return nil
}

// clone copies the slices and argument maps (one level), so that a caller
// changing them after the constructor changes nothing.
func (t Topology) clone() Topology {
	out := Topology{
		Exchanges: make([]Exchange, len(t.Exchanges)),
		Queues:    make([]Queue, len(t.Queues)),
		Bindings:  make([]Binding, len(t.Bindings)),
	}

	for i, e := range t.Exchanges {
		e.Args = maps.Clone(e.Args)
		out.Exchanges[i] = e
	}

	for i, q := range t.Queues {
		q.Args = maps.Clone(q.Args)
		out.Queues[i] = q
	}

	for i, b := range t.Bindings {
		b.Args = maps.Clone(b.Args)
		out.Bindings[i] = b
	}

	return out
}
