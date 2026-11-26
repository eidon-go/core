package rabbitmq

import (
	"context"
	"fmt"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// The amqp091 calls the components make, behind small interfaces so that the
// unit tests can run them on a fake broker. The real implementations below
// only forward.

// dialFunc dials one AMQP connection; ctx cancels the TCP connect.
type dialFunc func(ctx context.Context, addr string, cfg amqp.Config) (amqpConn, error)

type amqpConn interface {
	channel() (amqpChannel, error)
	notifyClose(receiver chan *amqp.Error) chan *amqp.Error
	notifyBlocked(receiver chan amqp.Blocking) chan amqp.Blocking
	// closeDeadline closes the connection; the handshake is bounded by
	// deadline (a socket deadline), after which the socket is closed.
	closeDeadline(deadline time.Time) error
	isClosed() bool
}

type amqpChannel interface {
	declarer
	qos(prefetchCount int) error
	consume(queue, tag string, exclusive bool, args amqp.Table) (<-chan amqp.Delivery, error)
	cancel(tag string) error
	confirm() error
	publish(ctx context.Context, exchange, key string, msg amqp.Publishing) (confirmation, error)
	notifyClose(receiver chan *amqp.Error) chan *amqp.Error
	close() error
	isClosed() bool
}

// declarer declares topology (Topology.declare).
type declarer interface {
	exchangeDeclare(e Exchange) error
	queueDeclare(q Queue) error
	queueBind(b Binding) error
}

// confirmation is the broker's answer to one published message.
type confirmation interface {
	// wait returns whether the message was acked; false also when the
	// channel closed first.
	wait(ctx context.Context) (bool, error)
}

// dialAMQP dials with amqp091. The TCP connect honors ctx and timeout; the
// deadline for the TLS and AMQP handshakes is cleared by amqp091 once the
// connection is open.
func dialAMQP(ctx context.Context, addr string, cfg amqp.Config, timeout time.Duration) (amqpConn, error) {
	cfg.Dial = func(network, address string) (net.Conn, error) {
		d := net.Dialer{Timeout: timeout} //nolint:exhaustruct_v5 // defaults for the rest

		conn, err := d.DialContext(ctx, network, address)
		if err != nil {
			return nil, err //nolint:wrapcheck // amqp091 wraps it
		}

		if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
			_ = conn.Close()

			return nil, err //nolint:wrapcheck // amqp091 wraps it
		}

		return conn, nil
	}

	conn, err := amqp.DialConfig(addr, cfg)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: dial %s: %w", addr, err)
	}

	return realConn{conn}, nil
}

type realConn struct{ c *amqp.Connection }

func (r realConn) channel() (amqpChannel, error) {
	ch, err := r.c.Channel()
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: open channel: %w", err)
	}

	return realChannel{ch}, nil
}

func (r realConn) notifyClose(c chan *amqp.Error) chan *amqp.Error { return r.c.NotifyClose(c) }

func (r realConn) notifyBlocked(c chan amqp.Blocking) chan amqp.Blocking {
	return r.c.NotifyBlocked(c)
}

func (r realConn) closeDeadline(deadline time.Time) error {
	return r.c.CloseDeadline(deadline) //nolint:wrapcheck // wrapped by the caller
}

func (r realConn) isClosed() bool { return r.c.IsClosed() }

type realChannel struct{ ch *amqp.Channel }

func (r realChannel) qos(prefetchCount int) error {
	return r.ch.Qos(prefetchCount, 0, false) //nolint:wrapcheck // wrapped by the caller
}

func (r realChannel) exchangeDeclare(ex Exchange) error {
	//nolint:wrapcheck // wrapped by the caller
	return r.ch.ExchangeDeclare(ex.Name, ex.Kind, ex.Durable, ex.AutoDelete, ex.Internal, false, toTable(ex.Args, 0))
}

func (r realChannel) queueDeclare(q Queue) error {
	_, err := r.ch.QueueDeclare(q.Name, q.Durable, q.AutoDelete, q.Exclusive, false, toTable(q.Args, 0))

	return err //nolint:wrapcheck // wrapped by the caller
}

func (r realChannel) queueBind(bind Binding) error {
	//nolint:wrapcheck // wrapped by the caller
	return r.ch.QueueBind(bind.Queue, bind.RoutingKey, bind.Exchange, false, toTable(bind.Args, 0))
}

func (r realChannel) consume(queue, tag string, exclusive bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return r.ch.Consume(queue, tag, false, exclusive, false, false, args) //nolint:wrapcheck // wrapped by the caller
}

func (r realChannel) cancel(tag string) error {
	return r.ch.Cancel(tag, false) //nolint:wrapcheck // logged by the caller
}

func (r realChannel) confirm() error {
	return r.ch.Confirm(false) //nolint:wrapcheck // wrapped by the caller
}

func (r realChannel) publish(ctx context.Context, exchange, key string, msg amqp.Publishing) (confirmation, error) {
	dc, err := r.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key, false, false, msg)
	if err != nil {
		return nil, err //nolint:wrapcheck // wrapped by the caller
	}

	return realConfirmation{dc}, nil
}

func (r realChannel) notifyClose(c chan *amqp.Error) chan *amqp.Error { return r.ch.NotifyClose(c) }

func (r realChannel) close() error   { return r.ch.Close() } //nolint:wrapcheck // logged by the caller
func (r realChannel) isClosed() bool { return r.ch.IsClosed() }

type realConfirmation struct{ dc *amqp.DeferredConfirmation }

func (r realConfirmation) wait(ctx context.Context) (bool, error) {
	return r.dc.WaitContext(ctx) //nolint:wrapcheck // the caller's context error
}
