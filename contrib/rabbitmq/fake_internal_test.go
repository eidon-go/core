package rabbitmq

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/retry"
)

// fakeBroker is an in-memory broker behind the amqpConn/amqpChannel
// interfaces: enough AMQP to test the components' state machines.
type fakeBroker struct {
	mu sync.Mutex

	dialErr    error // returned by dial while set
	failDials  int   // the first failDials dials fail with errBroker
	dials      int
	conn       *fakeConn
	declareErr error
	missing    map[string]bool // passive declares that fail with 404
	checked    []string        // passive declares
	consumeErr error

	declared  []string
	prefetch  []int
	consumers map[string]*fakeChannel // by tag
	nextTag   uint64

	published     []amqp.Publishing
	routingKeys   []string
	nackPublish   bool // nack every publish
	holdConfirms  bool // confirms wait for releaseConfirms
	held          []*fakeConfirmation
	blockClose    chan struct{} // while set, a channel close waits for it to close
	dropOnPublish int           // the channel dies on the publish of this number (1-based)

	acked    []uint64
	requeued []uint64 // nack with requeue, and unacked on channel close
	rejected []uint64
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{consumers: map[string]*fakeChannel{}}
}

// fastRetry is a connect policy for tests: quick, forever.
func fastRetry() retry.Policy {
	return retry.Policy{
		MaxAttempts:  retry.Forever,
		InitialDelay: time.Millisecond,
		MaxDelay:     5 * time.Millisecond,
	}
}

func (b *fakeBroker) dial(ctx context.Context, _ string, _ amqp.Config) (amqpConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.dials++

	if b.dialErr != nil {
		return nil, b.dialErr
	}

	if b.dials <= b.failDials {
		return nil, errBroker
	}

	b.conn = &fakeConn{broker: b}

	return b.conn, nil
}

func (b *fakeBroker) setDialErr(err error) {
	b.mu.Lock()
	b.dialErr = err
	b.mu.Unlock()
}

func (b *fakeBroker) dialCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.dials
}

// drop kills the current connection as a network failure would.
func (b *fakeBroker) drop() {
	b.mu.Lock()
	conn := b.conn
	b.mu.Unlock()

	if conn != nil {
		conn.kill(
			&amqp.Error{Code: amqp.ConnectionForced, Reason: "dropped by the test", Server: true},
		)
	}
}

// consumer waits for a consumer to subscribe and returns its channel.
func (b *fakeBroker) consumer(t *testing.T) *fakeChannel {
	t.Helper()

	var found *fakeChannel

	require.Eventually(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()

		for _, ch := range b.consumers {
			found = ch

			return true
		}

		return false
	}, 5*time.Second, time.Millisecond, "no consumer subscribed")

	return found
}

// deliver sends a message to a subscribed consumer.
func (b *fakeBroker) deliver(t *testing.T, msg amqp.Delivery) {
	t.Helper()

	for {
		ch := b.consumer(t)
		if ch.send(msg) {
			return
		}
	}
}

// snapshot returns the delivery tags acked, requeued and rejected so far.
func (b *fakeBroker) snapshot() ([]uint64, []uint64, []uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return append([]uint64(nil), b.acked...), append([]uint64(nil), b.requeued...), append([]uint64(nil), b.rejected...)
}

func (b *fakeBroker) publishedMessages() []amqp.Publishing {
	b.mu.Lock()
	defer b.mu.Unlock()

	return append([]amqp.Publishing(nil), b.published...)
}

func (b *fakeBroker) releaseConfirms(ack bool) {
	b.mu.Lock()
	held := b.held
	b.held = nil
	b.holdConfirms = false
	b.mu.Unlock()

	for _, c := range held {
		c.resolve(ack)
	}
}

func (b *fakeBroker) heldCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.held)
}

type fakeConn struct {
	broker *fakeBroker

	mu        sync.Mutex
	closed    bool
	listeners []chan *amqp.Error
	channels  []*fakeChannel
}

func (c *fakeConn) channel() (amqpChannel, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, amqp.ErrClosed
	}

	ch := &fakeChannel{broker: c.broker, unacked: map[uint64]bool{}}
	c.channels = append(c.channels, ch)

	return ch, nil
}

func (c *fakeConn) notifyClose(receiver chan *amqp.Error) chan *amqp.Error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		close(receiver)

		return receiver
	}

	c.listeners = append(c.listeners, receiver)

	return receiver
}

func (c *fakeConn) notifyBlocked(receiver chan amqp.Blocking) chan amqp.Blocking { return receiver }

func (c *fakeConn) closeDeadline(time.Time) error {
	c.kill(nil)

	return nil
}

func (c *fakeConn) kill(reason *amqp.Error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()

		return
	}

	c.closed = true
	listeners, channels := c.listeners, c.channels
	c.mu.Unlock()

	for _, ch := range channels {
		_ = ch.shutdown(reason)
	}

	for _, l := range listeners {
		if reason != nil {
			l <- reason
		}

		close(l)
	}
}

func (c *fakeConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.closed
}

type fakeChannel struct {
	broker *fakeBroker

	mu         sync.Mutex
	closed     bool
	deliveries chan amqp.Delivery
	tag        string
	unacked    map[uint64]bool
	pending    []*fakeConfirmation
	closes     []chan *amqp.Error
}

// Ack, Nack and Reject make the channel the amqp.Acknowledger of its
// deliveries.
func (ch *fakeChannel) Ack(tag uint64, _ bool) error { return ch.settle(tag, &ch.broker.acked) }

func (ch *fakeChannel) Nack(tag uint64, _, requeue bool) error {
	if requeue {
		return ch.settle(tag, &ch.broker.requeued)
	}

	return ch.settle(tag, &ch.broker.rejected)
}

func (ch *fakeChannel) Reject(tag uint64, requeue bool) error { return ch.Nack(tag, false, requeue) }

func (ch *fakeChannel) qos(prefetchCount int) error {
	ch.broker.mu.Lock()
	ch.broker.prefetch = append(ch.broker.prefetch, prefetchCount)
	ch.broker.mu.Unlock()

	return nil
}

func (ch *fakeChannel) declare(what string) error {
	ch.broker.mu.Lock()
	defer ch.broker.mu.Unlock()

	if ch.broker.declareErr != nil {
		return ch.broker.declareErr
	}

	ch.broker.declared = append(ch.broker.declared, what)

	return nil
}

func (ch *fakeChannel) exchangeDeclare(e Exchange) error { return ch.declare("exchange " + e.Name) }
func (ch *fakeChannel) queueDeclare(q Queue) error       { return ch.declare("queue " + q.Name) }
func (ch *fakeChannel) queueBind(b Binding) error {
	return ch.declare("bind " + b.Queue + " " + b.Exchange)
}

func (ch *fakeChannel) exchangeDeclarePassive(e Exchange) error {
	return ch.check("exchange " + e.Name)
}
func (ch *fakeChannel) queueDeclarePassive(q Queue) error { return ch.check("queue " + q.Name) }

// check is a passive declare: a name in the broker's missing set is 404.
func (ch *fakeChannel) check(what string) error {
	ch.broker.mu.Lock()
	defer ch.broker.mu.Unlock()

	if ch.broker.missing[what] {
		return &amqp.Error{Code: amqp.NotFound, Reason: "NOT_FOUND - no " + what}
	}

	ch.broker.checked = append(ch.broker.checked, what)

	return nil
}

func (ch *fakeChannel) consume(_, tag string, _ bool, _ amqp.Table) (<-chan amqp.Delivery, error) {
	// Lock order everywhere: channel, then broker.
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.closed {
		return nil, amqp.ErrClosed
	}

	ch.broker.mu.Lock()
	defer ch.broker.mu.Unlock()

	if ch.broker.consumeErr != nil {
		return nil, ch.broker.consumeErr
	}

	ch.tag = tag
	ch.deliveries = make(chan amqp.Delivery, 100)
	ch.broker.consumers[tag] = ch

	return ch.deliveries, nil
}

// send delivers msg on the channel; false if it is closed or cancelled.
func (ch *fakeChannel) send(msg amqp.Delivery) bool {
	ch.broker.mu.Lock()
	ch.broker.nextTag++
	tag := ch.broker.nextTag
	ch.broker.mu.Unlock()

	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.closed || ch.deliveries == nil {
		return false
	}

	msg.Acknowledger = ch
	msg.DeliveryTag = tag
	msg.ConsumerTag = ch.tag
	ch.unacked[tag] = true

	ch.deliveries <- msg

	return true
}

func (ch *fakeChannel) cancel(tag string) error {
	ch.broker.mu.Lock()
	delete(ch.broker.consumers, tag)
	ch.broker.mu.Unlock()

	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.closed {
		return amqp.ErrClosed
	}

	if ch.deliveries != nil {
		close(ch.deliveries)
		ch.deliveries = nil
	}

	return nil
}

func (ch *fakeChannel) confirm() error { return nil }

// missingExchange is an exchange the fake broker does not have: a publish
// to it closes the channel with 404, as RabbitMQ does.
const missingExchange = "missing"

func (ch *fakeChannel) publish(ctx context.Context, exchange, key string, msg amqp.Publishing) (confirmation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	b := ch.broker

	b.mu.Lock()

	drop := b.dropOnPublish > 0 && len(b.published)+1 == b.dropOnPublish
	if drop {
		b.dropOnPublish = 0
	}
	b.mu.Unlock()

	if drop {
		_ = ch.shutdown(&amqp.Error{Code: amqp.ChannelError, Reason: "dropped by the test", Server: true})

		return nil, amqp.ErrClosed
	}

	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.closed {
		return nil, amqp.ErrClosed
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.published = append(b.published, msg)
	b.routingKeys = append(b.routingKeys, key)

	conf := &fakeConfirmation{done: make(chan struct{})}

	switch {
	case exchange == missingExchange:
		ch.pending = append(ch.pending, conf)

		go func() {
			_ = ch.shutdown(&amqp.Error{Code: amqp.NotFound, Reason: "NOT_FOUND - no exchange 'missing'", Server: true})
		}()
	case b.holdConfirms:
		b.held = append(b.held, conf)
		ch.pending = append(ch.pending, conf)
	default:
		conf.resolve(!b.nackPublish)
	}

	return conf, nil
}

func (ch *fakeChannel) notifyClose(receiver chan *amqp.Error) chan *amqp.Error {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.closed {
		close(receiver)

		return receiver
	}

	ch.closes = append(ch.closes, receiver)

	return receiver
}

// close is a channel.close by the client; it waits while the broker's
// blockClose is set (a broker that does not answer).
func (ch *fakeChannel) close() error {
	ch.broker.mu.Lock()
	block := ch.broker.blockClose
	ch.broker.mu.Unlock()

	if block != nil {
		<-block
	}

	return ch.shutdown(nil)
}

// shutdown closes the channel; reason is the broker's error (nil for a close
// by the client). As amqp091 does, the close listeners get the reason before
// the pending confirms are released.
func (ch *fakeChannel) shutdown(reason *amqp.Error) error {
	ch.mu.Lock()
	if ch.closed {
		ch.mu.Unlock()

		return amqp.ErrClosed
	}

	ch.closed = true
	pending, unacked, consumerTag := ch.pending, ch.unacked, ch.tag

	for _, l := range ch.closes {
		if reason != nil {
			l <- reason
		}

		close(l)
	}

	ch.closes = nil
	ch.unacked = map[uint64]bool{}

	if ch.deliveries != nil {
		close(ch.deliveries)
		ch.deliveries = nil
	}
	ch.mu.Unlock()

	b := ch.broker
	b.mu.Lock()
	if consumerTag != "" {
		delete(b.consumers, consumerTag)
	}

	for tag := range unacked {
		b.requeued = append(b.requeued, tag)
	}
	b.mu.Unlock()

	for _, c := range pending {
		c.resolve(false)
	}

	return nil
}

func (ch *fakeChannel) isClosed() bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	return ch.closed
}

// settle records an ack, nack or reject of tag.
func (ch *fakeChannel) settle(tag uint64, into *[]uint64) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.closed {
		return amqp.ErrClosed
	}

	delete(ch.unacked, tag)

	ch.broker.mu.Lock()

	*into = append(*into, tag)
	ch.broker.mu.Unlock()

	return nil
}

type fakeConfirmation struct {
	once sync.Once
	done chan struct{}
	ack  bool
}

func (c *fakeConfirmation) resolve(ack bool) {
	c.once.Do(func() {
		c.ack = ack
		close(c.done)
	})
}

func (c *fakeConfirmation) wait(ctx context.Context) (bool, error) {
	select {
	case <-c.done:
		return c.ack, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

var (
	// errBroker is a dial or declare error of the tests.
	errBroker = errors.New("broker unavailable")

	// discard is the logger of the tests.
	discard = slog.New(slog.DiscardHandler) //nolint:gochecknoglobals // read-only
)

// newTestConnection returns a connection on b, started unless start is
// false, shut down at the end of the test.
func newTestConnection(t *testing.T, b *fakeBroker, start bool, opts ...Option) *Connection {
	t.Helper()

	opts = append([]Option{withDial(b.dial), WithConnectRetry(fastRetry()), WithLogger(discard)}, opts...)

	conn, err := NewConnection(Config{Host: "localhost"}, opts...)
	require.NoError(t, err)

	t.Cleanup(func() { _ = conn.Shutdown(cleanupContext(t)) })

	if start {
		require.NoError(t, conn.Start(t.Context()))
	}

	return conn
}

// testWait bounds every wait of the tests for a result.
const testWait = 5 * time.Second

// cleanupContext is the context of a Shutdown in a cleanup: t.Context() is
// already cancelled then, and a wait without a bound would hang a failed
// test.
func cleanupContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), testWait)
	t.Cleanup(cancel)

	return ctx
}

// receive returns the next value of ch, failing the test after testWait.
func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()

	select {
	case v := <-ch:
		return v
	case <-time.After(testWait):
		require.FailNow(t, "no result within the test wait")

		var zero T

		return zero
	}
}
