package integration_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcrabbitmq "github.com/testcontainers/testcontainers-go/modules/rabbitmq"

	"github.com/eidon-go/core/app"
	"github.com/eidon-go/core/contrib/rabbitmq"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
	"github.com/eidon-go/core/retry"
)

// rabbitImage is a RabbitMQ 4 broker.
const rabbitImage = "rabbitmq:4.1-management-alpine"

// rabbitWait bounds the waits of the RabbitMQ tests.
const rabbitWait = 30 * time.Second

// rabbitConfig connects to the RabbitMQ container shared by every test of
// the package; zero with -short.
var rabbitConfig rabbitmq.Config //nolint:gochecknoglobals // set once by TestMain, read-only in tests

// startRabbitMQ starts a broker with a non-guest user (guest may only
// connect from localhost) and returns the config to reach it.
func startRabbitMQ(ctx context.Context) (*tcrabbitmq.RabbitMQContainer, rabbitmq.Config, error) {
	ctr, err := tcrabbitmq.Run(ctx, rabbitImage,
		tcrabbitmq.WithAdminUsername("app"), tcrabbitmq.WithAdminPassword("secret"))
	if err != nil {
		return ctr, rabbitmq.Config{}, fmt.Errorf("start rabbitmq: %w", err)
	}

	host, err := ctr.Host(ctx)
	if err != nil {
		return ctr, rabbitmq.Config{}, fmt.Errorf("rabbitmq host: %w", err)
	}

	port, err := ctr.MappedPort(ctx, tcrabbitmq.DefaultAMQPPort)
	if err != nil {
		return ctr, rabbitmq.Config{}, fmt.Errorf("rabbitmq port: %w", err)
	}

	return ctr, rabbitmq.Config{Host: host, Port: int(port.Num()), User: "app", Password: "secret"}, nil
}

// rabbitName returns a queue or exchange name unique to the test.
func rabbitName(t *testing.T, suffix string) string {
	t.Helper()

	return strings.NewReplacer("/", ".", " ", "_").Replace(t.Name()) + "." + suffix
}

func rabbitOptions(name string) []rabbitmq.Option {
	return []rabbitmq.Option{
		rabbitmq.WithName(name),
		rabbitmq.WithLogger(slog.New(slog.DiscardHandler)),
		rabbitmq.WithConnectRetry(retry.Policy{MaxAttempts: retry.Forever, InitialDelay: 50 * time.Millisecond}),
	}
}

// newRabbitConnection returns a started connection, shut down at the end of
// the test.
func newRabbitConnection(t *testing.T, cfg rabbitmq.Config, name string) *rabbitmq.Connection {
	t.Helper()

	conn, err := rabbitmq.NewConnection(cfg, rabbitOptions(name)...)
	require.NoError(t, err)

	t.Cleanup(func() { shutdownWithin(t, conn) })

	ctx, cancel := context.WithTimeout(t.Context(), rabbitWait)
	defer cancel()

	require.NoError(t, conn.Start(ctx))

	return conn
}

func newRabbitPublisher(t *testing.T, conn *rabbitmq.Connection) *rabbitmq.Publisher {
	t.Helper()

	pub, err := rabbitmq.NewPublisher(conn, rabbitOptions("publisher")...)
	require.NoError(t, err)

	t.Cleanup(func() { shutdownWithin(t, pub) })

	return pub
}

// shutdownWithin shuts r down at the end of a test, bounded by rabbitWait so
// that a failed test does not hang in its cleanup.
func shutdownWithin(t *testing.T, r lifecycle.Resource) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), rabbitWait)
	defer cancel()

	_ = r.Shutdown(ctx)
}

// queueTopology declares a topic exchange and a queue bound to it with
// routing key "#".
func queueTopology(exchange, queue string, queueArgs map[string]any) rabbitmq.Topology {
	return rabbitmq.Topology{
		Exchanges: []rabbitmq.Exchange{{Name: exchange, Kind: rabbitmq.ExchangeTopic, AutoDelete: true}},
		Queues:    []rabbitmq.Queue{{Name: queue, Args: queueArgs}},
		Bindings:  []rabbitmq.Binding{{Queue: queue, Exchange: exchange, RoutingKey: "#"}},
	}
}

// rabbitCollector records the bodies it handles.
type rabbitCollector struct {
	seen chan string
}

func newRabbitCollector() *rabbitCollector { return &rabbitCollector{seen: make(chan string, 100)} }

func (c *rabbitCollector) Handle(_ context.Context, msg *rabbitmq.Message) error {
	c.seen <- string(msg.Body)

	return nil
}

func (c *rabbitCollector) wait(t *testing.T, n int) []string {
	t.Helper()

	got := make([]string, 0, n)

	for range n {
		select {
		case body := <-c.seen:
			got = append(got, body)
		case <-time.After(rabbitWait):
			require.Failf(t, "messages missing", "got %d of %d: %v", len(got), n, got)
		}
	}

	return got
}

// runRabbitConsumer runs c; the returned stop cancels Run, waits for it
// (bounded by rabbitWait) and returns the first error of Shutdown and Run.
// It may be called from any goroutine: it does not fail the test itself.
func runRabbitConsumer(t *testing.T, c *rabbitmq.Consumer) func() error {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() { done <- c.Run(ctx) }()

	var (
		once    sync.Once
		stopErr error
	)

	stop := func() error {
		once.Do(func() {
			cancel()

			sctx, scancel := context.WithTimeout(context.WithoutCancel(t.Context()), rabbitWait)
			defer scancel()

			stopErr = c.Shutdown(sctx)

			select {
			case err := <-done:
				stopErr = errors.Join(stopErr, err)
			case <-time.After(rabbitWait):
				stopErr = errors.Join(stopErr, errors.New("Run did not return after Shutdown"))
			}
		})

		return stopErr
	}

	t.Cleanup(func() { assert.NoError(t, stop()) })

	require.Eventually(t, func() bool { return c.Healthcheck(t.Context()) == nil },
		rabbitWait, 10*time.Millisecond, "consumer not subscribed")

	return stop
}

// rawChannel opens a plain amqp091 channel for inspecting queues.
func rawChannel(t *testing.T, cfg rabbitmq.Config) *amqp.Channel {
	t.Helper()

	addr := "amqp://" + net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)) + "/"

	conn, err := amqp.DialConfig(addr, amqp.Config{
		SASL:  []amqp.Authentication{&amqp.PlainAuth{Username: cfg.User, Password: cfg.Password}},
		Vhost: "/",
	})
	require.NoError(t, err)

	t.Cleanup(func() { _ = conn.Close() })

	ch, err := conn.Channel()
	require.NoError(t, err)

	return ch
}

// readyMessages returns the number of messages ready in queue.
func readyMessages(t *testing.T, ch *amqp.Channel, queue string) int {
	t.Helper()

	q, err := ch.QueueDeclarePassive(queue, false, false, false, false, nil)
	require.NoError(t, err)

	return q.Messages
}

func publishBodies(t *testing.T, pub *rabbitmq.Publisher, exchange string, bodies ...string) {
	t.Helper()

	msgs := make([]*rabbitmq.Message, 0, len(bodies))
	for _, b := range bodies {
		msgs = append(msgs, &rabbitmq.Message{Exchange: exchange, RoutingKey: "test", Body: []byte(b)})
	}

	ctx, cancel := context.WithTimeout(t.Context(), rabbitWait)
	defer cancel()

	require.NoError(t, pub.Publish(ctx, msgs...))
}

func TestRabbitMQ_ConnectionContract(t *testing.T) {
	t.Parallel()
	requireDocker(t)

	lifecycletest.Resource(t, func(t *testing.T) lifecycle.Resource {
		t.Helper()

		conn, err := rabbitmq.NewConnection(rabbitConfig, rabbitOptions("contract")...)
		require.NoError(t, err)

		return conn
	}, lifecycletest.WithTimeout(rabbitWait))
}

func TestRabbitMQ_PublisherContract(t *testing.T) {
	t.Parallel()
	requireDocker(t)

	lifecycletest.Resource(t, func(t *testing.T) lifecycle.Resource {
		t.Helper()

		pub, err := rabbitmq.NewPublisher(newRabbitConnection(t, rabbitConfig, "contract"))
		require.NoError(t, err)

		return pub
	}, lifecycletest.WithTimeout(rabbitWait))
}

func TestRabbitMQ_ConsumerContract(t *testing.T) {
	t.Parallel()
	requireDocker(t)

	lifecycletest.Runner(t, func(t *testing.T) lifecycle.Runner {
		t.Helper()

		queue := rabbitName(t, "q")
		c, err := rabbitmq.NewConsumer(newRabbitConnection(t, rabbitConfig, "contract"),
			rabbitmq.ConsumerConfig{Queue: queue, Topology: rabbitmq.Topology{
				Queues: []rabbitmq.Queue{{Name: queue, AutoDelete: true}},
			}}, newRabbitCollector(), rabbitOptions("consumer")...)
		require.NoError(t, err)

		return c
	}, lifecycletest.WithTimeout(rabbitWait))
}

func TestRabbitMQ_PublishAndConsume(t *testing.T) {
	t.Parallel()
	requireDocker(t)

	exchange, queue := rabbitName(t, "x"), rabbitName(t, "q")
	pub := newRabbitPublisher(t, newRabbitConnection(t, rabbitConfig, "publish"))
	collector := newRabbitCollector()

	c, err := rabbitmq.NewConsumer(newRabbitConnection(t, rabbitConfig, "consume"), rabbitmq.ConsumerConfig{
		Queue:       queue,
		Topology:    queueTopology(exchange, queue, nil),
		Concurrency: 4,
	}, collector, rabbitOptions("consumer")...)
	require.NoError(t, err)

	stop := runRabbitConsumer(t, c)

	bodies := make([]string, 20)
	for i := range bodies {
		bodies[i] = fmt.Sprintf("m%02d", i)
	}

	publishBodies(t, pub, exchange, bodies...)

	assert.ElementsMatch(t, bodies, collector.wait(t, len(bodies)))

	require.NoError(t, stop())
	assert.Zero(t, readyMessages(t, rawChannel(t, rabbitConfig), queue), "every message acked")
}

// gate holds a handler until it is released, or until the handler's ctx is
// done; the test releases it on cleanup too, so that a failed assertion
// does not leave the handler blocked.
type gate struct {
	entered, release chan struct{}
	enterOnce        sync.Once
	releaseOnce      sync.Once
}

func newGate(t *testing.T) *gate {
	t.Helper()

	g := &gate{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(g.open)

	return g
}

// wait marks the handler as entered and blocks until open or ctx is done.
func (g *gate) wait(ctx context.Context) {
	g.enterOnce.Do(func() { close(g.entered) })

	select {
	case <-g.release:
	case <-ctx.Done():
	}
}

func (g *gate) open() { g.releaseOnce.Do(func() { close(g.release) }) }

// waitEntered waits for the handler to enter the gate, bounded by rabbitWait.
func (g *gate) waitEntered(t *testing.T) {
	t.Helper()

	select {
	case <-g.entered:
	case <-time.After(rabbitWait):
		require.FailNow(t, "the handler did not start")
	}
}

// On stop the message in flight finishes and is acked; the prefetched ones
// are requeued.
func TestRabbitMQ_DrainOnStop(t *testing.T) {
	t.Parallel()
	requireDocker(t)

	exchange, queue := rabbitName(t, "x"), rabbitName(t, "q")
	pub := newRabbitPublisher(t, newRabbitConnection(t, rabbitConfig, "publish"))
	g := newGate(t)

	var handled []string

	h := rabbitmq.HandlerFunc(func(ctx context.Context, msg *rabbitmq.Message) error {
		if len(handled) == 0 {
			g.wait(ctx)
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		handled = append(handled, string(msg.Body))

		return nil
	})

	c, err := rabbitmq.NewConsumer(newRabbitConnection(t, rabbitConfig, "consume"), rabbitmq.ConsumerConfig{
		Queue: queue, Topology: queueTopology(exchange, queue, nil), Prefetch: 10,
	}, h, rabbitOptions("consumer")...)
	require.NoError(t, err)

	stop := runRabbitConsumer(t, c)

	publishBodies(t, pub, exchange, "a", "b", "c")
	g.waitEntered(t)

	stopped := make(chan error, 1)

	go func() { stopped <- stop() }()

	require.Eventually(t, func() bool { return c.Healthcheck(t.Context()) != nil }, rabbitWait, time.Millisecond)
	g.open()

	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(2 * rabbitWait):
		require.FailNow(t, "the consumer did not stop")
	}

	assert.Equal(t, []string{"a"}, handled)

	ch := rawChannel(t, rabbitConfig)
	require.Eventually(t, func() bool { return readyMessages(t, ch, queue) == 2 }, rabbitWait, 10*time.Millisecond)
}

// A message the handler cannot decode is rejected to the dead-letter
// exchange of its queue.
func TestRabbitMQ_DeadLetter(t *testing.T) {
	t.Parallel()
	requireDocker(t)

	exchange, queue := rabbitName(t, "x"), rabbitName(t, "q")
	dlx, dead := rabbitName(t, "dlx"), rabbitName(t, "dead")

	topology := queueTopology(exchange, queue, map[string]any{"x-dead-letter-exchange": dlx})
	topology.Exchanges = append(topology.Exchanges, rabbitmq.Exchange{Name: dlx, Kind: rabbitmq.ExchangeFanout})
	topology.Queues = append(topology.Queues, rabbitmq.Queue{Name: dead})
	topology.Bindings = append(topology.Bindings, rabbitmq.Binding{Queue: dead, Exchange: dlx})

	pub := newRabbitPublisher(t, newRabbitConnection(t, rabbitConfig, "publish"))
	collector := newRabbitCollector()

	type order struct{ ID string }

	h := rabbitmq.JSON(func(ctx context.Context, _ order, msg *rabbitmq.Message) error {
		return collector.Handle(ctx, msg)
	})

	c, err := rabbitmq.NewConsumer(newRabbitConnection(t, rabbitConfig, "consume"),
		rabbitmq.ConsumerConfig{Queue: queue, Topology: topology}, h,
		append(rabbitOptions("consumer"), rabbitmq.WithErrorHandler(rabbitmq.DeadLetter))...)
	require.NoError(t, err)

	runRabbitConsumer(t, c)
	publishBodies(t, pub, exchange, "not json", `{"ID":"1"}`)

	assert.Equal(t, []string{`{"ID":"1"}`}, collector.wait(t, 1))

	ch := rawChannel(t, rabbitConfig)

	var got amqp.Delivery

	require.Eventually(t, func() bool {
		d, ok, err := ch.Get(dead, true)
		require.NoError(t, err)

		got = d

		return ok
	}, rabbitWait, 10*time.Millisecond)
	assert.Equal(t, "not json", string(got.Body))
	assert.Contains(t, got.Headers, "x-death")
}

// The case the stop order is for: a handler publishes while its consumer
// drains, and app.App keeps the publisher and the connections until the
// consumer has returned.
func TestRabbitMQ_HandlerPublishesDuringDrain(t *testing.T) {
	t.Parallel()
	requireDocker(t)

	exchange, in, out := rabbitName(t, "x"), rabbitName(t, "in"), rabbitName(t, "out")

	topology := queueTopology(exchange, in, nil)
	topology.Queues = append(topology.Queues, rabbitmq.Queue{Name: out})

	newConn := func(name string) *rabbitmq.Connection {
		conn, err := rabbitmq.NewConnection(rabbitConfig, rabbitOptions(name)...)
		require.NoError(t, err)

		return conn
	}

	publishConn, consumeConn := newConn("publish"), newConn("consume")

	pub, err := rabbitmq.NewPublisher(publishConn, rabbitOptions("publisher")...)
	require.NoError(t, err)

	g := newGate(t)
	h := rabbitmq.HandlerFunc(func(ctx context.Context, msg *rabbitmq.Message) error {
		g.wait(ctx)

		return pub.Publish(ctx, &rabbitmq.Message{RoutingKey: out, Body: msg.Body}) // default exchange
	})

	c, err := rabbitmq.NewConsumer(consumeConn, rabbitmq.ConsumerConfig{Queue: in, Topology: topology}, h,
		rabbitOptions("consumer")...)
	require.NoError(t, err)

	a := app.New(app.WithSignals(), app.WithLogger(slog.New(slog.DiscardHandler)),
		app.WithShutdownTimeout(rabbitWait))
	a.Add(publishConn, consumeConn)
	a.Add(pub)
	a.Add(c)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ran := make(chan error, 1)

	go func() { ran <- a.Run(ctx) }()

	require.Eventually(t, func() bool { return c.Healthcheck(t.Context()) == nil }, rabbitWait, 10*time.Millisecond)

	// Published on a connection of its own: the app's are not shared.
	other := newRabbitPublisher(t, newRabbitConnection(t, rabbitConfig, "test"))
	publishBodies(t, other, exchange, "order-1")
	g.waitEntered(t)

	cancel() // the app stops: the consumer drains first

	// Release the handler only once the drain has begun, so that it
	// publishes during the drain, not before it.
	require.Eventually(t, func() bool { return c.Healthcheck(t.Context()) != nil }, rabbitWait, time.Millisecond)
	g.open()

	select {
	case err := <-ran:
		require.NoError(t, err)
	case <-time.After(2 * rabbitWait):
		require.FailNow(t, "the app did not stop")
	}

	ch := rawChannel(t, rabbitConfig)
	assert.Zero(t, readyMessages(t, ch, in), "acked")
	assert.Equal(t, 1, readyMessages(t, ch, out), "published during the drain")
}

// A broker restart drops every connection: the connections redial, the
// consumer declares its (transient, so lost) topology again and
// resubscribes, and Publish waits for the connection.
func TestRabbitMQ_ReconnectAfterBrokerRestart(t *testing.T) {
	t.Parallel()
	requireDocker(t)

	ctx := context.WithoutCancel(t.Context())

	ctr, cfg, err := startRabbitMQ(ctx)
	if ctr != nil {
		t.Cleanup(func() { terminate(ctx, "rabbitmq", ctr.Terminate) })
	}

	require.NoError(t, err)

	exchange, queue := "orders", "billing"
	publishConn := newRabbitConnection(t, cfg, "publish")
	consumeConn := newRabbitConnection(t, cfg, "consume")
	pub := newRabbitPublisher(t, publishConn)
	collector := newRabbitCollector()

	c, err := rabbitmq.NewConsumer(consumeConn, rabbitmq.ConsumerConfig{
		Queue: queue, Topology: queueTopology(exchange, queue, nil),
	}, collector, rabbitOptions("consumer")...)
	require.NoError(t, err)

	runRabbitConsumer(t, c)
	publishBodies(t, pub, exchange, "before")
	assert.Equal(t, []string{"before"}, collector.wait(t, 1))

	rabbitctl(t, ctr, "stop_app")
	require.Eventually(t, func() bool {
		return consumeConn.Healthcheck(t.Context()) != nil && publishConn.Healthcheck(t.Context()) != nil
	}, rabbitWait, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return errors.Is(c.Healthcheck(t.Context()), rabbitmq.ErrNotConnected)
	}, rabbitWait, 10*time.Millisecond)

	rabbitctl(t, ctr, "start_app")

	// Publishing waits for the connection; the consumer must be subscribed
	// first, or the message has no queue to go to.
	require.Eventually(t, func() bool { return c.Healthcheck(t.Context()) == nil }, rabbitWait, 10*time.Millisecond)
	publishBodies(t, pub, exchange, "after")
	assert.Equal(t, []string{"after"}, collector.wait(t, 1))
	require.NoError(t, publishConn.Healthcheck(t.Context()))
}

func rabbitctl(t *testing.T, ctr *tcrabbitmq.RabbitMQContainer, cmd string) {
	t.Helper()

	code, _, err := ctr.Exec(t.Context(), []string{"rabbitmqctl", cmd})
	require.NoError(t, err)
	require.Zero(t, code, "rabbitmqctl %s", cmd)
}
