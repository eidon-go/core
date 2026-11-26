package integration_test

// Load and edge-case tests of contrib/rabbitmq against a real broker. They
// are heavy (dedicated containers, tens of thousands of messages), so they
// run only on request:
//
//	cd internal/integration
//	RABBITMQ_LOAD=1 go test -race -run 'TestRabbitMQLoad' -timeout 20m -v .
//
// RABBITMQ_LOAD_MESSAGES scales the message counts (default 20000).

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	tcrabbitmq "github.com/testcontainers/testcontainers-go/modules/rabbitmq"

	"github.com/eidon-go/core/app"
	"github.com/eidon-go/core/contrib/rabbitmq"
	"github.com/eidon-go/core/retry"
)

// loadWait bounds the waits of the load tests.
const loadWait = 3 * time.Minute

func requireLoad(t *testing.T) {
	t.Helper()
	requireDocker(t)

	if os.Getenv("RABBITMQ_LOAD") != "1" {
		t.Skip("load test: set RABBITMQ_LOAD=1")
	}
}

// minLoadMessages is the smallest RABBITMQ_LOAD_MESSAGES: the tests take a
// tenth of it at least, and need enough messages to overlap with the faults
// they inject.
const minLoadMessages = 10000

// loadMessages is the base message count of the load tests,
// RABBITMQ_LOAD_MESSAGES (at least minLoadMessages) or 20000.
func loadMessages(t *testing.T) int {
	t.Helper()

	if s := os.Getenv("RABBITMQ_LOAD_MESSAGES"); s != "" {
		n, err := strconv.Atoi(s)
		require.NoError(t, err)
		require.GreaterOrEqual(t, n, minLoadMessages, "RABBITMQ_LOAD_MESSAGES is too small")

		return n
	}

	return 20000
}

// startDedicatedRabbitMQ starts a broker of the test's own, for tests that
// raise alarms or drop connections broker-wide.
func startDedicatedRabbitMQ(t *testing.T) (*tcrabbitmq.RabbitMQContainer, rabbitmq.Config) {
	t.Helper()

	ctx := context.WithoutCancel(t.Context())

	ctr, cfg, err := startRabbitMQ(ctx)
	if ctr != nil {
		t.Cleanup(func() { terminate(ctx, "rabbitmq", ctr.Terminate) })
	}

	require.NoError(t, err)

	return ctr, cfg
}

// rabbitctlOutput runs rabbitmqctl in the container and returns its output.
func rabbitctlOutput(t *testing.T, ctr *tcrabbitmq.RabbitMQContainer, args ...string) string {
	t.Helper()

	// Not t.Context(): it is already cancelled when a cleanup lifts an alarm.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), rabbitWait)
	defer cancel()

	code, out, err := ctr.Exec(ctx, append([]string{"rabbitmqctl", "-q"}, args...), tcexec.Multiplexed())
	require.NoError(t, err)

	b, err := io.ReadAll(out)
	require.NoError(t, err)
	require.Zero(t, code, "rabbitmqctl %v: %s", args, b)

	return string(b)
}

// connectionState returns the broker-side state ("running", "blocking",
// "blocked", …) of the connection named name, "" if there is none.
func connectionState(t *testing.T, ctr *tcrabbitmq.RabbitMQContainer, name string) string {
	t.Helper()

	// The client-provided name is in the client properties.
	quoted := `{"connection_name","` + name + `"}`

	for line := range strings.SplitSeq(rabbitctlOutput(t, ctr, "list_connections", "state", "client_properties"), "\n") {
		if strings.Contains(line, quoted) {
			state, _, _ := strings.Cut(line, "\t")

			return state
		}
	}

	return ""
}

// raiseDiskAlarm makes the broker block every connection that publishes, as
// it does when the disk runs low; the returned func clears the alarm.
func raiseDiskAlarm(t *testing.T, ctr *tcrabbitmq.RabbitMQContainer) func() {
	t.Helper()

	rabbitctlOutput(t, ctr, "set_disk_free_limit", "100000GB")
	require.Eventually(t, func() bool {
		return strings.Contains(rabbitctlOutput(t, ctr, "eval", "rabbit_alarm:get_alarms()."), "disk")
	}, loadWait, 100*time.Millisecond, "disk alarm not raised")

	var once sync.Once

	lift := func() {
		once.Do(func() { rabbitctlOutput(t, ctr, "set_disk_free_limit", "50MB") })
	}
	t.Cleanup(lift)

	return lift
}

// rabbitctlErr runs rabbitmqctl cmd; for goroutines, which must not call
// t.FailNow.
func rabbitctlErr(ctx context.Context, ctr *tcrabbitmq.RabbitMQContainer, cmd string) error {
	code, _, err := ctr.Exec(ctx, []string{"rabbitmqctl", "-q", cmd})
	if err == nil && code != 0 {
		err = fmt.Errorf("rabbitmqctl %s: exit code %d", cmd, code)
	}

	return err
}

// loadTracker counts how often every message ID of 0..total-1 was handled.
// An ID outside the workload is a stray: it is acked, counted, and fails
// the waits — it must not stand in for a missing ID.
type loadTracker struct {
	total int

	mu     sync.Mutex
	seen   map[int]int
	strays []string
	all    chan struct{}
}

func newLoadTracker(total int) *loadTracker {
	return &loadTracker{total: total, seen: make(map[int]int, total), all: make(chan struct{})}
}

func (lt *loadTracker) mark(msg *rabbitmq.Message) error {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	id, err := strconv.Atoi(msg.MessageID)
	if err != nil || id < 0 || id >= lt.total {
		lt.strays = append(lt.strays, msg.MessageID)

		return nil
	}

	lt.seen[id]++
	if lt.seen[id] == 1 && len(lt.seen) == lt.total {
		close(lt.all)
	}

	return nil
}

// requireNoStrays fails the test if an ID outside the workload was handled.
func (lt *loadTracker) requireNoStrays(t *testing.T) {
	t.Helper()

	lt.mu.Lock()
	defer lt.mu.Unlock()

	require.Empty(t, lt.strays, "message IDs outside the workload")
}

// waitUnique waits until at least n IDs were handled.
func (lt *loadTracker) waitUnique(t *testing.T, n int, timeout time.Duration) {
	t.Helper()

	require.Eventually(t, func() bool {
		unique, _ := lt.counts()

		return unique >= n
	}, timeout, time.Millisecond, "fewer than %d messages handled", n)
	lt.requireNoStrays(t)
}

func (lt *loadTracker) counts() (int, int) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	dups := 0
	for _, n := range lt.seen {
		dups += n - 1
	}

	return len(lt.seen), dups
}

func (lt *loadTracker) has(id int) bool {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	return lt.seen[id] > 0
}

// waitAll waits, at most loadWait, until every ID was handled.
func (lt *loadTracker) waitAll(t *testing.T) {
	t.Helper()

	select {
	case <-lt.all:
	case <-time.After(loadWait):
		unique, dups := lt.counts()
		require.Failf(t, "messages missing", "handled %d of %d unique (%d duplicates)", unique, lt.total, dups)
	}

	lt.requireNoStrays(t)
}

// loadMessage is message id: its ID in MessageID and in the body.
func loadMessage(exchange string, id int, body []byte) *rabbitmq.Message {
	if body == nil {
		body = []byte(strconv.Itoa(id))
	}

	return &rabbitmq.Message{Exchange: exchange, RoutingKey: "load", MessageID: strconv.Itoa(id), Body: body}
}

// publishLoad publishes IDs 0..total-1 from workers goroutines, in batches,
// and republishes a batch until it is confirmed: at least once, whatever
// happens to the connection.
func publishLoad(t *testing.T, pub *rabbitmq.Publisher, exchange string, total, workers, batch int) loadStats {
	t.Helper()

	return publishLoadWithFaults(t, pub, exchange, total, workers, batch, 0, newFaultPlan())
}

// publishLoadWithFaults is publishLoad that pauses every worker for pace
// after each batch, to spread the publishing over time, and holds the
// publishing at each checkpoint of plan until its fault is injected.
func publishLoadWithFaults(
	t *testing.T, pub *rabbitmq.Publisher, exchange string, total, workers, batch int, pace time.Duration,
	plan *faultPlan,
) loadStats {
	t.Helper()

	var (
		lp   = &loadPublisher{pub: pub}
		next atomic.Int64
		wg   sync.WaitGroup
	)

	ctx, cancel := context.WithTimeout(t.Context(), loadWait)
	defer cancel()

	for range workers {
		wg.Go(func() {
			for {
				from := int(next.Add(int64(batch))) - batch
				if from >= total {
					return
				}

				msgs := make([]*rabbitmq.Message, 0, batch)
				for id := from; id < min(from+batch, total); id++ {
					msgs = append(msgs, loadMessage(exchange, id, nil))
				}

				if !lp.publish(ctx, msgs) {
					return
				}

				plan.confirmed.Add(int64(len(msgs)))

				if !plan.hold(ctx) {
					return
				}

				time.Sleep(pace)
			}
		})
	}

	wg.Wait()
	require.NoError(t, ctx.Err(), "publishing did not finish")

	return loadStats{failures: lp.failures.Load(), slowest: time.Duration(lp.slowest.Load())}
}

// loadPublisher counts the failed Publish calls of publishLoad and keeps the
// longest one.
type loadPublisher struct {
	pub      *rabbitmq.Publisher
	failures atomic.Int64
	slowest  atomic.Int64 // a time.Duration
}

// publish publishes msgs until they are confirmed; false once ctx is done.
func (lp *loadPublisher) publish(ctx context.Context, msgs []*rabbitmq.Message) bool {
	for {
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		began := time.Now()
		err := lp.pub.Publish(pctx, msgs...)

		cancel()
		lp.observe(time.Since(began))

		switch {
		case err == nil:
			return true
		case ctx.Err() != nil:
			return false
		}

		lp.failures.Add(1)
	}
}

func (lp *loadPublisher) observe(took time.Duration) {
	for {
		old := lp.slowest.Load()
		if int64(took) <= old || lp.slowest.CompareAndSwap(old, int64(took)) {
			return
		}
	}
}

// loadStats describes a publishLoad: the failed Publish calls (each batch
// republished) and the longest Publish call.
type loadStats struct {
	failures int64
	slowest  time.Duration
}

// faultPlan ties faults to the publishing progress: once the confirmed
// messages reach a checkpoint, every publishing worker waits there until
// the fault of that checkpoint is injected. So each fault happens with work
// still to publish, whatever the throughput or the time a fault takes.
type faultPlan struct {
	confirmed atomic.Int64
	at        []int64         // the checkpoints, in confirmed messages, ascending
	injected  []chan struct{} // closed once the fault of at[i] is injected
}

func newFaultPlan(at ...int64) *faultPlan {
	p := &faultPlan{at: at, injected: make([]chan struct{}, len(at))}
	for i := range p.injected {
		p.injected[i] = make(chan struct{})
	}

	return p
}

// hold makes a worker wait at every checkpoint reached whose fault is not
// injected yet; false once ctx is done.
func (p *faultPlan) hold(ctx context.Context) bool {
	confirmed := p.confirmed.Load()

	for i, at := range p.at {
		if confirmed < at {
			return true
		}

		select {
		case <-p.injected[i]:
		case <-ctx.Done():
			return false
		}
	}

	return true
}

// reached waits until checkpoint i is reached (the workers then wait for
// its fault); false once ctx is done.
func (p *faultPlan) reached(ctx context.Context, i int) bool {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for p.confirmed.Load() < p.at[i] {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}

	return true
}

// done marks the fault of checkpoint i injected: the workers go on.
func (p *faultPlan) done(i int) { close(p.injected[i]) }

func loadOptions(name string) []rabbitmq.Option {
	return append(rabbitOptions(name), rabbitmq.WithRedeliveryPolicy(retry.Policy{
		MaxAttempts: 10, InitialDelay: time.Millisecond, MaxDelay: 20 * time.Millisecond,
	}))
}

// newLoadConsumer builds a consumer of a durable queue bound to exchange,
// with rabbitmq.StopOnFailure.
func newLoadConsumer(
	t *testing.T, conn *rabbitmq.Connection, exchange, queue string, concurrency int, h rabbitmq.Handler,
) *rabbitmq.Consumer {
	t.Helper()

	return newLoadConsumerOnError(t, conn, exchange, queue, concurrency, h, rabbitmq.StopOnFailure)
}

// newLoadConsumerOnError builds a consumer of a durable queue bound to
// exchange.
func newLoadConsumerOnError(
	t *testing.T, conn *rabbitmq.Connection, exchange, queue string, concurrency int, h rabbitmq.Handler,
	onError rabbitmq.ErrorHandler,
) *rabbitmq.Consumer {
	t.Helper()

	c, err := rabbitmq.NewConsumer(conn, rabbitmq.ConsumerConfig{
		Queue:       queue,
		Concurrency: concurrency,
		Prefetch:    concurrency * 4,
		Topology: rabbitmq.Topology{
			Exchanges: []rabbitmq.Exchange{{Name: exchange, Kind: rabbitmq.ExchangeTopic, Durable: true}},
			Queues:    []rabbitmq.Queue{{Name: queue, Durable: true}},
			Bindings:  []rabbitmq.Binding{{Queue: queue, Exchange: exchange, RoutingKey: "#"}},
		},
	}, h, onError, loadOptions("consumer")...)
	require.NoError(t, err)

	return c
}

// rate formats n per d.
func rate(n int, d time.Duration) string {
	return fmt.Sprintf("%d in %s (%.0f/s)", n, d.Round(time.Millisecond), float64(n)/d.Seconds())
}

// The two tests below show why publishing and consuming go on separate
// connections. Under a resource alarm the broker blocks every connection that
// publishes and stops reading from its socket: on a shared connection the
// consumer cannot even subscribe (or ack), so its queue stalls; on a
// connection of its own it drains the queue while the publisher waits. Once
// the alarm clears, both recover. Each has a broker of its own: the alarm is
// broker-wide.

func TestRabbitMQLoad_SharedConnectionStallsUnderAlarm(t *testing.T) {
	t.Parallel()
	requireLoad(t)
	testConnectionsUnderAlarm(t, true)
}

func TestRabbitMQLoad_SeparateConnectionsUnderAlarm(t *testing.T) {
	t.Parallel()
	requireLoad(t)
	testConnectionsUnderAlarm(t, false)
}

func testConnectionsUnderAlarm(t *testing.T, shared bool) {
	t.Helper()

	ctr, cfg := startDedicatedRabbitMQ(t)
	total := loadMessages(t) / 10
	exchange, queue := rabbitName(t, "x"), rabbitName(t, "q")

	// Fill the queue before the alarm.
	consumeConn := newRabbitConnection(t, cfg, rabbitName(t, "consume"))
	tracker := newLoadTracker(total + 1) // and the message published during the alarm
	c := newLoadConsumer(t, consumeConn, exchange, queue, 8,
		rabbitmq.HandlerFunc(func(_ context.Context, msg *rabbitmq.Message) error { return tracker.mark(msg) }))

	filler := newRabbitPublisher(t, newRabbitConnection(t, cfg, rabbitName(t, "filler")))
	declareLoadQueue(t, rawChannel(t, cfg), exchange, queue)
	publishLoad(t, filler, exchange, total, 4, 100)

	clearAlarm := raiseDiskAlarm(t, ctr)

	publishConn := consumeConn
	if !shared {
		publishConn = newRabbitConnection(t, cfg, rabbitName(t, "publish"))
	}

	pub := newRabbitPublisher(t, publishConn)
	published := make(chan error, 1)

	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), loadWait)
		defer cancel()

		published <- pub.Publish(ctx, loadMessage(exchange, total, nil))
	}()

	require.Eventually(t, func() bool {
		return connectionState(t, ctr, publishConn.Name()) == "blocked"
	}, loadWait, 100*time.Millisecond, "the publishing connection is not blocked")

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() { done <- c.Run(ctx) }()

	window := 5 * time.Second
	start := time.Now()

	if shared {
		// Not a single message for the whole window: the consumer cannot
		// even subscribe on the blocked connection.
		require.Never(t, func() bool {
			unique, _ := tracker.counts()

			return unique > 0
		}, window, 50*time.Millisecond, "the consumer must stall on a blocked connection")
		t.Logf("shared connection, blocked by the alarm: handled 0 of %d in %s; consumer health: %v",
			total, window, c.Healthcheck(t.Context()))
	} else {
		tracker.waitUnique(t, total, window*6)
		t.Logf("separate connections, publisher blocked: consumer handled %s",
			rate(total, time.Since(start)))
	}

	select {
	case err := <-published:
		t.Fatalf("Publish returned %v during the alarm", err)
	default:
	}

	clearAlarm()
	require.NoError(t, receiveWithin(t, published, loadWait), "the publish completes once the alarm clears")
	tracker.waitAll(t)

	cancel()
	require.NoError(t, receiveWithin(t, done, loadWait))
}

// receiveWithin returns the next value of ch, failing the test after
// timeout.
func receiveWithin[T any](t *testing.T, ch <-chan T, timeout time.Duration) T {
	t.Helper()

	select {
	case v := <-ch:
		return v
	case <-time.After(timeout):
		require.FailNow(t, "no result in time", "waited %s", timeout)

		var zero T

		return zero
	}
}

// TestRabbitMQLoad_ShutdownUnderAlarm shuts a publisher and its connection
// down while the broker blocks the connection: both channel.close and
// connection.close are calls the broker does not answer then, yet each
// Shutdown returns at its deadline, and the Publish waiting for a confirm
// fails with ErrNotConfirmed.
func TestRabbitMQLoad_ShutdownUnderAlarm(t *testing.T) {
	t.Parallel()
	requireLoad(t)

	ctr, cfg := startDedicatedRabbitMQ(t)

	conn, err := rabbitmq.NewConnection(cfg, rabbitOptions(rabbitName(t, "publish"))...)
	require.NoError(t, err)
	require.NoError(t, conn.Start(t.Context()))

	pub, err := rabbitmq.NewPublisher(conn, rabbitOptions("publisher")...)
	require.NoError(t, err)

	// Open the channel before the alarm: opening one would block.
	require.NoError(t, pub.Publish(t.Context(), loadMessage("", 0, nil)))

	raiseDiskAlarm(t, ctr)

	published := make(chan error, 1)

	go func() { published <- pub.Publish(t.Context(), loadMessage("", 1, nil)) }()

	require.Eventually(t, func() bool {
		return connectionState(t, ctr, conn.Name()) == "blocked"
	}, loadWait, 100*time.Millisecond, "the connection is not blocked")

	const deadline = 2 * time.Second

	for _, r := range []interface {
		Name() string
		Shutdown(ctx context.Context) error
	}{pub, conn} {
		ctx, cancel := context.WithTimeout(t.Context(), deadline)
		start := time.Now()
		result := make(chan error, 1)

		// A watchdog of its own: a Shutdown that never returns must fail the
		// test, not hang it.
		go func() { result <- r.Shutdown(ctx) }()

		err := receiveWithin(t, result, deadline+time.Second)
		took := time.Since(start)

		cancel()

		t.Logf("%s: Shutdown under the alarm returned %v after %s", r.Name(), err, took.Round(time.Millisecond))
		require.ErrorIs(t, err, context.DeadlineExceeded,
			"%s: the broker does not answer the close: Shutdown ends at its deadline", r.Name())

		if r == any(pub) {
			require.ErrorIs(t, receiveWithin(t, published, time.Second), rabbitmq.ErrNotConfirmed)
		}
	}
}

// declareLoadQueue declares the topology of newLoadConsumer on a plain
// channel, so that the queue exists before any consumer runs.
func declareLoadQueue(t *testing.T, ch *amqp.Channel, exchange, queue string) {
	t.Helper()

	require.NoError(t, ch.ExchangeDeclare(exchange, rabbitmq.ExchangeTopic, true, false, false, false, nil))
	_, err := ch.QueueDeclare(queue, true, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.QueueBind(queue, "#", exchange, false, nil))
}

// TestRabbitMQLoad_Throughput publishes and consumes many messages over two
// connections with concurrent publishers and consumer workers; every message
// arrives, none twice (no connection trouble here).
func TestRabbitMQLoad_Throughput(t *testing.T) {
	t.Parallel()
	requireLoad(t)

	total := loadMessages(t)
	exchange, queue := rabbitName(t, "x"), rabbitName(t, "q")
	tracker := newLoadTracker(total)

	c := newLoadConsumer(t, newRabbitConnection(t, rabbitConfig, "consume"), exchange, queue, 16,
		rabbitmq.HandlerFunc(func(_ context.Context, msg *rabbitmq.Message) error { return tracker.mark(msg) }))
	runRabbitConsumer(t, c)

	pub := newRabbitPublisher(t, newRabbitConnection(t, rabbitConfig, "publish"))

	start := time.Now()
	stats := publishLoad(t, pub, exchange, total, 8, 50)
	t.Logf("published %s, %d failed calls, slowest Publish %s",
		rate(total, time.Since(start)), stats.failures, stats.slowest.Round(time.Millisecond))

	tracker.waitAll(t)
	t.Logf("consumed %s", rate(total, time.Since(start)))

	unique, dups := tracker.counts()
	assert.Equal(t, total, unique)
	assert.Zero(t, dups, "no redelivery without failures")
	assert.Zero(t, stats.failures)
}

// TestRabbitMQLoad_FlakyHandlers fails the first attempt of every fifth
// message and of every seventh one panics: the even ones are requeued, the
// others retried in place. Every message is still handled, and the queue
// ends empty.
func TestRabbitMQLoad_FlakyHandlers(t *testing.T) {
	t.Parallel()
	requireLoad(t)

	total := loadMessages(t) / 4
	exchange, queue := rabbitName(t, "x"), rabbitName(t, "q")
	tracker := newLoadTracker(total)

	var (
		attempts sync.Map
		requeues atomic.Int64
		retries  atomic.Int64
	)

	h := rabbitmq.HandlerFunc(func(_ context.Context, msg *rabbitmq.Message) error {
		id, _ := strconv.Atoi(msg.MessageID)
		n, _ := attempts.LoadOrStore(id, new(atomic.Int32))

		if n.(*atomic.Int32).Add(1) == 1 { //nolint:forcetypeassert // only *atomic.Int32 is stored
			switch {
			case id%7 == 0:
				panic("flaky handler")
			case id%5 == 0:
				return errors.New("flaky")
			}
		}

		return tracker.mark(msg)
	})

	errorHandler := func(_ context.Context, msg *rabbitmq.Message, err error) rabbitmq.ErrorAction {
		if errors.Is(err, rabbitmq.ErrRetriesExhausted) {
			return rabbitmq.ErrorActionStop
		}

		id, _ := strconv.Atoi(msg.MessageID)
		if id%2 == 0 {
			requeues.Add(1)

			return rabbitmq.ErrorActionRequeue
		}

		retries.Add(1)

		return rabbitmq.ErrorActionRetry
	}

	c := newLoadConsumerOnError(t, newRabbitConnection(t, rabbitConfig, "consume"), exchange, queue, 16, h,
		errorHandler)
	stop := runRabbitConsumer(t, c)

	pub := newRabbitPublisher(t, newRabbitConnection(t, rabbitConfig, "publish"))
	publishLoad(t, pub, exchange, total, 8, 50)

	tracker.waitAll(t)
	require.NoError(t, stop())

	unique, dups := tracker.counts()
	t.Logf("handled %d unique, %d duplicates; %d requeued, %d retried in place",
		unique, dups, requeues.Load(), retries.Load())
	assert.Equal(t, total, unique)
	assert.Positive(t, requeues.Load())
	assert.Positive(t, retries.Load())
	assert.Zero(t, readyMessages(t, rawChannel(t, rabbitConfig), queue))
}

// TestRabbitMQLoad_ConnectionChaos drops every connection of the broker again
// and again while messages are published and consumed: the connections
// redial, the consumer resubscribes, the publishers republish what was not
// confirmed — no message is lost (some arrive twice: at least once).
func TestRabbitMQLoad_ConnectionChaos(t *testing.T) {
	t.Parallel()
	requireLoad(t)

	ctr, cfg := startDedicatedRabbitMQ(t)
	total := loadMessages(t)
	exchange, queue := "load", "load"
	tracker := newLoadTracker(total)

	c := newLoadConsumer(t, newRabbitConnection(t, cfg, "consume"), exchange, queue, 16,
		rabbitmq.HandlerFunc(func(_ context.Context, msg *rabbitmq.Message) error { return tracker.mark(msg) }))
	runRabbitConsumer(t, c)

	pub := newRabbitPublisher(t, newRabbitConnection(t, cfg, "publish"))

	const chaosRounds = 8

	// Drop every connection at each tenth of the publishing progress, from
	// 10% to 80%; the publishing waits at each point until the drop is done,
	// so every drop happens with a fifth or more of the messages to go.
	checkpoints := make([]int64, chaosRounds)
	for i := range checkpoints {
		checkpoints[i] = int64(i+1) * int64(total) / 10
	}

	plan := newFaultPlan(checkpoints...)
	chaosDone := make(chan struct{})
	rounds := 0

	go func() {
		defer close(chaosDone)

		for i := range checkpoints {
			if !plan.reached(t.Context(), i) {
				return
			}

			code, _, err := ctr.Exec(t.Context(), []string{"rabbitmqctl", "-q", "close_all_connections", "chaos"})
			if err == nil && code == 0 {
				rounds++
			}

			plan.done(i)
		}
	}()

	start := time.Now()
	stats := publishLoadWithFaults(t, pub, exchange, total, 8, 10, 0, plan)

	receiveWithin(t, chaosDone, loadWait)
	tracker.waitAll(t)

	unique, dups := tracker.counts()
	t.Logf("%d chaos rounds; delivered %s; %d duplicates; %d failed publish calls (republished); slowest Publish %s",
		rounds, rate(unique, time.Since(start)), dups, stats.failures, stats.slowest.Round(time.Millisecond))
	assert.Equal(t, total, unique)
	assert.Equal(t, chaosRounds, rounds, "every drop happened")
}

// TestRabbitMQLoad_BrokerRestart stops and starts the broker application —
// a graceful restart, not a crash — once a quarter of the messages are
// published, while the consumer runs. It checks that every published
// message still reaches the handler: the connections redial, the consumer
// resubscribes and Publish waits for the broker. It does not test what a
// crash would lose.
func TestRabbitMQLoad_BrokerRestart(t *testing.T) {
	t.Parallel()
	requireLoad(t)

	ctr, cfg := startDedicatedRabbitMQ(t)
	total := loadMessages(t)
	exchange, queue := "load", "load"
	tracker := newLoadTracker(total)

	c := newLoadConsumer(t, newRabbitConnection(t, cfg, "consume"), exchange, queue, 16,
		rabbitmq.HandlerFunc(func(_ context.Context, msg *rabbitmq.Message) error {
			time.Sleep(100 * time.Microsecond) // let a backlog build up

			return tracker.mark(msg)
		}))
	runRabbitConsumer(t, c)

	pub := newRabbitPublisher(t, newRabbitConnection(t, cfg, "publish"))

	// At a quarter of the publishing, the workers wait until the broker is
	// down, then publish into the downtime: the rest of the messages wait for
	// the restarted broker.
	plan := newFaultPlan(int64(total) / 4)
	restartErr := make(chan error, 1)

	go func() {
		if !plan.reached(t.Context(), 0) {
			restartErr <- errors.New("the publishing did not reach the restart")

			return
		}

		err := rabbitctlErr(t.Context(), ctr, "stop_app")

		plan.done(0)

		if err == nil {
			time.Sleep(2 * time.Second) // the downtime, not a synchronization

			err = rabbitctlErr(t.Context(), ctr, "start_app")
		}

		restartErr <- err
	}()

	start := time.Now()
	stats := publishLoadWithFaults(t, pub, exchange, total, 8, 10, 25*time.Millisecond, plan)

	tracker.waitAll(t)
	require.NoError(t, receiveWithin(t, restartErr, loadWait))

	unique, dups := tracker.counts()
	t.Logf("broker restarted mid-load; delivered %s; %d duplicates; %d failed publish calls (republished); "+
		"slowest Publish %s", rate(unique, time.Since(start)), dups, stats.failures,
		stats.slowest.Round(time.Millisecond))
	assert.Equal(t, total, unique)
	// The broker confirms what it took before it stops; the publishes that
	// come during the restart wait for the connection instead of failing.
	assert.Greater(t, stats.slowest, time.Second, "Publish must have waited for the restarted broker")
}

// TestRabbitMQLoad_GracefulStopUnderLoad stops an app.App in the middle of a
// backlog. Its consumer handler republishes every message to a second queue;
// after the stop, every message is either handled (and its copy published)
// or still in the input queue — none is lost, even those in flight during
// the drain.
func TestRabbitMQLoad_GracefulStopUnderLoad(t *testing.T) {
	t.Parallel()
	requireLoad(t)

	total := loadMessages(t) / 2
	exchange, in, out := rabbitName(t, "x"), rabbitName(t, "in"), rabbitName(t, "out")
	tracker := newLoadTracker(total)

	newConn := func(name string) *rabbitmq.Connection {
		conn, err := rabbitmq.NewConnection(rabbitConfig, rabbitOptions(name)...)
		require.NoError(t, err)

		return conn
	}

	publishConn, consumeConn := newConn("publish"), newConn("consume")

	pub, err := rabbitmq.NewPublisher(publishConn, rabbitOptions("publisher")...)
	require.NoError(t, err)

	var handled atomic.Int64

	h := rabbitmq.HandlerFunc(func(ctx context.Context, msg *rabbitmq.Message) error {
		time.Sleep(time.Millisecond)

		copied := *msg
		copied.Exchange, copied.RoutingKey = "", out // to the out queue by the default exchange

		if err := pub.Publish(ctx, &copied); err != nil {
			return err
		}

		handled.Add(1)

		return tracker.mark(msg)
	})

	c := newLoadConsumer(t, consumeConn, exchange, in, 16, h)

	// Declare the queues, then load the input queue on a connection of its
	// own.
	ch := rawChannel(t, rabbitConfig)
	_, err = ch.QueueDeclare(out, true, false, false, false, nil)
	require.NoError(t, err)
	declareLoadQueue(t, ch, exchange, in)

	feeder := newRabbitPublisher(t, newRabbitConnection(t, rabbitConfig, "feeder"))
	publishLoad(t, feeder, exchange, total, 8, 100)

	a := app.New(app.WithSignals(), app.WithLogger(slog.New(slog.DiscardHandler)), app.WithShutdownTimeout(loadWait))
	a.Add(publishConn)
	a.Add(consumeConn)
	a.Add(pub)
	a.Add(c)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ran := make(chan error, 1)

	go func() { ran <- a.Run(ctx) }()

	require.Eventually(t, func() bool { return handled.Load() >= int64(total/3) }, loadWait, time.Millisecond)

	stopAt := handled.Load()
	stopStart := time.Now()

	cancel()
	require.NoError(t, <-ran)

	finished := handled.Load()
	t.Logf("stopped at %d handled, %d more finished during the drain (%s)",
		stopAt, finished-stopAt, time.Since(stopStart).Round(time.Millisecond))

	remaining := drainQueue(t, ch, in)
	copies := drainQueue(t, ch, out)

	unique, dups := tracker.counts()
	assert.Zero(t, dups)
	tracker.requireNoStrays(t)
	assert.Equal(t, total, unique+len(remaining), "every message is handled or still queued")

	for id, n := range remaining {
		assert.Equal(t, 1, n, "message %d queued more than once", id)
		assert.False(t, tracker.has(id), "message %d both handled and still queued", id)
	}

	// Exactly one copy of each handled message, and no other.
	for id, n := range copies {
		assert.True(t, tracker.has(id), "copy of message %d, which was not handled", id)
		assert.Equal(t, 1, n, "message %d republished %d times", id, n)
	}

	assert.Len(t, copies, unique, "every handled message was republished, also during the drain")
}

// drainQueue takes every message of queue and returns how often each ID was
// in it.
func drainQueue(t *testing.T, ch *amqp.Channel, queue string) map[int]int {
	t.Helper()

	ids := map[int]int{}

	for {
		d, ok, err := ch.Get(queue, true)
		require.NoError(t, err)

		if !ok {
			return ids
		}

		id, err := strconv.Atoi(d.MessageId)
		require.NoError(t, err)

		ids[id]++
	}
}

// TestRabbitMQLoad_LargeMessages sends 1 MiB messages, split into many AMQP
// frames, concurrently, and checks that every body arrives intact.
func TestRabbitMQLoad_LargeMessages(t *testing.T) {
	t.Parallel()
	requireLoad(t)

	const size = 1 << 20

	total := 200
	exchange, queue := rabbitName(t, "x"), rabbitName(t, "q")
	tracker := newLoadTracker(total)

	var corrupt atomic.Int64

	body := func(id int) []byte {
		return bytes.Repeat([]byte{byte(id)}, size)
	}

	c := newLoadConsumer(t, newRabbitConnection(t, rabbitConfig, "consume"), exchange, queue, 8,
		rabbitmq.HandlerFunc(func(_ context.Context, msg *rabbitmq.Message) error {
			id, _ := strconv.Atoi(msg.MessageID)
			if !bytes.Equal(msg.Body, body(id)) {
				corrupt.Add(1)
			}

			return tracker.mark(msg)
		}))
	runRabbitConsumer(t, c)

	pub := newRabbitPublisher(t, newRabbitConnection(t, rabbitConfig, "publish"))

	var wg sync.WaitGroup

	start := time.Now()

	for w := range 8 {
		wg.Go(func() {
			for id := w; id < total; id += 8 {
				assert.NoError(t, pub.Publish(t.Context(), loadMessage(exchange, id, body(id))))
			}
		})
	}

	wg.Wait()
	tracker.waitAll(t)

	t.Logf("moved %d MiB: %s", total, rate(total, time.Since(start)))
	assert.Zero(t, corrupt.Load())
}
