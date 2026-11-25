package kafka_test

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/eidon-go/core/contrib/kafka"
	"github.com/eidon-go/core/retry"
)

const (
	testTopic = "orders"
	testGroup = "billing"
	waitFor   = 10 * time.Second
)

// newCluster starts an in-memory Kafka cluster with the given topics (one
// partition each, so that the order of all messages is defined).
func newCluster(t *testing.T, topics ...string) []string {
	t.Helper()

	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topics...))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	return cluster.ListenAddrs()
}

// testOptions are quiet, fast options for tests; extra ones come after.
func testOptions(extra ...kafka.Option) []kafka.Option {
	return append([]kafka.Option{
		kafka.WithLogger(slog.New(slog.DiscardHandler)),
		kafka.WithConnectRetry(
			retry.Policy{MaxAttempts: 3, InitialDelay: 10 * time.Millisecond},
		),
		kafka.WithTracerProvider(tracenoop.NewTracerProvider()),
		kafka.WithMeterProvider(noop.NewMeterProvider()),
		kafka.WithRedeliveryPolicy(
			retry.Policy{MaxAttempts: 3, InitialDelay: time.Millisecond},
		),
	}, extra...)
}

func consumerConfig(brokers []string) kafka.ConsumerConfig {
	return kafka.ConsumerConfig{
		Config: kafka.Config{Brokers: brokers},
		Group:  testGroup,
		Topics: []string{testTopic},
		Offset: kafka.OffsetOldest,
	}
}

// newProducer returns a started producer, shut down when the test ends.
func newProducer(t *testing.T, brokers []string, opts ...kafka.Option) *kafka.Producer {
	t.Helper()

	p, err := kafka.NewProducer(kafka.Config{Brokers: brokers}, testOptions(opts...)...)
	require.NoError(t, err)
	shutdownOnCleanup(t, p)
	require.NoError(t, p.Start(t.Context()))

	return p
}

// shutdownOnCleanup shuts r down, bounded, when the test ends.
func shutdownOnCleanup(t *testing.T, r interface {
	Shutdown(ctx context.Context) error
},
) {
	t.Helper()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), waitFor)
		defer cancel()

		_ = r.Shutdown(ctx)
	})
}

// boundedCtx is a context for cleanup: not cancelled with the test, but
// bounded.
func boundedCtx(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), waitFor)
	t.Cleanup(cancel)

	return ctx
}

// receive waits for a value of ch, failing the test after waitFor.
func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()

	select {
	case v := <-ch:
		return v
	case <-time.After(waitFor):
		require.FailNow(t, "nothing received")

		var zero T

		return zero
	}
}

// runToEnd runs c until Run returns by itself, failing the test after
// waitFor.
func runToEnd(t *testing.T, c *kafka.Consumer) error {
	t.Helper()

	done := make(chan error, 1)

	go func() { done <- c.Run(t.Context()) }()

	return receive(t, done)
}

// produce publishes values to topic, in order, with a plain franz-go client.
func produce(t *testing.T, brokers []string, topic string, values ...string) { //nolint:unparam // for any topic
	t.Helper()

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)

	defer client.Close()

	for _, v := range values {
		require.NoError(
			t,
			client.ProduceSync(t.Context(), &kgo.Record{Topic: topic, Value: []byte(v)}).FirstErr(),
		)
	}
}

// readAll reads every record of topic from the start, waiting until n have
// arrived.
func readAll(t *testing.T, brokers []string, topic string, n int) []*kgo.Record {
	t.Helper()

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)

	defer client.Close()

	ctx, cancel := context.WithTimeout(t.Context(), waitFor)
	defer cancel()

	var records []*kgo.Record

	for len(records) < n {
		fetches := client.PollFetches(ctx)
		require.NoError(t, ctx.Err(), "got %d of %d records", len(records), n)
		records = append(records, fetches.Records()...)
	}

	return records
}

// recorder is a Handler that records the values it handled and can fail or
// block on chosen values.
type recorder struct {
	mu      sync.Mutex
	handled []string
	calls   map[string]int
	handle  func(ctx context.Context, msg *kafka.Message, call int) error // optional
	seen    chan string                                                   // receives every value handled, if set
}

func newRecorder() *recorder {
	return &recorder{calls: map[string]int{}, seen: make(chan string, 100)}
}

func (r *recorder) Handle(ctx context.Context, msg *kafka.Message) error {
	v := string(msg.Value)

	r.mu.Lock()
	r.calls[v]++
	call := r.calls[v]
	r.mu.Unlock()

	if r.handle != nil {
		if err := r.handle(ctx, msg, call); err != nil {
			return err
		}
	}

	r.mu.Lock()
	r.handled = append(r.handled, v)
	r.mu.Unlock()

	r.seen <- v

	return nil
}

func (r *recorder) values() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.handled...)
}

func (r *recorder) callsOf(v string) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.calls[v]
}

// waitSeen waits until the recorder has handled every value in values.
func (r *recorder) waitSeen(t *testing.T, values ...string) {
	t.Helper()

	want := map[string]bool{}
	for _, v := range values {
		want[v] = true
	}

	timeout := time.After(waitFor)

	for len(want) > 0 {
		select {
		case v := <-r.seen:
			delete(want, v)
		case <-timeout:
			require.FailNow(t, "values not handled", strings.Join(keys(want), ", "))
		}
	}
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	return out
}

// startConsumer builds and starts a consumer and runs it; stop cancels Run's
// ctx and shuts it down, returning Run's error.
func startConsumer(
	t *testing.T,
	cfg kafka.ConsumerConfig,
	h kafka.Handler,
	opts ...kafka.Option,
) (*kafka.Consumer, func() error) {
	t.Helper()

	c, err := kafka.NewConsumer(cfg, h, testOptions(opts...)...)
	require.NoError(t, err)
	shutdownOnCleanup(t, c) // also when Start fails
	require.NoError(t, c.Start(t.Context()))

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() { done <- c.Run(ctx) }()

	var once sync.Once

	var runErr error

	stop := func() error {
		once.Do(func() {
			cancel()

			sctx, scancel := context.WithTimeout(context.WithoutCancel(t.Context()), waitFor)
			defer scancel()

			require.NoError(t, c.Shutdown(sctx))

			runErr = receive(t, done)
		})

		return runErr
	}

	t.Cleanup(func() { _ = stop() })

	return c, stop
}
