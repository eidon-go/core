package integration_test

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/eidon-go/core/contrib/kafka"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
)

const kafkaWait = 30 * time.Second

// kafkaName returns a topic or group name unique to the test.
func kafkaName(t *testing.T, suffix string) string {
	t.Helper()

	return strings.NewReplacer("/", ".", " ", "_").Replace(t.Name()) + "." + suffix
}

func kafkaOptions() []kafka.Option {
	return []kafka.Option{kafka.WithLogger(slog.New(slog.DiscardHandler))}
}

// newKafkaProducer returns a started producer that may create topics; the
// test shuts it down.
func newKafkaProducer(t *testing.T) *kafka.Producer {
	t.Helper()

	p, err := kafka.NewProducer(kafka.Config{Brokers: kafkaBrokers},
		append(kafkaOptions(), kafka.WithKgoOpts(kgo.AllowAutoTopicCreation()))...)
	require.NoError(t, err)
	require.NoError(t, p.Start(t.Context()))
	t.Cleanup(func() { assert.NoError(t, p.Shutdown(context.WithoutCancel(t.Context()))) })

	return p
}

func kafkaConsumerConfig(topic, group string) kafka.ConsumerConfig {
	return kafka.ConsumerConfig{
		Config: kafka.Config{Brokers: kafkaBrokers},
		Group:  group,
		Topics: []string{topic},
		Offset: kafka.OffsetOldest,
	}
}

// collector records the values it handles.
type collector struct {
	mu     sync.Mutex
	values []string
	seen   chan string
}

func newCollector() *collector { return &collector{seen: make(chan string, 100)} }

func (c *collector) Handle(_ context.Context, msg *kafka.Message) error {
	c.mu.Lock()
	c.values = append(c.values, string(msg.Value))
	c.mu.Unlock()

	c.seen <- string(msg.Value)

	return nil
}

func (c *collector) wait(t *testing.T, n int) {
	t.Helper()

	for range n {
		select {
		case <-c.seen:
		case <-time.After(kafkaWait):
			require.FailNow(t, "message not handled")
		}
	}
}

func (c *collector) got() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]string(nil), c.values...)
}

// runKafkaConsumer starts c and runs it until the returned stop is called —
// or the test ends, also when it fails before calling stop. stop is
// idempotent.
func runKafkaConsumer(t *testing.T, c *kafka.Consumer) func() {
	t.Helper()

	t.Cleanup(func() { // also when Start fails
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), kafkaWait)
		defer cancel()

		_ = c.Shutdown(ctx)
	})

	require.NoError(t, c.Start(t.Context()))

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() { done <- c.Run(ctx) }()

	var once sync.Once

	stop := func() {
		once.Do(func() {
			cancel()

			sctx, scancel := context.WithTimeout(context.WithoutCancel(t.Context()), kafkaWait)
			defer scancel()

			require.NoError(t, c.Shutdown(sctx))

			// Shutdown bounds the drain; once it has returned, so has Run.
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(kafkaWait):
				assert.Fail(t, "Run did not return after Shutdown")
			}
		})
	}

	t.Cleanup(stop)

	return stop
}

func TestKafka_ConsumerContract(t *testing.T) {
	t.Parallel()
	requireDocker(t)

	topic := kafkaName(t, "topic")
	require.NoError(t, newKafkaProducer(t).Produce(t.Context(), topic, nil, []byte("seed")))

	lifecycletest.Runner(t, func(t *testing.T) lifecycle.Runner {
		t.Helper()

		c, err := kafka.NewConsumer(
			kafkaConsumerConfig(topic, kafkaName(t, "group")),
			newCollector(),
			kafkaOptions()...)
		require.NoError(t, err)

		return c
	}, lifecycletest.WithTimeout(2*kafkaWait))
}

func TestKafka_ProducerContract(t *testing.T) {
	t.Parallel()
	requireDocker(t)

	lifecycletest.Resource(t, func(t *testing.T) lifecycle.Resource {
		t.Helper()

		p, err := kafka.NewProducer(kafka.Config{Brokers: kafkaBrokers}, kafkaOptions()...)
		require.NoError(t, err)

		return p
	}, lifecycletest.WithTimeout(kafkaWait))
}

// Messages are handled in order and committed: a consumer of the same group
// started later goes on after them.
func TestKafka_ConsumeAndCommit(t *testing.T) {
	t.Parallel()
	requireDocker(t)

	topic, group := kafkaName(t, "topic"), kafkaName(t, "group")
	producer := newKafkaProducer(t)

	for _, v := range []string{"a", "b", "c"} {
		require.NoError(t, producer.Produce(t.Context(), topic, []byte("key"), []byte(v)))
	}

	first := newCollector()
	c, err := kafka.NewConsumer(kafkaConsumerConfig(topic, group), first, kafkaOptions()...)
	require.NoError(t, err)

	stop := runKafkaConsumer(t, c)
	first.wait(t, 3)
	require.NoError(t, c.Healthcheck(t.Context()))
	stop()
	assert.Equal(t, []string{"a", "b", "c"}, first.got())

	require.NoError(t, producer.Produce(t.Context(), topic, []byte("key"), []byte("d")))

	second := newCollector()
	c2, err := kafka.NewConsumer(kafkaConsumerConfig(topic, group), second, kafkaOptions()...)
	require.NoError(t, err)

	stop2 := runKafkaConsumer(t, c2)
	second.wait(t, 1)
	stop2()
	assert.Equal(t, []string{"d"}, second.got())
}
