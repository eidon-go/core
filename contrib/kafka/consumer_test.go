package kafka_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/eidon-go/core/consume"
	"github.com/eidon-go/core/contrib/kafka"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
	"github.com/eidon-go/core/retry"
)

func TestConsumer_Contract(t *testing.T) {
	t.Parallel()

	lifecycletest.Runner(t, func(t *testing.T) lifecycle.Runner {
		t.Helper()

		c, err := kafka.NewConsumer(consumerConfig(newCluster(t, testTopic)), newRecorder(), kafka.StopOnFailure,
			testOptions()...)
		require.NoError(t, err)

		return c
	}, lifecycletest.WithTimeout(30*time.Second))
}

func TestNewConsumer_Errors(t *testing.T) {
	t.Parallel()

	cfg := consumerConfig([]string{"localhost:9092"})

	_, err := kafka.NewConsumer(cfg, nil, kafka.StopOnFailure)
	require.ErrorIs(t, err, kafka.ErrInvalidConfig)

	var nilHandler *recorder

	_, err = kafka.NewConsumer(cfg, nilHandler, kafka.StopOnFailure)
	require.ErrorIs(t, err, kafka.ErrInvalidConfig)

	_, err = kafka.NewConsumer(cfg, newRecorder(), nil)
	require.ErrorIs(t, err, kafka.ErrInvalidConfig, "the error strategy is required")

	cfg.Group = ""
	_, err = kafka.NewConsumer(cfg, newRecorder(), kafka.StopOnFailure)
	require.ErrorIs(t, err, kafka.ErrInvalidConfig)
}

func TestConsumer_StateErrors(t *testing.T) {
	t.Parallel()

	brokers := newCluster(t, testTopic)

	t.Run("Run without Start", func(t *testing.T) {
		t.Parallel()

		c, err := kafka.NewConsumer(consumerConfig(brokers), newRecorder(), kafka.StopOnFailure, testOptions()...)
		require.NoError(t, err)
		require.ErrorIs(t, c.Run(t.Context()), kafka.ErrNotStarted)
		require.NoError(t, c.Shutdown(t.Context()))
	})

	t.Run("Start twice, Start after Shutdown", func(t *testing.T) {
		t.Parallel()

		c, err := kafka.NewConsumer(consumerConfig(brokers), newRecorder(), kafka.StopOnFailure, testOptions()...)
		require.NoError(t, err)
		require.NoError(t, c.Start(t.Context()))
		require.ErrorIs(t, c.Start(t.Context()), kafka.ErrAlreadyStarted)
		require.NoError(t, c.Shutdown(t.Context()))
		require.ErrorIs(t, c.Start(t.Context()), kafka.ErrClosed)
		require.NoError(t, c.Run(t.Context()), "Run after Shutdown returns nil")
	})

	t.Run("Start fails when the cluster does not answer", func(t *testing.T) {
		t.Parallel()

		cfg := consumerConfig([]string{"127.0.0.1:1"})
		c, err := kafka.NewConsumer(cfg, newRecorder(), kafka.StopOnFailure, testOptions()...)
		require.NoError(t, err)
		require.Error(t, c.Start(t.Context()))
		require.NoError(t, c.Shutdown(t.Context()))
	})
}

func TestConsumer_HandlesInOrderAndCommits(t *testing.T) {
	t.Parallel()

	brokers := newCluster(t, testTopic)
	produce(t, brokers, testTopic, "a", "b", "c")

	rec := newRecorder()
	c, stop := startConsumer(t, consumerConfig(brokers), rec)
	rec.waitSeen(t, "a", "b", "c")
	require.NoError(t, c.Healthcheck(t.Context()))
	require.NoError(t, stop())
	assert.Equal(t, []string{"a", "b", "c"}, rec.values())
	require.ErrorIs(t, c.Healthcheck(t.Context()), kafka.ErrNotRunning)

	// The same group goes on after the committed messages.
	produce(t, brokers, testTopic, "d")

	next := newRecorder()
	_, stopNext := startConsumer(t, consumerConfig(brokers), next)
	next.waitSeen(t, "d")
	require.NoError(t, stopNext())
	assert.Equal(t, []string{"d"}, next.values())
}

func TestConsumer_MessageFields(t *testing.T) {
	t.Parallel()

	brokers := newCluster(t, testTopic)

	p := newProducer(t, brokers)

	ts := time.UnixMilli(1_700_000_000_000)
	require.NoError(t, p.Publish(t.Context(), &kafka.Message{
		Topic: testTopic, Key: []byte("k"), Value: []byte("v"), Timestamp: ts,
		Headers: []kafka.Header{{Key: "h", Value: []byte("1")}},
	}))

	got := make(chan *kafka.Message, 1)
	_, stop := startConsumer(
		t,
		consumerConfig(brokers),
		kafka.HandlerFunc(func(_ context.Context, m *kafka.Message) error {
			got <- m

			return nil
		}),
	)

	m := receive(t, got)

	require.NoError(t, stop())
	assert.Equal(t, testTopic, m.Topic)
	assert.Equal(t, int32(0), m.Partition)
	assert.Equal(t, int64(0), m.Offset)
	assert.Equal(t, []byte("k"), m.Key)
	assert.Equal(t, []byte("v"), m.Value)
	assert.Equal(t, []kafka.Header{{Key: "h", Value: []byte("1")}}, m.Headers)
	assert.True(t, ts.Equal(m.Timestamp))
}

// Stopping lets the message in flight finish on its context, commits it and
// takes no other message.
func TestConsumer_StopFinishesTheMessageInFlight(t *testing.T) {
	t.Parallel()

	brokers := newCluster(t, testTopic)
	produce(t, brokers, testTopic, "a", "b")

	entered := make(chan struct{})
	release := make(chan struct{})
	rec := newRecorder()
	rec.handle = func(ctx context.Context, msg *kafka.Message, _ int) error {
		if string(msg.Value) == "a" {
			close(entered)
			<-release

			return ctx.Err() // nil: not cancelled by the stop
		}

		return nil
	}

	c, err := kafka.NewConsumer(consumerConfig(brokers), rec, kafka.StopOnFailure, testOptions()...)
	require.NoError(t, err)
	shutdownOnCleanup(t, c)
	require.NoError(t, c.Start(t.Context()))

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() { done <- c.Run(ctx) }()

	receive(t, entered)
	cancel() // as app.App does on a signal

	close(release)
	require.NoError(t, receive(t, done))
	require.NoError(t, c.Shutdown(t.Context()))
	assert.Equal(t, []string{"a"}, rec.values())

	next := newRecorder()
	_, stop := startConsumer(t, consumerConfig(brokers), next)
	next.waitSeen(t, "b")
	require.NoError(t, stop())
	assert.Equal(t, []string{"b"}, next.values(), "a was committed")
}

// Shutdown's deadline cancels the handler's context and Run returns.
func TestConsumer_ShutdownDeadlineCancelsTheHandler(t *testing.T) {
	t.Parallel()

	brokers := newCluster(t, testTopic)
	produce(t, brokers, testTopic, "a")

	entered := make(chan struct{})
	handlerErr := make(chan error, 1)
	h := kafka.HandlerFunc(func(ctx context.Context, _ *kafka.Message) error {
		close(entered)
		<-ctx.Done()

		handlerErr <- ctx.Err()

		return ctx.Err()
	})

	c, err := kafka.NewConsumer(consumerConfig(brokers), h,
		func(context.Context, *kafka.Message, error) kafka.ErrorAction {
			return kafka.ErrorActionRetry
		}, testOptions()...)
	require.NoError(t, err)
	shutdownOnCleanup(t, c)
	require.NoError(t, c.Start(t.Context()))

	done := make(chan error, 1)

	go func() { done <- c.Run(t.Context()) }()

	receive(t, entered)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, c.Shutdown(ctx), context.DeadlineExceeded)
	require.ErrorIs(t, receive(t, handlerErr), context.Canceled)
	require.NoError(t, receive(t, done))
}

func TestConsumer_RetryThenSuccess(t *testing.T) {
	t.Parallel()

	brokers := newCluster(t, testTopic)
	produce(t, brokers, testTopic, "a", "b")

	var attempts []int

	rec := newRecorder()
	rec.handle = func(_ context.Context, msg *kafka.Message, call int) error {
		if string(msg.Value) == "a" && call < 3 {
			return errors.New("db is down")
		}

		return nil
	}

	policy := retry.Policy{
		MaxAttempts:  3,
		InitialDelay: time.Millisecond,
		OnRetry: func(_ context.Context, a retry.Attempt) {
			attempts = append(attempts, a.Number)
		},
	}

	_, stop := startConsumer(t, consumerConfig(brokers), rec, kafka.WithRedeliveryPolicy(policy))
	rec.waitSeen(t, "a", "b")
	require.NoError(t, stop())
	assert.Equal(t, []string{"a", "b"}, rec.values(), "order kept")
	assert.Equal(t, 3, rec.callsOf("a"))
	assert.Equal(t, []int{1, 2}, attempts)
}

// Without an ErrorHandler, a message that keeps failing stops the consumer
// once the attempts are exhausted, and is not committed.
func TestConsumer_DefaultStopsWhenRetriesAreExhausted(t *testing.T) {
	t.Parallel()

	brokers := newCluster(t, testTopic)
	produce(t, brokers, testTopic, "a", "b", "c")

	errDown := errors.New("db is down")
	rec := newRecorder()
	rec.handle = func(_ context.Context, msg *kafka.Message, _ int) error {
		if string(msg.Value) == "b" {
			return errDown
		}

		return nil
	}

	c, err := kafka.NewConsumer(consumerConfig(brokers), rec, kafka.StopOnFailure, testOptions()...)
	require.NoError(t, err)
	shutdownOnCleanup(t, c)
	require.NoError(t, c.Start(t.Context()))

	err = runToEnd(t, c)
	require.ErrorIs(t, err, kafka.ErrStopped)
	require.ErrorIs(t, err, kafka.ErrRetriesExhausted)
	require.ErrorIs(t, err, errDown)
	assert.Equal(t, 3, rec.callsOf("b"))
	assert.Equal(t, 0, rec.callsOf("c"))
	require.NoError(t, c.Shutdown(t.Context()))

	next := newRecorder()
	_, stop := startConsumer(t, consumerConfig(brokers), next)
	next.waitSeen(t, "b", "c")
	require.NoError(t, stop())
	assert.Equal(t, []string{"b", "c"}, next.values(), "a was committed, b was not")
}

func TestConsumer_PermanentErrorStopsAtOnce(t *testing.T) {
	t.Parallel()

	brokers := newCluster(t, testTopic)
	produce(t, brokers, testTopic, "bad")

	rec := newRecorder()
	rec.handle = func(context.Context, *kafka.Message, int) error {
		return retry.Permanent(errors.New("cannot decode"))
	}

	c, err := kafka.NewConsumer(consumerConfig(brokers), rec, kafka.StopOnFailure, testOptions()...)
	require.NoError(t, err)
	shutdownOnCleanup(t, c)
	require.NoError(t, c.Start(t.Context()))
	require.ErrorIs(t, runToEnd(t, c), kafka.ErrStopped)
	assert.Equal(t, 1, rec.callsOf("bad"))
	require.NoError(t, c.Shutdown(t.Context()))
}

// A batch franz-go will never decompress stalls its partition for good: Run
// fails instead of looking healthy.
func TestConsumer_UndecompressableBatchFailsRun(t *testing.T) {
	t.Parallel()

	brokers := newCluster(t, testTopic)
	gzip := kafka.WithKgoOpts(kgo.ProducerBatchCompression(kgo.GzipCompression()))
	require.NoError(t, newProducer(t, brokers, gzip).Produce(t.Context(), testTopic, nil, make([]byte, 4096)))

	c, err := kafka.NewConsumer(consumerConfig(brokers), newRecorder(), kafka.StopOnFailure,
		testOptions(kafka.WithKgoOpts(kgo.MaxDecompressBatchBytes(64)))...)
	require.NoError(t, err)
	shutdownOnCleanup(t, c)
	require.NoError(t, c.Start(t.Context()))

	err = runToEnd(t, c)
	require.ErrorIs(t, err, kgo.ErrMaxDecompress)
	require.ErrorContains(t, err, "kafka: fetch "+testTopic+"[0]")
}

func TestConsumer_SkipCommits(t *testing.T) {
	t.Parallel()

	brokers := newCluster(t, testTopic)
	produce(t, brokers, testTopic, "bad", "good")

	rec := newRecorder()
	rec.handle = func(_ context.Context, msg *kafka.Message, _ int) error {
		if string(msg.Value) == "bad" {
			return errors.New("invalid")
		}

		return nil
	}

	skip := func(context.Context, *kafka.Message, error) kafka.ErrorAction {
		return kafka.ErrorActionSkip
	}

	_, stop := startConsumerOnError(t, consumerConfig(brokers), rec, skip)
	rec.waitSeen(t, "good")
	require.NoError(t, stop())
	assert.Equal(t, 1, rec.callsOf("bad"))

	produce(t, brokers, testTopic, "next")

	next := newRecorder()
	_, stopNext := startConsumer(t, consumerConfig(brokers), next)
	next.waitSeen(t, "next")
	require.NoError(t, stopNext())
	assert.Equal(t, []string{"next"}, next.values())
}

func TestConsumer_PauseAndResume(t *testing.T) {
	t.Parallel()

	brokers := newCluster(t, testTopic)
	produce(t, brokers, testTopic, "a", "bad", "c")

	var fixed atomic.Bool

	rec := newRecorder()
	rec.handle = func(_ context.Context, msg *kafka.Message, _ int) error {
		if string(msg.Value) == "bad" && !fixed.Load() {
			return errors.New("invalid")
		}

		return nil
	}

	pause := func(context.Context, *kafka.Message, error) kafka.ErrorAction { return kafka.ErrorActionPause }

	c, stop := startConsumerOnError(t, consumerConfig(brokers), rec, pause)

	require.Eventually(t, func() bool { return errors.Is(c.Healthcheck(t.Context()), kafka.ErrPaused) },
		waitFor, 10*time.Millisecond)
	assert.Equal(t, []string{"a"}, rec.values(), "the partition stops at the paused message")
	assert.Zero(t, rec.callsOf("c"))

	fixed.Store(true)
	c.Resume()

	rec.waitSeen(t, "c")
	require.NoError(t, c.Healthcheck(t.Context()))
	require.NoError(t, stop())
	assert.Equal(t, []string{"a", "bad", "c"}, rec.values(), "fetched again from the paused message")
	assert.Equal(t, 2, rec.callsOf("bad"))
}

// With Concurrency 2, a partition whose handler waits does not hold up the
// other one.
func TestConsumer_ConcurrentPartitions(t *testing.T) {
	t.Parallel()

	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, testTopic))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	brokers := cluster.ListenAddrs()

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	require.NoError(t, err)

	for partition, value := range []string{"slow", "fast"} {
		rec := &kgo.Record{Topic: testTopic, Partition: int32(partition), Value: []byte(value)}
		require.NoError(t, client.ProduceSync(t.Context(), rec).FirstErr())
	}

	client.Close()

	fastDone := make(chan struct{})
	handled := make(chan string, 2)

	cfg := consumerConfig(brokers)
	cfg.Concurrency = 2

	_, stop := startConsumer(t, cfg, kafka.HandlerFunc(func(ctx context.Context, msg *kafka.Message) error {
		if string(msg.Value) == "slow" {
			select {
			case <-fastDone: // only reachable while the other partition is handled meanwhile
			case <-ctx.Done():
				return ctx.Err()
			}
		} else {
			close(fastDone)
		}

		handled <- string(msg.Value)

		return nil
	}))

	assert.ElementsMatch(t, []string{"fast", "slow"}, []string{receive(t, handled), receive(t, handled)})
	require.NoError(t, stop())
}

func TestConsumer_HandlerPanicGoesToTheErrorHandler(t *testing.T) {
	t.Parallel()

	brokers := newCluster(t, testTopic)
	produce(t, brokers, testTopic, "a")

	got := make(chan error, 1)
	h := kafka.HandlerFunc(func(context.Context, *kafka.Message) error { panic("boom") })

	_, stop := startConsumerOnError(t, consumerConfig(brokers), h,
		func(_ context.Context, _ *kafka.Message, err error) kafka.ErrorAction {
			got <- err

			return kafka.ErrorActionSkip
		})

	require.ErrorContains(t, receive(t, got), "kafka: handler panicked: boom")
	require.NoError(t, stop())
}

func TestConsumer_ErrorHandlerPanicStops(t *testing.T) {
	t.Parallel()

	brokers := newCluster(t, testTopic)
	produce(t, brokers, testTopic, "a")

	h := kafka.HandlerFunc(func(context.Context, *kafka.Message) error { return errors.New("failed") })
	c, err := kafka.NewConsumer(consumerConfig(brokers), h,
		func(context.Context, *kafka.Message, error) kafka.ErrorAction { panic("boom") }, testOptions()...)
	require.NoError(t, err)
	shutdownOnCleanup(t, c)
	require.NoError(t, c.Start(t.Context()))
	require.ErrorIs(t, runToEnd(t, c), kafka.ErrStopped)
	require.NoError(t, c.Shutdown(t.Context()))
}

// A stop during a redelivery delay ends the wait at once; the message is not
// committed.
func TestConsumer_StopDuringRedeliveryDelay(t *testing.T) {
	t.Parallel()

	brokers := newCluster(t, testTopic)
	produce(t, brokers, testTopic, "a")

	failed := make(chan struct{}, 10)
	rec := newRecorder()
	rec.handle = func(_ context.Context, _ *kafka.Message, call int) error {
		if call == 1 {
			failed <- struct{}{}

			return errors.New("transient")
		}

		return nil
	}

	slow := kafka.WithRedeliveryPolicy(
		retry.Policy{MaxAttempts: retry.Forever, InitialDelay: time.Hour},
	)
	_, stop := startConsumer(t, consumerConfig(brokers), rec, slow)

	receive(t, failed)

	start := time.Now()

	require.NoError(t, stop())
	assert.Less(t, time.Since(start), 5*time.Second)

	next := newRecorder()
	_, stopNext := startConsumer(t, consumerConfig(brokers), next)
	next.waitSeen(t, "a")
	require.NoError(t, stopNext())
}

func TestConsumer_DeadLetter(t *testing.T) {
	t.Parallel()

	const dlt = "orders.dlt"

	brokers := newCluster(t, testTopic, dlt)
	produce(t, brokers, testTopic, "bad", "good")

	p := newProducer(t, brokers)

	rec := newRecorder()
	rec.handle = func(_ context.Context, msg *kafka.Message, _ int) error {
		if string(msg.Value) == "bad" {
			return errors.New("invalid order")
		}

		return nil
	}

	_, stop := startConsumerOnError(t, consumerConfig(brokers), rec, kafka.DeadLetter(p, dlt))
	rec.waitSeen(t, "good")
	require.NoError(t, stop())
	assert.Equal(t, 3, rec.callsOf("bad"), "retried before dead-lettering")

	records := readAll(t, brokers, dlt, 1)
	require.Len(t, records, 1)
	assert.Equal(t, []byte("bad"), records[0].Value)

	headers := map[string]string{}
	for _, h := range records[0].Headers {
		headers[h.Key] = string(h.Value)
	}

	assert.Equal(t, testTopic, headers[kafka.HeaderDLTOriginalTopic])
	assert.Equal(t, "0", headers[kafka.HeaderDLTOriginalPartition])
	assert.Equal(t, "0", headers[kafka.HeaderDLTOriginalOffset])
	assert.Contains(t, headers[kafka.HeaderDLTError], "invalid order")
	assert.Contains(t, headers[kafka.HeaderDLTError], kafka.ErrRetriesExhausted.Error())
}

// The handler runs in a process span, child of the publisher's trace.
func TestConsumer_ProcessSpan(t *testing.T) {
	t.Parallel()

	brokers := newCluster(t, testTopic)
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	tracing := []kafka.Option{kafka.WithTracerProvider(tp), kafka.WithPropagators(propagation.TraceContext{})}

	// The producer gets its providers from WithTelemetry, the consumer one by one.
	p := newProducer(t, brokers, kafka.WithTelemetry(nil), kafka.WithTelemetry(telemetry{tp: tp}))

	ctx, parent := tp.Tracer("test").Start(t.Context(), "request")
	require.NoError(t, p.Produce(ctx, testTopic, nil, []byte("a")))
	parent.End()

	got := make(chan trace.SpanContext, 1)
	_, stop := startConsumer(
		t,
		consumerConfig(brokers),
		kafka.HandlerFunc(func(ctx context.Context, _ *kafka.Message) error {
			got <- trace.SpanContextFromContext(ctx)

			return nil
		}),
		tracing...)

	sc := receive(t, got)

	require.NoError(t, stop())
	assert.Equal(t, parent.SpanContext().TraceID(), sc.TraceID(), "trace continues from the publisher")

	spans := exporter.GetSpans()
	names := make([]string, 0, len(spans))

	for _, s := range spans {
		names = append(names, s.Name)
	}

	assert.Contains(t, names, testTopic+" publish")
	assert.Contains(t, names, testTopic+" process")
}

// The baggage of the publisher reaches the handler.
func TestConsumer_HandlerGetsBaggage(t *testing.T) {
	t.Parallel()

	brokers := newCluster(t, testTopic)
	props := kafka.WithPropagators(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))

	p := newProducer(t, brokers, props)

	tenant, err := baggage.NewMember("tenant", "acme")
	require.NoError(t, err)

	bag, err := baggage.New(tenant)
	require.NoError(t, err)

	require.NoError(t, p.Produce(baggage.ContextWithBaggage(t.Context(), bag), testTopic, nil, []byte("a")))

	got := make(chan string, 1)
	_, stop := startConsumer(t, consumerConfig(brokers),
		kafka.HandlerFunc(func(ctx context.Context, _ *kafka.Message) error {
			got <- baggage.FromContext(ctx).Member("tenant").Value()

			return nil
		}),
		props)

	assert.Equal(t, "acme", receive(t, got))
	require.NoError(t, stop())
}

// telemetry stands in for the Provider of the otel module.
type telemetry struct{ tp trace.TracerProvider }

func (t telemetry) TracerProvider() trace.TracerProvider { return t.tp }

func (telemetry) MeterProvider() metric.MeterProvider { return noop.NewMeterProvider() }

func (telemetry) Propagators() propagation.TextMapPropagator { return propagation.TraceContext{} }

// budgetOptions shrink the rebalance timeout, so that a poll's budget (three
// quarters of it) is 300ms.
func budgetOptions(extra ...kafka.Option) []kafka.Option {
	return testOptions(append([]kafka.Option{
		kafka.WithKgoOpts(kgo.RebalanceTimeout(400 * time.Millisecond)),
	}, extra...)...)
}

// Records that would hold a rebalance past the poll's budget are fetched
// again instead: each is handled once, in order, and the commit never skips
// one.
func TestConsumer_PollBudgetYields(t *testing.T) {
	t.Parallel()

	brokers := newCluster(t, testTopic)
	produce(t, brokers, testTopic, "a", "b", "c", "d", "e", "f")

	rec := newRecorder()
	rec.handle = func(ctx context.Context, _ *kafka.Message, _ int) error {
		// A slow handler is the point; the stop does not cut it short.
		select {
		case <-time.After(120 * time.Millisecond):
		case <-ctx.Done():
		}

		return nil
	}

	logs := &logRecorder{}
	c, err := kafka.NewConsumer(consumerConfig(brokers), rec, kafka.StopOnFailure,
		budgetOptions(kafka.WithLogger(slog.New(logs)))...)
	require.NoError(t, err)
	shutdownOnCleanup(t, c)
	require.NoError(t, c.Start(t.Context()))

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() { done <- c.Run(ctx) }()

	rec.waitSeen(t, "a", "b", "c", "d", "e", "f")
	cancel()
	require.NoError(t, receive(t, done))
	require.NoError(t, c.Shutdown(t.Context()))
	assert.Equal(t, []string{"a", "b", "c", "d", "e", "f"}, rec.values(), "each once, in order")
	assert.Positive(t, logs.count("kafka poll ran out of its budget, the rest is fetched again"),
		"six records of 120ms do not fit one 300ms budget")

	produce(t, brokers, testTopic, "next")

	next := newRecorder()
	_, stop := startConsumer(t, consumerConfig(brokers), next)
	next.waitSeen(t, "next")
	require.NoError(t, stop())
	assert.Equal(t, []string{"next"}, next.values(), "all six were committed")
}

// A redelivery delay past the poll's budget yields; when the record comes
// again its attempts go on instead of starting over, so MaxAttempts still
// bounds them.
func TestConsumer_PollBudgetKeepsAttempts(t *testing.T) {
	t.Parallel()

	brokers := newCluster(t, testTopic)
	produce(t, brokers, testTopic, "bad")

	errDown := errors.New("down")
	rec := newRecorder()
	rec.handle = func(context.Context, *kafka.Message, int) error { return errDown }

	c, err := kafka.NewConsumer(consumerConfig(brokers), rec, kafka.StopOnFailure, budgetOptions(
		kafka.WithRedeliveryPolicy(retry.Policy{MaxAttempts: 2, InitialDelay: time.Second}),
	)...)
	require.NoError(t, err)
	shutdownOnCleanup(t, c)
	require.NoError(t, c.Start(t.Context()))

	err = runToEnd(t, c)
	require.ErrorIs(t, err, kafka.ErrStopped)
	require.ErrorIs(t, err, kafka.ErrRetriesExhausted)
	assert.Equal(t, 2, rec.callsOf("bad"), "the second attempt came after the yield")
}

// countingOnError skips and counts the messages it is asked about.
func countingOnError(asked *atomic.Int32) kafka.ErrorHandler {
	return func(context.Context, *kafka.Message, error) kafka.ErrorAction {
		asked.Add(1)

		return kafka.ErrorActionSkip
	}
}

// A transient error of a dependency is waited out: the ErrorHandler is not
// asked and the message is handled once the dependency is back.
func TestConsumer_TransientErrorIsWaitedOut(t *testing.T) {
	t.Parallel()

	brokers := newCluster(t, testTopic)
	produce(t, brokers, testTopic, "a")

	rec := newRecorder()
	rec.handle = func(_ context.Context, _ *kafka.Message, call int) error {
		if call <= 3 {
			return consume.Transient(errors.New("db down"))
		}

		return nil
	}

	var asked atomic.Int32

	_, stop := startConsumerOnError(t, consumerConfig(brokers), rec, countingOnError(&asked),
		kafka.WithTransientPolicy(retry.Policy{MaxAttempts: retry.Forever, InitialDelay: time.Millisecond}))
	rec.waitSeen(t, "a")
	require.NoError(t, stop())

	assert.Equal(t, 4, rec.callsOf("a"))
	assert.Zero(t, asked.Load(), "a transient error is not the message's fault")
}

// A transient wait past the poll's budget yields to a rebalance like a
// redelivery delay does; the record is handled once the dependency is back.
func TestConsumer_TransientWaitYields(t *testing.T) {
	t.Parallel()

	brokers := newCluster(t, testTopic)
	produce(t, brokers, testTopic, "a")

	rec := newRecorder()
	rec.handle = func(_ context.Context, _ *kafka.Message, call int) error {
		if call <= 2 {
			return consume.Transient(errors.New("db down"))
		}

		return nil
	}

	var asked atomic.Int32

	logs := &logRecorder{}
	_, stop := startConsumerOnError(t, consumerConfig(brokers), rec, countingOnError(&asked),
		kafka.WithKgoOpts(kgo.RebalanceTimeout(400*time.Millisecond)),
		kafka.WithLogger(slog.New(logs)),
		kafka.WithTransientPolicy(retry.Policy{MaxAttempts: retry.Forever, InitialDelay: time.Second}))
	rec.waitSeen(t, "a")
	require.NoError(t, stop())

	assert.Equal(t, 3, rec.callsOf("a"))
	assert.Zero(t, asked.Load())
	assert.Positive(t, logs.count("kafka poll ran out of its budget, the rest is fetched again"))
}

// logRecorder is a slog.Handler that keeps the messages logged.
type logRecorder struct {
	mu   sync.Mutex
	msgs []string
}

func (*logRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (l *logRecorder) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.msgs = append(l.msgs, r.Message)

	return nil
}

func (l *logRecorder) WithAttrs([]slog.Attr) slog.Handler { return l }

func (l *logRecorder) WithGroup(string) slog.Handler { return l }

func (l *logRecorder) count(msg string) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	n := 0

	for _, m := range l.msgs {
		if m == msg {
			n++
		}
	}

	return n
}
