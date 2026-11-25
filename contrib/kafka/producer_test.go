package kafka_test

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/eidon-go/core/contrib/kafka"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
)

func TestClient_Contract(t *testing.T) {
	t.Parallel()

	lifecycletest.Resource(t, func(t *testing.T) lifecycle.Resource {
		t.Helper()

		c, err := kafka.NewClient(
			kafka.Config{Brokers: newCluster(t, testTopic)},
			testOptions()...)
		require.NoError(t, err)

		return c
	}, lifecycletest.WithTimeout(30*time.Second))
}

func TestProducer_Contract(t *testing.T) {
	t.Parallel()

	lifecycletest.Resource(t, func(t *testing.T) lifecycle.Resource {
		t.Helper()

		p, err := kafka.NewProducer(
			kafka.Config{Brokers: newCluster(t, testTopic)},
			testOptions()...)
		require.NoError(t, err)

		return p
	}, lifecycletest.WithTimeout(30*time.Second))
}

func TestClient_Lifecycle(t *testing.T) {
	t.Parallel()

	brokers := newCluster(t, testTopic)

	c, err := kafka.NewClient(kafka.Config{Brokers: brokers, ClientID: "svc"}, testOptions(kafka.WithName("events"))...)
	require.NoError(t, err)
	assert.Equal(t, "events", c.Name())
	require.NotNil(t, c.Kgo())

	require.NoError(t, c.Start(t.Context()))
	require.NoError(t, c.Healthcheck(t.Context()))
	require.NoError(t, c.Shutdown(t.Context()))
	require.ErrorIs(t, c.Healthcheck(t.Context()), kafka.ErrClosed)
	require.ErrorIs(t, c.Start(t.Context()), kafka.ErrClosed)
}

func TestClient_StartFailsWithoutCluster(t *testing.T) {
	t.Parallel()

	c, err := kafka.NewClient(kafka.Config{Brokers: []string{"127.0.0.1:1"}}, testOptions()...)
	require.NoError(t, err)

	err = c.Start(t.Context())
	require.ErrorContains(t, err, "kafka: ping")
	require.NoError(t, c.Shutdown(t.Context()))
}

func TestNew_InvalidConfig(t *testing.T) {
	t.Parallel()

	_, err := kafka.NewClient(kafka.Config{})
	require.ErrorIs(t, err, kafka.ErrInvalidConfig)

	_, err = kafka.NewProducer(kafka.Config{})
	require.ErrorIs(t, err, kafka.ErrInvalidConfig)
}

func TestOptions(t *testing.T) {
	t.Parallel()

	cfg := kafka.Config{Brokers: []string{"localhost:9092"}}
	opts := testOptions(
		kafka.WithName("events"),
		kafka.WithTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}),
		kafka.WithKgoOpts(kgo.ProducerLinger(time.Millisecond)),
	)

	p, err := kafka.NewProducer(cfg, opts...)
	require.NoError(t, err)
	assert.Equal(t, "events", p.Name())
	assert.NotNil(t, p.Kgo())
	require.NoError(t, p.Shutdown(t.Context()))

	c, err := kafka.NewConsumer(consumerConfig(cfg.Brokers), newRecorder(), opts...)
	require.NoError(t, err)
	assert.Equal(t, "events", c.Name())
	require.NoError(t, c.Shutdown(t.Context()))

	_, err = kafka.NewClient(cfg, kafka.WithKgoOpts(kgo.MaxBufferedRecords(-1)))
	require.ErrorIs(t, err, kafka.ErrInvalidConfig, "franz-go rejects the options")

	plain, err := kafka.NewClient(cfg)
	require.NoError(t, err)
	assert.Equal(t, kafka.DefaultName, plain.Name())
	require.NoError(t, plain.Shutdown(t.Context()))
}

func TestProducer_Publish(t *testing.T) {
	t.Parallel()

	brokers := newCluster(t, testTopic)

	p, err := kafka.NewProducer(kafka.Config{Brokers: brokers}, testOptions()...)
	require.NoError(t, err)
	require.NoError(t, p.Start(t.Context()))
	require.NoError(t, p.Healthcheck(t.Context()))

	require.NoError(t, p.Publish(t.Context(),
		&kafka.Message{Topic: testTopic, Key: []byte("k1"), Value: []byte("a")},
		&kafka.Message{Topic: testTopic, Value: []byte("b")},
	))
	require.NoError(t, p.Produce(t.Context(), testTopic, []byte("k3"), []byte("c")))
	require.NoError(t, p.Shutdown(t.Context()))

	records := readAll(t, brokers, testTopic, 3)
	require.Len(t, records, 3)
	assert.Equal(t, "a", string(records[0].Value))
	assert.Equal(t, "k1", string(records[0].Key))
	assert.Equal(t, "b", string(records[1].Value))
	assert.Equal(t, "c", string(records[2].Value))

	require.Error(t, p.Produce(t.Context(), testTopic, nil, []byte("late")), "publish after Shutdown fails")
}

func TestProducer_PublishErrors(t *testing.T) {
	t.Parallel()

	p, err := kafka.NewProducer(
		kafka.Config{Brokers: newCluster(t, testTopic)},
		testOptions()...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Shutdown(context.WithoutCancel(t.Context())) })

	require.ErrorContains(t, p.Publish(t.Context(), nil), "message 0 is nil")
	require.ErrorContains(
		t,
		p.Publish(t.Context(), &kafka.Message{Value: []byte("x")}),
		"kafka: publish",
	)
}

type publisherFunc func(ctx context.Context, msgs ...*kafka.Message) error

func (f publisherFunc) Publish(ctx context.Context, msgs ...*kafka.Message) error {
	return f(ctx, msgs...)
}

func TestJSONProducer(t *testing.T) {
	t.Parallel()

	type order struct {
		ID int `json:"id"`
	}

	var got []*kafka.Message

	jp := kafka.NewJSONProducer[order](publisherFunc(func(_ context.Context, msgs ...*kafka.Message) error {
		got = append(got, msgs...)

		return nil
	}), testTopic)

	require.NoError(t, jp.Produce(t.Context(), "k", order{ID: 7}))
	require.NoError(t, jp.Produce(t.Context(), "", order{ID: 8}))
	require.Len(t, got, 2)
	assert.Equal(t, testTopic, got[0].Topic)
	assert.Equal(t, []byte("k"), got[0].Key)
	assert.JSONEq(t, `{"id":7}`, string(got[0].Value))
	assert.Equal(t, []kafka.Header{{Key: "content-type", Value: []byte("application/json")}}, got[0].Headers)
	assert.Nil(t, got[1].Key)

	errBroker := errors.New("broker down")
	failing := kafka.NewJSONProducer[order](publisherFunc(func(context.Context, ...*kafka.Message) error {
		return errBroker
	}), testTopic)
	require.ErrorIs(t, failing.Produce(t.Context(), "", order{ID: 1}), errBroker)

	bad := kafka.NewJSONProducer[chan int](
		publisherFunc(func(context.Context, ...*kafka.Message) error { return nil }),
		testTopic,
	)
	require.ErrorContains(t, bad.Produce(t.Context(), "", make(chan int)), "kafka: encode")
}

func TestDefaultErrorHandler(t *testing.T) {
	t.Parallel()

	msg := &kafka.Message{}
	errDown := errors.New("down")

	assert.Equal(t, kafka.ErrorActionRetry, kafka.DefaultErrorHandler(t.Context(), msg, errDown))
	assert.Equal(t, kafka.ErrorActionStop,
		kafka.DefaultErrorHandler(t.Context(), msg, errors.Join(kafka.ErrRetriesExhausted, errDown)))
}

func TestDeadLetter_PublishFailureStops(t *testing.T) {
	t.Parallel()

	h := kafka.DeadLetter(publisherFunc(func(context.Context, ...*kafka.Message) error {
		return errors.New("dlt down")
	}), "dlt")

	msg := &kafka.Message{Topic: testTopic}
	assert.Equal(t, kafka.ErrorActionRetry, h(t.Context(), msg, errors.New("transient")))
	assert.Equal(t, kafka.ErrorActionStop, h(t.Context(), msg, kafka.ErrRetriesExhausted))
}
