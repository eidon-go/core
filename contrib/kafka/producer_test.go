package kafka_test

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/eidon-go/core/contrib/kafka"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
	"github.com/eidon-go/core/retry"
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
	shutdownOnCleanup(t, c)
	assert.Equal(t, "events", c.Name())
	require.Nil(t, c.Kgo(), "created in Start")
	require.ErrorIs(t, c.Healthcheck(t.Context()), kafka.ErrNotStarted)

	require.NoError(t, c.Start(t.Context()))
	require.NotNil(t, c.Kgo())
	require.ErrorIs(t, c.Start(t.Context()), kafka.ErrAlreadyStarted)
	require.NoError(t, c.Healthcheck(t.Context()))
	require.NoError(t, c.Shutdown(t.Context()))
	require.ErrorIs(t, c.Healthcheck(t.Context()), kafka.ErrClosed)
	require.ErrorIs(t, c.Start(t.Context()), kafka.ErrClosed)
}

func TestClient_StartFailsWithoutCluster(t *testing.T) {
	t.Parallel()

	c, err := kafka.NewClient(kafka.Config{Brokers: []string{"127.0.0.1:1"}}, testOptions()...)
	require.NoError(t, err)
	shutdownOnCleanup(t, c)

	err = c.Start(t.Context())
	require.ErrorContains(t, err, "kafka: ping")
	require.NoError(t, c.Shutdown(t.Context()))
}

// Shutdown ends a Start that is waiting between ping attempts.
func TestClient_ShutdownInterruptsStart(t *testing.T) {
	t.Parallel()

	slow := kafka.WithConnectRetry(retry.Policy{MaxAttempts: retry.Forever, InitialDelay: time.Hour})
	c, err := kafka.NewClient(kafka.Config{Brokers: []string{"127.0.0.1:1"}}, testOptions(slow)...)
	require.NoError(t, err)

	started := make(chan error, 1)

	go func() { started <- c.Start(t.Context()) }()

	require.Eventually(t, func() bool { return c.Kgo() != nil }, waitFor, time.Millisecond)
	require.NoError(t, c.Shutdown(boundedCtx(t)))
	require.ErrorIs(t, receive(t, started), kafka.ErrClosed)
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
	assert.Nil(t, p.Kgo(), "created in Start")
	require.NoError(t, p.Shutdown(t.Context()))

	c, err := kafka.NewConsumer(consumerConfig(cfg.Brokers), newRecorder(), kafka.StopOnFailure, opts...)
	require.NoError(t, err)
	assert.Equal(t, "events", c.Name())
	require.NoError(t, c.Shutdown(t.Context()))

	bad, err := kafka.NewClient(cfg, kafka.WithKgoOpts(kgo.MaxBufferedRecords(-1)))
	require.NoError(t, err)
	require.ErrorIs(t, bad.Start(t.Context()), kafka.ErrInvalidConfig, "franz-go rejects the options")
	require.NoError(t, bad.Shutdown(t.Context()))

	plain, err := kafka.NewClient(cfg)
	require.NoError(t, err)
	assert.Equal(t, kafka.DefaultName, plain.Name())
	require.NoError(t, plain.Shutdown(t.Context()))
}

func TestProducer_Publish(t *testing.T) {
	t.Parallel()

	brokers := newCluster(t, testTopic)

	p := newProducer(t, brokers)
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

	p, err := kafka.NewProducer(kafka.Config{Brokers: newCluster(t, testTopic)}, testOptions()...)
	require.NoError(t, err)
	shutdownOnCleanup(t, p)

	require.ErrorIs(t, p.Produce(t.Context(), testTopic, nil, nil), kafka.ErrNotStarted)
	require.NoError(t, p.Start(t.Context()))
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

func TestStopOnFailure(t *testing.T) {
	t.Parallel()

	msg := &kafka.Message{}
	errDown := errors.New("down")

	assert.Equal(t, kafka.ErrorActionRetry, kafka.StopOnFailure(t.Context(), msg, errDown))
	assert.Equal(t, kafka.ErrorActionStop,
		kafka.StopOnFailure(t.Context(), msg, errors.Join(kafka.ErrRetriesExhausted, errDown)))
}

func TestDeadLetter_PublishFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts []kafka.DeadLetterOption
		want kafka.ErrorAction
	}{
		{name: "pauses by default", want: kafka.ErrorActionPause},
		{
			name: "OnDLTFailure stop",
			opts: []kafka.DeadLetterOption{kafka.OnDLTFailure(kafka.ErrorActionStop)},
			want: kafka.ErrorActionStop,
		},
		{name: "nil option is ignored", opts: []kafka.DeadLetterOption{nil}, want: kafka.ErrorActionPause},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := kafka.DeadLetter(publisherFunc(func(context.Context, ...*kafka.Message) error {
				return errors.New("dlt down")
			}), "dlt", tt.opts...)

			msg := &kafka.Message{Topic: testTopic}
			assert.Equal(t, kafka.ErrorActionRetry, h(t.Context(), msg, errors.New("transient")))
			assert.Equal(t, tt.want, h(t.Context(), msg, kafka.ErrRetriesExhausted))
		})
	}
}

// The dead letter gets the time of publishing (the original goes to a
// header) and a bounded error header.
func TestDeadLetter_Record(t *testing.T) {
	t.Parallel()

	original := time.UnixMilli(1_700_000_000_123)

	tests := []struct {
		name      string
		err       error
		wantError string
	}{
		{
			name:      "short error is kept",
			err:       retry.Permanent(errors.New("bad order")),
			wantError: "bad order",
		},
		{
			name:      "long error is cut on a rune boundary",
			err:       retry.Permanent(errors.New(strings.Repeat("é", kafka.MaxDLTErrorLength))),
			wantError: strings.Repeat("é", (kafka.MaxDLTErrorLength-3)/2) + "...",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var dead *kafka.Message

			h := kafka.DeadLetter(publisherFunc(func(_ context.Context, msgs ...*kafka.Message) error {
				dead = msgs[0]

				return nil
			}), "dlt")

			msg := &kafka.Message{Topic: testTopic, Offset: 7, Timestamp: original, Value: []byte("v")}
			require.Equal(t, kafka.ErrorActionSkip, h(t.Context(), msg, tt.err))
			require.NotNil(t, dead)

			assert.True(t, dead.Timestamp.IsZero(), "set when published, so DLT retention counts from then")

			headers := map[string]string{}
			for _, hd := range dead.Headers {
				headers[hd.Key] = string(hd.Value)
			}

			assert.Equal(t, "1700000000123", headers[kafka.HeaderDLTOriginalTimestamp])
			assert.Equal(t, "7", headers[kafka.HeaderDLTOriginalOffset])
			assert.LessOrEqual(t, len(headers[kafka.HeaderDLTError]), kafka.MaxDLTErrorLength)
			assert.True(t, utf8.ValidString(headers[kafka.HeaderDLTError]))
			assert.Equal(t, tt.wantError, headers[kafka.HeaderDLTError])
		})
	}
}
