package redis_test

import (
	"context"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/extra/redisotel/v9"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/eidon-go/core/contrib/redis"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
	"github.com/eidon-go/core/retry"
)

// telemetry stands in for the Provider of the otel module.
type telemetry struct {
	tp trace.TracerProvider
	mp metric.MeterProvider
}

func (t telemetry) TracerProvider() trace.TracerProvider { return t.tp }

func (t telemetry) MeterProvider() metric.MeterProvider { return t.mp }

// fastRetry retries quickly; onRetry (optional) sees every retry.
func fastRetry(attempts int, onRetry func()) redis.Option {
	return redis.WithConnectRetry(retry.Policy{
		MaxAttempts:  attempts,
		InitialDelay: time.Millisecond,
		MaxDelay:     time.Millisecond,
		OnRetry: func(context.Context, retry.Attempt) {
			if onRetry != nil {
				onRetry()
			}
		},
	})
}

func quiet() redis.Option { return redis.WithLogger(slog.New(slog.DiscardHandler)) }

func newClient(t *testing.T, addr string, opts ...redis.Option) *redis.Client {
	t.Helper()

	opts = append([]redis.Option{quiet(), redis.WithTelemetry(telemetry{
		tp: tracenoop.NewTracerProvider(), mp: noop.NewMeterProvider(),
	})}, opts...)

	client, err := redis.New(redis.Config{Addrs: []string{addr}, MaxRetries: -1}, opts...)
	require.NoError(t, err)

	t.Cleanup(func() { _ = client.Shutdown(context.WithoutCancel(t.Context())) })

	return client
}

// deadAddr is an address nothing listens on.
func deadAddr(t *testing.T) string {
	t.Helper()

	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := l.Addr().String()
	require.NoError(t, l.Close())

	return addr
}

func TestNew_Errors(t *testing.T) {
	t.Parallel()

	_, err := redis.New(redis.Config{})
	require.ErrorIs(t, err, redis.ErrInvalidConfig)
}

func TestClient_Lifecycle(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	spans := tracetest.NewInMemoryExporter()
	reader := sdkmetric.NewManualReader()

	client, err := redis.New(redis.Config{Addrs: []string{server.Addr()}},
		quiet(), redis.WithName("cache"), redis.WithTelemetry(nil),
		redis.WithTelemetry(telemetry{tp: tracenoop.NewTracerProvider(), mp: noop.NewMeterProvider()}),
		redis.WithTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(spans))),
		redis.WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))),
		redis.WithTracingOptions(redisotel.WithDialFilter(true)))
	require.NoError(t, err)

	assert.Equal(t, "cache", client.Name())
	require.NoError(t, client.Start(t.Context()))

	rdb := client.Redis()
	require.NoError(t, rdb.Set(t.Context(), "k", "v", time.Minute).Err())

	got, err := rdb.Get(t.Context(), "k").Result()
	require.NoError(t, err)
	assert.Equal(t, "v", got)

	_, err = rdb.Get(t.Context(), "missing").Result()
	require.ErrorIs(t, err, goredis.Nil)

	names := make([]string, 0, len(spans.GetSpans()))
	for _, span := range spans.GetSpans() {
		names = append(names, span.Name)
	}

	assert.Contains(t, names, "set", "commands are traced")
	assert.NotContains(t, names, "redis.dial", "tracing options apply")

	var metrics metricdata.ResourceMetrics

	require.NoError(t, reader.Collect(t.Context(), &metrics))
	assert.NotEmpty(t, metrics.ScopeMetrics, "commands and the pool are measured")

	require.NoError(t, client.Healthcheck(t.Context()))

	require.NoError(t, client.Shutdown(t.Context()))
	require.NoError(t, client.Shutdown(t.Context()), "idempotent")

	require.ErrorIs(t, client.Start(t.Context()), redis.ErrClosed)
	require.ErrorIs(t, client.Healthcheck(t.Context()), redis.ErrClosed)
}

func TestClient_Name(t *testing.T) {
	t.Parallel()

	client := newClient(t, deadAddr(t))

	assert.Equal(t, redis.DefaultName, client.Name())
}

func TestClient_StartRetries(t *testing.T) {
	t.Parallel()

	t.Run("until the server answers", func(t *testing.T) {
		t.Parallel()

		server := miniredis.RunT(t)
		server.SetError("LOADING Redis is loading the dataset in memory")

		var retries atomic.Int32

		client := newClient(t, server.Addr(), fastRetry(5, func() {
			if retries.Add(1) == 2 {
				server.SetError("")
			}
		}))

		require.NoError(t, client.Start(t.Context()))
		assert.Equal(t, int32(2), retries.Load())
	})

	t.Run("gives up", func(t *testing.T) {
		t.Parallel()

		client := newClient(t, deadAddr(t), fastRetry(2, nil))

		err := client.Start(t.Context())
		require.Error(t, err)
		require.NotErrorIs(t, err, redis.ErrClosed)
		require.ErrorContains(t, err, "redis: ping")
	})

	t.Run("ended by Shutdown", func(t *testing.T) {
		t.Parallel()

		retrying := make(chan struct{})

		var once atomic.Bool

		client := newClient(t, deadAddr(t), redis.WithConnectRetry(retry.Policy{
			MaxAttempts:  retry.Forever,
			InitialDelay: time.Millisecond,
			MaxDelay:     time.Millisecond,
			OnRetry: func(context.Context, retry.Attempt) {
				if once.CompareAndSwap(false, true) {
					close(retrying)
				}
			},
		}))

		started := make(chan error, 1)

		go func() { started <- client.Start(t.Context()) }()

		<-retrying
		require.NoError(t, client.Shutdown(t.Context()))
		require.ErrorIs(t, <-started, redis.ErrClosed)
	})
}

func TestClient_Healthcheck(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	client := newClient(t, server.Addr())

	require.NoError(t, client.Healthcheck(t.Context()))

	server.Close()

	err := client.Healthcheck(t.Context())
	require.Error(t, err)
	require.ErrorContains(t, err, "redis: ping")
	require.NotErrorIs(t, err, redis.ErrClosed)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, client.Healthcheck(ctx), context.Canceled, "honors ctx")
}

func TestClient_ClosedThroughRedis(t *testing.T) {
	t.Parallel()

	client := newClient(t, miniredis.RunT(t).Addr())

	require.NoError(t, client.Redis().Close())
	require.ErrorIs(t, client.Healthcheck(t.Context()), redis.ErrClosed)
	require.NoError(t, client.Shutdown(t.Context()), "a client closed directly is tolerated")
}

func TestClient_ShutdownExpiredContext(t *testing.T) {
	t.Parallel()

	client := newClient(t, deadAddr(t))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := client.Shutdown(ctx)
	if err != nil { // the close may finish before the ctx is checked
		require.ErrorIs(t, err, context.Canceled)
	}

	require.ErrorIs(t, client.Healthcheck(t.Context()), redis.ErrClosed)
}

func TestClient_Contract(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)

	lifecycletest.Resource(t, func(t *testing.T) lifecycle.Resource {
		t.Helper()

		client, err := redis.New(redis.Config{Addrs: []string{server.Addr()}}, quiet())
		require.NoError(t, err)

		return client
	})
}
