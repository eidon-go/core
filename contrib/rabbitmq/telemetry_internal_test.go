package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// sentCount returns the value of messaging.client.sent.messages.
func sentCount(t *testing.T, reader *sdkmetric.ManualReader) int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	var total int64

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok && m.Name == "messaging.client.sent.messages" {
				for _, dp := range sum.DataPoints {
					total += dp.Value
				}
			}
		}
	}

	return total
}

// A message that never reached a channel (no connection) is not counted as
// sent; one that did is.
func TestPublisher_SentMetric(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	broker := newFakeBroker()
	conn := newTestConnection(t, broker, true)
	pub := newTestPublisher(t, conn, WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))

	require.NoError(t, pub.Publish(t.Context(), &Message{RoutingKey: "q"}))
	assert.EqualValues(t, 1, sentCount(t, reader))

	broker.setDialErr(errBroker)
	broker.drop()

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, pub.Publish(ctx, &Message{RoutingKey: "q"}), ErrNotConnected)
	assert.EqualValues(t, 1, sentCount(t, reader), "not sent: not counted")
}

// nilMeterProvider returns instruments that are nil, with an error.
type nilMeterProvider struct{ noop.MeterProvider }

func (nilMeterProvider) Meter(string, ...metric.MeterOption) metric.Meter { return nilMeter{} }

type nilMeter struct{ noop.Meter }

var errMeter = errors.New("no instruments")

func (nilMeter) Int64Counter(string, ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	return nil, errMeter
}

func (nilMeter) Float64Histogram(string, ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	return nil, errMeter
}

// A meter provider that returns nil instruments does not break publishing
// or consuming: no-op instruments take their place.
func TestTelemetry_NilInstruments(t *testing.T) {
	t.Parallel()

	broker := newFakeBroker()
	conn := newTestConnection(t, broker, true)
	opt := WithMeterProvider(nilMeterProvider{})

	pub := newTestPublisher(t, conn, opt)
	require.NoError(t, pub.Publish(t.Context(), &Message{RoutingKey: "q"}))

	h := &recordingHandler{}
	c := newTestConsumer(t, conn, ordersConfig, h, opt)

	ctx, cancel := context.WithCancel(t.Context())
	done := run(ctx, c)

	broker.deliver(t, body("a"))
	require.Eventually(t, func() bool { return len(h.handled()) == 1 }, testWait, time.Millisecond)

	cancel()
	require.NoError(t, receive(t, done))
}
