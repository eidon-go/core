package kafka_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/kafka"
)

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     kafka.Config
		wantErr []string
	}{
		{name: "valid", cfg: kafka.Config{Brokers: []string{"localhost:9092"}, ClientID: "svc"}},
		{
			name:    "no brokers",
			cfg:     kafka.Config{},
			wantErr: []string{"brokers must not be empty"},
		},
		{
			name:    "empty broker",
			cfg:     kafka.Config{Brokers: []string{"localhost:9092", " "}},
			wantErr: []string{"brokers[1] is empty"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.Validate()
			if len(tt.wantErr) == 0 {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, kafka.ErrInvalidConfig)

			for _, want := range tt.wantErr {
				assert.ErrorContains(t, err, want)
			}
		})
	}
}

func TestConsumerConfig_Validate(t *testing.T) {
	t.Parallel()

	valid := consumerConfig([]string{"localhost:9092"})

	tests := []struct {
		name    string
		mutate  func(c *kafka.ConsumerConfig)
		wantErr []string
	}{
		{name: "valid", mutate: func(*kafka.ConsumerConfig) {}},
		{name: "default offset", mutate: func(c *kafka.ConsumerConfig) { c.Offset = "" }},
		{name: "offset is case-insensitive", mutate: func(c *kafka.ConsumerConfig) { c.Offset = "Newest" }},
		{
			name: "everything wrong at once",
			mutate: func(c *kafka.ConsumerConfig) {
				*c = kafka.ConsumerConfig{
					Topics:         []string{""},
					Offset:         "latest",
					MaxPollRecords: -1,
				}
			},
			wantErr: []string{
				"brokers must not be empty", "group must not be empty", "topics[0] is empty",
				`offset must be "newest" or "oldest", got "latest"`, "maxPollRecords must not be negative",
			},
		},
		{
			name:    "no topics",
			mutate:  func(c *kafka.ConsumerConfig) { c.Topics = nil },
			wantErr: []string{"topics must not be empty"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := valid
			tt.mutate(&cfg)

			err := cfg.Validate()
			if len(tt.wantErr) == 0 {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, kafka.ErrInvalidConfig)

			for _, want := range tt.wantErr {
				assert.ErrorContains(t, err, want)
			}
		})
	}
}

func TestErrorAction_String(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "retry", kafka.ErrorActionRetry.String())
	assert.Equal(t, "skip", kafka.ErrorActionSkip.String())
	assert.Equal(t, "stop", kafka.ErrorActionStop.String())
	assert.Equal(t, "unknown", kafka.ErrorAction(42).String())
}
