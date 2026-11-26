package redis_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/redis"
)

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  redis.Config
		want []string
	}{
		{name: "single node", cfg: redis.Config{Addrs: []string{"localhost:6379"}, DB: 3}},
		{name: "cluster", cfg: redis.Config{Addrs: []string{"a:6379", "b:6379"}}},
		{name: "sentinel with a DB", cfg: redis.Config{Addrs: []string{"a:26379", "b:26379"}, MasterName: "m", DB: 1}},
		{
			name: "disabled features",
			cfg: redis.Config{
				Addrs: []string{"a:6379"}, ReadTimeout: -1, WriteTimeout: -1, ConnMaxIdleTime: -1,
				ConnMaxLifetime: -1, MaxRetries: -1,
			},
		},
		{name: "empty", cfg: redis.Config{}, want: []string{"Addrs is required"}},
		{
			name: "invalid",
			cfg: redis.Config{
				Addrs: []string{"a:6379", ""}, Cluster: true, MasterName: "m", DB: -1,
				PoolSize: -1, MinIdleConns: -1, DialTimeout: -time.Second, PoolTimeout: -time.Second,
				MinRetryBackoff: -1, MaxRetryBackoff: -1,
			},
			want: []string{
				"empty strings", "exclusive", "DB must not be negative", "PoolSize", "MinIdleConns",
				"DialTimeout", "PoolTimeout", "MinRetryBackoff", "MaxRetryBackoff",
			},
		},
		{
			name: "DB in a cluster",
			cfg:  redis.Config{Addrs: []string{"a:6379"}, Cluster: true, DB: 1},
			want: []string{"DB must be 0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.Validate()
			if len(tt.want) == 0 {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, redis.ErrInvalidConfig)

			for _, want := range tt.want {
				assert.ErrorContains(t, err, want)
			}
		})
	}
}
