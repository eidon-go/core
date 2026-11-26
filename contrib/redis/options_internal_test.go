package redis

import (
	"crypto/tls"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestUniversalOptions(t *testing.T) {
	t.Parallel()

	t.Run("defaults", func(t *testing.T) {
		t.Parallel()

		o := newOptions(nil)
		u := o.universalOptions(Config{Addrs: []string{"a:6379"}})

		assert.Equal(t, []string{"a:6379"}, u.Addrs)
		assert.Equal(t, DefaultConnMaxLifetime, u.ConnMaxLifetime)
		assert.Zero(t, u.ReadTimeout)
		assert.Zero(t, u.MaxRetries)
		assert.Nil(t, u.TLSConfig)
	})

	t.Run("every field", func(t *testing.T) {
		t.Parallel()

		cfg := Config{
			Addrs: []string{"a:26379"}, MasterName: "m", Username: "u", Password: "p",
			SentinelUsername: "su", SentinelPassword: "sp", DB: 2, TLS: true, PoolSize: 7, MinIdleConns: 1,
			DialTimeout: time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 3 * time.Second,
			PoolTimeout: 4 * time.Second, ConnMaxIdleTime: 5 * time.Second, ConnMaxLifetime: 6 * time.Second,
			MaxRetries: 8, MinRetryBackoff: time.Millisecond, MaxRetryBackoff: 9 * time.Millisecond,
		}

		o := newOptions(nil)
		u := o.universalOptions(cfg)

		assert.Equal(t, &goredis.UniversalOptions{
			Addrs: []string{"a:26379"}, MasterName: "m", Username: "u", Password: "p",
			SentinelUsername: "su", SentinelPassword: "sp", DB: 2, PoolSize: 7, MinIdleConns: 1,
			TLSConfig:   &tls.Config{MinVersion: tls.VersionTLS12},
			DialTimeout: time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 3 * time.Second,
			PoolTimeout: 4 * time.Second, ConnMaxIdleTime: 5 * time.Second, ConnMaxLifetime: 6 * time.Second,
			MaxRetries: 8, MinRetryBackoff: time.Millisecond, MaxRetryBackoff: 9 * time.Millisecond,
		}, u)

		cfg.Addrs[0] = "changed"
		assert.Equal(t, "a:26379", u.Addrs[0], "Addrs are copied")
	})

	t.Run("disabled", func(t *testing.T) {
		t.Parallel()

		o := newOptions(nil)
		u := o.universalOptions(Config{
			Addrs: []string{"a:6379"}, Cluster: true, ReadTimeout: -time.Second, WriteTimeout: -1,
			ConnMaxIdleTime: -1, ConnMaxLifetime: -1, MaxRetries: -5,
		})

		assert.True(t, u.IsClusterMode)
		assert.Equal(t, time.Duration(-1), u.ReadTimeout)
		assert.Equal(t, time.Duration(-1), u.WriteTimeout)
		assert.Equal(t, time.Duration(-1), u.ConnMaxIdleTime)
		assert.Zero(t, u.ConnMaxLifetime, "go-redis: never")
		assert.Equal(t, -1, u.MaxRetries)
	})

	t.Run("options", func(t *testing.T) {
		t.Parallel()

		custom := &tls.Config{ServerName: "redis.internal", MinVersion: tls.VersionTLS13}
		o := newOptions([]Option{
			WithTLSConfig(custom),
			WithUniversalOptions(nil),
			WithUniversalOptions(func(u *goredis.UniversalOptions) { u.Protocol = 2 }),
			WithUniversalOptions(func(u *goredis.UniversalOptions) { u.ClientName = "svc" }),
		})
		u := o.universalOptions(Config{Addrs: []string{"a:6379"}})

		assert.Equal(t, "redis.internal", u.TLSConfig.ServerName)
		assert.NotSame(t, custom, u.TLSConfig, "cloned")
		assert.Equal(t, 2, u.Protocol)
		assert.Equal(t, "svc", u.ClientName)
	})
}
