package redis

import (
	"errors"
	"fmt"
	"slices"
	"time"
)

// DefaultConnMaxLifetime is the default of Config.ConnMaxLifetime: pooled
// connections are replaced regularly, so they spread over the nodes behind
// a load balancer and follow a failover.
const DefaultConnMaxLifetime = 30 * time.Minute

// Config configures a Client. The zero value of a pool or timeout setting
// keeps go-redis's default; a negative value disables the feature only
// where the field says so.
type Config struct {
	// Addrs are the "host:port" addresses of the server. Required. One
	// address: a single node. Several: a cluster (any reachable nodes). With
	// MasterName: the Sentinel nodes.
	Addrs []string `json:"addrs" yaml:"addrs"`

	// MasterName, when set, makes the client follow the master of that
	// name through the Sentinels in Addrs.
	MasterName string `json:"masterName" yaml:"masterName"`

	// Cluster forces cluster mode with a single address (a cluster behind
	// one DNS name). Not with MasterName.
	Cluster bool `json:"cluster" yaml:"cluster"`

	// Username and Password authenticate the client (ACL user, or the
	// requirepass password with an empty Username). Password is a secret:
	// never logged.
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`

	// SentinelUsername and SentinelPassword authenticate to the Sentinels
	// (with MasterName).
	SentinelUsername string `json:"sentinelUsername" yaml:"sentinelUsername"`
	SentinelPassword string `json:"sentinelPassword" yaml:"sentinelPassword"`

	// DB is the logical database. Default 0. Clusters have only database 0.
	DB int `json:"db" yaml:"db"`

	// TLS connects over TLS with the system roots (WithTLSConfig sets a
	// custom configuration and implies TLS). Default false.
	TLS bool `json:"tls" yaml:"tls"`

	// PoolSize is the number of connections per node. Default 0: go-redis's
	// 10 per GOMAXPROCS.
	PoolSize int `json:"poolSize" yaml:"poolSize"`

	// MinIdleConns keeps this many idle connections open per node; the pool
	// dials them in the background as soon as the Client is built. Default 0.
	MinIdleConns int `json:"minIdleConns" yaml:"minIdleConns"`

	// DialTimeout bounds a new connection. Default 0: go-redis's 5s.
	DialTimeout time.Duration `json:"dialTimeout" yaml:"dialTimeout"`

	// ReadTimeout bounds reading a reply. Default 0: go-redis's 5s;
	// negative: no timeout. A deadline of the command's ctx applies too.
	// Without a read timeout the PING of Start and Healthcheck is still
	// bounded by 5s, so that they end when their ctx is cancelled.
	ReadTimeout time.Duration `json:"readTimeout" yaml:"readTimeout"`

	// WriteTimeout bounds writing a command. Default 0: ReadTimeout;
	// negative: no timeout.
	WriteTimeout time.Duration `json:"writeTimeout" yaml:"writeTimeout"`

	// PoolTimeout bounds the wait for a free connection when the pool is
	// exhausted. Default 0: ReadTimeout + 1s (30s without a read timeout).
	PoolTimeout time.Duration `json:"poolTimeout" yaml:"poolTimeout"`

	// ConnMaxIdleTime closes connections idle for longer than this. Default
	// 0: go-redis's 30m; negative: never.
	ConnMaxIdleTime time.Duration `json:"connMaxIdleTime" yaml:"connMaxIdleTime"`

	// ConnMaxLifetime replaces connections older than this. Default 0:
	// DefaultConnMaxLifetime; negative: never.
	ConnMaxLifetime time.Duration `json:"connMaxLifetime" yaml:"connMaxLifetime"`

	// MaxRetries is how often go-redis retries a command after a network
	// error (milliseconds apart; the connection at Start is retried by
	// WithConnectRetry instead). Default 0: go-redis's 3; negative: no
	// retries.
	MaxRetries int `json:"maxRetries" yaml:"maxRetries"`

	// MinRetryBackoff and MaxRetryBackoff bound the wait between command
	// retries. Default 0: go-redis's 10ms and 1s.
	MinRetryBackoff time.Duration `json:"minRetryBackoff" yaml:"minRetryBackoff"`
	MaxRetryBackoff time.Duration `json:"maxRetryBackoff" yaml:"maxRetryBackoff"`
}

// Validate reports every problem of c at once; each wraps ErrInvalidConfig.
func (c Config) Validate() error {
	var errs []error

	add := func(problem string) { errs = append(errs, fmt.Errorf("%w: %s", ErrInvalidConfig, problem)) }

	if len(c.Addrs) == 0 {
		add("Addrs is required")
	}

	if slices.Contains(c.Addrs, "") {
		add("Addrs must not contain empty strings")
	}

	if c.Cluster && c.MasterName != "" {
		add("Cluster and MasterName are exclusive")
	}

	if c.DB < 0 {
		add("DB must not be negative")
	}

	if c.DB != 0 && c.cluster() {
		add("DB must be 0 in a cluster")
	}

	for _, field := range []struct {
		name  string
		value int64
	}{
		{"PoolSize", int64(c.PoolSize)},
		{"MinIdleConns", int64(c.MinIdleConns)},
		{"DialTimeout", int64(c.DialTimeout)},
		{"PoolTimeout", int64(c.PoolTimeout)},
		{"MinRetryBackoff", int64(c.MinRetryBackoff)},
		{"MaxRetryBackoff", int64(c.MaxRetryBackoff)},
	} {
		if field.value < 0 {
			add(field.name + " must not be negative")
		}
	}

	return errors.Join(errs...)
}

// cluster reports whether c selects a cluster client.
func (c Config) cluster() bool {
	return c.MasterName == "" && (c.Cluster || len(c.Addrs) > 1)
}
