package etcd

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Defaults applied by New to zero Config fields.
const (
	DefaultDialTimeout          = 5 * time.Second
	DefaultDialKeepAliveTime    = 30 * time.Second
	DefaultDialKeepAliveTimeout = 10 * time.Second
)

// Config holds the connection settings of the etcd client. It is a plain
// struct: fill it from any configuration source (usually the environment or
// a local file, since it is needed before remote configuration is read).
// Only Endpoints is required.
type Config struct {
	// Endpoints lists the cluster members, "host:port" or
	// "scheme://host:port" ("https://etcd-0:2379"). Required.
	Endpoints []string

	// Username and Password enable etcd authentication. Set both or
	// neither. With credentials, the client authenticates when it is created
	// (by the first Get or Watch), bounded by DialTimeout. Default: no
	// authentication.
	Username string
	Password string

	// DialTimeout bounds establishing a connection where the client waits
	// for one (the authentication when the client is created). Default: 5s.
	DialTimeout time.Duration

	// DialKeepAliveTime is the interval of gRPC keepalive pings; they detect
	// a dead connection under a long-lived watch. A negative value disables
	// keepalive. Keep it at or above the server's --grpc-keepalive-min-time
	// (5s by default): the server closes connections that ping more often.
	// Default: 30s.
	DialKeepAliveTime time.Duration

	// DialKeepAliveTimeout is how long the client waits for a keepalive
	// response before closing the connection. Default: 10s.
	DialKeepAliveTimeout time.Duration

	// AutoSyncInterval, when positive, refreshes the member list from the
	// cluster at this interval. Default: 0 (the endpoints stay as given).
	AutoSyncInterval time.Duration
}

// Validate reports every invalid field at once. Each error wraps
// ErrInvalidConfig.
func (c Config) Validate() error {
	var errs []error

	if len(c.Endpoints) == 0 {
		errs = append(errs, fmt.Errorf("%w: Endpoints is empty", ErrInvalidConfig))
	}

	for i, ep := range c.Endpoints {
		if strings.TrimSpace(ep) == "" {
			errs = append(errs, fmt.Errorf("%w: Endpoints[%d] is empty", ErrInvalidConfig, i))
		}
	}

	if (c.Username == "") != (c.Password == "") {
		errs = append(errs, fmt.Errorf("%w: Username and Password must be set together", ErrInvalidConfig))
	}

	if c.DialTimeout < 0 {
		errs = append(errs, fmt.Errorf("%w: DialTimeout is negative", ErrInvalidConfig))
	}

	if c.DialKeepAliveTimeout < 0 {
		errs = append(errs, fmt.Errorf("%w: DialKeepAliveTimeout is negative", ErrInvalidConfig))
	}

	if c.AutoSyncInterval < 0 {
		errs = append(errs, fmt.Errorf("%w: AutoSyncInterval is negative", ErrInvalidConfig))
	}

	return errors.Join(errs...)
}

// withDefaults returns c with zero fields replaced by the defaults.
func (c Config) withDefaults() Config {
	if c.DialTimeout == 0 {
		c.DialTimeout = DefaultDialTimeout
	}

	switch {
	case c.DialKeepAliveTime == 0:
		c.DialKeepAliveTime = DefaultDialKeepAliveTime
	case c.DialKeepAliveTime < 0:
		c.DialKeepAliveTime = 0
	}

	if c.DialKeepAliveTimeout == 0 {
		c.DialKeepAliveTimeout = DefaultDialKeepAliveTimeout
	}

	return c
}
