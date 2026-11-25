package client

import (
	"errors"
	"fmt"
	"time"
)

// Defaults applied by New to zero Config fields.
const (
	DefaultMaxIdleConns          = 100
	DefaultMaxIdleConnsPerHost   = 20
	DefaultDialTimeout           = 5 * time.Second
	DefaultKeepAlive             = 30 * time.Second
	DefaultTLSHandshakeTimeout   = 5 * time.Second
	DefaultResponseHeaderTimeout = 10 * time.Second
	DefaultIdleConnTimeout       = 90 * time.Second

	// DefaultExpectContinueTimeout matches net/http.DefaultTransport; it only
	// matters for requests that send "Expect: 100-continue".
	DefaultExpectContinueTimeout = time.Second
)

// Config tunes the connection pool and the transport timeouts. It is a plain
// struct: fill it from any configuration source. Every field is optional; a
// zero value means the documented default, and a negative duration disables
// that timeout.
//
// There is deliberately no overall request timeout: bound each call with its
// context, so that deadlines propagate through retries and request chains.
type Config struct {
	// MaxIdleConns caps the idle (keep-alive) connections across all hosts.
	// Default: 100.
	MaxIdleConns int

	// MaxIdleConnsPerHost caps the idle connections per host. Default: 20
	// (net/http's own default of 2 forces reconnects under load).
	MaxIdleConnsPerHost int

	// MaxConnsPerHost caps all connections per host, dialing, active and
	// idle. Default: 0, no limit.
	MaxConnsPerHost int

	// DialTimeout bounds establishing a TCP connection. Default: 5s.
	DialTimeout time.Duration

	// KeepAlive is the TCP keep-alive interval. Default: 30s; negative
	// disables TCP keep-alives.
	KeepAlive time.Duration

	// TLSHandshakeTimeout bounds the TLS handshake. Default: 5s.
	TLSHandshakeTimeout time.Duration

	// ResponseHeaderTimeout bounds the wait for the response headers after
	// the request has been written. Raise it for slow upstreams (high time to
	// first byte). Default: 10s.
	ResponseHeaderTimeout time.Duration

	// IdleConnTimeout is how long an idle connection stays in the pool.
	// Default: 90s.
	IdleConnTimeout time.Duration

	// UnencryptedHTTP2 makes the client speak HTTP/2 over cleartext TCP (h2c,
	// prior knowledge) to http:// URLs — for services that serve h2c, such as
	// an http2.Server with Config.UnencryptedHTTP2. The client then speaks
	// HTTP/2 only, over TLS too. Default: false (HTTP/1.1 for http://,
	// HTTP/2 or HTTP/1.1 negotiated over TLS).
	UnencryptedHTTP2 bool
}

// ErrInvalidConfig is wrapped by every error from Config.Validate.
var ErrInvalidConfig = errors.New("http2/client: invalid config")

// Validate reports every invalid field at once. Each error wraps
// ErrInvalidConfig.
func (c Config) Validate() error {
	var errs []error

	for _, f := range []struct {
		name  string
		value int
	}{
		{"maxIdleConns", c.MaxIdleConns},
		{"maxIdleConnsPerHost", c.MaxIdleConnsPerHost},
		{"maxConnsPerHost", c.MaxConnsPerHost},
	} {
		if f.value < 0 {
			errs = append(errs, fmt.Errorf("%w: %s must not be negative, got %d", ErrInvalidConfig, f.name, f.value))
		}
	}

	return errors.Join(errs...)
}

// withDefaults returns c with zero fields replaced by defaults. Negative
// durations become net/http's "disabled" values.
func (c Config) withDefaults() Config {
	if c.MaxIdleConns == 0 {
		c.MaxIdleConns = DefaultMaxIdleConns
	}

	if c.MaxIdleConnsPerHost == 0 {
		c.MaxIdleConnsPerHost = DefaultMaxIdleConnsPerHost
	}

	c.DialTimeout = disabledAsZero(c.DialTimeout, DefaultDialTimeout)
	c.TLSHandshakeTimeout = disabledAsZero(c.TLSHandshakeTimeout, DefaultTLSHandshakeTimeout)
	c.ResponseHeaderTimeout = disabledAsZero(c.ResponseHeaderTimeout, DefaultResponseHeaderTimeout)
	c.IdleConnTimeout = disabledAsZero(c.IdleConnTimeout, DefaultIdleConnTimeout)

	// net.Dialer: zero means the OS default interval, negative disables.
	if c.KeepAlive == 0 {
		c.KeepAlive = DefaultKeepAlive
	}

	return c
}

// disabledAsZero applies the default to zero and maps a negative value to
// zero, which net/http's transport and dialer treat as "no timeout".
func disabledAsZero(v, def time.Duration) time.Duration {
	switch {
	case v == 0:
		return def
	case v < 0:
		return 0
	default:
		return v
	}
}
