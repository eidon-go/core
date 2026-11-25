package http2

import (
	"errors"
	"fmt"
	"net"
	"time"
)

// Defaults applied by New to zero Config fields.
const (
	DefaultAddr              = ":8080"
	DefaultReadHeaderTimeout = 10 * time.Second
	DefaultReadTimeout       = 30 * time.Second
	DefaultWriteTimeout      = 30 * time.Second
	DefaultIdleTimeout       = 120 * time.Second
)

// Config holds the server's network settings. It is a plain struct: fill it
// from any configuration source. Every field is optional; a zero value means
// the documented default. For the timeouts, a negative value disables the
// timeout.
type Config struct {
	// Addr is the TCP address to listen on, "host:port" (":8080" listens on
	// every interface). Use port 0 to pick a free port; Server.Addr reports
	// it after Start. Ignored when WithListener is used. Default: ":8080".
	Addr string

	// ReadHeaderTimeout bounds reading the request headers; it is the main
	// protection against slow-header (Slowloris) clients. Default: 10s.
	ReadHeaderTimeout time.Duration

	// ReadTimeout bounds reading the whole request, body included. Raise it
	// (or extend it per request with http.ResponseController) for uploads.
	// Default: 30s.
	ReadTimeout time.Duration

	// WriteTimeout bounds writing the response, measured from the end of the
	// request headers. Streaming handlers (SSE, long polling) extend it per
	// request with http.ResponseController.SetWriteDeadline. Default: 30s.
	WriteTimeout time.Duration

	// IdleTimeout bounds how long a keep-alive connection waits for the next
	// request. Default: 120s.
	IdleTimeout time.Duration

	// MaxHeaderBytes limits the size of the request headers in bytes.
	// Default: http.DefaultMaxHeaderBytes (1 MB).
	MaxHeaderBytes int

	// UnencryptedHTTP2 enables HTTP/2 over cleartext TCP (h2c, prior
	// knowledge only) next to HTTP/1.1 — for traffic behind a TLS-terminating
	// proxy or inside a service mesh. With WithTLSConfig, HTTP/2 is negotiated
	// over TLS regardless of this field. Default: false.
	UnencryptedHTTP2 bool
}

// Validate reports every invalid field at once. Each error wraps
// ErrInvalidConfig.
func (c Config) Validate() error {
	var errs []error

	if c.Addr != "" {
		if _, _, err := net.SplitHostPort(c.Addr); err != nil {
			errs = append(errs, fmt.Errorf("%w: addr %q: %w", ErrInvalidConfig, c.Addr, err))
		}
	}

	if c.MaxHeaderBytes < 0 {
		errs = append(errs, fmt.Errorf("%w: maxHeaderBytes must not be negative, got %d",
			ErrInvalidConfig, c.MaxHeaderBytes))
	}

	return errors.Join(errs...)
}

// withDefaults returns c with zero fields replaced by defaults. Negative
// timeouts stay negative: net/http treats them as "no timeout", whereas zero
// would make ReadHeaderTimeout and IdleTimeout fall back to ReadTimeout.
func (c Config) withDefaults() Config {
	if c.Addr == "" {
		c.Addr = DefaultAddr
	}

	c.ReadHeaderTimeout = timeout(c.ReadHeaderTimeout, DefaultReadHeaderTimeout)
	c.ReadTimeout = timeout(c.ReadTimeout, DefaultReadTimeout)
	c.WriteTimeout = timeout(c.WriteTimeout, DefaultWriteTimeout)
	c.IdleTimeout = timeout(c.IdleTimeout, DefaultIdleTimeout)

	return c
}

func timeout(v, def time.Duration) time.Duration {
	if v == 0 {
		return def
	}

	return v
}
