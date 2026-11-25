package grpc

import (
	"errors"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// DefaultAddr is the listen address used when Config.Addr is empty.
const DefaultAddr = ":50051"

// Config holds the server's network settings. It is a plain struct: fill it
// from any configuration source. Every field is optional; a zero value means
// the documented default, which for everything but Addr is grpc-go's own.
type Config struct {
	// Addr is the TCP address to listen on, "host:port" (":50051" listens on
	// every interface). Use port 0 to pick a free port; Server.Addr reports
	// it after Start. Ignored when WithListener is used. Default: ":50051".
	Addr string

	// MaxRecvMsgSize limits the size of a received message in bytes.
	// Default: 4 MB.
	MaxRecvMsgSize int

	// MaxSendMsgSize limits the size of a sent message in bytes.
	// Default: math.MaxInt32.
	MaxSendMsgSize int

	// MaxConcurrentStreams limits the concurrent streams (RPCs) per client
	// connection. Default: no limit.
	MaxConcurrentStreams uint32

	// ConnectionTimeout bounds the setup of a new connection, the TLS
	// handshake included. Default: 120s.
	ConnectionTimeout time.Duration

	// MaxConnectionIdle closes a connection that has had no RPC for this
	// long. Default: no limit.
	MaxConnectionIdle time.Duration

	// MaxConnectionAge closes a connection (with a GOAWAY, so clients
	// reconnect) once it is this old. Set it behind a layer-4 load balancer
	// or in Kubernetes, where long-lived HTTP/2 connections otherwise pin
	// clients to the instances that were up when they connected. grpc-go
	// adds ±10% jitter. Default: no limit.
	MaxConnectionAge time.Duration

	// MaxConnectionAgeGrace is how long RPCs may still run on a connection
	// closed by MaxConnectionAge before it is force-closed; it requires
	// MaxConnectionAge. Default: no limit.
	MaxConnectionAgeGrace time.Duration

	// KeepaliveTime is how long a connection may stay silent before the
	// server pings the client. Values below 1s are raised to 1s.
	// Default: 2h.
	KeepaliveTime time.Duration

	// KeepaliveTimeout is how long the server waits for the ping's answer
	// before closing the connection. Default: 20s.
	KeepaliveTimeout time.Duration

	// KeepaliveMinTime is the shortest interval between client pings the
	// server tolerates; a client that pings more often is disconnected
	// (GOAWAY "too_many_pings"). It must not exceed the clients' keepalive
	// time. Default: 5m.
	KeepaliveMinTime time.Duration

	// KeepalivePermitWithoutStream lets clients ping while they have no
	// active RPC; without it such pings count as too many. Enable it when
	// clients set their own PermitWithoutStream. Default: false.
	KeepalivePermitWithoutStream bool
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

	for _, f := range []struct {
		name  string
		value int64
	}{
		{"maxRecvMsgSize", int64(c.MaxRecvMsgSize)},
		{"maxSendMsgSize", int64(c.MaxSendMsgSize)},
		{"connectionTimeout", int64(c.ConnectionTimeout)},
		{"maxConnectionIdle", int64(c.MaxConnectionIdle)},
		{"maxConnectionAge", int64(c.MaxConnectionAge)},
		{"maxConnectionAgeGrace", int64(c.MaxConnectionAgeGrace)},
		{"keepaliveTime", int64(c.KeepaliveTime)},
		{"keepaliveTimeout", int64(c.KeepaliveTimeout)},
		{"keepaliveMinTime", int64(c.KeepaliveMinTime)},
	} {
		if f.value < 0 {
			errs = append(errs, fmt.Errorf("%w: %s must not be negative, got %d", ErrInvalidConfig, f.name, f.value))
		}
	}

	if c.MaxConnectionAgeGrace > 0 && c.MaxConnectionAge == 0 {
		errs = append(errs, fmt.Errorf("%w: maxConnectionAgeGrace has no effect without maxConnectionAge",
			ErrInvalidConfig))
	}

	return errors.Join(errs...)
}

// serverOptions turns the non-zero fields into grpc.ServerOptions; zero
// fields are left to grpc-go, whose keepalive settings treat zero as default.
func (c Config) serverOptions() []grpc.ServerOption {
	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     c.MaxConnectionIdle,
			MaxConnectionAge:      c.MaxConnectionAge,
			MaxConnectionAgeGrace: c.MaxConnectionAgeGrace,
			Time:                  c.KeepaliveTime,
			Timeout:               c.KeepaliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             c.KeepaliveMinTime,
			PermitWithoutStream: c.KeepalivePermitWithoutStream,
		}),
	}

	if c.MaxRecvMsgSize > 0 {
		opts = append(opts, grpc.MaxRecvMsgSize(c.MaxRecvMsgSize))
	}

	if c.MaxSendMsgSize > 0 {
		opts = append(opts, grpc.MaxSendMsgSize(c.MaxSendMsgSize))
	}

	if c.MaxConcurrentStreams > 0 {
		opts = append(opts, grpc.MaxConcurrentStreams(c.MaxConcurrentStreams))
	}

	if c.ConnectionTimeout > 0 {
		opts = append(opts, grpc.ConnectionTimeout(c.ConnectionTimeout))
	}

	return opts
}
