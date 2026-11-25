package client

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
)

// Load-balancing policies for Config.LoadBalancingPolicy.
const (
	// PickFirst connects to the first reachable address and sends every RPC
	// there. grpc-go's default.
	PickFirst = "pick_first"

	// RoundRobin connects to every address the resolver returns and spreads
	// RPCs over them — with a "dns:///" target of a Kubernetes headless
	// service, over every pod.
	RoundRobin = "round_robin"
)

// Config holds the connection settings. It is a plain struct: fill it from any
// configuration source. Target is required; every other field is optional,
// and a zero value means the documented default, which is grpc-go's own.
type Config struct {
	// Target is the server to connect to, as a gRPC name: "host:port"
	// (resolved through DNS), "dns:///host:port", "unix:///path" or any
	// scheme of a registered resolver. Required.
	Target string

	// Plaintext connects without TLS — for servers inside a service mesh or
	// on localhost. Without it the connection uses TLS, with WithTLSConfig or
	// the system roots. Default: false.
	Plaintext bool

	// LoadBalancingPolicy is PickFirst or RoundRobin. It is the default
	// service config: a service config the resolver supplies (a DNS
	// "grpc_config" TXT record) takes precedence; pass
	// grpc.WithDisableServiceConfig through WithDialOptions to ignore those.
	// Default: PickFirst.
	LoadBalancingPolicy string

	// MinConnectTimeout is the least time one attempt to establish a
	// connection gets, the TLS handshake included. Failed attempts are retried
	// with exponential backoff (up to 120s between attempts), and a later
	// attempt gets at least the current backoff delay. Default: 20s.
	MinConnectTimeout time.Duration

	// IdleTimeout puts the connection into idle mode (closing the
	// transports) after this long without RPCs; the next RPC reconnects. A
	// negative value disables idle mode. Default: 30m.
	IdleTimeout time.Duration

	// KeepaliveTime pings the server after this long without activity, to
	// detect a dead connection. Values below 10s are raised to 10s. Keep it
	// at or above the server's keepalive min time (contrib/grpc's
	// KeepaliveMinTime, 5m by default), or the server closes the connection
	// for pinging too often. Default: no pings.
	KeepaliveTime time.Duration

	// KeepaliveTimeout is how long to wait for the ping's answer before
	// closing the connection; it requires KeepaliveTime. Default: 20s.
	KeepaliveTimeout time.Duration

	// KeepalivePermitWithoutStream pings also while no RPC is active; the
	// server must allow it (contrib/grpc's KeepalivePermitWithoutStream). It
	// requires KeepaliveTime. Default: false.
	KeepalivePermitWithoutStream bool

	// MaxRecvMsgSize limits the size of a received message in bytes.
	// Default: 4 MB.
	MaxRecvMsgSize int

	// MaxSendMsgSize limits the size of a sent message in bytes.
	// Default: math.MaxInt32.
	MaxSendMsgSize int
}

// Validate reports every invalid field at once. Each error wraps
// ErrInvalidConfig.
func (c Config) Validate() error {
	var errs []error

	if c.Target == "" {
		errs = append(errs, fmt.Errorf("%w: target is required", ErrInvalidConfig))
	}

	switch c.LoadBalancingPolicy {
	case "", PickFirst, RoundRobin:
	default:
		errs = append(errs, fmt.Errorf("%w: loadBalancingPolicy %q: want %q or %q",
			ErrInvalidConfig, c.LoadBalancingPolicy, PickFirst, RoundRobin))
	}

	for _, f := range []struct {
		name  string
		value int64
	}{
		{"minConnectTimeout", int64(c.MinConnectTimeout)},
		{"keepaliveTime", int64(c.KeepaliveTime)},
		{"keepaliveTimeout", int64(c.KeepaliveTimeout)},
		{"maxRecvMsgSize", int64(c.MaxRecvMsgSize)},
		{"maxSendMsgSize", int64(c.MaxSendMsgSize)},
	} {
		if f.value < 0 {
			errs = append(errs, fmt.Errorf("%w: %s must not be negative, got %d", ErrInvalidConfig, f.name, f.value))
		}
	}

	if c.KeepaliveTime == 0 && (c.KeepaliveTimeout != 0 || c.KeepalivePermitWithoutStream) {
		errs = append(errs, fmt.Errorf("%w: keepaliveTimeout and keepalivePermitWithoutStream require keepaliveTime",
			ErrInvalidConfig))
	}

	return errors.Join(errs...)
}

// dialOptions turns the non-zero fields into grpc.DialOptions; zero fields
// are left to grpc-go.
func (c Config) dialOptions() []grpc.DialOption {
	var opts []grpc.DialOption

	// Only with a Time: grpc.WithKeepaliveParams raises a zero Time to 10s,
	// which servers with the default 5m keepalive min time punish with
	// GOAWAY "too_many_pings".
	if c.KeepaliveTime > 0 {
		opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                c.KeepaliveTime,
			Timeout:             c.KeepaliveTimeout,
			PermitWithoutStream: c.KeepalivePermitWithoutStream,
		}))
	}

	if c.LoadBalancingPolicy != "" {
		opts = append(opts, grpc.WithDefaultServiceConfig(
			fmt.Sprintf(`{"loadBalancingConfig":[{%q:{}}]}`, c.LoadBalancingPolicy)))
	}

	if c.MinConnectTimeout > 0 {
		opts = append(opts, grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.DefaultConfig,
			MinConnectTimeout: c.MinConnectTimeout,
		}))
	}

	switch {
	case c.IdleTimeout > 0:
		opts = append(opts, grpc.WithIdleTimeout(c.IdleTimeout))
	case c.IdleTimeout < 0:
		opts = append(opts, grpc.WithIdleTimeout(0)) // zero disables idle mode in grpc-go
	}

	var callOpts []grpc.CallOption

	if c.MaxRecvMsgSize > 0 {
		callOpts = append(callOpts, grpc.MaxCallRecvMsgSize(c.MaxRecvMsgSize))
	}

	if c.MaxSendMsgSize > 0 {
		callOpts = append(callOpts, grpc.MaxCallSendMsgSize(c.MaxSendMsgSize))
	}

	if len(callOpts) > 0 {
		opts = append(opts, grpc.WithDefaultCallOptions(callOpts...))
	}

	return opts
}
