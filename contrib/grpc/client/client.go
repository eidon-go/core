// Package client builds a gRPC client connection for service-to-service
// calls: TLS by default (Config.Plaintext opts out), load balancing, keepalive
// and idle settings in a plain Config, interceptors, and OpenTelemetry
// instrumentation on by default.
//
// The result is a *Conn — a *grpc.ClientConn that is also a lifecycle
// Resource, so app.App closes it on shutdown, after the servers that use it
// have drained. Pass it to generated NewXxxClient functions as it is.
package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/eidon-go/core/lifecycle"
)

var (
	// ErrInvalidConfig is wrapped by every error from Config.Validate and by
	// New for invalid options.
	ErrInvalidConfig = errors.New("grpc/client: invalid config")

	// ErrClosed is returned by Start after Shutdown.
	ErrClosed = errors.New("grpc/client: connection closed")
)

// Conn is a client connection: a *grpc.ClientConn (a
// grpc.ClientConnInterface for generated clients) that implements
// lifecycle.Starter, Resource and Namer.
//
// It is not a lifecycle.Healthchecker on purpose: the readiness of a service
// should not follow the reachability of every service it calls, or one
// failing dependency takes all its callers out of load balancing. Check the
// dependency in your own Healthchecker if it really is required.
type Conn struct {
	*grpc.ClientConn

	name   string
	logger *slog.Logger

	releaser *lifecycle.Releaser
}

var (
	_ lifecycle.Starter        = (*Conn)(nil)
	_ lifecycle.Resource       = (*Conn)(nil)
	_ lifecycle.Namer          = (*Conn)(nil)
	_ grpc.ClientConnInterface = (*Conn)(nil)
)

// New validates cfg and builds a connection. It does no network I/O: the
// connection is established by Start or by the first RPC, and re-established
// by grpc-go whenever it breaks. RPCs carry no connection-wide deadline:
// bound each one with its context.
func New(cfg Config, opts ...Option) (*Conn, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	o := newOptions(opts)
	if o.tlsConfig != nil && cfg.Plaintext {
		return nil, fmt.Errorf("%w: WithTLSConfig cannot be combined with plaintext", ErrInvalidConfig)
	}

	clientConn, err := grpc.NewClient(cfg.Target, dialOptions(cfg, o)...)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}

	conn := &Conn{ //nolint:exhaustruct_v5 // releaser is set below: it closes conn
		ClientConn: clientConn,
		name:       o.name,
		logger:     o.logger.With(slog.String("component", o.name)),
	}
	conn.releaser = lifecycle.CloseOnce(conn.close)

	return conn, nil
}

// dialOptions assembles the grpc.DialOptions: Config, credentials, the
// interceptor chains, the otelgrpc stats handler, then WithDialOptions.
func dialOptions(cfg Config, o options) []grpc.DialOption {
	var creds credentials.TransportCredentials

	switch {
	case cfg.Plaintext:
		creds = insecure.NewCredentials()
	case o.tlsConfig != nil:
		creds = credentials.NewTLS(o.tlsConfig.Clone())
	default:
		creds = credentials.NewTLS(nil) // system roots
	}

	otelOpts := append([]otelgrpc.Option{
		otelgrpc.WithTracerProvider(o.tracerProvider),
		otelgrpc.WithMeterProvider(o.meterProvider),
		otelgrpc.WithPropagators(o.propagators),
	}, o.otelOptions...)

	opts := cfg.dialOptions()
	opts = append(opts,
		grpc.WithTransportCredentials(creds),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler(otelOpts...)),
	)

	if len(o.unaryInterceptors) > 0 {
		opts = append(opts, grpc.WithChainUnaryInterceptor(o.unaryInterceptors...))
	}

	if len(o.streamInterceptors) > 0 {
		opts = append(opts, grpc.WithChainStreamInterceptor(o.streamInterceptors...))
	}

	return append(opts, o.dialOptions...)
}

// Name implements lifecycle.Namer.
func (c *Conn) Name() string { return c.name }

// Start begins connecting in the background, so that the first RPC does not
// pay for the connection setup. It does not wait for the connection and does
// not fail when the server is unreachable: grpc-go keeps retrying, and RPCs
// fail (or wait, with grpc.WaitForReady) until it succeeds. Implements
// lifecycle.Starter.
func (c *Conn) Start(context.Context) error {
	if c.releaser.Closing() {
		return ErrClosed
	}

	// Connect on a connection that a concurrent Shutdown has closed does
	// nothing.
	c.Connect()

	return nil
}

// Shutdown closes the connection: RPCs in flight fail with codes.Canceled and
// later ones fail at once. It waits for the close until ctx is done, then
// returns an error wrapping ctx.Err() while the close finishes in the
// background. It is idempotent and safe without Start. Implements
// lifecycle.Resource.
func (c *Conn) Shutdown(ctx context.Context) error {
	if err := c.releaser.Shutdown(ctx); err != nil {
		return fmt.Errorf("grpc/client: shutdown: %w", err)
	}

	return nil
}

// close is the release of the Releaser: it runs once, in the background.
func (c *Conn) close() error {
	// Close after the caller's own Close reports "closing": not a failure.
	switch err := c.Close(); {
	case err == nil:
		c.logger.InfoContext(context.Background(), "grpc client closed", slog.String("target", c.Target()))
	case status.Code(err) != codes.Canceled:
		return err //nolint:wrapcheck // Shutdown wraps it
	}

	return nil
}
