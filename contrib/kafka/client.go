package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/retry"
)

// Client is a franz-go client with the lifecycle of a component: Start
// creates it and pings the cluster with retry, Shutdown closes it,
// Healthcheck pings it. Use it for what Producer and Consumer do not cover —
// admin requests (wrap Kgo in kadm.Client), transactions, direct partition
// consumption — through Kgo.
//
// It is a Starter, Resource, Healthchecker and Namer.
type Client struct {
	name     string
	connect  retry.Policy
	lazy     *lazyClient
	releaser *lifecycle.Releaser
}

// NewClient validates cfg and prepares the client; it does no network I/O:
// the franz-go client is created in Start.
func NewClient(cfg Config, opts ...Option) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	cfg = cfg.clone()
	o := newOptions(opts)
	lazy := newLazyClient(o.clientOpts(cfg, o.tracer(cfg.ClientID, "")), (*kgo.Client).Close)

	return &Client{
		name:     o.name,
		connect:  o.connectPolicy(),
		lazy:     lazy,
		releaser: lazy.releaser,
	}, nil
}

// Kgo returns the franz-go client, nil before Start. It is owned by Client:
// do not close it, call Shutdown.
func (c *Client) Kgo() *kgo.Client { return c.lazy.get() }

// Name returns the component name (WithName). Implements lifecycle.Namer.
func (c *Client) Name() string { return c.name }

// Start creates the client and pings the cluster until it answers, retried
// with the WithConnectRetry policy; Shutdown ends the retries. It returns
// ErrClosed after Shutdown and ErrAlreadyStarted when called twice.
// Implements lifecycle.Starter.
func (c *Client) Start(ctx context.Context) error {
	return c.lazy.start(ctx, c.connect)
}

// Shutdown closes the client: buffered records not yet produced fail. When
// ctx is done first it returns ctx.Err() and the close goes on in the
// background. Idempotent, safe without Start. Implements lifecycle.Resource.
func (c *Client) Shutdown(ctx context.Context) error {
	return wrap(c.releaser.Shutdown(ctx))
}

// Healthcheck pings the cluster within ctx; ErrNotStarted before Start,
// ErrClosed after Shutdown. Implements lifecycle.Healthchecker.
func (c *Client) Healthcheck(ctx context.Context) error {
	return c.lazy.ping(ctx)
}

// wrap prefixes an error returned by a lifecycle method with the module
// name.
func wrap(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("kafka: %w", err)
}

// Compile-time checks.
var (
	_ lifecycle.Starter       = (*Client)(nil)
	_ lifecycle.Resource      = (*Client)(nil)
	_ lifecycle.Healthchecker = (*Client)(nil)
	_ lifecycle.Namer         = (*Client)(nil)
)
