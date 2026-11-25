package kafka

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/retry"
)

// Client is a franz-go client with the lifecycle of a component: Start pings
// the cluster with retry, Shutdown closes it, Healthcheck pings it. Use it
// for what Producer and Consumer do not cover — admin requests (wrap Kgo in
// kadm.Client), transactions, direct partition consumption — through Kgo.
//
// It is a Starter, Resource, Healthchecker and Namer.
type Client struct {
	name     string
	logger   *slog.Logger
	connect  retry.Policy
	client   *kgo.Client
	releaser *lifecycle.Releaser
}

// NewClient validates cfg and builds the client; it does no network I/O —
// franz-go connects on first use, and Start pings.
func NewClient(cfg Config, opts ...Option) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	o := newOptions(opts)

	client, err := kgo.NewClient(o.clientOpts(cfg, o.tracer(cfg.ClientID, ""))...)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}

	return &Client{
		name:    o.name,
		logger:  o.logger,
		connect: o.connectPolicy(),
		client:  client,
		releaser: lifecycle.CloseOnce(func() error {
			client.Close()

			return nil
		}),
	}, nil
}

// Kgo returns the franz-go client. It is owned by Client: do not close it,
// call Shutdown.
func (c *Client) Kgo() *kgo.Client { return c.client }

// Name returns the component name (WithName). Implements lifecycle.Namer.
func (c *Client) Name() string { return c.name }

// Start pings the cluster until it answers, retried with the WithConnectRetry
// policy. It returns ErrClosed after Shutdown. Implements lifecycle.Starter.
func (c *Client) Start(ctx context.Context) error {
	return startPing(ctx, c.connect, c.client, c.releaser)
}

// Shutdown closes the client: buffered records not yet produced fail. When
// ctx is done first it returns ctx.Err() and the close goes on in the
// background. Idempotent. Implements lifecycle.Resource.
func (c *Client) Shutdown(ctx context.Context) error {
	return wrap(c.releaser.Shutdown(ctx))
}

// Healthcheck pings the cluster within ctx. Implements lifecycle.Healthchecker.
func (c *Client) Healthcheck(ctx context.Context) error {
	return healthPing(ctx, c.client, c.releaser)
}

// startPing pings until the cluster answers, giving up at once after
// Shutdown.
func startPing(ctx context.Context, policy retry.Policy, client *kgo.Client, releaser *lifecycle.Releaser) error {
	if releaser.Closing() {
		return ErrClosed
	}

	err := policy.Do(ctx, func(ctx context.Context) error {
		if releaser.Closing() {
			return retry.Permanent(ErrClosed)
		}

		return client.Ping(ctx)
	})
	if err != nil {
		return fmt.Errorf("kafka: ping: %w", err)
	}

	return nil
}

func healthPing(ctx context.Context, client *kgo.Client, releaser *lifecycle.Releaser) error {
	if releaser.Closing() {
		return ErrClosed
	}

	if err := client.Ping(ctx); err != nil {
		return fmt.Errorf("kafka: ping: %w", err)
	}

	return nil
}

// wrap prefixes an error returned by a lifecycle method with the module
// name, unless it has it already.
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
