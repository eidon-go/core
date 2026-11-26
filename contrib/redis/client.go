// Package redis is a Redis client component on go-redis v9: a single node,
// a cluster or a Sentinel-managed master, behind the lifecycle contract.
//
// New builds the go-redis client (no I/O) and instruments it with redisotel
// (command spans, command and pool metrics); Start pings it with retry,
// Shutdown closes it, Healthcheck pings it. Commands go through the go-redis
// API returned by Client.Redis.
//
//	client, err := redis.New(redis.Config{Addrs: []string{"localhost:6379"}})
//	a.Add(client) // app.App
//	err = client.Redis().Set(ctx, "key", "value", time.Hour).Err()
package redis

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/extra/redisotel/v9"
	goredis "github.com/redis/go-redis/v9"

	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/retry"
)

var (
	// ErrInvalidConfig is wrapped by every error from Config.Validate and by
	// New for invalid arguments.
	ErrInvalidConfig = errors.New("redis: invalid config")

	// ErrClosed is returned by Start and Healthcheck after Shutdown. It is
	// lifecycle.ErrClosed.
	ErrClosed = lifecycle.ErrClosed
)

// Client is a go-redis client with the lifecycle of a component. It is a
// Starter, Resource, Healthchecker and Namer.
//
// Register the components that use it (consumers, servers) after the
// Client, so that they shut down first.
type Client struct {
	name     string
	rdb      goredis.UniversalClient
	connect  retry.Policy
	releaser *lifecycle.Releaser

	closing context.Context //nolint:containedctx // a close signal for a Start in progress
}

// New validates cfg and builds the client: a cluster client for several
// addresses (or Config.Cluster), a failover client with
// Config.MasterName, a single-node client otherwise. It does no I/O, except
// for the idle connections of Config.MinIdleConns, which go-redis dials in
// the background.
func New(cfg Config, opts ...Option) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	o := newOptions(opts)
	rdb := goredis.NewUniversalClient(o.universalOptions(cfg))

	metricsDone := make(chan struct{})

	if err := instrument(rdb, &o, metricsDone); err != nil {
		close(metricsDone)

		_ = rdb.Close()

		return nil, err
	}

	closing, cancel := context.WithCancel(context.Background())

	return &Client{
		name:    o.name,
		rdb:     rdb,
		connect: o.connectPolicy(),
		releaser: lifecycle.CloseOnce(func() error {
			cancel()
			close(metricsDone)

			if err := rdb.Close(); err != nil && !errors.Is(err, goredis.ErrClosed) {
				return fmt.Errorf("close: %w", err)
			}

			return nil
		}),
		closing: closing,
	}, nil
}

func instrument(rdb goredis.UniversalClient, o *options, metricsDone chan struct{}) error {
	tracing := append([]redisotel.TracingOption{redisotel.WithTracerProvider(o.tracerProvider)}, o.tracingOpts...)

	if err := redisotel.InstrumentTracing(rdb, tracing...); err != nil {
		return fmt.Errorf("redis: instrument tracing: %w", err)
	}

	err := redisotel.InstrumentMetrics(rdb,
		redisotel.WithMeterProvider(o.meterProvider), redisotel.WithCloseChan(metricsDone))
	if err != nil {
		return fmt.Errorf("redis: instrument metrics: %w", err)
	}

	return nil
}

// Redis returns the go-redis client for commands, pipelines, transactions,
// Pub/Sub and scripts. It is owned by Client: do not close it, call
// Shutdown. It is a *goredis.Client for a single node or a Sentinel master
// and a *goredis.ClusterClient for a cluster.
//
//nolint:ireturn // go-redis's interface over the three client kinds
func (c *Client) Redis() goredis.UniversalClient { return c.rdb }

// Name returns the component name (WithName). Implements lifecycle.Namer.
func (c *Client) Name() string { return c.name }

// Start pings the server until it answers, retried with the
// WithConnectRetry policy; Shutdown ends the retries. It returns ErrClosed
// after Shutdown. Implements lifecycle.Starter.
func (c *Client) Start(ctx context.Context) error {
	if c.releaser.Closing() {
		return wrap(ErrClosed)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stop := context.AfterFunc(c.closing, cancel) //nolint:contextcheck // the close signal, not a parent context
	defer stop()

	if err := c.connect.Do(ctx, c.ping); err != nil {
		if c.closing.Err() != nil {
			return wrap(ErrClosed)
		}

		return fmt.Errorf("redis: ping: %w", err)
	}

	return nil
}

// Shutdown closes the client and its connection pools. When ctx is done
// first it returns ctx.Err() and the close goes on in the background.
// Idempotent, safe without Start and after the client was closed through
// Redis(). Implements lifecycle.Resource.
func (c *Client) Shutdown(ctx context.Context) error {
	return wrap(c.releaser.Shutdown(ctx))
}

// Healthcheck pings the server within ctx; ErrClosed after Shutdown.
// Implements lifecycle.Healthchecker.
func (c *Client) Healthcheck(ctx context.Context) error {
	if c.releaser.Closing() {
		return wrap(ErrClosed)
	}

	if err := c.ping(ctx); err != nil {
		if c.releaser.Closing() || errors.Is(err, goredis.ErrClosed) {
			return wrap(ErrClosed)
		}

		return fmt.Errorf("redis: ping: %w", err)
	}

	return nil
}

func (c *Client) ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err() //nolint:wrapcheck // the callers wrap
}

// wrap prefixes an error returned by a lifecycle method with the module
// name.
func wrap(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("redis: %w", err)
}

// Compile-time checks.
var (
	_ lifecycle.Starter       = (*Client)(nil)
	_ lifecycle.Resource      = (*Client)(nil)
	_ lifecycle.Healthchecker = (*Client)(nil)
	_ lifecycle.Namer         = (*Client)(nil)
)
