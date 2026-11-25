package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/eidon-go/core/retry"
)

// lazyClient creates the franz-go client in Start: kgo.NewClient starts
// background loops (metadata, group membership) that talk to the brokers,
// and constructors do no network I/O.
type lazyClient struct {
	opts    []kgo.Opt
	closeFn func(*kgo.Client)

	mu     sync.Mutex
	client *kgo.Client
	closed bool

	closing       context.Context //nolint:containedctx // a close signal for a Start in progress
	cancelClosing context.CancelFunc
}

func newLazyClient(opts []kgo.Opt, closeFn func(*kgo.Client)) *lazyClient {
	closing, cancel := context.WithCancel(context.Background())

	return &lazyClient{ //nolint:exhaustruct_v5 // state starts at zero
		opts:          opts,
		closeFn:       closeFn,
		closing:       closing,
		cancelClosing: cancel,
	}
}

// get returns the client, nil before Start.
func (l *lazyClient) get() *kgo.Client {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.client
}

func (l *lazyClient) isClosed() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.closed
}

// start creates the client and pings until the cluster answers. A close
// during the pings (Shutdown) ends them at once with ErrClosed.
func (l *lazyClient) start(ctx context.Context, policy retry.Policy) error {
	client, err := l.create()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stop := context.AfterFunc(l.closing, cancel) //nolint:contextcheck // the close signal, not a parent context
	defer stop()

	if err := policy.Do(ctx, client.Ping); err != nil {
		if l.closing.Err() != nil {
			return ErrClosed
		}

		return fmt.Errorf("kafka: ping: %w", err)
	}

	return nil
}

func (l *lazyClient) create() (*kgo.Client, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	switch {
	case l.closed:
		return nil, ErrClosed
	case l.client != nil:
		return nil, ErrAlreadyStarted
	}

	client, err := kgo.NewClient(l.opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}

	l.client = client

	return client, nil
}

// close closes the client, if Start created it; later Starts fail with
// ErrClosed. It is run once, by a Releaser.
func (l *lazyClient) close() error {
	l.mu.Lock()
	l.closed = true
	client := l.client
	l.mu.Unlock()

	l.cancelClosing()

	if client != nil {
		l.closeFn(client)
	}

	return nil
}

// ping pings the cluster within ctx.
func (l *lazyClient) ping(ctx context.Context) error {
	client := l.get()

	switch {
	case l.isClosed():
		return ErrClosed
	case client == nil:
		return ErrNotStarted
	}

	if err := client.Ping(ctx); err != nil {
		return fmt.Errorf("kafka: ping: %w", err)
	}

	return nil
}
