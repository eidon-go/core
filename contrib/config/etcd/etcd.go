// Package etcd implements config.KV on top of the etcd v3 client, so that
// configuration documents stored in etcd can be read and hot-reloaded by
// github.com/eidon-go/core/config:
//
//	kv, err := etcd.New(etcd.Config{Endpoints: []string{"etcd:2379"}})
//	if err != nil {
//		return err
//	}
//	cfg, err := config.New(ctx,
//		config.WithKV(kv), // Config owns kv from here on and closes it
//		config.WithKeys("/svc/base.json"),
//		config.WithWatchedKeys("/svc/flags.json"),
//	)
//
// Every key holds one whole document (JSON, or any format registered with
// config.WithParser); the key's extension selects the parser.
package etcd

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"

	"github.com/eidon-go/core/config"
)

// watchBuffer is the capacity of the channel returned by Watch: it absorbs
// a short burst of events while Config parses the previous one.
const watchBuffer = 16

// KV reads and watches single keys of an etcd v3 cluster. It implements
// config.KV and is safe for concurrent use.
//
// The etcd client is created by the first Get or Watch, not by New: creating
// it may already talk to the cluster (authentication). A failed creation is
// retried by the next call.
type KV struct {
	dial func() (*conn, error) // nil when the client was given to NewFromClient

	mu      sync.Mutex
	conn    *conn
	pending *dialAttempt
	closed  bool

	closeOnce sync.Once
	closeErr  error
}

var _ config.KV = (*KV)(nil)

// New validates cfg and the options and prepares the etcd client. It does no
// network I/O: the client is created, and the connection opened, by the
// first Get or Watch. Close releases the client.
func New(cfg Config, opts ...Option) (*KV, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	o := newOptions(opts)

	if o.tlsConfig != nil {
		for _, ep := range cfg.Endpoints {
			if strings.HasPrefix(strings.ToLower(ep), "http://") {
				// clientv3 silently drops TLS for http:// endpoints.
				return nil, fmt.Errorf("%w: endpoint %q is http:// but WithTLSConfig is set", ErrInvalidConfig, ep)
			}
		}
	}

	ccfg := clientConfig(cfg.withDefaults(), o)

	dial := func() (*conn, error) {
		cli, err := clientv3.New(ccfg)
		if err != nil {
			return nil, fmt.Errorf("etcd: create client: %w", err)
		}

		return &conn{kv: cli.KV, watcher: cli.Watcher, closer: cli}, nil
	}

	return &KV{dial: dial}, nil //nolint:exhaustruct_v5 // connection state starts at zero
}

// watchMethod is the gRPC method of etcd watch streams.
const watchMethod = "/etcdserverpb.Watch/Watch"

// notWatchStream keeps watch streams out of the instrumentation: clientv3
// multiplexes every watch on one stream that lives for hours, so its span
// would be exported only when the stream ends and its metrics say nothing
// useful. Reads and other calls are instrumented.
func notWatchStream(info *stats.RPCTagInfo) bool {
	return info.FullMethodName != watchMethod
}

// clientConfig builds the clientv3.Config for cfg (with defaults applied).
func clientConfig(cfg Config, o options) clientv3.Config {
	logger := o.logger.With(slog.String("component", "etcd"))

	ccfg := clientv3.Config{ //nolint:exhaustruct_v5 // zero values are clientv3's defaults
		Endpoints:            cfg.Endpoints,
		Username:             cfg.Username,
		Password:             cfg.Password,
		DialTimeout:          cfg.DialTimeout,
		DialKeepAliveTime:    cfg.DialKeepAliveTime,
		DialKeepAliveTimeout: cfg.DialKeepAliveTimeout,
		AutoSyncInterval:     cfg.AutoSyncInterval,
		TLS:                  o.tlsConfig,
		Logger:               zap.New(newSlogCore(logger.Handler(), o.clientLogLevel)),
		DialOptions: []grpc.DialOption{
			grpc.WithStatsHandler(otelgrpc.NewClientHandler(
				otelgrpc.WithTracerProvider(o.tracerProvider),
				otelgrpc.WithMeterProvider(o.meterProvider),
				otelgrpc.WithPropagators(o.propagators),
				otelgrpc.WithFilter(notWatchStream),
			)),
		},
	}

	for _, f := range o.clientFuncs {
		f(&ccfg)
	}

	return ccfg
}

// NewFromClient returns a KV that uses an existing client, for an
// application that already has one (for leases or elections). The client
// stays the caller's: Close does not close it, and it must stay open until
// the KV is closed.
func NewFromClient(cli *clientv3.Client) (*KV, error) {
	if cli == nil {
		return nil, ErrNilClient
	}

	c := &conn{kv: cli.KV, watcher: cli.Watcher, closer: nil}

	return &KV{conn: c}, nil //nolint:exhaustruct_v5 // no dial: the connection is given
}

// Get returns the value of key, or an error wrapping config.ErrKVKeyNotFound
// if the key does not exist. It is bounded by ctx only (config bounds every
// read with config.WithKVTimeout).
func (k *KV) Get(ctx context.Context, key string) ([]byte, error) {
	c, err := k.connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("etcd: get %q: %w", key, err)
	}

	resp, err := c.kv.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("etcd: get %q: %w", key, err)
	}

	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("etcd: get %q: %w", key, config.ErrKVKeyNotFound)
	}

	return resp.Kvs[0].Value, nil
}

// Watch follows the config.KV contract: the first event is the key's current
// state (a put with its value, or a delete if the key does not exist), then
// every later change in order, with none missed.
//
// The channel closes when ctx is done, when the client is closed, or right
// after an event with Err set: the initial read failed, the cluster lost its
// leader, or the revision was compacted. Config then reopens the watch with
// backoff.
func (k *KV) Watch(ctx context.Context, key string) <-chan config.KVEvent {
	out := make(chan config.KVEvent, watchBuffer)

	go func() {
		defer close(out)

		c, err := k.connect(ctx)
		if err != nil {
			send(ctx, out, errorEvent(fmt.Errorf("etcd: watch %q: %w", key, err)))

			return
		}

		c.watch(ctx, key, out)
	}()

	return out
}

// Close releases the client created by New (a client passed to
// NewFromClient is left open). A client still being created is closed as
// soon as its creation ends. Get and Watch fail with ErrClosed afterwards.
// Cancel the contexts of all watches first: config.Config does, it closes the
// KV after every watch has returned. Repeated calls return the result of the
// first one.
func (k *KV) Close() error {
	k.closeOnce.Do(func() {
		k.mu.Lock()
		k.closed = true
		c := k.conn
		k.conn = nil
		k.mu.Unlock()

		if c != nil {
			k.closeErr = c.close()
		}
	})

	return k.closeErr
}

func putEvent(value []byte) config.KVEvent {
	return config.KVEvent{Type: config.KVEventPut, Value: value, Err: nil}
}

func deleteEvent() config.KVEvent {
	return config.KVEvent{Type: config.KVEventDelete, Value: nil, Err: nil}
}

func errorEvent(err error) config.KVEvent {
	return config.KVEvent{Type: 0, Value: nil, Err: err}
}

// send delivers ev unless ctx is done first; it reports whether it did.
func send(ctx context.Context, out chan<- config.KVEvent, ev config.KVEvent) bool {
	select {
	case out <- ev:
		return true
	case <-ctx.Done():
		return false
	}
}
