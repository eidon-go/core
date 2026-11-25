package etcd

import (
	"context"
	"errors"
	"fmt"
	"io"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/eidon-go/core/config"
)

// conn is a created etcd client: the parts KV uses.
type conn struct {
	kv      clientv3.KV
	watcher clientv3.Watcher
	closer  io.Closer // nil when the client belongs to the caller
}

// dialAttempt is one creation of the client; waiters block on done.
type dialAttempt struct {
	done chan struct{}
	err  error // set before done is closed
}

// connect returns the client, creating it on first use. One creation runs at
// a time and is shared by every caller; ctx bounds only the wait, so a
// caller that gives up does not abandon the creation for the others.
func (k *KV) connect(ctx context.Context) (*conn, error) {
	k.mu.Lock()

	if k.closed {
		k.mu.Unlock()

		return nil, ErrClosed
	}

	if k.conn != nil {
		c := k.conn
		k.mu.Unlock()

		return c, nil
	}

	if k.dial == nil { // a zero KV, not made by New or NewFromClient
		k.mu.Unlock()

		return nil, ErrNoClient
	}

	attempt := k.pending
	if attempt == nil {
		attempt = &dialAttempt{done: make(chan struct{}), err: nil}
		k.pending = attempt

		go k.runDial(attempt)
	}

	k.mu.Unlock()

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("waiting for the client: %w", ctx.Err())
	case <-attempt.done:
	}

	if attempt.err != nil {
		return nil, attempt.err
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	if k.closed {
		return nil, ErrClosed
	}

	return k.conn, nil
}

// runDial creates the client and publishes the result. A failure is not
// kept: the next connect tries again. A client created after Close is closed
// right away.
func (k *KV) runDial(attempt *dialAttempt) {
	c, err := k.dial()

	var orphan *conn

	k.mu.Lock()
	k.pending = nil

	switch {
	case err != nil:
		attempt.err = err
	case k.closed:
		attempt.err = ErrClosed
		orphan = c
	default:
		k.conn = c
	}

	k.mu.Unlock()
	close(attempt.done)

	if orphan != nil {
		_ = orphan.close() // nobody is left to report it to
	}
}

// close releases the client if it is ours.
func (c *conn) close() error {
	if c.closer == nil {
		return nil
	}

	// clientv3 reports its own cancellation as the close error.
	if err := c.closer.Close(); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("etcd: close client: %w", err)
	}

	return nil
}

// watch reads key, sends its state, then streams the changes made after that
// read. Both come from the same revision, as the config.KV contract
// requires: clientv3.Watcher.Watch registers the watch asynchronously, so a
// watch "from now" could miss a change made while it is being set up.
func (c *conn) watch(ctx context.Context, key string, out chan<- config.KVEvent) {
	resp, err := c.kv.Get(ctx, key)
	if err != nil {
		send(ctx, out, errorEvent(fmt.Errorf("etcd: watch %q: read: %w", key, err)))

		return
	}

	first := deleteEvent()
	if len(resp.Kvs) > 0 {
		first = putEvent(resp.Kvs[0].Value)
	}

	if !send(ctx, out, first) {
		return
	}

	// WithRequireLeader ends the watch with an error when the member loses
	// its leader (a partitioned member would otherwise go silent); the
	// cancel releases the server-side watch on every return path.
	wctx, cancel := context.WithCancel(clientv3.WithRequireLeader(ctx))
	defer cancel()

	stream(ctx, key, c.watcher.Watch(wctx, key, clientv3.WithRev(resp.Header.GetRevision()+1)), out)
}

// stream forwards the events of wch until ctx is done, wch closes (the
// client was closed) or a response carries an error.
func stream(ctx context.Context, key string, wch clientv3.WatchChan, out chan<- config.KVEvent) {
	for {
		var (
			wresp clientv3.WatchResponse
			ok    bool
		)

		select {
		case <-ctx.Done():
			return
		case wresp, ok = <-wch:
			if !ok {
				return
			}
		}

		if err := wresp.Err(); err != nil {
			send(ctx, out, errorEvent(fmt.Errorf("etcd: watch %q: %w", key, err)))

			return
		}

		for _, ev := range wresp.Events {
			if !send(ctx, out, translate(ev)) {
				return
			}
		}
	}
}

// translate converts an etcd event into a config event.
func translate(ev *clientv3.Event) config.KVEvent {
	if ev.Type == clientv3.EventTypeDelete || ev.Kv == nil {
		return deleteEvent()
	}

	return putEvent(ev.Kv.Value)
}
