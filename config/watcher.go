package config

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"math/rand/v2"
	"sync"
	"time"
)

// watchJitter is the largest fraction a reconnect delay is randomly reduced
// by, so many instances do not reconnect in lockstep.
const watchJitter = 0.3

type watcherConfig struct {
	kv      KV
	parsers parsers
	keys    []string
	base    map[string]any
	initial map[string]map[string]any
	timeout time.Duration
	backoff backoff
	logger  *slog.Logger
	publish func(map[string]any)
}

// backoff computes reconnect delays: initial, doubling up to max, with jitter.
type backoff struct {
	initial time.Duration
	max     time.Duration
}

// delay returns the delay after the given number of consecutive failures
// (starting at 1).
func (b backoff) delay(failures int) time.Duration {
	d := b.initial
	for i := 1; i < failures && d < b.max; i++ {
		d *= 2
	}

	d = min(d, b.max)

	return d - time.Duration(float64(d)*watchJitter*rand.Float64()) //nolint:gosec // jitter needs no crypto source
}

// watcher keeps the last parsed document of every watched key and publishes
// base merged with all of them on every change. Keeping per-key state means
// a key removed from a document disappears from the snapshot on the next
// update.
type watcher struct {
	watcherConfig

	mu    sync.Mutex
	state map[string]map[string]any
}

// newWatcher builds the watcher and publishes the initial snapshot.
func newWatcher(c watcherConfig) *watcher {
	w := &watcher{watcherConfig: c, mu: sync.Mutex{}, state: maps.Clone(c.initial)}
	if w.state == nil {
		w.state = map[string]map[string]any{}
	}

	w.mu.Lock()
	w.publishLocked()
	w.mu.Unlock()

	return w
}

// run watches every key until ctx is done.
func (w *watcher) run(ctx context.Context) {
	var wg sync.WaitGroup

	for _, key := range w.keys {
		wg.Go(func() { w.watchKey(ctx, key) })
	}

	wg.Wait()
}

// watchKey streams changes of key and reconnects with backoff when the stream
// ends. The backoff grows while streams keep failing without delivering
// anything and resets once a stream delivers an event.
func (w *watcher) watchKey(ctx context.Context, key string) {
	failures := 0

	for {
		if w.stream(ctx, key) {
			failures = 0
		}

		if ctx.Err() != nil {
			return
		}

		failures++
		delay := w.backoff.delay(failures)
		w.logger.WarnContext(ctx, "config: watch ended, reconnecting",
			slog.String("key", key), slog.Duration("delay", delay))

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()

			return
		case <-timer.C:
		}
	}
}

// stream opens a watch on key, re-reads the key to catch up on changes made
// before the watch was established (since New, or while disconnected), then
// applies events until the channel closes or ctx is done. It reports whether
// at least one event was applied.
func (w *watcher) stream(ctx context.Context, key string) bool {
	ch := w.kv.Watch(ctx, key)
	if ch == nil {
		w.logger.WarnContext(ctx, "config: KV returned no watch channel", slog.String("key", key))

		return false
	}

	w.resync(ctx, key)

	applied := false

	for {
		var (
			ev KVEvent
			ok bool
		)

		select {
		case <-ctx.Done():
			return applied
		case ev, ok = <-ch:
			if !ok {
				return applied
			}
		}

		if ev.Err != nil {
			w.logger.WarnContext(ctx, "config: watch error", slog.String("key", key), slog.Any("err", ev.Err))

			return applied
		}

		if w.handle(ctx, key, ev) {
			applied = true
		}
	}
}

// handle applies one event and reports whether it changed the state.
func (w *watcher) handle(ctx context.Context, key string, ev KVEvent) bool {
	switch ev.Type {
	case KVEventDelete:
		w.apply(key, map[string]any{})

		return true
	case KVEventPut:
		parsed, err := w.parsers.parse(ev.Value, key)
		if err != nil {
			w.logger.ErrorContext(ctx, "config: invalid watched document, keeping previous values",
				slog.String("key", key), slog.Any("err", err))

			return false
		}

		w.apply(key, parsed)

		return true
	default:
		return false
	}
}

func (w *watcher) resync(ctx context.Context, key string) {
	parsed, err := loadKey(ctx, w.kv, w.parsers, w.timeout, key)
	if errors.Is(err, ErrKVKeyNotFound) {
		// Deleted while disconnected: same as a delete event.
		parsed, err = map[string]any{}, nil
	}

	if err != nil {
		w.logger.WarnContext(ctx, "config: resync failed", slog.String("key", key), slog.Any("err", err))

		return
	}

	w.apply(key, parsed)
}

// apply stores the document of key and publishes. Publishing under the lock
// keeps snapshots in order: a stale one never overwrites a newer one.
func (w *watcher) apply(key string, parsed map[string]any) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.state[key] = parsed
	w.publishLocked()
}

func (w *watcher) publishLocked() {
	snap := w.base
	for _, key := range w.keys {
		snap = merge(snap, w.state[key])
	}

	w.publish(snap)
}
