package config

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"sync"
	"time"

	"github.com/eidon-go/core/retry"
)

type watcherConfig struct {
	kv      KV
	keys    []string
	base    map[string]any
	initial map[string]map[string]any
	timeout time.Duration
	backoff retry.Policy
	logger  *slog.Logger
	publish func(map[string]any)
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
		delay := w.backoff.Delay(failures)
		w.logger.WarnContext(ctx, "config: watch ended, reconnecting",
			slog.String("key", key), slog.Duration("delay", delay))

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()

			return
		case <-timer.C:
		}

		// Catch up on changes missed while disconnected.
		w.resync(ctx, key)
	}
}

// stream consumes events of key until the channel closes. It reports whether
// at least one event was applied.
func (w *watcher) stream(ctx context.Context, key string) bool {
	applied := false

	for ev := range w.kv.Watch(ctx, key) {
		if ev.Err != nil {
			w.logger.WarnContext(ctx, "config: watch error", slog.String("key", key), slog.Any("err", ev.Err))

			return applied
		}

		switch ev.Type {
		case KVEventDelete:
			w.apply(key, map[string]any{})
		case KVEventPut:
			parsed, err := parse(ev.Value, key)
			if err != nil {
				w.logger.ErrorContext(ctx, "config: invalid watched document, keeping previous values",
					slog.String("key", key), slog.Any("err", err))

				continue
			}

			w.apply(key, parsed)
		}

		applied = true
	}

	return applied
}

func (w *watcher) resync(ctx context.Context, key string) {
	parsed, err := loadKey(ctx, w.kv, w.timeout, key)
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
