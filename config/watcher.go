package config

import (
	"context"
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
		if d > b.max/2 { // doubling would pass max (or overflow for a huge max)
			d = b.max

			break
		}

		d *= 2
	}

	d = min(d, b.max)

	return d - time.Duration(float64(d)*watchJitter*rand.Float64()) //nolint:gosec // jitter needs no crypto source
}

// resets reports whether a watch stream that ended was healthy, so the next
// reconnect starts from the initial delay again: it delivered a change after
// its initial state, or it stayed open for at least the maximum delay after
// that state. The initial state alone does not count — every watch delivers
// one, including one that a server cancels right after it opens — nor does
// the wait for it (a cluster without quorum may hang every read until the KV
// timeout).
func (b backoff) resets(changed bool, lasted time.Duration) bool {
	return changed || lasted >= b.max
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
// ends. The backoff grows while streams keep failing and resets after a
// healthy one (see backoff.resets).
func (w *watcher) watchKey(ctx context.Context, key string) {
	failures := 0

	for {
		if w.backoff.resets(w.stream(ctx, key)) {
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

// stream opens a watch on key and applies its events until the channel
// closes, an error event arrives or ctx is done. The first event is the key's
// current state (see KV.Watch): it brings the values up to date after New or
// a reconnect. stream reports whether a change arrived after that first
// event and how long the watch was open after it (zero if it never reported
// its state): the wait for the initial state does not count as healthy.
func (w *watcher) stream(ctx context.Context, key string) (bool, time.Duration) {
	// The watch lives only as long as this stream: returning cancels it.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ch := w.kv.Watch(ctx, key)
	if ch == nil {
		w.logger.WarnContext(ctx, "config: KV returned no watch channel", slog.String("key", key))

		return false, 0
	}

	if !w.initial(ctx, key, ch) {
		return false, 0
	}

	opened := time.Now()
	changed := false

	for {
		var (
			ev KVEvent
			ok bool
		)

		select {
		case <-ctx.Done():
			return changed, time.Since(opened)
		case ev, ok = <-ch:
			if !ok {
				return changed, time.Since(opened)
			}
		}

		if ev.Err != nil {
			w.logger.WarnContext(ctx, "config: watch error", slog.String("key", key), slog.Any("err", ev.Err))

			return changed, time.Since(opened)
		}

		if w.handle(ctx, key, ev) {
			changed = true
		}
	}
}

// initial waits for the first event of a watch — the key's current state —
// and applies it. Like every KV read it is bounded by the KV timeout; a watch
// that cannot report the state in time (or reports an error) is reopened with
// backoff, since the values may be stale.
func (w *watcher) initial(ctx context.Context, key string, ch <-chan KVEvent) bool {
	var timeout <-chan time.Time

	if w.timeout > 0 {
		timer := time.NewTimer(w.timeout)
		defer timer.Stop()

		timeout = timer.C
	}

	select {
	case <-ctx.Done():
		return false
	case <-timeout:
		w.logger.WarnContext(ctx, "config: watch did not report the key's state in time",
			slog.String("key", key), slog.Duration("timeout", w.timeout))

		return false
	case ev, ok := <-ch:
		if !ok {
			return false
		}

		if ev.Err != nil {
			w.logger.WarnContext(ctx, "config: watch error", slog.String("key", key), slog.Any("err", ev.Err))

			return false
		}

		if ev.Type != KVEventPut && ev.Type != KVEventDelete {
			// Breaks the KV.Watch contract: without a state the values
			// may be stale, so the watch is reopened.
			w.logger.ErrorContext(ctx, "config: watch did not start with the key's state",
				slog.String("key", key), slog.Int("type", int(ev.Type)))

			return false
		}

		w.handle(ctx, key, ev)

		return true
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
