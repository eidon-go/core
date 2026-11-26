package config

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"sync"
)

// OnChange registers callback to be called after an update of the watched
// keys has changed a value at prefix or below it ("" for any key; relative to
// a Sub view), and returns a function that unregisters it. Use it to rebuild
// what was built from earlier values — a rate limiter, a client pool — which
// Live cannot do.
//
// callback runs on the watcher's goroutine once the new values are visible
// to Get. Updates are delivered one at a time and in order, so keep it short:
// a slow callback delays the next update. A panic in it is recovered and
// logged. The environment layer is not watched, so callback may be called
// for a key that an environment variable pins. FromMap configs and configs
// without watched keys never call it.
func (c *Config) OnChange(prefix string, callback func()) func() {
	if c == nil || c.store == nil || callback == nil {
		return func() {}
	}

	return c.store.changes.add(viewKey(c.prefix, prefix), callback)
}

// changeHub holds the OnChange listeners of a store.
type changeHub struct {
	mu        sync.Mutex
	next      uint64
	listeners map[uint64]listener
}

type listener struct {
	prefix string
	fn     func()
}

func (h *changeHub) add(prefix string, callback func()) func() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.listeners == nil {
		h.listeners = map[uint64]listener{}
	}

	id := h.next
	h.next++
	h.listeners[id] = listener{prefix: prefix, fn: callback}

	return func() {
		h.mu.Lock()
		delete(h.listeners, id)
		h.mu.Unlock()
	}
}

// snapshot returns the listeners in registration order.
func (h *changeHub) snapshot() []listener {
	h.mu.Lock()
	defer h.mu.Unlock()

	ids := make([]uint64, 0, len(h.listeners))
	for id := range h.listeners {
		ids = append(ids, id)
	}

	slices.Sort(ids)

	out := make([]listener, 0, len(ids))
	for _, id := range ids {
		out = append(out, h.listeners[id])
	}

	return out
}

// notify calls the listeners whose prefix covers a key that differs between
// old and cur.
func (s *store) notify(old, cur map[string]any) {
	listeners := s.changes.snapshot()
	if len(listeners) == 0 {
		return
	}

	changed := changedKeys(old, cur)
	if len(changed) == 0 {
		return
	}

	for _, l := range listeners {
		if covers(l.prefix, changed) {
			s.call(l)
		}
	}
}

func (s *store) call(target listener) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.ErrorContext(context.Background(), "config: OnChange callback panicked",
				slog.String("prefix", target.prefix), slog.String("panic", fmt.Sprint(r)))
		}
	}()

	target.fn()
}

// changedKeys returns the keys whose value differs between old and cur,
// added and removed keys included.
func changedKeys(old, cur map[string]any) []string {
	var changed []string

	for k, v := range cur {
		if prev, ok := old[k]; !ok || !reflect.DeepEqual(prev, v) {
			changed = append(changed, k)
		}
	}

	for k := range old {
		if _, ok := cur[k]; !ok {
			changed = append(changed, k)
		}
	}

	return changed
}

// covers reports whether one of keys is prefix or lies below it.
func covers(prefix string, keys []string) bool {
	if prefix == "" {
		return len(keys) > 0
	}

	for _, k := range keys {
		if k == prefix || strings.HasPrefix(k, prefix+".") {
			return true
		}
	}

	return false
}
