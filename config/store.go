package config

import (
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
)

// store holds the flat snapshot of loaded values plus the environment layer
// and defaults. The snapshot is replaced atomically on every update.
type store struct {
	snapshot  atomic.Pointer[map[string]any]
	envLookup func(string) (string, bool)
	envName   func(string) string
	defaults  map[string]any

	// envNames caches envName per key (string → string): lookup runs on
	// every read, often in a request path through Live. It holds one entry
	// per distinct key ever read: small for keys fixed in code, but it grows
	// with keys built at run time (tenants.<id>.limit) — a few dozen bytes per
	// key, acceptable for config keys.
	envNames sync.Map

	// changes holds the OnChange listeners; notify calls them on publish.
	changes changeHub
	// logger reports mistyped values and panicking listeners.
	logger *slog.Logger
	// warned holds the keys whose mistyped value has been logged (string →
	// struct{}), so that a Live read in a request path logs it once.
	warned sync.Map
}

// newStore builds a store. A nil envLookup or envName disables the
// environment layer.
func newStore(envLookup func(string) (string, bool), envName func(string) string, defaults map[string]any) *store {
	if envLookup == nil || envName == nil {
		envLookup, envName = nil, nil
	}

	if defaults == nil {
		defaults = map[string]any{}
	}

	s := &store{ //nolint:exhaustruct_v5 // snapshot is set below; caches and listeners start empty
		envLookup: envLookup,
		envName:   envName,
		defaults:  defaults,
		logger:    slog.Default(),
	}
	s.setSnapshot(nil)

	return s
}

// setSnapshot publishes m without notifying the OnChange listeners. The
// caller must not mutate m afterwards.
func (s *store) setSnapshot(m map[string]any) {
	if m == nil {
		m = map[string]any{}
	}

	s.snapshot.Store(&m)
}

// publish publishes m and then notifies the OnChange listeners of the keys it
// changed. Calls must not overlap (the watcher serializes them). The caller
// must not mutate m afterwards.
func (s *store) publish(snapshot map[string]any) {
	if snapshot == nil {
		snapshot = map[string]any{}
	}

	old := s.snapshot.Swap(&snapshot)
	s.notify(*old, snapshot)
}

// lookup resolves key in priority order: environment, snapshot, defaults.
// Environment values are strings; the others keep their decoded type.
//
// Unset values do not shadow lower layers: an environment variable set to ""
// (the usual result of KEY=${KEY} with KEY unset) and a null value are
// skipped.
func (s *store) lookup(key string) (any, bool) {
	if s.envLookup != nil {
		if v, ok := s.envLookup(s.envVar(key)); ok && strings.TrimSpace(v) != "" {
			return v, true
		}
	}

	if v, ok := (*s.snapshot.Load())[key]; ok && v != nil {
		return v, true
	}

	if v, ok := s.defaults[key]; ok && v != nil {
		return v, true
	}

	return nil, false
}

// envVar returns the environment variable name for key, computed once.
func (s *store) envVar(key string) string {
	if name, ok := s.envNames.Load(key); ok {
		return name.(string) //nolint:forcetypeassert // only strings are stored
	}

	name := s.envName(key)
	s.envNames.Store(key, name)

	return name
}

// flatten converts a nested map to a flat map keyed by dotted paths. Slices
// are kept as values, copied so that the caller's map (a WithDefaults or
// FromMap literal) shares no memory with the config. Nested empty maps
// contribute no keys; nil values are kept (they mean "not set").
//
// Two entries of one document that resolve to the same dotted key — a nested
// "a": {"b": 1} next to a literal "a.b": 2, or the non-string key 1 next to
// the string key "1" — are an error wrapping ErrKeyCollision: which of them
// wins would depend on map iteration order.
func flatten(nested map[string]any) (map[string]any, error) {
	out := map[string]any{}
	if err := flattenInto(out, "", nested); err != nil {
		return nil, err
	}

	return out, nil
}

func flattenInto(out map[string]any, prefix string, m map[string]any) error {
	for k, v := range m {
		key := joinKey(prefix, k)

		switch child := v.(type) {
		case map[string]any:
			if err := flattenInto(out, key, child); err != nil {
				return err
			}
		case map[any]any: // decoders produce this for non-string keys (e.g. 200: ok)
			converted := make(map[string]any, len(child))
			for childKey, childValue := range child {
				name := fmt.Sprint(childKey)
				if _, dup := converted[name]; dup {
					return fmt.Errorf("%w: %q", ErrKeyCollision, joinKey(key, name))
				}

				converted[name] = childValue
			}

			if err := flattenInto(out, key, converted); err != nil {
				return err
			}
		default:
			if _, dup := out[key]; dup {
				return fmt.Errorf("%w: %q", ErrKeyCollision, key)
			}

			out[key] = cloneValue(v)
		}
	}

	return nil
}

// cloneValue deep-copies the lists and maps of a leaf value; other values are
// immutable or not decoded by config, and are returned as is.
func cloneValue(v any) any {
	switch x := v.(type) {
	case []any:
		out := make([]any, len(x))
		for i, item := range x {
			out[i] = cloneValue(item)
		}

		return out
	case []string:
		return slices.Clone(x)
	case []int:
		return slices.Clone(x)
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, item := range x {
			out[k] = cloneValue(item)
		}

		return out
	case map[any]any:
		out := make(map[any]any, len(x))
		for k, item := range x {
			out[k] = cloneValue(item)
		}

		return out
	default:
		return v
	}
}

func joinKey(prefix, key string) string {
	if prefix == "" {
		return key
	}

	return prefix + "." + key
}

// merge returns a new flat map with src layered over dst. A nil value in src
// is skipped: null in a higher-priority source means "not set" and must not
// erase a lower-priority value.
func merge(dst, src map[string]any) map[string]any {
	out := make(map[string]any, len(dst)+len(src))
	maps.Copy(out, dst)

	for k, v := range src {
		if v != nil {
			out[k] = v
		}
	}

	return out
}
