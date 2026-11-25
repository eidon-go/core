package config

import (
	"fmt"
	"maps"
	"strings"
	"sync/atomic"
)

// store holds the flat snapshot of loaded values plus the environment layer
// and defaults. The snapshot is replaced atomically on every update.
type store struct {
	snapshot  atomic.Pointer[map[string]any]
	envLookup func(string) (string, bool)
	envName   func(string) string
	defaults  map[string]any
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

	s := &store{ //nolint:exhaustruct_v5 // snapshot is set below
		envLookup: envLookup,
		envName:   envName,
		defaults:  defaults,
	}
	s.setSnapshot(nil)

	return s
}

// setSnapshot publishes m. The caller must not mutate m afterwards.
func (s *store) setSnapshot(m map[string]any) {
	if m == nil {
		m = map[string]any{}
	}

	s.snapshot.Store(&m)
}

// lookup resolves key in priority order: environment, snapshot, defaults.
// Environment values are strings; the others keep their decoded type.
//
// Unset values do not shadow lower layers: an environment variable set to ""
// (the usual result of KEY=${KEY} with KEY unset) and a null value are
// skipped.
func (s *store) lookup(key string) (any, bool) {
	if s.envLookup != nil {
		if v, ok := s.envLookup(s.envName(key)); ok && strings.TrimSpace(v) != "" {
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

// flatten converts a nested map to a flat map keyed by dotted paths. Slices
// are kept as values. Nested empty maps contribute no keys; nil values are
// kept (they mean "not set").
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

			out[key] = v
		}
	}

	return nil
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
