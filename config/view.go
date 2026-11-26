package config

import (
	"fmt"
	"slices"
	"strings"
)

// Sub returns a view of the keys under prefix: Get[string](cfg.Sub("db"),
// "host") reads "db.host" through every layer — the environment variable
// DB_HOST included — and follows the watched keys like cfg does. Pass a view
// to code that reads one part of the configuration without knowing where
// that part lives.
//
// A view shares the sources of cfg; it is not a component of its own. Run
// and Shutdown of a view do nothing to cfg: register cfg itself with app.App.
func (c *Config) Sub(prefix string) *Config {
	if c == nil || c.store == nil {
		return nil
	}

	view := newConfig(c.store, c.logger)
	view.prefix = viewKey(c.prefix, prefix)

	return view
}

// Keys returns the names directly below prefix ("" for the top level),
// sorted: for {"tenants": {"a": {...}, "b": {...}}}, Keys("tenants") is
// ["a", "b"]. Read each one with Sub("tenants." + name).
//
// Keys sees the loaded sources and the defaults. It does not see the
// environment: a variable name cannot be mapped back to a key, so a key set
// only by an environment variable is read by Get but not listed by Keys.
func (c *Config) Keys(prefix string) []string {
	if c == nil || c.store == nil {
		return nil
	}

	return c.store.children(viewKey(c.prefix, prefix))
}

// Items returns the list of objects at key, one config per element: for
// {"routes": [{"path": "/a"}, {"path": "/b"}]}, Items("routes") holds two
// configs that each have the key "path". A list is the way to hold entries
// whose names contain dots (host names), which nested keys cannot tell apart
// from nesting.
//
// Each item holds a copy of its element: it has no environment layer and no
// defaults, and it does not follow later updates — read Items again, for
// example in OnChange. A key that is not set returns no items and no error;
// a value that is not a list of objects returns a *KeyError wrapping
// ErrKeyMistyped.
func (c *Config) Items(key string) ([]*Config, error) {
	if c == nil || c.store == nil {
		return nil, nil
	}

	full := viewKey(c.prefix, key)

	raw, ok := c.store.lookup(full)
	if !ok {
		return nil, nil
	}

	list, ok := raw.([]any)
	if !ok {
		return nil, &KeyError{Key: full, Err: fmt.Errorf("%w: want a list of objects, got %T", ErrKeyMistyped, raw)}
	}

	items := make([]*Config, 0, len(list))

	for i, element := range list {
		flat, err := flattenElement(element)
		if err != nil {
			return nil, &KeyError{Key: fmt.Sprintf("%s[%d]", full, i), Err: err}
		}

		item := newConfig(newStore(nil, nil, nil), c.logger)
		item.store.setSnapshot(flat)
		items = append(items, item)
	}

	return items, nil
}

// viewKey returns key as a view with prefix sees it. Unlike joinKey, an
// empty key stands for the prefix itself: Keys("") of a view lists its own
// children.
func viewKey(prefix, key string) string {
	switch {
	case prefix == "":
		return key
	case key == "":
		return prefix
	default:
		return prefix + "." + key
	}
}

// flattenElement flattens one element of a list of objects.
func flattenElement(element any) (map[string]any, error) {
	switch m := element.(type) {
	case map[string]any:
		return flatten(m)
	case map[any]any:
		// flattenInto converts the keys of a nested map[any]any.
		flat, err := flatten(map[string]any{"item": m})
		if err != nil {
			return nil, err
		}

		out := make(map[string]any, len(flat))
		for k, v := range flat {
			out[strings.TrimPrefix(k, "item.")] = v
		}

		return out, nil
	default:
		return nil, fmt.Errorf("%w: want an object, got %T", ErrKeyMistyped, element)
	}
}

// children returns the sorted distinct names directly below prefix among the
// set keys of the snapshot and the defaults.
func (s *store) children(prefix string) []string {
	seen := map[string]bool{}

	collect := func(m map[string]any) {
		for k, v := range m {
			if v == nil {
				continue
			}

			rest := k

			if prefix != "" {
				var ok bool
				if rest, ok = strings.CutPrefix(k, prefix+"."); !ok {
					continue
				}
			}

			name, _, _ := strings.Cut(rest, ".")
			seen[name] = true
		}
	}

	collect(*s.snapshot.Load())
	collect(s.defaults)

	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}

	slices.Sort(names)

	return names
}
