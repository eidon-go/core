// Package yaml adds YAML support to github.com/eidon-go/core/config:
//
//	cfg, err := config.New(ctx,
//	    config.WithParser(yaml.Parse, yaml.Extensions()...),
//	    config.WithFiles("config.yaml"),
//	)
//
// It is a separate module so that the core module stays free of third-party
// dependencies. It does not import config either: Parse matches the
// config.Parser signature, so any version of config can use it.
package yaml

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"go.yaml.in/yaml/v3"
)

// ErrKeyCollision is returned (wrapped, with the key's path) by Parse when two
// nested keys that decode to different values have the same string form,
// e.g. 1 and 1.0 (both "1"): which value survived would depend on map
// iteration order. Keys that decode to the same value (1 and 0x1) are a
// repeated key, and the YAML decoder keeps the last one.
var ErrKeyCollision = errors.New("yaml: mapping keys collide after conversion to strings")

// Extensions returns the file and KV key extensions to register Parse for:
// ".yaml", ".yml" and "" (no extension), so KV keys such as "/svc/flags" are
// read as YAML too.
func Extensions() []string {
	return []string{".yaml", ".yml", ""}
}

// Parse is a config.Parser for YAML. Mapping keys that are not strings
// (e.g. 200: ok) become their string form; two keys of one mapping with the
// same string form are an error wrapping ErrKeyCollision. An empty document
// yields an empty map. Timestamps stay strings (date: 2024-01-01 reads as
// "2024-01-01"), as config has no time type. Only the first document of a
// multi-document stream is read.
func Parse(data []byte) (map[string]any, error) {
	var node yaml.Node

	if err := yaml.NewDecoder(bytes.NewReader(data)).Decode(&node); err != nil {
		if errors.Is(err, io.EOF) {
			return map[string]any{}, nil
		}

		return nil, fmt.Errorf("decode yaml: %w", err)
	}

	keepTimestampsAsStrings(&node)

	var doc map[string]any
	if err := node.Decode(&doc); err != nil {
		return nil, fmt.Errorf("decode yaml: %w", err)
	}

	if doc == nil {
		return map[string]any{}, nil
	}

	if err := normalize(doc, ""); err != nil {
		return nil, err
	}

	return doc, nil
}

// keepTimestampsAsStrings retags implicit timestamp scalars as strings, so
// they decode to their original text instead of time.Time.
func keepTimestampsAsStrings(n *yaml.Node) {
	if n.Kind == yaml.ScalarNode && n.ShortTag() == "!!timestamp" {
		n.Tag = "!!str"
	}

	for _, child := range n.Content {
		keepTimestampsAsStrings(child)
	}
}

// normalize converts nested map[any]any (produced for non-string keys) to
// map[string]any in place, recursively, including maps inside lists. path is
// the location of doc, for error messages.
func normalize(doc map[string]any, path string) error {
	for k, v := range doc {
		nv, err := normalizeValue(v, joinPath(path, k))
		if err != nil {
			return err
		}

		doc[k] = nv
	}

	return nil
}

func normalizeValue(v any, path string) (any, error) {
	switch typed := v.(type) {
	case map[string]any:
		if err := normalize(typed, path); err != nil {
			return nil, err
		}

		return typed, nil
	case map[any]any:
		return normalizeAnyKeys(typed, path)
	case []any:
		return normalizeList(typed, path)
	default:
		return v, nil
	}
}

// normalizeAnyKeys converts a mapping with non-string keys to map[string]any,
// rejecting keys with the same string form.
func normalizeAnyKeys(m map[any]any, path string) (map[string]any, error) {
	out := make(map[string]any, len(m))

	for rawKey, val := range m {
		key := fmt.Sprint(rawKey)
		if _, dup := out[key]; dup {
			return nil, fmt.Errorf("%w: %q", ErrKeyCollision, joinPath(path, key))
		}

		nv, err := normalizeValue(val, joinPath(path, key))
		if err != nil {
			return nil, err
		}

		out[key] = nv
	}

	return out, nil
}

// normalizeList normalizes the items of list in place.
func normalizeList(list []any, path string) ([]any, error) {
	for idx, item := range list {
		nv, err := normalizeValue(item, fmt.Sprintf("%s[%d]", path, idx))
		if err != nil {
			return nil, err
		}

		list[idx] = nv
	}

	return list, nil
}

func joinPath(prefix, key string) string {
	if prefix == "" {
		return key
	}

	return prefix + "." + key
}
