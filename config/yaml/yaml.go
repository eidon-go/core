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

// Extensions returns the file and KV key extensions to register Parse for:
// ".yaml", ".yml" and "" (no extension), so KV keys such as "/svc/flags" are
// read as YAML too.
func Extensions() []string {
	return []string{".yaml", ".yml", ""}
}

// Parse is a config.Parser for YAML. Mapping keys that are not strings
// (e.g. 200: ok) become their string form; an empty document yields an empty
// map. Only the first document of a multi-document stream is read.
func Parse(data []byte) (map[string]any, error) {
	var doc map[string]any

	if err := yaml.NewDecoder(bytes.NewReader(data)).Decode(&doc); err != nil {
		if errors.Is(err, io.EOF) {
			return map[string]any{}, nil
		}

		return nil, fmt.Errorf("decode yaml: %w", err)
	}

	if doc == nil {
		return map[string]any{}, nil
	}

	return normalize(doc), nil
}

// normalize converts nested map[any]any (produced for non-string keys) to
// map[string]any, recursively, including maps inside lists.
func normalize(m map[string]any) map[string]any {
	for k, v := range m {
		m[k] = normalizeValue(v)
	}

	return m
}

func normalizeValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		return normalize(x)
	case map[any]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			out[fmt.Sprint(k)] = normalizeValue(val)
		}

		return out
	case []any:
		for i := range x {
			x[i] = normalizeValue(x[i])
		}

		return x
	default:
		return v
	}
}
