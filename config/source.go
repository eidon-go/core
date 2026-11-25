package config

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"time"
)

// MaxFileSize is the largest configuration file WithFiles accepts.
const MaxFileSize = 10 << 20 // 10 MiB

// Errors returned by New for invalid sources.
var (
	// ErrFileNotRegular means a configuration path is not a regular file.
	ErrFileNotRegular = errors.New("config: not a regular file")
	// ErrFileTooLarge means a configuration file exceeds MaxFileSize.
	ErrFileTooLarge = errors.New("config: file exceeds the size limit")
	// ErrUnknownFormat means no Parser is registered for the extension of a
	// file or KV key (see WithParser).
	ErrUnknownFormat = errors.New("config: no parser for format")
	// ErrKeyCollision means two entries of one document (or of one
	// WithDefaults map) resolve to the same dotted key, e.g. a nested
	// "a": {"b": 1} next to a literal "a.b": 2. Layering different sources
	// over each other is not a duplicate.
	ErrKeyCollision = errors.New("config: entries collide after flattening")
)

var errTrailingData = errors.New("unexpected data after the document")

// Parser decodes one document into a map. Nested objects must be
// map[string]any; values are scalars, slices or nested maps. An empty
// document yields an empty map. Register parsers with WithParser; JSON is
// built in (ParseJSON), YAML lives in github.com/eidon-go/core/contrib/config/yaml.
// A Parser is called concurrently (one watched KV key per goroutine), so it
// must be safe for concurrent use. Distinct entries of the returned map must
// not flatten to the same dotted key, e.g. {"a": {"b": 1}, "a.b": 2}
// (ErrKeyCollision); a key repeated in the source text is the parser's
// business (ParseJSON keeps the last one, like encoding/json).
type Parser func(data []byte) (map[string]any, error)

// ParseJSON is the built-in Parser for ".json". Numbers are decoded as
// json.Number, so large integers keep their precision. The document must be
// a single object; an empty document or null yields an empty map.
func ParseJSON(data []byte) (map[string]any, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return map[string]any{}, nil
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()

	var doc map[string]any
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("decode json: %w", err)
	}

	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("decode json: %w", errTrailingData)
	}

	if doc == nil {
		return map[string]any{}, nil
	}

	return doc, nil
}

// parsers maps a lower-case extension ("" for none) to its Parser.
type parsers map[string]Parser

// parse decodes data with the parser registered for the extension of name
// and flattens the result.
func (p parsers) parse(data []byte, name string) (map[string]any, error) {
	ext := strings.ToLower(path.Ext(name))

	parser, ok := p[ext]
	if !ok {
		hint := ""
		if ext == "" || ext == ".yaml" || ext == ".yml" {
			hint = " (for YAML use github.com/eidon-go/core/contrib/config/yaml)"
		}

		return nil, fmt.Errorf("%w: %q%s", ErrUnknownFormat, ext, hint)
	}

	doc, err := parser(data)
	if err != nil {
		return nil, err
	}

	return flatten(doc)
}

// loadFiles reads every file and merges them in order. Missing optional
// files are skipped.
func loadFiles(fsys fs.FS, p parsers, files []fileSource) (map[string]any, error) {
	acc := map[string]any{}

	for _, file := range files {
		name := file.path

		data, err := readFile(fsys, name)
		if file.optional && errors.Is(err, fs.ErrNotExist) {
			continue
		}

		if err != nil {
			return nil, fmt.Errorf("config: read %s: %w", name, err)
		}

		parsed, err := p.parse(data, name)
		if err != nil {
			return nil, fmt.Errorf("config: parse %s: %w", name, err)
		}

		acc = merge(acc, parsed)
	}

	return acc, nil
}

func readFile(fsys fs.FS, name string) ([]byte, error) {
	var (
		f   fs.File
		err error
	)

	if fsys != nil {
		f, err = fsys.Open(name)
	} else {
		f, err = os.Open(name)
	}

	if err != nil {
		return nil, err //nolint:wrapcheck // wrapped by the caller with the path
	}

	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return nil, err //nolint:wrapcheck // wrapped by the caller with the path
	}

	if !info.Mode().IsRegular() {
		return nil, ErrFileNotRegular
	}

	if info.Size() > MaxFileSize {
		return nil, ErrFileTooLarge
	}

	data, err := io.ReadAll(io.LimitReader(f, MaxFileSize+1))
	if err != nil {
		return nil, err //nolint:wrapcheck // wrapped by the caller with the path
	}

	if len(data) > MaxFileSize {
		return nil, ErrFileTooLarge
	}

	return data, nil
}

// loadKeys reads KV keys and merges them in order.
func loadKeys(ctx context.Context, kv KV, p parsers, timeout time.Duration, keys []string) (map[string]any, error) {
	acc := map[string]any{}

	for _, key := range keys {
		parsed, err := loadKey(ctx, kv, p, timeout, key)
		if err != nil {
			return nil, err
		}

		acc = merge(acc, parsed)
	}

	return acc, nil
}

// loadKey reads and parses one KV key.
func loadKey(ctx context.Context, kv KV, p parsers, timeout time.Duration, key string) (map[string]any, error) {
	if timeout > 0 {
		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	data, err := kv.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("config: read KV key %s: %w", key, err)
	}

	parsed, err := p.parse(data, key)
	if err != nil {
		return nil, fmt.Errorf("config: parse KV key %s: %w", key, err)
	}

	return parsed, nil
}
