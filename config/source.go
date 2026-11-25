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

	"go.yaml.in/yaml/v3"
)

// MaxFileSize is the largest configuration file WithFiles accepts.
const MaxFileSize = 10 << 20 // 10 MiB

// Errors returned by New for invalid sources.
var (
	// ErrFileNotRegular means a configuration path is not a regular file.
	ErrFileNotRegular = errors.New("config: not a regular file")
	// ErrFileTooLarge means a configuration file exceeds MaxFileSize.
	ErrFileTooLarge = errors.New("config: file exceeds the size limit")
	// ErrUnknownFormat means a file or KV key has an extension other than
	// .yaml, .yml or .json.
	ErrUnknownFormat = errors.New("config: unknown format")
)

// loadFiles reads every file and merges them in order.
func loadFiles(fsys fs.FS, paths []string) (map[string]any, error) {
	acc := map[string]any{}

	for _, p := range paths {
		data, err := readFile(fsys, p)
		if err != nil {
			return nil, fmt.Errorf("config: read %s: %w", p, err)
		}

		parsed, err := parse(data, p)
		if err != nil {
			return nil, fmt.Errorf("config: parse %s: %w", p, err)
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
func loadKeys(ctx context.Context, kv KV, timeout time.Duration, keys []string) (map[string]any, error) {
	acc := map[string]any{}

	for _, key := range keys {
		parsed, err := loadKey(ctx, kv, timeout, key)
		if err != nil {
			return nil, err
		}

		acc = merge(acc, parsed)
	}

	return acc, nil
}

// loadKey reads and parses one KV key.
func loadKey(ctx context.Context, kv KV, timeout time.Duration, key string) (map[string]any, error) {
	if timeout > 0 {
		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	data, err := kv.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("config: read KV key %s: %w", key, err)
	}

	parsed, err := parse(data, key)
	if err != nil {
		return nil, fmt.Errorf("config: parse KV key %s: %w", key, err)
	}

	return parsed, nil
}

// parse decodes a yaml or json document, chosen by the extension of name,
// into a flat map. An empty document yields an empty map.
func parse(data []byte, name string) (map[string]any, error) {
	var parsed map[string]any

	switch ext := strings.ToLower(path.Ext(name)); ext {
	case "", ".yaml", ".yml":
		if err := yaml.NewDecoder(bytes.NewReader(data)).Decode(&parsed); err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("decode yaml: %w", err)
		}
	case ".json":
		if len(bytes.TrimSpace(data)) == 0 {
			return map[string]any{}, nil
		}

		if err := json.Unmarshal(data, &parsed); err != nil {
			return nil, fmt.Errorf("decode json: %w", err)
		}
	default:
		return nil, fmt.Errorf("%w: %q (want .yaml, .yml or .json)", ErrUnknownFormat, ext)
	}

	return flatten(parsed), nil
}
