package config

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
)

// Errors reported for required keys; match them with errors.Is. The
// *KeyError that wraps them carries the key.
var (
	// ErrKeyMissing means a required key is not set in any source. An
	// environment variable set to "" and a null value count as not set.
	ErrKeyMissing = errors.New("config: required key is missing")
	// ErrKeyMistyped means a required key is set but its value cannot be
	// converted to the requested type.
	ErrKeyMistyped = errors.New("config: value has the wrong type")
)

// KeyError describes a problem with one required key.
type KeyError struct {
	// Key is the dotted key.
	Key string
	// Err is ErrKeyMissing or ErrKeyMistyped, possibly with details.
	Err error
}

func (e *KeyError) Error() string { return e.Err.Error() + ": " + e.Key }

func (e *KeyError) Unwrap() error { return e.Err }

// Reader collects the errors of required keys so that a whole config struct
// can be read in one literal and checked once:
//
//	r := config.NewReader(cfg)
//	smtp := smtp.Config{
//	    Host:    config.Required[string](r, "smtp.host"),
//	    Port:    config.Required[int](r, "smtp.port"),
//	    Timeout: config.GetOrDefault(cfg, "smtp.timeout", 10*time.Second),
//	}
//	if err := r.Err(); err != nil {
//	    return err // lists every missing or mistyped key
//	}
//
// A Reader is safe for concurrent use. A Reader over a nil *Config reports
// every required key as missing.
type Reader struct {
	cfg *Config

	mu   sync.Mutex
	errs []error
}

// NewReader returns a Reader over cfg.
func NewReader(cfg *Config) *Reader {
	return &Reader{cfg: cfg, mu: sync.Mutex{}, errs: nil}
}

// Required returns the value of key as T. If the key is not set or has the
// wrong type, it records a *KeyError in r (see Reader.Err) and returns the
// zero value of T.
//
// Required checks presence and type, not content: an empty string, 0 or an
// empty list is a present value. Validate such values where they are used.
func Required[T ValueType](r *Reader, key string) T {
	v, raw, res := lookupAs[T](r.cfg, key)
	if res != lookupFound {
		r.record(keyError(r.cfg, key, v, raw, res))
	}

	return v
}

// Optional returns the value of key as T, or def when the key is not set. Unlike
// GetOrDefault, a value of the wrong type is not silently replaced by def: it
// records a *KeyError wrapping ErrKeyMistyped in r and returns def.
func Optional[T ValueType](r *Reader, key string, def T) T {
	v, raw, res := lookupAs[T](r.cfg, key)

	switch res {
	case lookupFound:
		return v
	case lookupMistyped:
		r.record(keyError(r.cfg, key, v, raw, res))
	case lookupMissing:
	}

	return def
}

// Err returns every error recorded by Required and Optional, joined with
// errors.Join, or nil. Errors appear in the order they were recorded.
func (r *Reader) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return errors.Join(r.errs...)
}

func (r *Reader) record(err error) {
	r.mu.Lock()
	r.errs = append(r.errs, err)
	r.mu.Unlock()
}

// keyError describes a failed lookup of key; want is the zero value of the
// requested type. The key is the full one, the prefix of a Sub view included.
func keyError(cfg *Config, key string, want, raw any, res lookupResult) *KeyError {
	if cfg != nil {
		key = viewKey(cfg.prefix, key)
	}

	if res == lookupMistyped {
		return &KeyError{Key: key, Err: fmt.Errorf("%w: want %T, got %T", ErrKeyMistyped, want, raw)}
	}

	return &KeyError{Key: key, Err: ErrKeyMissing}
}

// warnMistyped logs, once per key, that Get or GetOrDefault replaced a value
// of the wrong type by the zero value or the default.
func warnMistyped(cfg *Config, key string, want, raw any) {
	full := viewKey(cfg.prefix, key)
	if _, logged := cfg.store.warned.LoadOrStore(full, struct{}{}); logged {
		return
	}

	cfg.logger.WarnContext(
		context.Background(),
		"config: value has the wrong type, using the default",
		slog.String(
			"key",
			full,
		),
		slog.String("want", fmt.Sprintf("%T", want)),
		slog.String("got", fmt.Sprintf("%T", raw)),
	)
}
