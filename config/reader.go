package config

import (
	"errors"
	"fmt"
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

	var err error

	switch res {
	case lookupFound:
		return v
	case lookupMissing:
		err = ErrKeyMissing
	case lookupMistyped:
		err = fmt.Errorf("%w: want %T, got %T", ErrKeyMistyped, v, raw)
	}

	r.record(&KeyError{Key: key, Err: err})

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
		r.record(&KeyError{Key: key, Err: fmt.Errorf("%w: want %T, got %T", ErrKeyMistyped, v, raw)})
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
