package config

import (
	"context"
	"errors"
)

// ErrKVKeyNotFound is returned (wrapped) by KV.Get when a key does not exist.
var ErrKVKeyNotFound = errors.New("config: KV key not found")

// KVEventType distinguishes the events delivered by KV.Watch. The zero value
// is not a valid event type; such events are ignored.
type KVEventType int

const (
	// KVEventPut means the key was created or updated; Value holds the new
	// payload.
	KVEventPut KVEventType = iota + 1
	// KVEventDelete means the key was deleted; Value is nil.
	KVEventDelete
)

// KVEvent is one event delivered by KV.Watch. Either Type and Value describe
// a change, or Err is set: a terminal error after which the channel closes.
type KVEvent struct {
	Type  KVEventType
	Value []byte
	Err   error
}

// KV is a key-value backend for remote configuration. The core module ships
// no implementation; github.com/eidon-go/core/config/etcd provides one for
// etcd v3. The interface is small so a backend or a test fake is easy to
// write.
type KV interface {
	// Get returns the current value of key, or an error wrapping
	// ErrKVKeyNotFound if the key does not exist.
	Get(ctx context.Context, key string) ([]byte, error)

	// Watch returns a channel of changes to key. The channel is closed when
	// ctx is done or after a terminal error event. Config re-reads the key
	// with Get after every close, so delivery does not have to be gap-free
	// across reconnects.
	Watch(ctx context.Context, key string) <-chan KVEvent

	// Close releases the backend. Config calls it once, after every Watch
	// has returned.
	Close() error
}
