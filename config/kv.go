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
// the key's state (the initial one or a change), or Err is set: a terminal
// error after which the channel closes.
type KVEvent struct {
	Type  KVEventType
	Value []byte
	Err   error
}

// KV is a key-value backend for remote configuration. The core module ships
// no implementation; github.com/eidon-go/core/contrib/config/etcd provides
// one for etcd v3. The interface is small so a backend or a test fake is easy
// to write.
type KV interface {
	// Get returns the current value of key, or an error wrapping
	// ErrKVKeyNotFound if the key does not exist.
	Get(ctx context.Context, key string) ([]byte, error)

	// Watch returns a channel of the states of key from the moment the watch
	// starts:
	//
	//   - the first event is the key's current state: KVEventPut with its
	//     value, or KVEventDelete if the key does not exist;
	//   - after it, the key's later states in the order they happened. A
	//     backend that only sees the latest value (polling, blocking
	//     queries) may skip intermediate states, but it must never deliver
	//     a state older than one it already delivered, and a change must
	//     eventually be followed by an event with the key's latest state.
	//
	// A backend that registers its watch asynchronously must take the
	// initial state and the start of the change stream from the same point
	// (in etcd: read the key, then watch from the revision after the read),
	// or it could miss the latest state. If it cannot, it sends a terminal
	// error instead.
	//
	// The channel is closed when ctx is done or after a terminal error event
	// (which may be the first one, e.g. when reading the initial state
	// fails). Config reopens a closed watch with backoff; the initial state
	// of the new watch brings it up to date, so nothing has to be replayed
	// across reopens. Config waits for the initial state at most as long as
	// for any other KV read (WithKVTimeout), and reopens a watch whose first
	// event is not a state.
	Watch(ctx context.Context, key string) <-chan KVEvent

	// Close releases the backend. Config calls it once, after it has
	// cancelled the context of every watch and stopped reading their
	// channels; goroutines of the backend that serve those watches may still
	// be finishing, and Close must tolerate that.
	Close() error
}
