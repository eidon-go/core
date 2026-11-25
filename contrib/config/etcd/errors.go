package etcd

import "errors"

var (
	// ErrInvalidConfig is wrapped by every error from Config.Validate and by
	// New for options that contradict the Config.
	ErrInvalidConfig = errors.New("etcd: invalid config")

	// ErrNilClient is returned by NewFromClient when the client is nil.
	ErrNilClient = errors.New("etcd: nil client")

	// ErrNoClient is returned (wrapped) by Get and Watch of a KV that was not
	// created with New or NewFromClient (a zero KV).
	ErrNoClient = errors.New("etcd: KV has no client, create it with New or NewFromClient")

	// ErrClosed is returned (wrapped) by Get and Watch after Close.
	ErrClosed = errors.New("etcd: KV closed")
)
