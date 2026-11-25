package lifecycle

import (
	"context"
	"sync"
	"sync/atomic"
)

// Releaser releases a resource once and implements Shutdown for it, so that a
// component needs no sync.Once, flag or channel of its own:
//
//	type Client struct {
//	    pool     *pgxpool.Pool
//	    releaser *lifecycle.Releaser
//	}
//
//	func New(...) *Client {
//	    c := &Client{pool: pool}
//	    c.releaser = lifecycle.CloseOnce(func() error { c.pool.Close(); return nil })
//	    return c
//	}
//
//	func (c *Client) Shutdown(ctx context.Context) error { return c.releaser.Shutdown(ctx) }
//
// The first Shutdown starts the release. Every Shutdown — the first one, a
// concurrent one and a later one — waits for it until its own ctx is done,
// then returns ctx.Err(); a release that has finished wins over an expired
// ctx. The error of the release is returned by the first Shutdown only;
// later ones return nil once the release has finished, as Resource requires.
// A panic in the release is recovered and returned as its error.
//
// A Releaser is created by ShutdownOnce or CloseOnce; the zero value is not
// usable.
type Releaser struct {
	release    func(ctx context.Context) error
	background bool // release ignores ctx: run it in its own goroutine

	once    sync.Once
	closing atomic.Bool
	done    chan struct{} // closed when the release has returned
	err     error         // set before done is closed
}

// ShutdownOnce returns a Releaser for a release function that honors ctx,
// such as an SDK's Shutdown(ctx): the first Shutdown calls release with its
// own ctx and returns its result. It panics if release is nil.
func ShutdownOnce(release func(ctx context.Context) error) *Releaser {
	if release == nil {
		panic("lifecycle: ShutdownOnce: nil release")
	}

	return &Releaser{release: release, done: make(chan struct{})} //nolint:exhaustruct_v5 // state starts at zero
}

// CloseOnce returns a Releaser for a blocking Close without ctx (database
// pools, message clients). The first Shutdown runs it in the background; if
// ctx is done first, Shutdown returns ctx.Err() and the close finishes on its
// own. It panics if closeFn is nil.
func CloseOnce(closeFn func() error) *Releaser {
	if closeFn == nil {
		panic("lifecycle: CloseOnce: nil close")
	}

	return &Releaser{ //nolint:exhaustruct_v5 // state starts at zero
		release:    func(context.Context) error { return closeFn() },
		background: true,
		done:       make(chan struct{}),
	}
}

// Shutdown starts the release on the first call and waits for it until ctx
// is done. Implements Resource.
func (r *Releaser) Shutdown(ctx context.Context) error {
	first := false

	// Only the claim happens under the Once: Do blocks concurrent callers
	// until its function returns, and they must wait with their own ctx.
	r.once.Do(func() {
		first = true

		r.closing.Store(true)
	})

	switch {
	case first && r.background:
		go r.run(context.WithoutCancel(ctx))
	case first:
		r.run(ctx)
	}

	select {
	case <-r.done:
	default:
		select {
		case <-r.done:
		case <-ctx.Done():
			return ctx.Err() //nolint:wrapcheck // the caller's own context error
		}
	}

	if first {
		return r.err
	}

	return nil
}

// Closing reports whether Shutdown has been called. It is advisory: a Start
// or a method that checks it to refuse work races with a concurrent
// Shutdown, so the work it lets through must still cope with the release.
func (r *Releaser) Closing() bool { return r.closing.Load() }

// Done returns a channel that is closed when the release has finished.
func (r *Releaser) Done() <-chan struct{} { return r.done }

// run calls release once and publishes its result; done is closed even if
// release panics, so that no Shutdown waits forever.
func (r *Releaser) run(ctx context.Context) {
	defer close(r.done)

	r.err = protect("release", func() error { return r.release(ctx) })
}

var _ Resource = (*Releaser)(nil)
