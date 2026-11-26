// Package txcore is the transaction core shared by the sql and sql/pgx
// packages: the modes of a nested WithTx, rollback-only scopes, savepoints
// and after-commit callbacks, generic over the transaction type of a driver.
//
// A Core belongs to one database (one *sql.DB, one pgx pool): the context
// key of its transactions is the Core itself, so transactions of two
// databases in one context never mix, and two managers over the same
// database share them.
//
// Scopes. A transaction is the outermost scope; every ModeNested call opens
// a savepoint scope inside the current one. A ModeJoin call runs in the
// current scope: an error or a panic in its function marks that scope
// rollback-only. The call that opened the scope then rolls it back (the
// transaction, or to the savepoint) and returns ErrRollbackOnly joined with
// the first cause, even when its own function returned nil.
package txcore

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"
)

// Mode is the behavior of WithTx inside a transaction of the same database.
// Outside one, every mode begins a new transaction.
type Mode int

// Modes of a nested WithTx.
const (
	// ModeJoin runs in the current transaction (or savepoint); an error
	// marks it rollback-only.
	ModeJoin Mode = iota
	// ModeNested runs in a savepoint of the current transaction: an error
	// rolls back to the savepoint and leaves the transaction usable.
	ModeNested
	// ModeRequiresNew begins a separate transaction on another connection.
	ModeRequiresNew
	// ModeForbid fails with ErrNestedTx.
	ModeForbid
)

// String returns the name of the mode.
func (m Mode) String() string {
	switch m {
	case ModeJoin:
		return "join"
	case ModeNested:
		return "nested"
	case ModeRequiresNew:
		return "requires-new"
	case ModeForbid:
		return "forbid"
	default:
		return "mode(" + strconv.Itoa(int(m)) + ")"
	}
}

// Options of one WithTx call.
type Options struct {
	Mode      Mode
	Isolation sql.IsolationLevel
	ReadOnly  bool
}

// ManagerOptions configure a manager, a view over a Core.
type ManagerOptions struct {
	// DefaultMode is the Mode of a call without an explicit one.
	DefaultMode Mode
}

// The option types of package sql (TxOption, ManagerOption) keep their
// function unexported, so that no internal type shows in its API. Package
// sql sets these in init to apply them; package sql/pgx, which imports sql,
// applies the same options through them.
var (
	ApplyTxOption      func(opt any, o *Options)        //nolint:gochecknoglobals // set once by package sql
	ApplyManagerOption func(opt any, o *ManagerOptions) //nolint:gochecknoglobals // set once by package sql
)

var (
	// ErrNestedTx is returned by WithTx with ModeForbid inside a transaction.
	ErrNestedTx = errors.New("sql: nested transaction forbidden")

	// ErrRollbackOnly is returned, joined with the first cause, by the WithTx
	// that rolled back a scope marked rollback-only by a joined call, and by
	// a joined or nested call that starts in such a scope.
	ErrRollbackOnly = errors.New("sql: transaction is rollback-only")

	// ErrTxOptionsIncompatible is returned by a joined or nested WithTx that
	// asks for a stricter isolation level than the transaction has, or for a
	// read-write scope inside a read-only transaction.
	ErrTxOptionsIncompatible = errors.New("sql: transaction options incompatible with the enclosing transaction")

	// ErrInvalidTxMode is returned by WithTx for a Mode that is none of the
	// declared ones.
	ErrInvalidTxMode = errors.New("sql: invalid transaction mode")

	// ErrAborted is the cause recorded when a joined function panics or
	// exits its goroutine (runtime.Goexit, t.FailNow).
	ErrAborted = errors.New("sql: transaction function panicked or exited")

	// errNilFunc is returned by WithTx for a nil function.
	errNilFunc = errors.New("sql: nil transaction function")
)

// DefaultRollbackTimeout bounds a rollback: it runs on a context detached
// from the caller's, which may be what failed the transaction.
const DefaultRollbackTimeout = 5 * time.Second

// Driver begins and ends transactions of type T.
type Driver[T any] interface {
	// Begin begins a transaction with opts.Isolation and opts.ReadOnly.
	Begin(ctx context.Context, opts Options) (T, error)
	// Commit commits tx.
	Commit(ctx context.Context, tx T) error
	// Rollback rolls tx back.
	Rollback(ctx context.Context, tx T) error
	// Exec runs a statement without arguments in tx (savepoints).
	Exec(ctx context.Context, tx T, query string) error
}

// Core runs transactions of one database through its Driver.
type Core[T any] struct {
	driver          Driver[T]
	rollbackTimeout time.Duration
}

// New returns a Core over driver.
func New[T any](driver Driver[T]) *Core[T] {
	return &Core[T]{driver: driver, rollbackTimeout: DefaultRollbackTimeout}
}

// ctxKey is the context key of the transactions of one Core.
type ctxKey[T any] struct{ core *Core[T] }

type status int

const (
	statusActive status = iota
	statusCommitted
	statusRolledBack
)

// state is one transaction; mu guards it and all of its scopes.
type state[T any] struct {
	tx   T
	opts Options

	mu         sync.Mutex
	savepoints int
	callbacks  uint64 // sequence of OnCommit registrations
	status     status
}

// callback is an OnCommit function with its registration sequence: a scope
// that hands its callbacks to its parent merges them by it, so they run in
// registration order even when the parent registered more meanwhile (with
// its own ctx, or from another goroutine).
type callback struct {
	seq uint64
	fn  func()
}

// scope is the transaction itself (savepoint == "") or one savepoint in it.
type scope[T any] struct {
	state     *state[T]
	parent    *scope[T]
	savepoint string

	// Guarded by state.mu.
	done         bool
	rollbackOnly bool
	cause        error
	onCommit     []callback // sorted by seq
}

// WithTx runs fn in a transaction according to opts.Mode; see the package
// documentation for the scopes. fn's own error is returned as is; a
// rollback that fails too is joined to it.
//
// A panic in fn (or runtime.Goexit) rolls back what the call opened and
// propagates.
func (c *Core[T]) WithTx(ctx context.Context, opts Options, body func(context.Context) error) error {
	if body == nil {
		return errNilFunc
	}

	if opts.Mode < ModeJoin || opts.Mode > ModeForbid {
		return fmt.Errorf("%w: %s", ErrInvalidTxMode, opts.Mode)
	}

	cur := c.current(ctx)
	if cur == nil {
		return c.root(ctx, opts, body)
	}

	switch opts.Mode {
	case ModeJoin:
		return c.join(ctx, cur, opts, body)
	case ModeNested:
		return c.nested(ctx, cur, opts, body)
	case ModeRequiresNew:
		return c.root(ctx, opts, body)
	case ModeForbid:
		return ErrNestedTx
	default: // unreachable: validated above
		return fmt.Errorf("%w: %s", ErrInvalidTxMode, opts.Mode)
	}
}

// Tx returns the transaction of ctx, if ctx carries one of this Core that
// has not finished.
func (c *Core[T]) Tx(ctx context.Context) (T, bool) {
	cur := c.current(ctx)
	if cur == nil {
		var zero T

		return zero, false
	}

	return cur.state.tx, true
}

// OnCommit registers fn to run after the outermost commit of the
// transaction of ctx, in registration order. It is dropped when the
// transaction — or the savepoint it was registered in — rolls back. Outside
// a transaction of this Core, fn runs at once.
func (c *Core[T]) OnCommit(ctx context.Context, hook func()) {
	if hook == nil {
		return
	}

	if found, _ := ctx.Value(ctxKey[T]{c}).(*scope[T]); found != nil {
		found.state.mu.Lock()

		if live := found.live(); live != nil {
			found.state.callbacks++
			live.onCommit = append(live.onCommit, callback{seq: found.state.callbacks, fn: hook})
			found.state.mu.Unlock()

			return
		}

		found.state.mu.Unlock()
	}

	hook()
}

// current returns the innermost open scope of ctx, nil when ctx carries no
// transaction of this Core or it has finished (a context kept after WithTx
// returned).
func (c *Core[T]) current(ctx context.Context) *scope[T] {
	found, _ := ctx.Value(ctxKey[T]{c}).(*scope[T])
	if found == nil {
		return nil
	}

	found.state.mu.Lock()
	defer found.state.mu.Unlock()

	return found.live()
}

// root begins a transaction and commits or rolls it back after fn.
func (c *Core[T]) root(ctx context.Context, opts Options, body func(context.Context) error) error {
	tx, err := c.driver.Begin(ctx, opts)
	if err != nil {
		return fmt.Errorf("sql: begin transaction: %w", err)
	}

	st := &state[T]{tx: tx, opts: opts} //nolint:exhaustruct_v5 // state starts at zero
	top := &scope[T]{state: st}         //nolint:exhaustruct_v5 // the transaction scope

	finished := false

	defer func() {
		if !finished { // fn panicked or exited the goroutine
			top.finish(statusRolledBack)

			_ = c.rollback(ctx, tx) // nowhere to report it; the panic goes on
		}
	}()

	fnErr := body(context.WithValue(ctx, ctxKey[T]{c}, top))
	finished = true

	if fnErr == nil {
		fnErr = top.rollbackOnlyErr()
	}

	if fnErr != nil {
		top.finish(statusRolledBack)

		return withCleanup(fnErr, c.rollback(ctx, tx))
	}

	if err := c.driver.Commit(ctx, tx); err != nil {
		top.finish(statusRolledBack)

		return fmt.Errorf("sql: commit: %w", err)
	}

	runCallbacks(top.finish(statusCommitted))

	return nil
}

// runCallbacks runs every callback, even after one panicked, then re-raises
// the first panic: the transaction is committed, and a panicking metrics
// callback must not drop the outbox one registered after it.
func runCallbacks(callbacks []func()) {
	var (
		first    any
		panicked bool
	)

	for _, callback := range callbacks {
		func() {
			defer func() {
				if r := recover(); r != nil && !panicked {
					first, panicked = r, true
				}
			}()

			callback()
		}()
	}

	if panicked {
		panic(first)
	}
}

// join runs fn in the current scope and marks it rollback-only on failure.
func (c *Core[T]) join(ctx context.Context, cur *scope[T], opts Options, body func(context.Context) error) error {
	if err := cur.enter(opts); err != nil {
		return err
	}

	finished := false

	defer func() {
		if !finished {
			cur.markRollbackOnly(ErrAborted)
		}
	}()

	err := body(context.WithValue(ctx, ctxKey[T]{c}, cur))
	finished = true

	if err != nil {
		cur.markRollbackOnly(err)
	}

	return err
}

// nested runs fn in a new savepoint of the current scope.
func (c *Core[T]) nested(ctx context.Context, parent *scope[T], opts Options, body func(context.Context) error) error {
	if err := parent.enter(opts); err != nil {
		return err
	}

	txState := parent.state

	txState.mu.Lock()
	txState.savepoints++
	name := "sp_" + strconv.Itoa(txState.savepoints)
	txState.mu.Unlock()

	if err := c.driver.Exec(ctx, txState.tx, "SAVEPOINT "+name); err != nil {
		// The server may have aborted the transaction (PostgreSQL does on any
		// error): a caller that ignores this error must not commit it.
		err = fmt.Errorf("sql: savepoint: %w", err)
		parent.markRollbackOnly(err)

		return err
	}

	child := &scope[T]{state: txState, parent: parent, savepoint: name} //nolint:exhaustruct_v5 // state starts at zero

	finished := false

	defer func() {
		if !finished {
			_ = c.rollbackTo(ctx, child) // recorded on the parent; the panic goes on
		}
	}()

	fnErr := body(context.WithValue(ctx, ctxKey[T]{c}, child))
	finished = true

	if fnErr == nil {
		fnErr = child.rollbackOnlyErr()
	}

	if fnErr != nil {
		return withCleanup(fnErr, c.rollbackTo(ctx, child))
	}

	if err := c.driver.Exec(ctx, txState.tx, "RELEASE SAVEPOINT "+name); err != nil {
		err = fmt.Errorf("sql: release savepoint: %w", err)

		child.close(false)
		parent.markRollbackOnly(err)

		return err
	}

	child.close(true)

	return nil
}

// rollback rolls tx back on a context that the caller's cancellation does
// not reach.
func (c *Core[T]) rollback(ctx context.Context, tx T) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.rollbackTimeout)
	defer cancel()

	if err := c.driver.Rollback(ctx, tx); err != nil {
		return fmt.Errorf("sql: rollback: %w", err)
	}

	return nil
}

// rollbackTo rolls back to the savepoint of sc, drops its callbacks and
// releases the savepoint (PostgreSQL and MySQL keep it after ROLLBACK TO).
// If either fails, the transaction state is unknown: the parent scope
// becomes rollback-only.
func (c *Core[T]) rollbackTo(ctx context.Context, child *scope[T]) error {
	child.close(false)

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.rollbackTimeout)
	defer cancel()

	if err := c.driver.Exec(ctx, child.state.tx, "ROLLBACK TO SAVEPOINT "+child.savepoint); err != nil {
		err = fmt.Errorf("sql: rollback to savepoint: %w", err)
		child.parent.markRollbackOnly(err)

		return err
	}

	if err := c.driver.Exec(ctx, child.state.tx, "RELEASE SAVEPOINT "+child.savepoint); err != nil {
		err = fmt.Errorf("sql: release savepoint: %w", err)
		child.parent.markRollbackOnly(err)

		return err
	}

	return nil
}

// withCleanup returns err with the error of the rollback that followed it,
// err itself when the rollback succeeded.
func withCleanup(err, rollbackErr error) error {
	if rollbackErr == nil {
		return err
	}

	return errors.Join(err, rollbackErr)
}

// finish records how the transaction of the root scope sc ended and returns
// the callbacks to run after a commit.
func (sc *scope[T]) finish(s status) []func() {
	sc.state.mu.Lock()
	defer sc.state.mu.Unlock()

	sc.state.status = s
	registered := sc.onCommit
	sc.onCommit = nil

	if s != statusCommitted {
		return nil
	}

	callbacks := make([]func(), len(registered))
	for i, cb := range registered {
		callbacks[i] = cb.fn
	}

	return callbacks
}

// live returns sc or its innermost ancestor that is still open, nil when
// the transaction has finished. The caller holds state.mu.
func (sc *scope[T]) live() *scope[T] {
	if sc.state.status != statusActive {
		return nil
	}

	for sc != nil && sc.done {
		sc = sc.parent
	}

	return sc
}

// enter checks that a joined or nested call may run in sc.
func (sc *scope[T]) enter(opts Options) error {
	txState := sc.state

	if opts.Isolation > txState.opts.Isolation {
		return fmt.Errorf(
			"%w: isolation %s inside %s",
			ErrTxOptionsIncompatible,
			opts.Isolation,
			txState.opts.Isolation,
		)
	}

	if !opts.ReadOnly && txState.opts.ReadOnly {
		return fmt.Errorf("%w: read-write inside a read-only transaction", ErrTxOptionsIncompatible)
	}

	return sc.rollbackOnlyErr()
}

// rollbackOnlyErr returns ErrRollbackOnly joined with the cause when sc is
// rollback-only.
func (sc *scope[T]) rollbackOnlyErr() error {
	sc.state.mu.Lock()
	defer sc.state.mu.Unlock()

	if !sc.rollbackOnly {
		return nil
	}

	return errors.Join(ErrRollbackOnly, sc.cause)
}

// markRollbackOnly marks sc rollback-only; the first cause is kept.
func (sc *scope[T]) markRollbackOnly(cause error) {
	sc.state.mu.Lock()
	defer sc.state.mu.Unlock()

	if !sc.rollbackOnly {
		sc.rollbackOnly = true
		sc.cause = cause
	}
}

// close ends a savepoint scope; a released one hands its callbacks to the
// parent, a rolled back one drops them.
func (sc *scope[T]) close(released bool) {
	sc.state.mu.Lock()
	defer sc.state.mu.Unlock()

	if sc.done {
		return
	}

	sc.done = true

	if released {
		merged := slices.Concat(sc.parent.onCommit, sc.onCommit)
		slices.SortStableFunc(merged, func(a, b callback) int { return cmp.Compare(a.seq, b.seq) })
		sc.parent.onCommit = merged
	}

	sc.onCommit = nil
}
