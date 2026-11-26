package txcore_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/sql/internal/txcore"
)

// fakeTx is a transaction of fakeDriver.
type fakeTx struct{ id int }

// fakeDriver records every call as "<tx id>:<op>" and fails the operations
// listed in fail.
type fakeDriver struct {
	mu     sync.Mutex
	nextID int
	log    []string
	opts   []txcore.Options
	fail   map[string]error // op ("begin", "commit", "rollback" or the statement) → error

	// rollbackCtxErr is the ctx.Err() seen by the last Rollback or
	// ROLLBACK TO.
	rollbackCtxErr error
}

func newDriver() *fakeDriver { return &fakeDriver{fail: map[string]error{}} }

func (d *fakeDriver) Begin(_ context.Context, opts txcore.Options) (*fakeTx, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.fail["begin"]; err != nil {
		return nil, err
	}

	d.nextID++
	d.log = append(d.log, fmt.Sprintf("%d:begin", d.nextID))
	d.opts = append(d.opts, opts)

	return &fakeTx{id: d.nextID}, nil
}

func (d *fakeDriver) Commit(_ context.Context, tx *fakeTx) error { return d.record(tx, "commit") }

func (d *fakeDriver) Rollback(ctx context.Context, tx *fakeTx) error {
	d.mu.Lock()
	d.rollbackCtxErr = ctx.Err()
	d.mu.Unlock()

	return d.record(tx, "rollback")
}

func (d *fakeDriver) Exec(ctx context.Context, tx *fakeTx, query string) error {
	if strings.HasPrefix(query, "ROLLBACK TO") {
		d.mu.Lock()
		d.rollbackCtxErr = ctx.Err()
		d.mu.Unlock()
	}

	return d.record(tx, query)
}

func (d *fakeDriver) failOn(op string, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.fail[op] = err
}

func (d *fakeDriver) record(tx *fakeTx, op string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.log = append(d.log, fmt.Sprintf("%d:%s", tx.id, op))

	return d.fail[op]
}

func (d *fakeDriver) calls() []string {
	d.mu.Lock()
	defer d.mu.Unlock()

	return append([]string(nil), d.log...)
}

var errBoom = errors.New("boom")

func ok(context.Context) error { return nil }

func fail(context.Context) error { return errBoom }

func opts(mode txcore.Mode) txcore.Options {
	return txcore.Options{Mode: mode}
}

func TestWithTx_Root(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		fn      func(context.Context) error
		wantErr error
		want    []string
	}{
		{name: "commit", fn: ok, want: []string{"1:begin", "1:commit"}},
		{name: "rollback on error", fn: fail, wantErr: errBoom, want: []string{"1:begin", "1:rollback"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := newDriver()
			err := txcore.New(d).WithTx(t.Context(), opts(txcore.ModeJoin), tt.fn)

			require.ErrorIs(t, err, tt.wantErr)
			assert.Equal(t, tt.want, d.calls())
		})
	}
}

func TestWithTx_FnErrorReturnedAsIs(t *testing.T) {
	t.Parallel()

	err := txcore.New(newDriver()).WithTx(t.Context(), opts(txcore.ModeJoin), fail)

	assert.Same(t, errBoom, err)
}

func TestWithTx_PassesOptionsToBegin(t *testing.T) {
	t.Parallel()

	d := newDriver()
	o := txcore.Options{Mode: txcore.ModeJoin, Isolation: sql.LevelSerializable, ReadOnly: true}

	require.NoError(t, txcore.New(d).WithTx(t.Context(), o, ok))
	assert.Equal(t, []txcore.Options{o}, d.opts)
}

func TestWithTx_Errors(t *testing.T) {
	t.Parallel()

	t.Run("begin", func(t *testing.T) {
		t.Parallel()

		d := newDriver()
		d.failOn("begin", errBoom)

		called := false
		err := txcore.New(d).WithTx(t.Context(), opts(txcore.ModeJoin), func(context.Context) error {
			called = true

			return nil
		})

		require.ErrorIs(t, err, errBoom)
		assert.False(t, called)
	})

	t.Run("commit", func(t *testing.T) {
		t.Parallel()

		d := newDriver()
		d.failOn("commit", errBoom)
		c := txcore.New(d)

		ran := false
		err := c.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
			c.OnCommit(ctx, func() { ran = true })

			return nil
		})

		require.ErrorIs(t, err, errBoom)
		assert.False(t, ran, "callbacks must not run after a failed commit")
	})

	t.Run("rollback joined to fn error", func(t *testing.T) {
		t.Parallel()

		errRollback := errors.New("rollback failed")
		d := newDriver()
		d.failOn("rollback", errRollback)

		err := txcore.New(d).WithTx(t.Context(), opts(txcore.ModeJoin), fail)

		require.ErrorIs(t, err, errBoom)
		require.ErrorIs(t, err, errRollback)
	})

	t.Run("nil fn", func(t *testing.T) {
		t.Parallel()

		d := newDriver()

		require.Error(t, txcore.New(d).WithTx(t.Context(), opts(txcore.ModeJoin), nil))
		assert.Empty(t, d.calls())
	})

	t.Run("invalid mode", func(t *testing.T) {
		t.Parallel()

		d := newDriver()

		err := txcore.New(d).WithTx(t.Context(), opts(txcore.Mode(42)), ok)

		require.ErrorIs(t, err, txcore.ErrInvalidTxMode)
		assert.Empty(t, d.calls())
	})
}

func TestWithTx_RollbackDetachedFromCancel(t *testing.T) {
	t.Parallel()

	d := newDriver()
	c := txcore.New(d)
	ctx, cancel := context.WithCancel(t.Context())

	err := c.WithTx(ctx, opts(txcore.ModeJoin), func(ctx context.Context) error {
		innerErr := c.WithTx(ctx, opts(txcore.ModeNested), func(context.Context) error {
			cancel()

			return context.Canceled
		})
		require.ErrorIs(t, innerErr, context.Canceled)
		require.NoError(t, d.rollbackCtxErr, "ROLLBACK TO must not see the cancellation")

		return context.Canceled
	})

	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, d.rollbackCtxErr)
	assert.Equal(
		t,
		[]string{
			"1:begin",
			"1:SAVEPOINT sp_1",
			"1:ROLLBACK TO SAVEPOINT sp_1",
			"1:RELEASE SAVEPOINT sp_1",
			"1:rollback",
		},
		d.calls(),
	)
}

func TestWithTx_Panic(t *testing.T) {
	t.Parallel()

	for _, mode := range []txcore.Mode{txcore.ModeJoin, txcore.ModeNested, txcore.ModeRequiresNew} {
		t.Run(mode.String(), func(t *testing.T) {
			t.Parallel()

			d := newDriver()
			c := txcore.New(d)

			assert.PanicsWithValue(t, "inner", func() {
				_ = c.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
					return c.WithTx(ctx, opts(mode), func(context.Context) error { panic("inner") })
				})
			})

			want := map[txcore.Mode][]string{
				txcore.ModeJoin: {"1:begin", "1:rollback"},
				txcore.ModeNested: {
					"1:begin",
					"1:SAVEPOINT sp_1",
					"1:ROLLBACK TO SAVEPOINT sp_1",
					"1:RELEASE SAVEPOINT sp_1",
					"1:rollback",
				},
				txcore.ModeRequiresNew: {"1:begin", "2:begin", "2:rollback", "1:rollback"},
			}
			assert.Equal(t, want[mode], d.calls())
		})
	}
}

func TestWithTx_Goexit(t *testing.T) {
	t.Parallel()

	d := newDriver()
	c := txcore.New(d)
	done := make(chan struct{})

	go func() {
		defer close(done)

		_ = c.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
			return c.WithTx(ctx, opts(txcore.ModeNested), func(context.Context) error {
				runtime.Goexit()

				return nil
			})
		})
	}()

	<-done

	assert.Equal(
		t,
		[]string{
			"1:begin",
			"1:SAVEPOINT sp_1",
			"1:ROLLBACK TO SAVEPOINT sp_1",
			"1:RELEASE SAVEPOINT sp_1",
			"1:rollback",
		},
		d.calls(),
	)
}

func TestWithTx_Join(t *testing.T) {
	t.Parallel()

	t.Run("shares the transaction", func(t *testing.T) {
		t.Parallel()

		d := newDriver()
		c := txcore.New(d)

		err := c.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
			outer, _ := c.Tx(ctx)

			return c.WithTx(ctx, opts(txcore.ModeJoin), func(ctx context.Context) error {
				inner, _ := c.Tx(ctx)
				assert.Same(t, outer, inner)

				return nil
			})
		})

		require.NoError(t, err)
		assert.Equal(t, []string{"1:begin", "1:commit"}, d.calls())
	})

	t.Run("swallowed inner error poisons the transaction", func(t *testing.T) {
		t.Parallel()

		d := newDriver()
		c := txcore.New(d)

		err := c.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
			innerErr := c.WithTx(ctx, opts(txcore.ModeJoin), fail)
			assert.Same(t, errBoom, innerErr)

			return nil // handled, but the transaction is rollback-only
		})

		require.ErrorIs(t, err, txcore.ErrRollbackOnly)
		require.ErrorIs(t, err, errBoom)
		assert.Equal(t, []string{"1:begin", "1:rollback"}, d.calls())
	})

	t.Run("first cause is kept", func(t *testing.T) {
		t.Parallel()

		errOther := errors.New("other")
		c := txcore.New(newDriver())

		err := c.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
			_ = c.WithTx(ctx, opts(txcore.ModeJoin), fail)
			_ = c.WithTx(ctx, opts(txcore.ModeJoin), func(context.Context) error { return errOther })

			return nil
		})

		require.ErrorIs(t, err, errBoom)
		assert.NotErrorIs(t, err, errOther)
	})

	t.Run("fails fast in a rollback-only scope", func(t *testing.T) {
		t.Parallel()

		c := txcore.New(newDriver())

		err := c.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
			_ = c.WithTx(ctx, opts(txcore.ModeJoin), fail)

			called := false
			err := c.WithTx(ctx, opts(txcore.ModeJoin), func(context.Context) error {
				called = true

				return nil
			})

			require.ErrorIs(t, err, txcore.ErrRollbackOnly)
			require.ErrorIs(t, err, errBoom)
			assert.False(t, called)

			return nil
		})

		require.ErrorIs(t, err, txcore.ErrRollbackOnly)
	})

	t.Run("recovered panic poisons the transaction", func(t *testing.T) {
		t.Parallel()

		c := txcore.New(newDriver())

		err := c.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
			func() {
				defer func() { _ = recover() }()

				_ = c.WithTx(ctx, opts(txcore.ModeJoin), func(context.Context) error { panic("x") })
			}()

			return nil
		})

		require.ErrorIs(t, err, txcore.ErrRollbackOnly)
		require.ErrorIs(t, err, txcore.ErrAborted)
	})
}

func TestWithTx_Nested(t *testing.T) {
	t.Parallel()

	t.Run("release", func(t *testing.T) {
		t.Parallel()

		d := newDriver()
		c := txcore.New(d)

		err := c.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
			return c.WithTx(ctx, opts(txcore.ModeNested), ok)
		})

		require.NoError(t, err)
		assert.Equal(t, []string{"1:begin", "1:SAVEPOINT sp_1", "1:RELEASE SAVEPOINT sp_1", "1:commit"}, d.calls())
	})

	t.Run("error rolls back to the savepoint only", func(t *testing.T) {
		t.Parallel()

		d := newDriver()
		c := txcore.New(d)

		err := c.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
			assert.Same(t, errBoom, c.WithTx(ctx, opts(txcore.ModeNested), fail))

			return nil
		})

		require.NoError(t, err)
		assert.Equal(
			t,
			[]string{
				"1:begin",
				"1:SAVEPOINT sp_1",
				"1:ROLLBACK TO SAVEPOINT sp_1",
				"1:RELEASE SAVEPOINT sp_1",
				"1:commit",
			},
			d.calls(),
		)
	})

	t.Run("joined error poisons the savepoint, not the transaction", func(t *testing.T) {
		t.Parallel()

		d := newDriver()
		c := txcore.New(d)

		err := c.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
			err := c.WithTx(ctx, opts(txcore.ModeNested), func(ctx context.Context) error {
				_ = c.WithTx(ctx, opts(txcore.ModeJoin), fail)

				return nil
			})
			require.ErrorIs(t, err, txcore.ErrRollbackOnly)
			require.ErrorIs(t, err, errBoom)

			return nil
		})

		require.NoError(t, err)
		assert.Equal(
			t,
			[]string{
				"1:begin",
				"1:SAVEPOINT sp_1",
				"1:ROLLBACK TO SAVEPOINT sp_1",
				"1:RELEASE SAVEPOINT sp_1",
				"1:commit",
			},
			d.calls(),
		)
	})

	t.Run("savepoint names are a per-transaction counter", func(t *testing.T) {
		t.Parallel()

		d := newDriver()
		c := txcore.New(d)

		err := c.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
			require.NoError(t, c.WithTx(ctx, opts(txcore.ModeNested), func(ctx context.Context) error {
				return c.WithTx(ctx, opts(txcore.ModeNested), ok)
			}))

			return c.WithTx(ctx, opts(txcore.ModeNested), ok)
		})

		require.NoError(t, err)
		assert.Equal(t, []string{
			"1:begin",
			"1:SAVEPOINT sp_1", "1:SAVEPOINT sp_2", "1:RELEASE SAVEPOINT sp_2", "1:RELEASE SAVEPOINT sp_1",
			"1:SAVEPOINT sp_3", "1:RELEASE SAVEPOINT sp_3",
			"1:commit",
		}, d.calls())
	})

	t.Run("fails fast in a rollback-only scope", func(t *testing.T) {
		t.Parallel()

		d := newDriver()
		c := txcore.New(d)

		err := c.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
			_ = c.WithTx(ctx, opts(txcore.ModeJoin), fail)

			err := c.WithTx(ctx, opts(txcore.ModeNested), ok)
			require.ErrorIs(t, err, txcore.ErrRollbackOnly)

			return nil
		})

		require.ErrorIs(t, err, txcore.ErrRollbackOnly)
		assert.Equal(t, []string{"1:begin", "1:rollback"}, d.calls())
	})

	t.Run("failed savepoint", func(t *testing.T) {
		t.Parallel()

		d := newDriver()
		d.failOn("SAVEPOINT sp_1", errBoom)
		c := txcore.New(d)

		err := c.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
			return c.WithTx(ctx, opts(txcore.ModeNested), ok)
		})

		require.ErrorIs(t, err, errBoom)
		assert.Equal(t, []string{"1:begin", "1:SAVEPOINT sp_1", "1:rollback"}, d.calls())
	})

	t.Run("failed release poisons the parent", func(t *testing.T) {
		t.Parallel()

		d := newDriver()
		d.failOn("RELEASE SAVEPOINT sp_1", errBoom)
		c := txcore.New(d)

		err := c.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
			require.ErrorIs(t, c.WithTx(ctx, opts(txcore.ModeNested), ok), errBoom)

			return nil
		})

		require.ErrorIs(t, err, txcore.ErrRollbackOnly)
		require.ErrorIs(t, err, errBoom)
	})

	t.Run("failed rollback to savepoint poisons the parent", func(t *testing.T) {
		t.Parallel()

		errRollback := errors.New("rollback to failed")
		d := newDriver()
		d.failOn("ROLLBACK TO SAVEPOINT sp_1", errRollback)
		c := txcore.New(d)

		err := c.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
			err := c.WithTx(ctx, opts(txcore.ModeNested), fail)
			require.ErrorIs(t, err, errBoom)
			require.ErrorIs(t, err, errRollback)

			return nil
		})

		require.ErrorIs(t, err, txcore.ErrRollbackOnly)
		require.ErrorIs(t, err, errRollback)
	})
}

func TestWithTx_RequiresNew(t *testing.T) {
	t.Parallel()

	d := newDriver()
	c := txcore.New(d)

	err := c.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
		outer, _ := c.Tx(ctx)

		require.NoError(t, c.WithTx(ctx, opts(txcore.ModeRequiresNew), func(ctx context.Context) error {
			inner, _ := c.Tx(ctx)
			assert.NotSame(t, outer, inner)

			return nil
		}))

		after, _ := c.Tx(ctx)
		assert.Same(t, outer, after)

		return fail(ctx)
	})

	require.ErrorIs(t, err, errBoom)
	assert.Equal(t, []string{"1:begin", "2:begin", "2:commit", "1:rollback"}, d.calls())
}

func TestWithTx_Forbid(t *testing.T) {
	t.Parallel()

	d := newDriver()
	c := txcore.New(d)

	require.NoError(t, c.WithTx(t.Context(), opts(txcore.ModeForbid), func(ctx context.Context) error {
		require.ErrorIs(t, c.WithTx(ctx, opts(txcore.ModeForbid), ok), txcore.ErrNestedTx)

		return nil
	}))

	assert.Equal(t, []string{"1:begin", "1:commit"}, d.calls())
}

func TestWithTx_EveryModeBeginsOutsideTx(t *testing.T) {
	t.Parallel()

	for _, mode := range []txcore.Mode{txcore.ModeJoin, txcore.ModeNested, txcore.ModeRequiresNew, txcore.ModeForbid} {
		t.Run(mode.String(), func(t *testing.T) {
			t.Parallel()

			d := newDriver()

			require.NoError(t, txcore.New(d).WithTx(t.Context(), opts(mode), ok))
			assert.Equal(t, []string{"1:begin", "1:commit"}, d.calls())
		})
	}
}

func TestWithTx_OptionsCompatibility(t *testing.T) {
	t.Parallel()

	rw := func(l sql.IsolationLevel) txcore.Options {
		return txcore.Options{Mode: txcore.ModeJoin, Isolation: l}
	}
	ro := func(l sql.IsolationLevel) txcore.Options {
		return txcore.Options{Mode: txcore.ModeJoin, Isolation: l, ReadOnly: true}
	}

	tests := []struct {
		name         string
		outer, inner txcore.Options
		wantErr      bool
	}{
		{name: "equal", outer: rw(sql.LevelSerializable), inner: rw(sql.LevelSerializable)},
		{name: "weaker isolation", outer: rw(sql.LevelSerializable), inner: rw(sql.LevelReadCommitted)},
		{name: "default isolation", outer: rw(sql.LevelReadCommitted), inner: rw(sql.LevelDefault)},
		{
			name:  "stricter isolation",
			outer: rw(sql.LevelReadCommitted), inner: rw(sql.LevelSerializable), wantErr: true,
		},
		{
			name:  "explicit inside default",
			outer: rw(sql.LevelDefault), inner: rw(sql.LevelReadCommitted), wantErr: true,
		},
		{name: "read-only inside read-write", outer: rw(sql.LevelDefault), inner: ro(sql.LevelDefault)},
		{name: "read-only inside read-only", outer: ro(sql.LevelDefault), inner: ro(sql.LevelDefault)},
		{
			name:  "read-write inside read-only",
			outer: ro(sql.LevelDefault), inner: rw(sql.LevelDefault), wantErr: true,
		},
	}

	for _, tt := range tests {
		for _, mode := range []txcore.Mode{txcore.ModeJoin, txcore.ModeNested} {
			t.Run(tt.name+"/"+mode.String(), func(t *testing.T) {
				t.Parallel()

				c := txcore.New(newDriver())
				inner := tt.inner
				inner.Mode = mode

				err := c.WithTx(t.Context(), tt.outer, func(ctx context.Context) error {
					return c.WithTx(ctx, inner, ok)
				})

				if tt.wantErr {
					require.ErrorIs(t, err, txcore.ErrTxOptionsIncompatible)

					return
				}

				require.NoError(t, err)
			})
		}
	}
}

func TestCore_KeyPerDatabase(t *testing.T) {
	t.Parallel()

	da, db := newDriver(), newDriver()
	a, b := txcore.New(da), txcore.New(db)

	err := a.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
		_, inA := a.Tx(ctx)
		_, inB := b.Tx(ctx)

		assert.True(t, inA)
		assert.False(t, inB)

		return b.WithTx(ctx, opts(txcore.ModeForbid), ok) // not nested: another database
	})

	require.NoError(t, err)
	assert.Equal(t, []string{"1:begin", "1:commit"}, da.calls())
	assert.Equal(t, []string{"1:begin", "1:commit"}, db.calls())
}

func TestCore_FinishedTxLeavesContext(t *testing.T) {
	t.Parallel()

	d := newDriver()
	c := txcore.New(d)

	var leaked context.Context

	require.NoError(t, c.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
		leaked = ctx //nolint:fatcontext // the point of the test

		return nil
	}))

	_, inTx := c.Tx(leaked)

	assert.False(t, inTx)

	ran := false

	c.OnCommit(leaked, func() { ran = true })
	assert.True(t, ran)

	require.NoError(t, c.WithTx(leaked, opts(txcore.ModeForbid), ok))
	assert.Equal(t, []string{"1:begin", "1:commit", "2:begin", "2:commit"}, d.calls())
}

func TestCore_LeakedSavepointContextJoinsParent(t *testing.T) {
	t.Parallel()

	d := newDriver()
	c := txcore.New(d)

	//nolint:contextcheck // the body deliberately continues on a leaked savepoint context
	require.NoError(t, c.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
		var leaked context.Context

		require.NoError(t, c.WithTx(ctx, opts(txcore.ModeNested), func(ctx context.Context) error {
			leaked = ctx //nolint:fatcontext // the point of the test

			return nil
		}))

		return c.WithTx(leaked, opts(txcore.ModeNested), ok)
	}))

	assert.Equal(t, []string{
		"1:begin",
		"1:SAVEPOINT sp_1",
		"1:RELEASE SAVEPOINT sp_1",
		"1:SAVEPOINT sp_2",
		"1:RELEASE SAVEPOINT sp_2",
		"1:commit",
	}, d.calls())
}

func TestOnCommit(t *testing.T) {
	t.Parallel()

	type step func(c *txcore.Core[*fakeTx], ctx context.Context, record func(string)) error

	tests := []struct {
		name string
		body step
		want []string
	}{
		{
			name: "after commit in order",
			body: func(c *txcore.Core[*fakeTx], ctx context.Context, record func(string)) error {
				c.OnCommit(ctx, func() { record("a") })
				c.OnCommit(ctx, func() { record("b") })
				record("body")

				return nil
			},
			want: []string{"body", "a", "b"},
		},
		{
			name: "dropped on rollback",
			body: func(c *txcore.Core[*fakeTx], ctx context.Context, record func(string)) error {
				c.OnCommit(ctx, func() { record("a") })

				return errBoom
			},
			want: nil,
		},
		{
			name: "joined call registers on the transaction",
			body: func(c *txcore.Core[*fakeTx], ctx context.Context, record func(string)) error {
				return c.WithTx(ctx, opts(txcore.ModeJoin), func(ctx context.Context) error {
					c.OnCommit(ctx, func() { record("a") })

					return nil
				})
			},
			want: []string{"a"},
		},
		{
			name: "released savepoint keeps callbacks",
			body: func(c *txcore.Core[*fakeTx], ctx context.Context, record func(string)) error {
				return c.WithTx(ctx, opts(txcore.ModeNested), func(ctx context.Context) error {
					c.OnCommit(ctx, func() { record("a") })

					return nil
				})
			},
			want: []string{"a"},
		},
		{
			name: "rolled back savepoint drops callbacks",
			body: func(c *txcore.Core[*fakeTx], ctx context.Context, record func(string)) error {
				err := c.WithTx(ctx, opts(txcore.ModeNested), func(ctx context.Context) error {
					c.OnCommit(ctx, func() { record("dropped") })

					inner := c.WithTx(ctx, opts(txcore.ModeNested), func(ctx context.Context) error {
						c.OnCommit(ctx, func() { record("dropped too") })

						return nil
					})

					return errors.Join(inner, errBoom)
				})
				if !errors.Is(err, errBoom) {
					return err
				}

				c.OnCommit(ctx, func() { record("kept") })

				return nil
			},
			want: []string{"kept"},
		},
		{
			name: "requires-new runs after its own commit",
			body: func(c *txcore.Core[*fakeTx], ctx context.Context, record func(string)) error {
				c.OnCommit(ctx, func() { record("outer") })

				err := c.WithTx(ctx, opts(txcore.ModeRequiresNew), func(ctx context.Context) error {
					c.OnCommit(ctx, func() { record("inner") })

					return nil
				})

				record("after inner")

				return err
			},
			want: []string{"inner", "after inner", "outer"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := txcore.New(newDriver())

			var got []string

			record := func(s string) { got = append(got, s) }

			_ = c.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
				return tt.body(c, ctx, record)
			})

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestOnCommit_OutsideTxRunsAtOnce(t *testing.T) {
	t.Parallel()

	c := txcore.New(newDriver())
	ran := false

	c.OnCommit(t.Context(), func() { ran = true })
	c.OnCommit(t.Context(), nil)

	assert.True(t, ran)
}

func TestMode_String(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "join", txcore.ModeJoin.String())
	assert.Equal(t, "nested", txcore.ModeNested.String())
	assert.Equal(t, "requires-new", txcore.ModeRequiresNew.String())
	assert.Equal(t, "forbid", txcore.ModeForbid.String())
	assert.Equal(t, "mode(9)", txcore.Mode(9).String())
}

func TestWithTx_SwallowedSavepointFailurePoisons(t *testing.T) {
	t.Parallel()

	d := newDriver()
	d.failOn("SAVEPOINT sp_1", errBoom)
	c := txcore.New(d)

	err := c.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
		_ = c.WithTx(ctx, opts(txcore.ModeNested), ok) // "try it, never mind if it fails"

		return nil
	})

	require.ErrorIs(t, err, txcore.ErrRollbackOnly)
	require.ErrorIs(t, err, errBoom)
	assert.Equal(t, []string{"1:begin", "1:SAVEPOINT sp_1", "1:rollback"}, d.calls())
}

func TestWithTx_PanickingJoinInSavepointMarksOnlyTheSavepoint(t *testing.T) {
	t.Parallel()

	d := newDriver()
	c := txcore.New(d)

	err := c.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
		err := c.WithTx(ctx, opts(txcore.ModeNested), func(ctx context.Context) error {
			func() {
				defer func() { _ = recover() }()

				_ = c.WithTx(ctx, opts(txcore.ModeJoin), func(context.Context) error { panic("x") })
			}()

			return nil
		})
		require.ErrorIs(t, err, txcore.ErrRollbackOnly)
		require.ErrorIs(t, err, txcore.ErrAborted)

		return nil
	})

	require.NoError(t, err)
	assert.Equal(
		t,
		[]string{"1:begin", "1:SAVEPOINT sp_1", "1:ROLLBACK TO SAVEPOINT sp_1", "1:RELEASE SAVEPOINT sp_1", "1:commit"},
		d.calls(),
	)
}

func TestOnCommit_PanicRunsTheRestThenPropagates(t *testing.T) {
	t.Parallel()

	c := txcore.New(newDriver())

	var ran []string

	assert.PanicsWithValue(t, "first", func() {
		_ = c.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
			c.OnCommit(ctx, func() { panic("first") })
			c.OnCommit(ctx, func() { ran = append(ran, "outbox") })
			c.OnCommit(ctx, func() { panic("second") })
			c.OnCommit(ctx, func() { ran = append(ran, "cache") })

			return nil
		})
	})

	assert.Equal(t, []string{"outbox", "cache"}, ran)
}

func TestWithTx_ConcurrentJoinedCalls(t *testing.T) {
	t.Parallel()

	const workers = 20

	d := newDriver()
	c := txcore.New(d)

	var (
		mu  sync.Mutex
		ran int
	)

	err := c.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
		var wg sync.WaitGroup

		for i := range workers {
			wg.Go(func() {
				_ = c.WithTx(ctx, opts(txcore.ModeJoin), func(ctx context.Context) error {
					c.OnCommit(ctx, func() {
						mu.Lock()
						ran++
						mu.Unlock()
					})

					if i == workers/2 {
						return errBoom
					}

					return nil
				})
			})
		}

		wg.Wait()

		return nil
	})

	require.ErrorIs(t, err, txcore.ErrRollbackOnly)
	require.ErrorIs(t, err, errBoom)
	assert.Zero(t, ran, "a rolled back transaction runs no callback")
	assert.Equal(t, []string{"1:begin", "1:rollback"}, d.calls())
}

func TestOnCommit_RegistrationOrderAcrossScopes(t *testing.T) {
	t.Parallel()

	c := txcore.New(newDriver())

	var got []string

	record := func(s string) func() { return func() { got = append(got, s) } }

	require.NoError(t, c.WithTx(t.Context(), opts(txcore.ModeJoin), func(outer context.Context) error {
		c.OnCommit(outer, record("a"))

		return c.WithTx(outer, opts(txcore.ModeNested), func(inner context.Context) error {
			c.OnCommit(inner, record("b"))
			c.OnCommit(outer, record("c")) // the parent's ctx while the savepoint is open
			c.OnCommit(inner, record("d"))

			return nil
		})
	}))

	assert.Equal(t, []string{"a", "b", "c", "d"}, got)
}

func TestWithTx_FailedReleaseAfterRollbackToPoisons(t *testing.T) {
	t.Parallel()

	d := newDriver()
	d.failOn("RELEASE SAVEPOINT sp_1", errBoom)
	c := txcore.New(d)

	err := c.WithTx(t.Context(), opts(txcore.ModeJoin), func(ctx context.Context) error {
		err := c.WithTx(ctx, opts(txcore.ModeNested), fail)
		require.ErrorIs(t, err, errBoom)

		return nil
	})

	require.ErrorIs(t, err, txcore.ErrRollbackOnly)
}
