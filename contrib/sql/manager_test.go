package sql_test

import (
	"context"
	stdsql "database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/sql"
)

var errDomain = errors.New("domain")

func exec(ctx context.Context, m sql.Manager, query string) error {
	_, err := sql.Exec(ctx, m.GetQuerier(ctx), sql.Raw(query))

	return err
}

func TestManager_WithTx(t *testing.T) {
	t.Parallel()

	t.Run("commit", func(t *testing.T) {
		t.Parallel()

		db, mock := newMock(t)
		m := newManager(t, db)

		mock.ExpectBegin()
		mock.ExpectExec("UPDATE t SET a = 1").WillReturnResult(result(1))
		mock.ExpectCommit()

		err := m.WithTx(t.Context(), func(ctx context.Context) error {
			assert.True(t, m.HasTx(ctx))

			tx, ok := sql.Tx(ctx, db)
			assert.True(t, ok)
			assert.NotNil(t, tx)

			return exec(ctx, m, "UPDATE t SET a = 1")
		})

		require.NoError(t, err)
		assert.False(t, m.HasTx(t.Context()))
	})

	t.Run("rollback on error", func(t *testing.T) {
		t.Parallel()

		db, mock := newMock(t)
		m := newManager(t, db)

		mock.ExpectBegin()
		mock.ExpectRollback()

		err := m.WithTx(t.Context(), func(context.Context) error { return errDomain })

		assert.Same(t, errDomain, err)
	})

	t.Run("read-only with isolation", func(t *testing.T) {
		t.Parallel()

		db, mock := newMock(t)
		m := newManager(t, db)

		mock.ExpectBegin()
		mock.ExpectCommit()

		err := m.WithTx(t.Context(), func(context.Context) error { return nil },
			sql.ReadOnly(), sql.WithIsolation(stdsql.LevelSerializable))

		require.NoError(t, err)
	})

	t.Run("cancelled context returns its error only", func(t *testing.T) {
		t.Parallel()

		db, mock := newMock(t)
		m := newManager(t, db)
		ctx, cancel := context.WithCancel(t.Context())

		mock.ExpectBegin()
		mock.ExpectRollback()

		err := m.WithTx(ctx, func(ctx context.Context) error {
			cancel()
			<-ctx.Done()

			return ctx.Err()
		})

		assert.Same(t, context.Canceled, err, "database/sql's own rollback is not an error")

		// database/sql rolls back a cancelled transaction from its own
		// goroutine, which may finish after WithTx returned.
		assert.Eventually(t, func() bool { return mock.ExpectationsWereMet() == nil }, 5*time.Second, time.Millisecond)
	})
}

func TestManager_Nested(t *testing.T) {
	t.Parallel()

	t.Run("savepoint rolled back, transaction committed", func(t *testing.T) {
		t.Parallel()

		db, mock := newMock(t)
		m := newManager(t, db)

		mock.ExpectBegin()
		mock.ExpectExec("SAVEPOINT sp_1").WillReturnResult(result(0))
		mock.ExpectExec("INSERT INTO log VALUES (1)").WillReturnResult(result(1))
		mock.ExpectExec("ROLLBACK TO SAVEPOINT sp_1").WillReturnResult(result(0))
		mock.ExpectExec("RELEASE SAVEPOINT sp_1").WillReturnResult(result(0))
		mock.ExpectCommit()

		err := m.WithTx(t.Context(), func(ctx context.Context) error {
			err := m.WithTx(ctx, func(ctx context.Context) error {
				require.NoError(t, exec(ctx, m, "INSERT INTO log VALUES (1)"))

				return errDomain
			}, sql.WithTxMode(sql.TxModeNested))
			assert.ErrorIs(t, err, errDomain)

			return nil
		})

		require.NoError(t, err)
	})

	t.Run("joined error poisons the transaction", func(t *testing.T) {
		t.Parallel()

		db, mock := newMock(t)
		m := newManager(t, db)

		mock.ExpectBegin()
		mock.ExpectRollback()

		err := m.WithTx(t.Context(), func(ctx context.Context) error {
			_ = m.WithTx(ctx, func(context.Context) error { return errDomain })

			return nil
		})

		require.ErrorIs(t, err, sql.ErrRollbackOnly)
		require.ErrorIs(t, err, errDomain)
	})
}

func TestManager_DefaultTxMode(t *testing.T) {
	t.Parallel()

	db, mock := newMock(t)
	m := newManager(t, db, sql.WithDefaultTxMode(sql.TxModeForbid), sql.ManagerOption{})

	mock.ExpectBegin()
	mock.ExpectCommit()

	err := m.WithTx(t.Context(), func(ctx context.Context) error {
		require.ErrorIs(t, m.WithTx(ctx, func(context.Context) error { return nil }), sql.ErrNestedTx)

		return m.WithTx(ctx, func(context.Context) error { return nil }, sql.WithTxMode(sql.TxModeJoin), sql.TxOption{})
	})

	require.NoError(t, err)
}

func TestManager_InvalidTxMode(t *testing.T) {
	t.Parallel()

	db, _ := newMock(t)

	err := newManager(t, db).WithTx(t.Context(), func(context.Context) error { return nil }, sql.WithTxMode(42))

	require.ErrorIs(t, err, sql.ErrInvalidTxMode)
}

func TestManager_OnCommit(t *testing.T) {
	t.Parallel()

	db, mock := newMock(t)
	m := newManager(t, db)

	mock.ExpectBegin()
	mock.ExpectCommit()

	var events []string

	err := m.WithTx(t.Context(), func(ctx context.Context) error {
		m.OnCommit(ctx, func() { events = append(events, "published") })

		events = append(events, "body")

		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, []string{"body", "published"}, events)
}

func TestManager_SharedPerDB(t *testing.T) {
	t.Parallel()

	db1, mock1 := newMock(t)
	db2, mock2 := newMock(t)
	m1, m1b, m2 := newManager(t, db1), newManager(t, db1), newManager(t, db2)

	mock1.ExpectBegin()
	mock1.ExpectCommit()
	mock2.ExpectExec("UPDATE other SET a = 1").WillReturnResult(result(1)) // outside a transaction

	err := m1.WithTx(t.Context(), func(ctx context.Context) error {
		assert.True(t, m1b.HasTx(ctx), "managers over one DB share its transactions")
		assert.False(t, m2.HasTx(ctx), "transactions of another DB do not leak")

		return exec(ctx, m2, "UPDATE other SET a = 1")
	})

	require.NoError(t, err)
}

func TestNewManager_NilDB(t *testing.T) {
	t.Parallel()

	_, err := sql.NewManager(nil)

	require.ErrorIs(t, err, sql.ErrInvalidConfig)
}

// newManager returns a Manager over db; it fails the test on an error.
func newManager(t *testing.T, db *sql.DB, opts ...sql.ManagerOption) sql.Manager {
	t.Helper()

	m, err := sql.NewManager(db, opts...)
	require.NoError(t, err)

	return m
}

func TestTxMode_String(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "join", sql.TxModeJoin.String())
	assert.Equal(t, "nested", sql.TxModeNested.String())
	assert.Equal(t, "requires-new", sql.TxModeRequiresNew.String())
	assert.Equal(t, "forbid", sql.TxModeForbid.String())
}
