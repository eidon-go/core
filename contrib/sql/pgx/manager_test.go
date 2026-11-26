package pgx_test

import (
	"context"
	stdsql "database/sql"
	"errors"
	"testing"

	pgxv5 "github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/sql"
	corepgx "github.com/eidon-go/core/contrib/sql/pgx"
)

var errDomain = errors.New("domain")

func TestManager_WithTx(t *testing.T) {
	t.Parallel()

	t.Run("commit, sqlc-style code on the transaction", func(t *testing.T) {
		t.Parallel()

		db, mock := newMock(t)
		m := newManager(t, db)

		mock.ExpectBegin()
		mock.ExpectExec("UPDATE t SET a = 1").WillReturnResult(pgxmock.NewResult("UPDATE", 1))
		mock.ExpectCommit()

		err := m.WithTx(t.Context(), func(ctx context.Context) error {
			assert.True(t, m.HasTx(ctx))

			tx, ok := corepgx.Tx(ctx, db)
			assert.True(t, ok)
			assert.NotNil(t, tx)

			_, err := m.GetQuerier(ctx).Exec(ctx, "UPDATE t SET a = 1") // native pgx

			return err
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

	t.Run("nested savepoint", func(t *testing.T) {
		t.Parallel()

		db, mock := newMock(t)
		m := newManager(t, db)

		mock.ExpectBegin()
		mock.ExpectExec("SAVEPOINT sp_1").WillReturnResult(pgxmock.NewResult("SAVEPOINT", 0))
		mock.ExpectExec("ROLLBACK TO SAVEPOINT sp_1").WillReturnResult(pgxmock.NewResult("ROLLBACK", 0))
		mock.ExpectExec("RELEASE SAVEPOINT sp_1").WillReturnResult(pgxmock.NewResult("RELEASE", 0))
		mock.ExpectCommit()

		err := m.WithTx(t.Context(), func(ctx context.Context) error {
			err := m.WithTx(ctx, func(context.Context) error { return errDomain }, sql.WithTxMode(sql.TxModeNested))
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

func TestManager_TxOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		level stdsql.IsolationLevel
		want  pgxv5.TxIsoLevel
	}{
		{level: stdsql.LevelDefault, want: ""},
		{level: stdsql.LevelReadUncommitted, want: pgxv5.ReadUncommitted},
		{level: stdsql.LevelReadCommitted, want: pgxv5.ReadCommitted},
		{level: stdsql.LevelRepeatableRead, want: pgxv5.RepeatableRead},
		{level: stdsql.LevelSerializable, want: pgxv5.Serializable},
	}

	for _, tt := range tests {
		t.Run(tt.level.String(), func(t *testing.T) {
			t.Parallel()

			db, mock := newMock(t)

			mock.ExpectBeginTx(pgxv5.TxOptions{IsoLevel: tt.want, AccessMode: pgxv5.ReadOnly})
			mock.ExpectCommit()

			err := newManager(t, db).WithTx(t.Context(), func(context.Context) error { return nil },
				sql.WithIsolation(tt.level), sql.ReadOnly(), sql.TxOption{})

			require.NoError(t, err)
		})
	}

	t.Run("unsupported level", func(t *testing.T) {
		t.Parallel()

		db, _ := newMock(t)

		err := newManager(t, db).WithTx(t.Context(), func(context.Context) error { return nil },
			sql.WithIsolation(stdsql.LevelSnapshot))

		require.ErrorIs(t, err, corepgx.ErrUnsupportedIsolation)
	})
}

func TestManager_DefaultTxModeAndOnCommit(t *testing.T) {
	t.Parallel()

	db, mock := newMock(t)
	m := newManager(t, db, sql.WithDefaultTxMode(sql.TxModeForbid), sql.ManagerOption{})

	mock.ExpectBegin()
	mock.ExpectRollback()

	committed := false

	err := m.WithTx(t.Context(), func(ctx context.Context) error {
		m.OnCommit(ctx, func() { committed = true })

		return m.WithTx(ctx, func(context.Context) error { return nil })
	})

	require.ErrorIs(t, err, sql.ErrNestedTx)
	assert.False(t, committed, "the forbidden call failed the transaction")
}

func TestManager_SharedPerDB(t *testing.T) {
	t.Parallel()

	db1, mock1 := newMock(t)
	db2, mock2 := newMock(t)
	m1, m1b, m2 := newManager(t, db1), newManager(t, db1), newManager(t, db2)

	mock1.ExpectBegin()
	mock1.ExpectCommit()
	mock2.ExpectExec("UPDATE other SET a = 1").WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	err := m1.WithTx(t.Context(), func(ctx context.Context) error {
		assert.True(t, m1b.HasTx(ctx))
		assert.False(t, m2.HasTx(ctx))

		_, err := sql.Exec(ctx, m2.GetQuerier(ctx), sql.Raw("UPDATE other SET a = 1"))

		return err
	})

	require.NoError(t, err)
}

// transactor is what domain code depends on: both worlds implement it.
var _ sql.Transactor = corepgx.Manager(nil)

func TestNewManager_NilDB(t *testing.T) {
	t.Parallel()

	_, err := corepgx.NewManager(nil)

	require.ErrorIs(t, err, sql.ErrInvalidConfig)
}

// newManager returns a Manager over db; it fails the test on an error.
func newManager(t *testing.T, db *corepgx.DB, opts ...sql.ManagerOption) corepgx.Manager {
	t.Helper()

	m, err := corepgx.NewManager(db, opts...)
	require.NoError(t, err)

	return m
}
