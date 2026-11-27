package sqlx_test

import (
	"context"
	stdsql "database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/sql"
	coresqlx "github.com/eidon-go/core/contrib/sqlx"
)

// The test types have no tags on purpose: sqlx's lower-case mapping is under test.
type user struct {
	ID       int64
	Name     string
	Nickname stdsql.NullString `db:"nick"`
}

type base struct {
	ID int64
}

type withBase struct {
	base

	Name string
}

func newDB(t *testing.T, opts ...sql.Option) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()

	sdb, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)

	db, err := sql.NewFromDB(sdb, append([]sql.Option{sql.WithDriverName("postgres")}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Shutdown(context.Background())

		assert.NoError(t, mock.ExpectationsWereMet())
	})

	return db, mock
}

func newManager(t *testing.T, db *sql.DB, opts ...coresqlx.Option) coresqlx.Manager {
	t.Helper()

	m, err := coresqlx.NewManager(db, opts...)
	require.NoError(t, err)

	return m
}

func TestNewManager_Errors(t *testing.T) {
	t.Parallel()

	_, err := coresqlx.NewManager(nil)
	require.ErrorIs(t, err, coresqlx.ErrNilDB)

	sdb, _, err := sqlmock.New()
	require.NoError(t, err)

	db, err := sql.NewFromDB(sdb)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Shutdown(context.Background()) })

	_, err = coresqlx.NewManager(db)
	require.ErrorIs(t, err, coresqlx.ErrUnknownDriver, "NewFromDB without WithDriverName")

	m, err := coresqlx.NewManager(db, nil)
	require.ErrorIs(t, err, coresqlx.ErrUnknownDriver)
	assert.Nil(t, m)
}

func TestQuerier_Dialect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		driver string
		want   string
	}{
		{driver: "postgres", want: "SELECT id FROM users WHERE id = $1 AND name = $2"},
		{driver: "mysql", want: "SELECT id FROM users WHERE id = ? AND name = ?"},
		{driver: "sqlserver", want: "SELECT id FROM users WHERE id = @p1 AND name = @p2"},
	}

	for _, tt := range tests {
		t.Run(tt.driver, func(t *testing.T) {
			t.Parallel()

			db, _ := newDB(t, sql.WithDriverName(tt.driver))
			q := newManager(t, db).GetQuerier(t.Context())

			assert.Equal(t, tt.driver, q.DriverName())
			assert.Equal(t, tt.want, q.Rebind("SELECT id FROM users WHERE id = ? AND name = ?"))

			query, args, err := q.BindNamed("SELECT id FROM users WHERE id = :id AND name = :name",
				map[string]any{"id": 1, "name": "ann"})
			require.NoError(t, err)
			assert.Equal(t, tt.want, query)
			assert.Equal(t, []any{1, "ann"}, args)
		})
	}
}

func TestQuerier_Get(t *testing.T) {
	t.Parallel()

	db, mock := newDB(t)
	q := newManager(t, db).GetQuerier(t.Context())

	const query = "SELECT id, name, nick FROM users WHERE id = $1"

	columns := []string{"id", "name", "nick"}

	mock.ExpectQuery(query).WithArgs(1).WillReturnRows(sqlmock.NewRows(columns).AddRow(1, "ann", nil))

	got, err := sql.Get[user](t.Context(), q, sql.Raw(query, 1))
	require.NoError(t, err)
	assert.Equal(t, user{ID: 1, Name: "ann"}, got, "untagged fields map to lower case")

	mock.ExpectQuery(query).WithArgs(2).WillReturnRows(sqlmock.NewRows(columns))

	_, err = sql.Get[user](t.Context(), q, sql.Raw(query, 2))
	require.ErrorIs(t, err, sql.ErrNoRows)

	twoRows := func() *sqlmock.Rows {
		return sqlmock.NewRows(columns).AddRow(1, "ann", "a").AddRow(2, "bob", "b")
	}

	mock.ExpectQuery(query).WithArgs(3).WillReturnRows(twoRows())

	_, err = sql.Get[user](t.Context(), q, sql.Raw(query, 3))
	require.ErrorIs(t, err, sql.ErrTooManyRows, "the method keeps the contract of sql.Querier")

	mock.ExpectQuery(query).WithArgs(3).WillReturnRows(twoRows())

	var first user

	err = sqlx.GetContext(t.Context(), q, &first, query, 3) //nolint:musttag // untagged on purpose
	require.NoError(t, err, "sqlx's function takes the first row")
	assert.Equal(t, user{ID: 1, Name: "ann", Nickname: stdsql.NullString{String: "a", Valid: true}}, first)

	mock.ExpectQuery("SELECT count(*) FROM users").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(7))

	n, err := sql.Get[int](t.Context(), q, sql.Raw("SELECT count(*) FROM users"))
	require.NoError(t, err)
	assert.Equal(t, 7, n, "scalars scan directly")

	mock.ExpectQuery("SELECT id, name FROM users").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(5, "eve"))

	embedded, err := sql.Get[withBase](t.Context(), q, sql.Raw("SELECT id, name FROM users"))
	require.NoError(t, err)
	assert.Equal(t, withBase{base: base{ID: 5}, Name: "eve"}, embedded, "embedded structs are flattened")

	mock.ExpectQuery("SELECT id, extra FROM users").
		WillReturnRows(sqlmock.NewRows([]string{"id", "extra"}).AddRow(5, "x"))

	_, err = sql.Get[user](t.Context(), q, sql.Raw("SELECT id, extra FROM users"))
	require.ErrorContains(t, err, "missing destination name extra")

	queryErr := errors.New("boom")
	mock.ExpectQuery("SELECT 1").WillReturnError(queryErr)

	_, err = sql.Get[int](t.Context(), q, sql.Raw("SELECT 1"))
	require.ErrorIs(t, err, queryErr)
}

func TestQuerier_Select(t *testing.T) {
	t.Parallel()

	db, mock := newDB(t)
	q := newManager(t, db).GetQuerier(t.Context())

	mock.ExpectQuery("SELECT id, name, nick FROM users").WillReturnRows(
		sqlmock.NewRows([]string{"id", "name", "nick"}).AddRow(1, "ann", "a").AddRow(2, "bob", nil))

	users, err := sql.Select[user](t.Context(), q, sql.Raw("SELECT id, name, nick FROM users"))
	require.NoError(t, err)
	assert.Equal(t, []user{
		{ID: 1, Name: "ann", Nickname: stdsql.NullString{String: "a", Valid: true}},
		{ID: 2, Name: "bob"},
	}, users)

	idRows := func() *sqlmock.Rows { return sqlmock.NewRows([]string{"id"}) }

	mock.ExpectQuery("SELECT id FROM users").WillReturnRows(idRows().AddRow(1).AddRow(2))

	ids, err := sql.Select[int64](t.Context(), q, sql.Raw("SELECT id FROM users"))
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 2}, ids)

	mock.ExpectQuery("SELECT id FROM users").WillReturnRows(idRows())

	ids, err = sql.Select[int64](t.Context(), q, sql.Raw("SELECT id FROM users"))
	require.NoError(t, err)
	assert.Empty(t, ids)

	mock.ExpectQuery("SELECT id FROM users").WillReturnRows(idRows().AddRow(3))

	var viaSqlx []int64
	require.NoError(t, sqlx.SelectContext(t.Context(), q, &viaSqlx, "SELECT id FROM users"))
	assert.Equal(t, []int64{3}, viaSqlx)

	queryErr := errors.New("boom")
	mock.ExpectQuery("SELECT id FROM users").WillReturnError(queryErr)

	_, err = sql.Select[int64](t.Context(), q, sql.Raw("SELECT id FROM users"))
	require.ErrorIs(t, err, queryErr)
}

func TestQuerier_Named(t *testing.T) {
	t.Parallel()

	db, mock := newDB(t)
	q := newManager(t, db).GetQuerier(t.Context())

	const insert = "INSERT INTO users (id, name) VALUES (:id, :name)"

	mock.ExpectExec("INSERT INTO users (id, name) VALUES ($1, $2)").WithArgs(1, "ann").
		WillReturnResult(sqlmock.NewResult(0, 1))

	res, err := q.NamedExecContext(t.Context(), insert, user{ID: 1, Name: "ann"})
	require.NoError(t, err)

	affected, err := res.RowsAffected()
	require.NoError(t, err)
	assert.EqualValues(t, 1, affected)

	mock.ExpectExec("INSERT INTO users (id, name) VALUES ($1, $2),($3, $4)").
		WithArgs(1, "ann", 2, "bob").WillReturnResult(sqlmock.NewResult(0, 2))

	_, err = q.NamedExecContext(t.Context(), insert, []user{{ID: 1, Name: "ann"}, {ID: 2, Name: "bob"}})
	require.NoError(t, err, "a slice is a batch insert")

	mock.ExpectQuery("SELECT id, name FROM users WHERE name = $1").WithArgs("ann").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "ann"))

	rows, err := q.NamedQueryContext(t.Context(), "SELECT id, name FROM users WHERE name = :name",
		map[string]any{"name": "ann"})
	require.NoError(t, err)

	defer func() { _ = rows.Close() }()

	var got []withBase
	require.NoError(t, sqlx.StructScan(rows, &got)) //nolint:musttag // untagged on purpose
	assert.Equal(t, []withBase{{base: base{ID: 1}, Name: "ann"}}, got)

	_, err = q.NamedExecContext(t.Context(), "UPDATE users SET name = :name", map[string]any{})
	require.ErrorContains(t, err, "could not find name")

	_, err = q.NamedQueryContext(t.Context(), "SELECT :name", map[string]any{}) //nolint:sqlclosecheck // error
	require.ErrorContains(t, err, "could not find name")
}

func TestManager_WithTx(t *testing.T) {
	t.Parallel()

	db, mock := newDB(t)
	m := newManager(t, db)

	plain, err := sql.NewManager(db)
	require.NoError(t, err)

	assert.False(t, m.HasTx(t.Context()))

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE users SET name = $1 WHERE id = $2").WithArgs("ann", 1).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT name FROM users WHERE id = $1").WithArgs(1).
		WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("ann"))
	mock.ExpectQuery("SELECT id, name FROM users").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "ann"))
	mock.ExpectCommit()

	committed := false

	err = plain.WithTx(t.Context(), func(ctx context.Context) error {
		assert.True(t, m.HasTx(ctx), "the transaction of sql.Manager is the one of the DB")

		q := m.GetQuerier(ctx)

		_, err := q.ExecContext(ctx, q.Rebind("UPDATE users SET name = ? WHERE id = ?"), "ann", 1)
		if err != nil {
			return err
		}

		var name string
		if err := q.QueryRowxContext(ctx, "SELECT name FROM users WHERE id = $1", 1).Scan(&name); err != nil {
			return err
		}

		assert.Equal(t, "ann", name)

		rows, err := q.QueryxContext(ctx, "SELECT id, name FROM users")
		if err != nil {
			return err
		}

		defer func() { _ = rows.Close() }()

		var got []withBase
		if err := sqlx.StructScan(rows, &got); err != nil { //nolint:musttag // untagged on purpose
			return err
		}

		assert.Equal(t, []withBase{{base: base{ID: 1}, Name: "ann"}}, got, "the mapper inside a transaction")

		m.OnCommit(ctx, func() { committed = true })

		return nil
	})
	require.NoError(t, err)
	assert.True(t, committed)

	mock.ExpectBegin()
	mock.ExpectRollback()

	fnErr := errors.New("fn failed")
	require.ErrorIs(t, m.WithTx(t.Context(), func(context.Context) error { return fnErr }), fnErr)
}

func TestManager_Options(t *testing.T) {
	t.Parallel()

	db, mock := newDB(t)
	m := newManager(t, db, coresqlx.WithDefaultTxMode(sql.TxModeForbid),
		coresqlx.WithMapper(reflectx.NewMapperFunc("json", strings.ToUpper)))

	const query = `SELECT 1 AS "ID", 'x' AS "NAME"`

	mock.ExpectBegin()
	mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"ID", "NAME"}).AddRow(1, "x"))
	mock.ExpectRollback()

	err := m.WithTx(t.Context(), func(ctx context.Context) error {
		got, err := sql.Get[withBase](ctx, m.GetQuerier(ctx), sql.Raw(query))
		require.NoError(t, err)
		assert.Equal(t, withBase{base: base{ID: 1}, Name: "x"}, got)

		return m.WithTx(ctx, func(context.Context) error { return nil })
	})
	require.ErrorIs(t, err, sql.ErrNestedTx)
}
