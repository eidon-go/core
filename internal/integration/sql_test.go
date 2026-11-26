package integration_test

import (
	"context"
	stdsql "database/sql"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql" // database/sql driver "mysql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/sql"
	corepgx "github.com/eidon-go/core/contrib/sql/pgx"
	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
)

var errSQLDomain = errors.New("domain")

// world is one way of reaching a database through contrib/sql: package sql
// over pgx's stdlib driver or MySQL's driver, or package sql/pgx.
type world struct {
	name     string
	postgres bool
	format   sql.PlaceholderFormat
	tx       sql.Transactor
	querier  func(ctx context.Context) sql.Querier
	// count runs SELECT count(*) through the world's own DBTX of ctx, as
	// sqlc-generated code does.
	count func(ctx context.Context, table string) (int, error)
}

// q renders a query written with "?" placeholders in the world's format,
// the way a squirrel builder does.
func (w world) q(query string, args ...any) sql.Builder {
	return builder{format: w.format, query: query, args: args}
}

type builder struct {
	format sql.PlaceholderFormat
	query  string
	args   []any
}

func (b builder) ToSql() (string, []any, error) { //nolint:revive // sql.Builder
	query, err := b.format.ReplacePlaceholders(b.query)

	return query, b.args, err
}

var tableSeq atomic.Int64 //nolint:gochecknoglobals // unique table names across parallel tests

// table creates a table with the given columns, dropped after the test.
func (w world) table(t *testing.T, columns string) string {
	t.Helper()

	name := "t" + strconv.FormatInt(tableSeq.Add(1), 10)
	ctx := t.Context()

	_, err := sql.Exec(ctx, w.querier(ctx), sql.Raw("CREATE TABLE "+name+" ("+columns+")"))
	require.NoError(t, err)

	t.Cleanup(func() {
		ctx := context.Background()
		_, err := sql.Exec(ctx, w.querier(ctx), sql.Raw("DROP TABLE "+name))
		assert.NoError(t, err)
	})

	return name
}

func (w world) mustCount(t *testing.T, ctx context.Context, table string) int { //nolint:revive // t first
	t.Helper()

	n, err := w.count(ctx, table)
	require.NoError(t, err)

	return n
}

// shutdownAtCleanup shuts db down after the test and its subtests.
func shutdownAtCleanup(t *testing.T, db lifecycle.Resource) {
	t.Helper()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		assert.NoError(t, db.Shutdown(ctx))
	})
}

func newSQLWorld(t *testing.T, name, driver, dsn string, format sql.PlaceholderFormat) world {
	t.Helper()

	db, err := sql.New(sql.Config{DriverName: driver, DataSource: dsn, MaxOpenConns: 10}, sql.WithName(name))
	require.NoError(t, err)
	require.NoError(t, db.Start(t.Context()))
	shutdownAtCleanup(t, db)

	m := sql.NewManager(db)

	return world{
		name:     name,
		postgres: driver == corepgx.DriverName,
		format:   format,
		tx:       m,
		querier:  func(ctx context.Context) sql.Querier { return m.GetQuerier(ctx) },
		count: func(ctx context.Context, table string) (int, error) {
			var n int

			err := m.GetQuerier(ctx).QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&n)

			return n, err
		},
	}
}

func newPgxWorld(t *testing.T) (world, *corepgx.DB) {
	t.Helper()

	db, err := corepgx.New(corepgx.Config{DSN: postgresDSN, MaxConns: 10})
	require.NoError(t, err)
	require.NoError(t, db.Start(t.Context()))
	shutdownAtCleanup(t, db)

	m := corepgx.NewManager(db)

	return world{
		name:     "postgres/pgx",
		postgres: true,
		format:   sql.Dollar,
		tx:       m,
		querier:  func(ctx context.Context) sql.Querier { return m.GetQuerier(ctx) },
		count: func(ctx context.Context, table string) (int, error) {
			var n int

			err := m.GetQuerier(ctx).QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n)

			return n, err
		},
	}, db
}

func worlds(t *testing.T) []world {
	t.Helper()

	pgx, _ := newPgxWorld(t)

	return []world{
		newSQLWorld(t, "postgres/sql", corepgx.DriverName, postgresDSN, sql.Dollar),
		pgx,
		newSQLWorld(t, "mysql/sql", "mysql", mysqlDSN, sql.Question),
	}
}

const (
	personColumns = "id BIGINT PRIMARY KEY, name VARCHAR(100) NOT NULL, " +
		"email VARCHAR(100) NULL, created_at TIMESTAMP NULL"
	selectPerson = "SELECT id, name, email, created_at FROM "
)

type entity struct {
	ID int64
}

type person struct {
	entity

	FullName  string `db:"name"`
	Email     stdsql.Null[string]
	CreatedAt stdsql.Null[time.Time]
}

func insertPerson(ctx context.Context, w world, table string, id int64) error {
	_, err := sql.Exec(ctx, w.querier(ctx),
		w.q("INSERT INTO "+table+" (id, name) VALUES (?, ?)", id, "p"+strconv.FormatInt(id, 10)))

	return err
}

func TestSQL_Helpers(t *testing.T) {
	requireDocker(t)
	t.Parallel()

	created := time.Date(2026, 11, 26, 10, 30, 0, 0, time.UTC)

	for _, w := range worlds(t) {
		t.Run(w.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			tbl := w.table(t, personColumns)
			insert := "INSERT INTO " + tbl + " (id, name, email, created_at) VALUES (?, ?, ?, ?)"

			res, err := sql.Exec(ctx, w.querier(ctx), w.q(insert, 1, "Ann", "ann@example.com", created))
			require.NoError(t, err)

			n, err := res.RowsAffected()
			require.NoError(t, err)
			assert.EqualValues(t, 1, n)

			_, err = sql.Exec(ctx, w.querier(ctx), w.q(insert, 2, "Bob", nil, nil))
			require.NoError(t, err)

			ann, err := sql.Get[person](ctx, w.querier(ctx), w.q(selectPerson+tbl+" WHERE id = ?", 1))
			require.NoError(t, err)
			assert.Equal(t, int64(1), ann.ID, "embedded struct")
			assert.Equal(t, "Ann", ann.FullName, "db tag")
			assert.Equal(t, stdsql.Null[string]{V: "ann@example.com", Valid: true}, ann.Email)
			assert.True(t, ann.CreatedAt.Valid)
			assert.True(t, created.Equal(ann.CreatedAt.V), "created_at = %v", ann.CreatedAt.V)

			all, err := sql.Select[person](ctx, w.querier(ctx), w.q(selectPerson+tbl+" ORDER BY id"))
			require.NoError(t, err)
			require.Len(t, all, 2)
			assert.False(t, all[1].Email.Valid, "NULL")

			count, err := sql.Get[int](ctx, w.querier(ctx), w.q("SELECT count(*) FROM "+tbl))
			require.NoError(t, err)
			assert.Equal(t, 2, count)

			name, err := sql.Get[string](ctx, w.querier(ctx), w.q("SELECT name FROM "+tbl+" WHERE id = ?", 2))
			require.NoError(t, err)
			assert.Equal(t, "Bob", name)

			_, err = sql.Get[person](ctx, w.querier(ctx), w.q(selectPerson+tbl+" WHERE id = ?", 99))
			require.ErrorIs(t, err, sql.ErrNoRows)

			_, err = sql.Get[person](ctx, w.querier(ctx), w.q(selectPerson+tbl))
			require.ErrorIs(t, err, sql.ErrTooManyRows)

			none, err := sql.Select[person](ctx, w.querier(ctx), w.q(selectPerson+tbl+" WHERE id > ?", 99))
			require.NoError(t, err)
			assert.Empty(t, none)
		})
	}
}

func TestSQL_TxModes(t *testing.T) {
	requireDocker(t)
	t.Parallel()

	for _, w := range worlds(t) {
		t.Run(w.name, func(t *testing.T) {
			t.Parallel()

			t.Run("commit is visible, queries in fn use the transaction", func(t *testing.T) {
				t.Parallel()

				tbl := w.table(t, personColumns)
				outside := t.Context()

				err := w.tx.WithTx(t.Context(), func(ctx context.Context) error {
					require.NoError(t, insertPerson(ctx, w, tbl, 1))
					assert.Equal(t, 1, w.mustCount(t, ctx, tbl), "sqlc-style code inside sees its own insert")

					n := w.mustCount(t, outside, tbl) // on purpose: outside the transaction
					assert.Equal(t, 0, n, "outside the transaction: not yet")

					return nil
				})

				require.NoError(t, err)
				assert.Equal(t, 1, w.mustCount(t, t.Context(), tbl))
			})

			t.Run("join: a handled inner error rolls everything back", func(t *testing.T) {
				t.Parallel()

				tbl := w.table(t, personColumns)

				err := w.tx.WithTx(t.Context(), func(ctx context.Context) error {
					require.NoError(t, insertPerson(ctx, w, tbl, 1))

					_ = w.tx.WithTx(ctx, func(ctx context.Context) error {
						require.NoError(t, insertPerson(ctx, w, tbl, 2))

						return errSQLDomain
					})

					return nil
				})

				require.ErrorIs(t, err, sql.ErrRollbackOnly)
				require.ErrorIs(t, err, errSQLDomain)
				assert.Equal(t, 0, w.mustCount(t, t.Context(), tbl))
			})

			t.Run("nested: savepoint rolled back, transaction committed", func(t *testing.T) {
				t.Parallel()

				tbl := w.table(t, personColumns)

				err := w.tx.WithTx(t.Context(), func(ctx context.Context) error {
					require.NoError(t, insertPerson(ctx, w, tbl, 1))

					err := w.tx.WithTx(ctx, func(ctx context.Context) error {
						require.NoError(t, insertPerson(ctx, w, tbl, 2))

						return errSQLDomain
					}, sql.WithTxMode(sql.TxModeNested))
					require.ErrorIs(t, err, errSQLDomain)

					return w.tx.WithTx(ctx, func(ctx context.Context) error {
						return insertPerson(ctx, w, tbl, 3)
					}, sql.WithTxMode(sql.TxModeNested))
				})

				require.NoError(t, err)

				ids, err := sql.Select[int64](
					t.Context(),
					w.querier(t.Context()),
					w.q("SELECT id FROM "+tbl+" ORDER BY id"),
				)
				require.NoError(t, err)
				assert.Equal(t, []int64{1, 3}, ids)
			})

			t.Run("requires-new commits on its own", func(t *testing.T) {
				t.Parallel()

				tbl := w.table(t, personColumns)

				err := w.tx.WithTx(t.Context(), func(ctx context.Context) error {
					require.NoError(t, insertPerson(ctx, w, tbl, 1))

					require.NoError(t, w.tx.WithTx(ctx, func(ctx context.Context) error {
						return insertPerson(ctx, w, tbl, 2)
					}, sql.WithTxMode(sql.TxModeRequiresNew)))

					return errSQLDomain
				})

				require.ErrorIs(t, err, errSQLDomain)

				ids, err := sql.Select[int64](t.Context(), w.querier(t.Context()), w.q("SELECT id FROM "+tbl))
				require.NoError(t, err)
				assert.Equal(t, []int64{2}, ids)
			})

			t.Run("forbid", func(t *testing.T) {
				t.Parallel()

				err := w.tx.WithTx(t.Context(), func(ctx context.Context) error {
					return w.tx.WithTx(
						ctx,
						func(context.Context) error { return nil },
						sql.WithTxMode(sql.TxModeForbid),
					)
				})

				require.ErrorIs(t, err, sql.ErrNestedTx)
			})

			t.Run("on commit: after the data is visible; dropped on rollback", func(t *testing.T) {
				t.Parallel()

				tbl := w.table(t, personColumns)
				seen := -1

				outside := t.Context()
				countOutside := func() { seen = w.mustCount(t, outside, tbl) } // after the commit, outside it

				require.NoError(t, w.tx.WithTx(t.Context(), func(ctx context.Context) error {
					w.tx.OnCommit(ctx, countOutside)

					return insertPerson(ctx, w, tbl, 1)
				}))
				assert.Equal(t, 1, seen)

				ran := false
				err := w.tx.WithTx(t.Context(), func(ctx context.Context) error {
					w.tx.OnCommit(ctx, func() { ran = true })

					return errSQLDomain
				})
				require.ErrorIs(t, err, errSQLDomain)
				assert.False(t, ran)
			})

			t.Run("read-only rejects writes", func(t *testing.T) {
				t.Parallel()

				tbl := w.table(t, personColumns)

				err := w.tx.WithTx(t.Context(), func(ctx context.Context) error {
					return insertPerson(ctx, w, tbl, 1)
				}, sql.ReadOnly())

				// PostgreSQL: "cannot execute INSERT in a read-only transaction";
				// MySQL: "Cannot execute statement in a READ ONLY transaction".
				require.Error(t, err)
				assert.Regexp(t, `(?i)read[- ]only transaction`, err.Error())
				assert.Equal(t, 0, w.mustCount(t, t.Context(), tbl))
			})

			t.Run("serializable is applied", func(t *testing.T) {
				t.Parallel()

				// MySQL: @@transaction_isolation is the session's level, not the
				// level SET TRANSACTION gave the current transaction.
				query := "SELECT REPLACE(ISOLATION_LEVEL, ' ', '-') FROM performance_schema.events_transactions_current " +
					"WHERE THREAD_ID = PS_CURRENT_THREAD_ID()"
				if w.postgres {
					query = "SELECT current_setting('transaction_isolation')"
				}

				var level string

				err := w.tx.WithTx(t.Context(), func(ctx context.Context) error {
					var err error

					level, err = sql.Get[string](ctx, w.querier(ctx), sql.Raw(query))

					return err
				}, sql.WithIsolation(stdsql.LevelSerializable))

				require.NoError(t, err)
				assert.Equal(t, "serializable", strings.ToLower(level))
			})
		})
	}
}

func TestSQL_PostgresJSONBOperator(t *testing.T) {
	requireDocker(t)
	t.Parallel()

	for _, w := range worlds(t) {
		if !w.postgres {
			continue
		}

		t.Run(w.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			tbl := w.table(t, "id BIGINT PRIMARY KEY, doc JSONB NOT NULL")

			_, err := sql.Exec(ctx, w.querier(ctx), w.q("INSERT INTO "+tbl+" VALUES (?, ?::jsonb), (?, ?::jsonb)",
				1, `{"tag": 1}`, 2, `{"other": 1}`))
			require.NoError(t, err)

			// "??" is jsonb's "?" operator once Dollar has numbered the placeholders.
			ids, err := sql.Select[int64](
				ctx,
				w.querier(ctx),
				w.q("SELECT id FROM "+tbl+" WHERE doc ?? ? AND id > ?", "tag", 0),
			)
			require.NoError(t, err)
			assert.Equal(t, []int64{1}, ids)
		})
	}
}

type tagged struct {
	ID   uuid.UUID
	Tags []string
}

// The column mapping is the same in both worlds, the value decoding is not:
// pgx decodes text[] natively, database/sql does not.
func TestSQL_PostgresValueDecoding(t *testing.T) {
	requireDocker(t)
	t.Parallel()

	id := uuid.New()

	for _, w := range worlds(t) {
		if !w.postgres {
			continue
		}

		t.Run(w.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			tbl := w.table(t, "id UUID PRIMARY KEY, tags TEXT[] NOT NULL")

			_, err := sql.Exec(ctx, w.querier(ctx), w.q("INSERT INTO "+tbl+" VALUES (?, '{a,b}')", id))
			require.NoError(t, err)

			got, err := sql.Get[tagged](ctx, w.querier(ctx), w.q("SELECT id, tags FROM "+tbl))

			if w.name == "postgres/sql" {
				require.Error(t, err, "database/sql cannot scan text[] into []string")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tagged{ID: id, Tags: []string{"a", "b"}}, got)
		})
	}
}

func TestSQL_PgxExtensionPoints(t *testing.T) {
	requireDocker(t)
	t.Parallel()

	w, db := newPgxWorld(t)
	tbl := w.table(t, personColumns)

	require.NotNil(t, db.Pool())

	err := w.tx.WithTx(t.Context(), func(ctx context.Context) error {
		tx, ok := corepgx.Tx(ctx, db) // what River's InsertTx takes
		require.True(t, ok)

		_, err := tx.Exec(ctx, "INSERT INTO "+tbl+" (id, name) VALUES (1, 'a')")

		return err
	})
	require.NoError(t, err)

	sqlDB, err := db.SQLDB() // what goose takes
	require.NoError(t, err)

	var n int

	require.NoError(t, sqlDB.QueryRowContext(t.Context(), "SELECT count(*) FROM "+tbl).Scan(&n))
	assert.Equal(t, 1, n)
}

func TestSQL_Contract(t *testing.T) {
	requireDocker(t)
	t.Parallel()

	factories := map[string]func(t *testing.T) lifecycle.Resource{
		"postgres/sql": func(t *testing.T) lifecycle.Resource {
			t.Helper()

			db, err := sql.New(sql.Config{DriverName: corepgx.DriverName, DataSource: postgresDSN})
			require.NoError(t, err)

			return db
		},
		"postgres/pgx": func(t *testing.T) lifecycle.Resource {
			t.Helper()

			db, err := corepgx.New(corepgx.Config{DSN: postgresDSN})
			require.NoError(t, err)

			return db
		},
		"mysql/sql": func(t *testing.T) lifecycle.Resource {
			t.Helper()

			db, err := sql.New(sql.Config{DriverName: "mysql", DataSource: mysqlDSN})
			require.NoError(t, err)

			return db
		},
	}

	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			lifecycletest.Resource(t, factory, lifecycletest.WithTimeout(30*time.Second))
		})
	}
}
