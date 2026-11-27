package sqlx_test

import (
	"context"
	"fmt"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"

	"github.com/eidon-go/core/contrib/sql"
	coresqlx "github.com/eidon-go/core/contrib/sqlx"
)

type account struct {
	ID      int64  `db:"id"`
	Owner   string `db:"owner"`
	Balance int64  `db:"balance"`
}

// customer has no tags: sqlx maps its fields in lower case.
type customer struct {
	ID       int64
	FullName string
}

// newExampleDB stands in for sql.New(sql.Config{DriverName: "pgx", DataSource: dsn}).
func newExampleDB() (*sql.DB, sqlmock.Sqlmock) {
	sdb, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		panic(err)
	}

	db, err := sql.NewFromDB(sdb, sql.WithDriverName("pgx")) // New takes it from Config.DriverName
	if err != nil {
		panic(err)
	}

	return db, mock
}

func Example() {
	db, mock := newExampleDB()
	ctx := context.Background()

	m, err := coresqlx.NewManager(db)
	if err != nil {
		panic(err)
	}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, owner, balance FROM accounts WHERE id IN ($1, $2)").WithArgs(1, 2).
		WillReturnRows(sqlmock.NewRows([]string{"id", "owner", "balance"}).AddRow(1, "ann", 100).AddRow(2, "bob", 50))
	mock.ExpectExec("UPDATE accounts SET balance = $1 WHERE id = $2").WithArgs(0, 1).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err = m.WithTx(ctx, func(ctx context.Context) error {
		q := m.GetQuerier(ctx) // the transaction of ctx, as an sqlx.ExtContext

		query, args, err := sqlx.In("SELECT id, owner, balance FROM accounts WHERE id IN (?)", []int64{1, 2})
		if err != nil {
			return err
		}

		var accounts []account
		if err := sqlx.SelectContext(ctx, q, &accounts, q.Rebind(query), args...); err != nil {
			return err
		}

		for _, a := range accounts {
			fmt.Println(a.Owner, a.Balance)
		}

		_, err = q.NamedExecContext(ctx, "UPDATE accounts SET balance = :balance WHERE id = :id",
			account{ID: 1, Balance: 0})

		return err
	})
	if err != nil {
		panic(err)
	}

	// Output:
	// ann 100
	// bob 50
}

func ExampleNewScanner() {
	sdb, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		panic(err)
	}

	// sqlx's mapping (untagged fields in lower case) for the helpers of package sql.
	db, err := sql.NewFromDB(sdb, sql.WithScanner(coresqlx.NewScanner(nil)))
	if err != nil {
		panic(err)
	}

	m, err := sql.NewManager(db)
	if err != nil {
		panic(err)
	}

	mock.ExpectQuery("SELECT id, fullname FROM customers WHERE id = $1").WithArgs(1).
		WillReturnRows(sqlmock.NewRows([]string{"id", "fullname"}).AddRow(1, "Ann Lee"))

	ctx := context.Background()

	c, err := sql.Get[customer](ctx, m.GetQuerier(ctx),
		sql.Raw("SELECT id, fullname FROM customers WHERE id = $1", 1))
	if err != nil {
		panic(err)
	}

	fmt.Println(c.FullName)
	// Output: Ann Lee
}
