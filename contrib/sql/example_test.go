package sql_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/eidon-go/core/contrib/sql"
)

type account struct {
	ID      int64
	Balance int64
}

// newExampleDB stands in for sql.New(sql.Config{DriverName: "pgx", DataSource: dsn}).
func newExampleDB() (*sql.DB, sqlmock.Sqlmock) {
	sdb, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		panic(err)
	}

	db, err := sql.NewFromDB(sdb)
	if err != nil {
		panic(err)
	}

	return db, mock
}

func Example() {
	db, mock := newExampleDB()
	ctx := context.Background()

	if err := db.Start(ctx); err != nil { // app.App calls Start and Shutdown
		panic(err)
	}

	m := sql.NewManager(db) // repositories depend on sql.Transactor / sql.QuerierProvider

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id, balance FROM accounts WHERE id = $1 FOR UPDATE").WithArgs(1).
		WillReturnRows(sqlmock.NewRows([]string{"id", "balance"}).AddRow(1, 100))
	mock.ExpectExec("UPDATE accounts SET balance = $1 WHERE id = $2").WithArgs(70, 1).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := m.WithTx(ctx, func(ctx context.Context) error {
		q := m.GetQuerier(ctx) // the transaction of ctx

		acc, err := sql.Get[account](ctx, q,
			sql.Raw("SELECT id, balance FROM accounts WHERE id = $1 FOR UPDATE", 1))
		if err != nil {
			return err
		}

		m.OnCommit(ctx, func() { fmt.Println("event published after commit") })

		_, err = sql.Exec(ctx, q, sql.Raw("UPDATE accounts SET balance = $1 WHERE id = $2", acc.Balance-30, acc.ID))

		return err
	})
	fmt.Println("err:", err)

	mock.ExpectClose()
	fmt.Println("shutdown:", db.Shutdown(ctx))

	// Output:
	// event published after commit
	// err: <nil>
	// shutdown: <nil>
}

func ExampleTxModeNested() {
	db, mock := newExampleDB()
	ctx := context.Background()
	m := sql.NewManager(db)

	mock.ExpectBegin()
	mock.ExpectExec("SAVEPOINT sp_1").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("ROLLBACK TO SAVEPOINT sp_1").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	errOptional := errors.New("optional step failed")

	err := m.WithTx(ctx, func(ctx context.Context) error {
		err := m.WithTx(ctx, func(context.Context) error {
			return errOptional // rolls back to the savepoint only
		}, sql.WithTxMode(sql.TxModeNested))
		fmt.Println("nested:", err)

		return nil // the transaction commits
	})
	fmt.Println("outer:", err)

	// Output:
	// nested: optional step failed
	// outer: <nil>
}

func ExampleDollar() {
	query, _ := sql.Dollar.ReplacePlaceholders("SELECT id FROM docs WHERE body ?? 'tag' AND owner = ? AND id > ?")
	fmt.Println(query)

	// Output:
	// SELECT id FROM docs WHERE body ? 'tag' AND owner = $1 AND id > $2
}
