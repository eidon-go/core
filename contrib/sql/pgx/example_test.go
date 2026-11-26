package pgx_test

import (
	"context"
	"fmt"

	"github.com/pashagolub/pgxmock/v4"

	"github.com/eidon-go/core/contrib/sql"
	corepgx "github.com/eidon-go/core/contrib/sql/pgx"
)

// The pool stands in for corepgx.New(corepgx.Config{DSN: dsn}); in tests,
// NewFromDBTX takes pgxmock's pool.
func Example() {
	mock, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	if err != nil {
		panic(err)
	}

	db, err := corepgx.NewFromDBTX(mock)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	m, err := corepgx.NewManager(db)
	if err != nil {
		panic(err)
	}

	mock.ExpectBegin()
	mock.ExpectQuery("INSERT INTO orders (total) VALUES ($1) RETURNING id").WithArgs(250).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(17)))
	mock.ExpectCommit()

	var id int64

	err = m.WithTx(ctx, func(ctx context.Context) error {
		// PostgreSQL has no LastInsertId: RETURNING with sql.Get.
		id, err = sql.Get[int64](ctx, m.GetQuerier(ctx),
			sql.Raw("INSERT INTO orders (total) VALUES ($1) RETURNING id", 250))

		return err
	})
	fmt.Println(id, err)

	mock.ExpectClose()
	fmt.Println("shutdown:", db.Shutdown(ctx))

	// Output:
	// 17 <nil>
	// shutdown: <nil>
}
