// Package scan holds the row scanning shared by the sql and sql/pgx
// packages, over scany's dbscan.
package scan

import (
	"database/sql"
	"errors"

	"github.com/georgysavva/scany/v2/dbscan"
)

// ErrTooManyRows is returned by One when the query returned more than one
// row.
var ErrTooManyRows = errors.New("sql: query returned more than one row")

// Options are the scany options of both worlds: the db tag, snake_case for
// untagged fields, an error on a column that maps to no field.
func Options() []dbscan.APIOption {
	return []dbscan.APIOption{
		dbscan.WithStructTagKey("db"),
		dbscan.WithFieldNameMapper(dbscan.SnakeCaseMapper),
		dbscan.WithAllowUnknownColumns(false),
	}
}

// One scans the only row of rows into dest and closes rows: sql.ErrNoRows
// when there is none, ErrTooManyRows when there are more. An error from
// closing rows is joined to the result.
func One(api *dbscan.API, rows dbscan.Rows, dest any) (err error) {
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	return one(api, rows, dest)
}

func one(api *dbscan.API, rows dbscan.Rows, dest any) error {
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err //nolint:wrapcheck // the driver's error, as is
		}

		return sql.ErrNoRows
	}

	if err := api.ScanRow(dest, rows); err != nil {
		return err //nolint:wrapcheck // scany's error, as is
	}

	if rows.Next() {
		return ErrTooManyRows
	}

	return rows.Err() //nolint:wrapcheck // the driver's error, as is
}

// All scans every row of rows into dest, a pointer to a slice, and closes
// rows.
func All(api *dbscan.API, rows dbscan.Rows, dest any) error {
	return api.ScanAll(dest, rows) //nolint:wrapcheck // scany's error, as is
}
