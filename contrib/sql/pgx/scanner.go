package pgx

import (
	"github.com/georgysavva/scany/v2/dbscan"
	"github.com/georgysavva/scany/v2/pgxscan"
	pgxv5 "github.com/jackc/pgx/v5"

	"github.com/eidon-go/core/contrib/sql/internal/scan"
)

// Scanner maps result rows to Go values for Get and Select; the pgx-world
// twin of sql.Scanner. Implementations close rows.
type Scanner interface {
	// ScanOne scans the only row of rows into dest, a pointer:
	// sql.ErrNoRows when there is none, sql.ErrTooManyRows when there are
	// more.
	ScanOne(rows pgxv5.Rows, dest any) error
	// ScanAll scans every row of rows into dest, a pointer to a slice.
	ScanAll(rows pgxv5.Rows, dest any) error
}

// DefaultScanner returns the scanner used without WithScanner: scany with
// the same column-to-field mapping as sql.DefaultScanner (`db` tag,
// snake_case, embedded structs flattened, unknown columns are an error).
// Values are decoded by pgx: arrays, jsonb, uuid and the other PostgreSQL
// types scan natively.
func DefaultScanner() Scanner { return defaultScanner }

var defaultScanner Scanner = scanyScanner{api: mustNewAPI()} //nolint:gochecknoglobals // immutable, built once

func mustNewAPI() *dbscan.API {
	api, err := pgxscan.NewDBScanAPI(scan.Options()...)
	if err != nil {
		panic("sql/pgx: scany: " + err.Error()) // the options are constant
	}

	return api
}

type scanyScanner struct{ api *dbscan.API }

func (s scanyScanner) ScanOne(rows pgxv5.Rows, dest any) error {
	return scan.One(s.api, pgxscan.NewRowsAdapter(rows), dest) //nolint:wrapcheck // sql.ErrNoRows and pgx errors, as is
}

func (s scanyScanner) ScanAll(rows pgxv5.Rows, dest any) error {
	return scan.All(s.api, pgxscan.NewRowsAdapter(rows), dest) //nolint:wrapcheck // pgx errors, as is
}
