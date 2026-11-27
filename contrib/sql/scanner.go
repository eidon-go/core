package sql

import (
	stdsql "database/sql"

	"github.com/georgysavva/scany/v2/dbscan"
	"github.com/georgysavva/scany/v2/sqlscan"

	"github.com/eidon-go/core/contrib/sql/internal/scan"
)

// Scanner maps result rows to Go values for Get and Select. Replace the
// default with WithScanner (an sqlx-style mapper, a hand-written one).
//
// Implementations close rows.
type Scanner interface {
	// ScanOne scans the only row of rows into dest, a pointer: ErrNoRows
	// when there is none, ErrTooManyRows when there are more.
	ScanOne(rows *stdsql.Rows, dest any) error
	// ScanAll scans every row of rows into dest, a pointer to a slice.
	ScanAll(rows *stdsql.Rows, dest any) error
}

// DefaultScanner returns the scanner used without WithScanner, built on
// scany: a struct field takes the column named by its `db` tag, or its name
// in snake_case (UserID → user_id); embedded structs are flattened; a
// column that maps to no field is an error; scalars and types implementing
// sql.Scanner are scanned directly.
//
// The pgx world maps columns to fields the same way, but decodes values
// differently: pgx decodes arrays, jsonb and uuid natively, database/sql
// does not (pgx's driver scans arrays into slices from pgx v5.11 on Go
// 1.27). A struct that scans in the pgx world may fail here.
func DefaultScanner() Scanner { return defaultScanner }

var defaultScanner Scanner = scanyScanner{api: mustNewAPI()} //nolint:gochecknoglobals // immutable, built once

func mustNewAPI() *dbscan.API {
	api, err := sqlscan.NewDBScanAPI(scan.Options()...)
	if err != nil {
		panic("sql: scany: " + err.Error()) // the options are constant
	}

	return api
}

type scanyScanner struct{ api *dbscan.API }

func (s scanyScanner) ScanOne(rows *stdsql.Rows, dest any) error {
	return scan.One(s.api, rows, dest) //nolint:wrapcheck // ErrNoRows and the driver's errors, as is
}

func (s scanyScanner) ScanAll(rows *stdsql.Rows, dest any) error {
	return scan.All(s.api, rows, dest) //nolint:wrapcheck // the driver's errors, as is
}
