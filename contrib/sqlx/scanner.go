package sqlx

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"

	"github.com/eidon-go/core/contrib/sql"
)

// DefaultMapper returns sqlx's default mapper: a field takes the column
// named by its `db` tag, or its name in lower case (UserID → userid).
func DefaultMapper() *reflectx.Mapper { return reflectx.NewMapperFunc("db", sqlx.NameMapper) }

// NewScanner returns an sql.Scanner with sqlx's mapping (mapper, or
// DefaultMapper when nil), for sql.WithScanner: code written for sqlx keeps
// its untagged lower-case fields with the helpers of package sql. Embedded
// structs are flattened; a column that maps to no field is an error; scalars
// and sql.Scanner types scan directly. ScanOne keeps the contract of
// sql.Scanner: sql.ErrNoRows when there is no row, sql.ErrTooManyRows when
// there are more (sqlx's Get takes the first one).
func NewScanner(mapper *reflectx.Mapper) sql.Scanner {
	if mapper == nil {
		mapper = DefaultMapper()
	}

	return scanner{mapper: mapper}
}

type scanner struct{ mapper *reflectx.Mapper }

var errDestination = errors.New("sqlx: scan destination must be a non-nil pointer")

func (s scanner) ScanOne(rows *stdsql.Rows, dest any) error {
	return closeRows(rows, s.scanOne(rows, dest))
}

func (s scanner) ScanAll(rows *stdsql.Rows, dest any) error {
	// sqlx scans scalars and structs into a slice only through Select: hand
	// it the rows as the result of a query (no I/O, so no context to pass).
	q := rowsQueryer{rows: &sqlx.Rows{Rows: rows, Mapper: s.mapper}}
	err := sqlx.SelectContext(context.Background(), q, dest, "")

	return closeRows(rows, err)
}

func (s scanner) scanOne(rows *stdsql.Rows, dest any) error {
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return fmt.Errorf("%w, got %T", errDestination, dest)
	}

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err //nolint:wrapcheck // the driver's error, as is
		}

		return sql.ErrNoRows
	}

	var err error
	if s.scannable(v.Type().Elem()) {
		err = rows.Scan(dest)
	} else {
		err = (&sqlx.Rows{Rows: rows, Mapper: s.mapper}).StructScan(dest)
	}

	if err != nil {
		return err //nolint:wrapcheck // the driver's or sqlx's error, as is
	}

	if rows.Next() {
		return sql.ErrTooManyRows
	}

	return rows.Err() //nolint:wrapcheck // the driver's error, as is
}

// scannable mirrors sqlx: a type scans directly (not field by field) when
// it is an sql.Scanner, not a struct, or a struct without mapped fields.
func (s scanner) scannable(t reflect.Type) bool {
	if reflect.PointerTo(t).Implements(reflect.TypeFor[stdsql.Scanner]()) || t.Kind() != reflect.Struct {
		return true
	}

	return len(s.mapper.TypeMap(t).Index) == 0
}

// closeRows closes rows and returns err, or the error of Close when err is
// nil. Errors are the driver's or sqlx's, as is.
func closeRows(rows *stdsql.Rows, err error) error {
	if cerr := rows.Close(); cerr != nil && err == nil {
		return cerr //nolint:wrapcheck // see above
	}

	return err
}

// rowsQueryer is an sqlx.QueryerContext whose only query returns rows.
type rowsQueryer struct{ rows *sqlx.Rows }

var errNotAQuery = errors.New("sqlx: not a query")

func (q rowsQueryer) QueryxContext(context.Context, string, ...any) (*sqlx.Rows, error) {
	return q.rows, nil
}

func (rowsQueryer) QueryContext(context.Context, string, ...any) (*stdsql.Rows, error) {
	return nil, errNotAQuery
}

func (rowsQueryer) QueryRowxContext(context.Context, string, ...any) *sqlx.Row {
	return nil
}
