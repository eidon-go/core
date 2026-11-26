package sql

import (
	"context"
	stdsql "database/sql"
	"fmt"
)

// Builder renders a query and its arguments. squirrel's builders satisfy it
// without this module importing squirrel; Raw wraps hand-written SQL.
type Builder interface {
	ToSql() (string, []any, error)
}

// Raw returns a Builder for a hand-written query; the query is used as is.
func Raw(query string, args ...any) Builder { return raw{query: query, args: args} }

type raw struct {
	query string
	args  []any
}

func (r raw) ToSql() (string, []any, error) { return r.query, r.args, nil } //nolint:revive // Builder

// Get runs the query of b and scans its only row into a T: a struct, a
// scalar or anything the Scanner supports. It returns ErrNoRows when there
// is no row and ErrTooManyRows when there are more (unlike sqlx, which took
// the first one). Errors are wrapped ("sql: get: …"): check them with
// errors.Is / errors.As.
//
//	user, err := sql.Get[User](ctx, m.GetQuerier(ctx), sql.Raw("SELECT * FROM users WHERE id = $1", id))
func Get[T any](ctx context.Context, q Querier, b Builder) (T, error) {
	var dest T

	query, args, err := build(b)
	if err != nil {
		return dest, err
	}

	if err := q.GetContext(ctx, &dest, query, args...); err != nil {
		return dest, fmt.Errorf("sql: get: %w", err)
	}

	return dest, nil
}

// Select runs the query of b and scans every row into a []T; nil when there
// are none.
func Select[T any](ctx context.Context, q Querier, b Builder) ([]T, error) {
	query, args, err := build(b)
	if err != nil {
		return nil, err
	}

	var dest []T

	if err := q.SelectContext(ctx, &dest, query, args...); err != nil {
		return nil, fmt.Errorf("sql: select: %w", err)
	}

	return dest, nil
}

// Exec runs the statement of b. In the pgx world Result.LastInsertId is not
// supported: use RETURNING with Get.
func Exec(ctx context.Context, q Querier, b Builder) (stdsql.Result, error) {
	query, args, err := build(b)
	if err != nil {
		return nil, err
	}

	res, err := q.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("sql: exec: %w", err)
	}

	return res, nil
}

func build(b Builder) (string, []any, error) {
	query, args, err := b.ToSql()
	if err != nil {
		return "", nil, fmt.Errorf("sql: build query: %w", err)
	}

	return query, args, nil
}
