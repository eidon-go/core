package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
)

// Group returns a Resource that shuts down every non-nil member in reverse
// argument order — so members passed in construction order are torn down
// dependents-first — joining the errors with errors.Join.
//
// Nil members, including a typed nil pointer stored in the interface (a
// struct field that was never assigned), are skipped. This lets callers pass
// fields of a partially constructed struct without a nil guard per field:
//
//	func (i *Infrastructure) Shutdown(ctx context.Context) error {
//	    return lifecycle.Group(i.OTel, i.DB, i.Redis).Shutdown(ctx)
//	}
//
// The returned value also implements Healthchecker: it checks every member
// that implements Healthchecker and joins their errors, so registering a
// Group with app.App does not hide the members' health checks.
func Group(resources ...Resource) Resource {
	return group(resources)
}

type group []Resource

// Shutdown implements Resource.
func (g group) Shutdown(ctx context.Context) error {
	var errs []error

	for _, r := range slices.Backward(g) {
		if IsNil(r) {
			continue
		}

		if err := r.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", NameOf(r), err))
		}
	}

	return errors.Join(errs...)
}

// Healthcheck implements Healthchecker.
func (g group) Healthcheck(ctx context.Context) error {
	var errs []error

	for _, r := range g {
		if IsNil(r) {
			continue
		}

		if hc, ok := r.(Healthchecker); ok {
			if err := hc.Healthcheck(ctx); err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", NameOf(r), err))
			}
		}
	}

	return errors.Join(errs...)
}

// IsNil reports whether v is nil, including the classic Go gotcha of a nil
// pointer (or nil slice, map, func, chan) stored in a non-nil interface value.
func IsNil(v any) bool {
	if v == nil {
		return true
	}

	switch rv := reflect.ValueOf(v); rv.Kind() { //nolint:exhaustive // only kinds that can be nil are relevant
	case reflect.Pointer, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		return rv.IsNil()
	default:
		return false
	}
}

// NameOf returns v.Name() if v implements Namer, otherwise v's Go type
// (e.g. "*redis.Client").
func NameOf(v any) string {
	if n, ok := v.(Namer); ok && !IsNil(v) {
		return n.Name()
	}

	return fmt.Sprintf("%T", v)
}

// Compile-time checks.
var (
	_ Resource      = group(nil)
	_ Healthchecker = group(nil)
)
