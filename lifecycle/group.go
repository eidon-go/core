package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
)

// defaultGroupName is the Name of a Group created without GroupNamed.
const defaultGroupName = "group"

// Group combines resources into one component, e.g. the infrastructure of a
// service or the parts of one integration. Members are given in dependency
// order:
//
//   - Start starts every member that implements Starter, in order. If one
//     fails, the members before it are shut down in reverse order and the
//     error is returned.
//   - Shutdown shuts every member down in reverse order and joins the errors.
//   - Healthcheck checks every member that implements Healthchecker and joins
//     the errors, each prefixed with the member's name.
//
// Nil members, including a typed nil pointer stored in the interface (a
// struct field that was never assigned), are skipped, so the fields of a
// partially constructed struct can be passed without a nil guard:
//
//	func (i *Infrastructure) Shutdown(ctx context.Context) error {
//	    return lifecycle.Group(i.Telemetry, i.DB, i.Cache).Shutdown(ctx)
//	}
//
// A Group cannot run members: Group panics if a member implements Runner.
// Register runners with app.App directly.
func Group(members ...Resource) Resource {
	return GroupNamed(defaultGroupName, members...)
}

// GroupNamed is Group with a name for logs, errors and health reports.
func GroupNamed(name string, members ...Resource) Resource {
	for idx, member := range members {
		if _, ok := member.(Runner); ok && !IsNil(member) {
			panic(
				fmt.Sprintf(
					"lifecycle: Group: member %d (%s) is a Runner; add runners to app.App directly",
					idx,
					NameOf(member),
				),
			)
		}
	}

	return &group{name: name, members: slices.Clone(members)}
}

type group struct {
	name    string
	members []Resource
}

// Name implements Namer.
func (g *group) Name() string { return g.name }

// Start implements Starter.
func (g *group) Start(ctx context.Context) error {
	for idx, member := range g.members {
		s, ok := member.(Starter)
		if !ok || IsNil(member) {
			continue
		}

		if err := s.Start(ctx); err != nil {
			startErr := fmt.Errorf("start %s: %w", NameOf(member), err)
			// The failed member cleaned up after itself; roll back the rest.
			rollback := &group{name: g.name, members: g.members[:idx]}

			return errors.Join(startErr, rollback.Shutdown(context.WithoutCancel(ctx)))
		}
	}

	return nil
}

// Shutdown implements Resource.
func (g *group) Shutdown(ctx context.Context) error {
	var errs []error

	for _, member := range slices.Backward(g.members) {
		if IsNil(member) {
			continue
		}

		if err := member.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("shutdown %s: %w", NameOf(member), err))
		}
	}

	return errors.Join(errs...)
}

// Healthcheck implements Healthchecker.
func (g *group) Healthcheck(ctx context.Context) error {
	var errs []error

	for _, member := range g.members {
		if IsNil(member) {
			continue
		}

		if hc, ok := member.(Healthchecker); ok {
			if err := hc.Healthcheck(ctx); err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", NameOf(member), err))
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
	_ Resource      = (*group)(nil)
	_ Starter       = (*group)(nil)
	_ Healthchecker = (*group)(nil)
	_ Namer         = (*group)(nil)
)
