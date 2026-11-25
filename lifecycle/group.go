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
//   - Shutdown shuts every member down in reverse order and joins the errors.
//   - If any member implements Starter, the Group does too: Start starts those
//     members in order and stops at the first error, which it returns. It
//     does not shut members down itself: as for any component whose Start
//     failed, the caller calls Shutdown (app.App does), and that releases
//     every member, started or not, with the shutdown's own deadline.
//   - If any member implements Healthchecker, the Group does too: Healthcheck
//     checks those members and joins the errors, each prefixed with the
//     member's name.
//   - The Group implements Namer ("group", or the name given to GroupNamed).
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
	hasStarter, hasChecker := scanMembers(members)
	g := &group{name: name, members: slices.Clone(members)}

	// Expose only the optional interfaces the members have: a Group that
	// claimed Start or Healthcheck without a member behind it would be
	// started, timed and reported for nothing.
	switch {
	case hasStarter && hasChecker:
		return &startCheckGroup{g}
	case hasStarter:
		return &startGroup{g}
	case hasChecker:
		return &checkGroup{g}
	default:
		return g
	}
}

// scanMembers reports whether any non-nil member is a Starter or a
// Healthchecker, and panics on a Runner member.
func scanMembers(members []Resource) (bool, bool) {
	var hasStarter, hasChecker bool

	for idx, member := range members {
		if IsNil(member) {
			continue
		}

		if _, ok := member.(Runner); ok {
			panic(fmt.Sprintf("lifecycle: Group: member %d (%s) is a Runner; add runners to app.App directly",
				idx, NameOf(member)))
		}

		_, isStarter := member.(Starter)
		_, isChecker := member.(Healthchecker)
		hasStarter = hasStarter || isStarter
		hasChecker = hasChecker || isChecker
	}

	return hasStarter, hasChecker
}

type group struct {
	name    string
	members []Resource
}

type (
	startGroup      struct{ *group }
	checkGroup      struct{ *group }
	startCheckGroup struct{ *group }
)

// Name implements Namer.
func (g *group) Name() string { return g.name }

// Shutdown implements Resource.
func (g *group) Shutdown(ctx context.Context) error {
	return shutdownAll(ctx, g.members)
}

// Start implements Starter.
func (g *startGroup) Start(ctx context.Context) error { return g.start(ctx) }

// Start implements Starter.
func (g *startCheckGroup) Start(ctx context.Context) error { return g.start(ctx) }

// Healthcheck implements Healthchecker.
func (g *checkGroup) Healthcheck(ctx context.Context) error { return g.healthcheck(ctx) }

// Healthcheck implements Healthchecker.
func (g *startCheckGroup) Healthcheck(ctx context.Context) error { return g.healthcheck(ctx) }

func (g *group) start(ctx context.Context) error {
	for _, member := range g.members {
		s, ok := member.(Starter)
		if !ok || IsNil(member) {
			continue
		}

		if err := s.Start(ctx); err != nil {
			// No rollback here: the caller shuts the whole Group down, with
			// a fresh deadline rather than this ctx, which may have expired.
			return fmt.Errorf("start %s: %w", NameOf(member), err)
		}
	}

	return nil
}

func (g *group) healthcheck(ctx context.Context) error {
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

// shutdownAll shuts down the non-nil members in reverse order.
func shutdownAll(ctx context.Context, members []Resource) error {
	var errs []error

	for _, member := range slices.Backward(members) {
		if IsNil(member) {
			continue
		}

		if err := member.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("shutdown %s: %w", NameOf(member), err))
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
	_ Namer         = (*group)(nil)
	_ Starter       = (*startGroup)(nil)
	_ Healthchecker = (*checkGroup)(nil)
	_ Starter       = (*startCheckGroup)(nil)
	_ Healthchecker = (*startCheckGroup)(nil)
)
