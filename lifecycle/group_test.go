package lifecycle_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
)

type fakeResource struct {
	shutdownErr error
	called      bool
}

func (f *fakeResource) Shutdown(context.Context) error {
	f.called = true

	return f.shutdownErr
}

type ctxKey struct{}

// orderedResource records its name into a shared slice on Shutdown, and
// asserts the ctx passed by Group still carries the value Shutdown was
// originally invoked with — proving Group forwards ctx unmodified rather
// than substituting its own.
type orderedResource struct {
	name  string
	order *[]string
}

func (r *orderedResource) Shutdown(ctx context.Context) error {
	if v, _ := ctx.Value(ctxKey{}).(string); v != "propagated" {
		return errors.New("ctx value not propagated to resource")
	}

	*r.order = append(*r.order, r.name)

	return nil
}

func TestGroup_ShutsDownEveryResource(t *testing.T) {
	t.Parallel()

	a := &fakeResource{}
	b := &fakeResource{}

	err := lifecycle.Group(a, b).Shutdown(t.Context())

	require.NoError(t, err)
	assert.True(t, a.called)
	assert.True(t, b.called)
}

func TestGroup_JoinsErrors(t *testing.T) {
	t.Parallel()

	errA := errors.New("a failed")
	errB := errors.New("b failed")

	a := &fakeResource{shutdownErr: errA}
	b := &fakeResource{shutdownErr: errB}

	err := lifecycle.Group(a, b).Shutdown(t.Context())

	require.Error(t, err)
	require.ErrorIs(t, err, errA)
	require.ErrorIs(t, err, errB)
}

func TestGroup_SkipsUntypedNil(t *testing.T) {
	t.Parallel()

	a := &fakeResource{}

	err := lifecycle.Group(a, nil).Shutdown(t.Context())

	require.NoError(t, err)
	assert.True(t, a.called)
}

func TestGroup_SkipsTypedNilPointer(t *testing.T) {
	t.Parallel()

	var nilResource *fakeResource

	a := &fakeResource{}

	// nilResource is a nil *fakeResource stored in the Resource interface —
	// this is the classic Go gotcha where the interface value itself is
	// non-nil even though the underlying pointer is nil. Group must not
	// call Shutdown on it (that would nil-pointer-deref inside a real
	// implementation's method body).
	err := lifecycle.Group(a, nilResource).Shutdown(t.Context())

	require.NoError(t, err)
	assert.True(t, a.called)
}

func TestGroup_Empty(t *testing.T) {
	t.Parallel()

	err := lifecycle.Group().Shutdown(t.Context())

	require.NoError(t, err)
}

func TestGroup_ShutsDownInReverseOrderAndForwardsCtx(t *testing.T) {
	t.Parallel()

	var order []string

	a := &orderedResource{name: "a", order: &order}
	b := &orderedResource{name: "b", order: &order}
	c := &orderedResource{name: "c", order: &order}

	ctx := context.WithValue(t.Context(), ctxKey{}, "propagated")

	err := lifecycle.Group(a, b, c).Shutdown(ctx)

	require.NoError(t, err)
	assert.Equal(t, []string{"c", "b", "a"}, order)
}

type checkedResource struct {
	fakeResource

	name string
	err  error
}

func (c *checkedResource) Healthcheck(context.Context) error { return c.err }

func (c *checkedResource) Name() string { return c.name }

func TestGroup_HealthcheckAggregatesMembers(t *testing.T) {
	t.Parallel()

	errDown := errors.New("down")

	var nilChecked *checkedResource

	g := lifecycle.Group(
		&fakeResource{}, // no Healthcheck: skipped
		&checkedResource{name: "ok"},
		&checkedResource{name: "db", err: errDown},
		nilChecked,
	)

	hc, ok := g.(lifecycle.Healthchecker)
	require.True(t, ok)

	err := hc.Healthcheck(t.Context())
	require.ErrorIs(t, err, errDown)
	assert.Contains(t, err.Error(), "db: down")
}

func TestGroup_ShutdownErrorsAreNamed(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")

	err := lifecycle.Group(&checkedResource{name: "cache", fakeResource: fakeResource{shutdownErr: errBoom}}).
		Shutdown(t.Context())
	require.ErrorIs(t, err, errBoom)
	assert.Contains(t, err.Error(), "cache: boom")
}

func TestNameOf(t *testing.T) {
	t.Parallel()

	var nilChecked *checkedResource

	assert.Equal(t, "db", lifecycle.NameOf(&checkedResource{name: "db"}))
	assert.Equal(t, "*lifecycle_test.fakeResource", lifecycle.NameOf(&fakeResource{}))
	assert.Equal(t, "*lifecycle_test.checkedResource", lifecycle.NameOf(nilChecked), "typed nil must not call Name")
}

func TestIsNil(t *testing.T) {
	t.Parallel()

	var (
		nilPtr  *fakeResource
		nilFunc lifecycle.ShutdownFunc
	)

	assert.True(t, lifecycle.IsNil(nil))
	assert.True(t, lifecycle.IsNil(nilPtr))
	assert.True(t, lifecycle.IsNil(nilFunc))
	assert.False(t, lifecycle.IsNil(&fakeResource{}))
	assert.False(t, lifecycle.IsNil(42))
}

func TestFuncAdapters(t *testing.T) {
	t.Parallel()

	errShutdown := errors.New("shutdown")
	errHealth := errors.New("health")

	var r lifecycle.Resource = lifecycle.ShutdownFunc(func(context.Context) error { return errShutdown })

	var h lifecycle.Healthchecker = lifecycle.HealthcheckFunc(func(context.Context) error { return errHealth })

	require.ErrorIs(t, r.Shutdown(t.Context()), errShutdown)
	require.ErrorIs(t, h.Healthcheck(t.Context()), errHealth)
}

func TestGroup_SkipsEmptyNestedGroup(t *testing.T) {
	t.Parallel()

	a := &fakeResource{}

	// An empty nested Group() is itself a Resource backed by a nil slice,
	// not a pointer — isNilResource must still recognize it as nil so it
	// isn't relied upon to merely no-op safely by accident.
	err := lifecycle.Group(a, lifecycle.Group()).Shutdown(t.Context())

	require.NoError(t, err)
	assert.True(t, a.called)
}

func TestGroup_ShutsDownNonEmptyNestedGroup(t *testing.T) {
	t.Parallel()

	inner := &fakeResource{}
	outer := &fakeResource{}

	err := lifecycle.Group(outer, lifecycle.Group(inner)).Shutdown(t.Context())

	require.NoError(t, err)
	assert.True(t, outer.called)
	assert.True(t, inner.called)
}

func TestGroup_ExposesOnlyMembersInterfaces(t *testing.T) {
	t.Parallel()

	var log []string

	plain := lifecycle.Group(&fakeResource{})
	_, isStarter := plain.(lifecycle.Starter)
	_, isChecker := plain.(lifecycle.Healthchecker)

	assert.False(t, isStarter)
	assert.False(t, isChecker)

	both := lifecycle.Group(&member{name: "a", log: &log}, &checkedResource{name: "b"})
	_, isStarter = both.(lifecycle.Starter)
	_, isChecker = both.(lifecycle.Healthchecker)

	assert.True(t, isStarter)
	assert.True(t, isChecker)

	var nilMember *member

	onlyNil := lifecycle.Group(nilMember)
	_, isStarter = onlyNil.(lifecycle.Starter)
	assert.False(t, isStarter, "nil members do not count")
}

func TestGroup_Contract(t *testing.T) {
	t.Parallel()

	lifecycletest.Resource(t, func(*testing.T) lifecycle.Resource {
		var log []string

		return lifecycle.GroupNamed("infra", &member{name: "db", log: &log}, &checkedResource{name: "cache"})
	})
}
