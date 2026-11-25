package lifecycle_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/lifecycle"
	"github.com/eidon-go/core/lifecycle/lifecycletest"
)

// ctxShutdown blocks in Shutdown until its ctx is done.
type ctxShutdown struct{ called bool }

func (c *ctxShutdown) Shutdown(ctx context.Context) error {
	c.called = true

	<-ctx.Done()

	return ctx.Err()
}

type failingStarter struct{ fakeResource }

func (*failingStarter) Start(context.Context) error { return errors.New("refused") }

// The rollback is bounded by the ctx passed to Start: with a start timeout,
// Start returns instead of hanging on a member that waits for its ctx.
func TestGroup_RollbackBoundedByStartContext(t *testing.T) {
	t.Parallel()

	slow := &ctxShutdown{}
	g := lifecycle.Group(slow, &failingStarter{})

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	s, ok := g.(lifecycle.Starter)
	require.True(t, ok)

	done := make(chan error, 1)

	go func() { done <- s.Start(ctx) }()

	select {
	case err := <-done:
		require.ErrorContains(t, err, "refused")
		assert.True(t, slow.called)
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return: rollback is not bounded")
	}
}

func TestGroup_RollbackIncludesLaterMembersWithoutStart(t *testing.T) {
	t.Parallel()

	before, after := &fakeResource{}, &fakeResource{}
	g := lifecycle.Group(before, &failingStarter{}, after)

	s, _ := g.(lifecycle.Starter)
	require.Error(t, s.Start(t.Context()))
	assert.True(t, before.called)
	assert.True(t, after.called, "a later member without Start holds resources too")
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
