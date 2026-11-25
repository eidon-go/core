package lifecycle_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/lifecycle"
)

// member records Start and Shutdown calls into a shared log.
type member struct {
	name     string
	log      *[]string
	startErr error
}

func (m *member) Name() string { return m.name }

func (m *member) Start(context.Context) error {
	*m.log = append(*m.log, "start "+m.name)

	return m.startErr
}

func (m *member) Shutdown(context.Context) error {
	*m.log = append(*m.log, "shutdown "+m.name)

	return nil
}

type runner struct{ member }

func (*runner) Run(context.Context) error { return nil }

type failingStarter struct{ fakeResource }

func (*failingStarter) Start(context.Context) error { return errors.New("refused") }

func TestGroup_StartInOrder(t *testing.T) {
	t.Parallel()

	var log []string

	g := lifecycle.Group(&member{name: "a", log: &log}, &fakeResource{}, &member{name: "b", log: &log})

	s, ok := g.(lifecycle.Starter)
	require.True(t, ok)
	require.NoError(t, s.Start(t.Context()))
	assert.Equal(t, []string{"start a", "start b"}, log)
}

// A failed Start stops at the failing member and leaves the rollback to the
// caller's Shutdown, which releases every member, started or not.
func TestGroup_StartFailureStopsAndShutdownReleasesAll(t *testing.T) {
	t.Parallel()

	var log []string

	errStart := errors.New("refused")
	plain := &fakeResource{}

	g := lifecycle.Group(
		&member{name: "a", log: &log},
		plain,
		&member{name: "b", log: &log, startErr: errStart},
		&member{name: "c", log: &log},
	)

	s, _ := g.(lifecycle.Starter)
	err := s.Start(t.Context())

	require.ErrorIs(t, err, errStart)
	require.ErrorContains(t, err, "start b: refused")
	assert.Equal(t, []string{"start a", "start b"}, log, "c is never started, nothing is shut down yet")
	assert.False(t, plain.called)

	require.NoError(t, g.Shutdown(t.Context()))
	assert.Equal(t, []string{"start a", "start b", "shutdown c", "shutdown b", "shutdown a"}, log)
	assert.True(t, plain.called)
}

func TestGroup_Name(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "group", lifecycle.NameOf(lifecycle.Group()))
	assert.Equal(t, "infra", lifecycle.NameOf(lifecycle.GroupNamed("infra")))
}

func TestGroup_PanicsOnRunner(t *testing.T) {
	t.Parallel()

	var log []string

	assert.PanicsWithValue(t,
		"lifecycle: Group: member 1 (r) is a Runner; add runners to app.App directly",
		func() { lifecycle.Group(&fakeResource{}, &runner{member{name: "r", log: &log}}) })

	var nilRunner *runner

	assert.NotPanics(t, func() { lifecycle.Group(nilRunner) }, "a nil member is skipped, whatever its type")
}

// A failed Start shuts nothing down with its own (possibly expired) ctx; the
// caller's Shutdown does, with a fresh deadline.
func TestGroup_FailedStartDoesNotShutDown(t *testing.T) {
	t.Parallel()

	before, failing, after := &fakeResource{}, &failingStarter{}, &fakeResource{}
	g := lifecycle.Group(before, failing, after)

	s, ok := g.(lifecycle.Starter)
	require.True(t, ok)
	require.ErrorContains(t, s.Start(t.Context()), "refused")
	assert.False(t, before.called || failing.called || after.called)

	require.NoError(t, g.Shutdown(t.Context()))
	assert.True(t, before.called && failing.called && after.called, "Shutdown releases every member")
}
