package app_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/app"
	"github.com/eidon-go/core/lifecycle"
)

// panickingChecker panics in Healthcheck.
type panickingChecker struct{}

func (panickingChecker) Name() string                   { return "broken" }
func (panickingChecker) Shutdown(context.Context) error { return nil }
func (panickingChecker) Healthcheck(context.Context) error {
	panic("nil map")
}

func TestHealthReport_RecoversPanickingCheck(t *testing.T) {
	t.Parallel()

	a := newApp()
	a.Add(panickingChecker{}, &checker{name: "ok"})

	var report []app.HealthResult

	require.NotPanics(t, func() { report = a.HealthReport(t.Context()) })
	require.Len(t, report, 2)

	assert.Equal(t, "broken", report[0].Name)
	require.ErrorIs(t, report[0].Err, app.ErrHealthcheckPanicked)
	require.ErrorContains(t, report[0].Err, "nil map")

	assert.Equal(t, "ok", report[1].Name)
	assert.NoError(t, report[1].Err, "other checks are unaffected")
}

func TestHealthReport_GroupWithoutCheckersIsNotListed(t *testing.T) {
	t.Parallel()

	a := newApp()
	a.Add(lifecycle.GroupNamed("infra", &resource{name: "cache", rec: &recorder{}}))

	assert.Empty(t, a.HealthReport(t.Context()))
}

// barrierChecker blocks until all checkers of a barrier have entered, which
// only happens if they run concurrently.
type barrierChecker struct {
	name    string
	entered *sync.WaitGroup
}

func (b *barrierChecker) Name() string                   { return b.name }
func (b *barrierChecker) Shutdown(context.Context) error { return nil }

func (b *barrierChecker) Healthcheck(ctx context.Context) error {
	b.entered.Done()

	done := make(chan struct{})

	go func() {
		b.entered.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestHealthReport_RunsChecksConcurrently(t *testing.T) {
	t.Parallel()

	var entered sync.WaitGroup

	entered.Add(3)

	a := newApp()
	a.Add(
		&barrierChecker{name: "a", entered: &entered},
		&barrierChecker{name: "b", entered: &entered},
		&barrierChecker{name: "c", entered: &entered},
	)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	for _, r := range a.HealthReport(ctx) {
		require.NoError(t, r.Err, "%s: checks must run concurrently", r.Name)
	}
}
