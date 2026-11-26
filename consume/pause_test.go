package consume_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/eidon-go/core/consume"
)

func TestPauseBackoff_Lengths(t *testing.T) {
	t.Parallel()

	t.Run("doubles after a quick automatic end, up to the ceiling", func(t *testing.T) {
		t.Parallel()

		p := consume.NewPauseBackoff(5*time.Minute, 15*time.Minute)
		now := time.Now()
		got := make([]time.Duration, 0, 4)

		for range 4 {
			length := p.Begin(now)
			got = append(got, length)
			assert.Equal(t, length, p.Remaining(now))

			now = now.Add(length)
			assert.Zero(t, p.Remaining(now))
			p.End(now, true)

			now = now.Add(time.Second) // paused again soon after
		}

		assert.Equal(t, []time.Duration{5 * time.Minute, 10 * time.Minute, 15 * time.Minute, 15 * time.Minute}, got)
	})

	t.Run("handlers that pause together share one pause", func(t *testing.T) {
		t.Parallel()

		p := consume.NewPauseBackoff(time.Second, time.Minute)
		now := time.Now()

		assert.Equal(t, time.Second, p.Begin(now))
		p.End(now.Add(time.Second), true)

		now = now.Add(1500 * time.Millisecond)
		assert.Equal(t, 2*time.Second, p.Begin(now))
		assert.Equal(t, 2*time.Second, p.Begin(now.Add(time.Millisecond)))
		assert.Equal(t, 1999*time.Millisecond, p.Remaining(now.Add(time.Millisecond)))
	})

	t.Run("resets after a manual end or a long run", func(t *testing.T) {
		t.Parallel()

		p := consume.NewPauseBackoff(time.Second, time.Minute)
		now := time.Now()

		p.Begin(now)
		p.End(now, false)
		assert.Equal(t, time.Second, p.Begin(now), "a manual end does not count")

		p.End(now, true)
		assert.Equal(t, time.Second, p.Begin(now.Add(time.Minute)), "ran longer than the pause")
	})

	t.Run("a base above the ceiling is kept", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, time.Hour, consume.NewPauseBackoff(time.Hour, time.Minute).Begin(time.Now()))
	})

	t.Run("disabled", func(t *testing.T) {
		t.Parallel()

		for _, p := range []*consume.PauseBackoff{consume.NewPauseBackoff(0, time.Minute), nil, {}} {
			assert.False(t, p.Enabled())
			assert.Zero(t, p.Begin(time.Now()))
			assert.Zero(t, p.Remaining(time.Now()))
			p.End(time.Now(), true)
		}
	})
}
