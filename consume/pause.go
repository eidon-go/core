package consume

import (
	"sync"
	"time"
)

// PauseBackoff tracks the pauses of a consumer (Paused outcomes) that end by
// themselves after a while, so that a cause that went away needs no manual
// resume. A pause lasts the base length; one that begins within the length of
// the previous pause after that ended by itself lasts twice as long as it, up
// to the ceiling. A pause ended by hand (a Resume) does not lengthen the next
// one.
//
// Handlers of one consumer that pause together share one pause: Begin returns
// the length of the active pause until End. The zero value and a base of 0
// mean pauses that only end by hand: Begin returns 0. Safe for concurrent
// use.
type PauseBackoff struct {
	base, ceiling time.Duration

	mu      sync.Mutex
	active  bool          // a pause began and has not ended
	began   time.Time     // of the active pause
	length  time.Duration // of the active or the last pause
	streak  int           // pauses in a row, each soon after an automatic end
	expired bool          // the last pause ended by itself
	ended   time.Time     // when the last pause ended
}

// NewPauseBackoff returns a PauseBackoff whose pauses last base, doubling for
// pauses in a row up to maxLength (or base, if that is longer). A base of 0
// or less disables it: pauses then only end by hand.
func NewPauseBackoff(base, maxLength time.Duration) *PauseBackoff {
	base = max(base, 0)

	return &PauseBackoff{base: base, ceiling: max(base, maxLength)} //nolint:exhaustruct_v5 // state starts at zero
}

// Enabled reports whether pauses end by themselves (a base above 0).
func (p *PauseBackoff) Enabled() bool { return p != nil && p.base > 0 }

// Begin begins a pause at now and returns its length; while one is active it
// returns that one's length. It returns 0 when p is not Enabled.
func (p *PauseBackoff) Begin(now time.Time) time.Duration {
	if !p.Enabled() {
		return 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.active {
		return p.length
	}

	if p.expired && now.Sub(p.ended) < p.length {
		p.streak++
	} else {
		p.streak = 0
	}

	length := p.base

	for range p.streak {
		if length >= p.ceiling {
			break
		}

		length *= 2
	}

	p.active = true
	p.began = now
	p.length = min(length, p.ceiling)

	return p.length
}

// Remaining returns what is left at now of the active pause: 0 once it is
// over, when none is active, or when p is not Enabled.
func (p *PauseBackoff) Remaining(now time.Time) time.Duration {
	if !p.Enabled() {
		return 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.active {
		return 0
	}

	return max(p.length-now.Sub(p.began), 0)
}

// End ends the active pause at now; expired tells whether it ended by itself
// (only such a pause lengthens the next one). Without an active pause it
// only records how the last one ended.
func (p *PauseBackoff) End(now time.Time, expired bool) {
	if !p.Enabled() {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.active = false
	p.expired = expired
	p.ended = now
}
