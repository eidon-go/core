package nats

import (
	"sync"
	"time"
)

// MaxPause bounds the automatic pauses of WithPauseFor (unless its duration
// is longer).
const MaxPause = 15 * time.Minute

// WithPauseFor makes a pause (ErrorActionPause) end by itself after d, so
// that one cause that went away does not need Consumer.Resume. A pause that
// begins within the length of the previous one after it ended by itself lasts
// twice as long, up to MaxPause; Resume still ends a pause at once. The
// paused message is given back to the server with a delay of the pause's
// length, so the other instances do not get it at once. Default 0: a pause
// lasts until Resume, and the message is given back with the redelivery
// delay. Consumer only.
func WithPauseFor(d time.Duration) Option {
	return func(o *options) { o.pauseFor = max(d, 0) }
}

// pauser tracks the pauses of a consumer and their lengths under
// WithPauseFor. Safe for concurrent use: handlers begin a pause, the loop
// waits for and ends it.
type pauser struct {
	base time.Duration // WithPauseFor; 0: only Resume ends a pause

	mu        sync.Mutex
	active    bool          // a pause began and has not ended
	length    time.Duration // of the active or the last pause
	streak    int           // pauses in a row, each soon after an automatic end
	auto      bool          // the last pause ended by itself
	resumedAt time.Time
}

// begin begins a pause, once while one is active, and returns its length;
// 0 without WithPauseFor.
func (p *pauser) begin(now time.Time) time.Duration {
	if p.base <= 0 {
		return 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.active {
		return p.length
	}

	if p.auto && now.Sub(p.resumedAt) < p.length {
		p.streak++
	} else {
		p.streak = 0
	}

	ceiling := max(p.base, MaxPause)
	length := p.base

	for range p.streak {
		if length >= ceiling {
			break
		}

		length *= 2
	}

	p.active = true
	p.length = min(length, ceiling)

	return p.length
}

// current returns the length of the active pause; 0 when none is active or
// without WithPauseFor.
func (p *pauser) current() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.active {
		return 0
	}

	return p.length
}

// end ends the active pause; auto tells whether it ended by itself.
func (p *pauser) end(now time.Time, auto bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.active = false
	p.auto = auto
	p.resumedAt = now
}
