package nats

import "time"

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
