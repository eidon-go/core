package rabbitmq

import "time"

// MaxPause bounds the automatic pauses of WithPauseFor (unless its duration
// is longer). It stays below the broker's default consumer_timeout (30m),
// which closes a channel that holds a message unacked for longer.
const MaxPause = 15 * time.Minute

// WithPauseFor makes a pause (ErrorActionPause) end by itself after d, so
// that one cause that went away does not need Consumer.Resume. A pause that
// begins within the length of the previous one after it ended by itself lasts
// twice as long, up to MaxPause; Resume still ends a pause at once. The
// consumer holds the paused message (and the ones prefetched with it) unacked
// for the pause's length before the broker requeues it, so the other
// consumers of the queue do not get it at once. Default 0: a pause lasts
// until Resume, and the message is held for the redelivery delay. Consumer
// only.
func WithPauseFor(d time.Duration) Option {
	return func(o *options) { o.pauseFor = max(d, 0) }
}
