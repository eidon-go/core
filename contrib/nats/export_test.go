package nats

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// ProcessingLimit exposes the MaxProcessingTime a Consumer built from cfg
// applies, 0 for none.
func ProcessingLimit(cfg ConsumerConfig) time.Duration {
	return cfg.processingLimit(ackDeadline(jetstream.ConsumerConfig{AckWait: cfg.AckWait}))
}

// Pauser exposes the pause lengths of WithPauseFor(base).
type Pauser struct{ p *pauser }

// NewPauser returns a Pauser for WithPauseFor(base).
func NewPauser(base time.Duration) Pauser {
	return Pauser{p: &pauser{base: base}}
}

// Begin begins a pause at now and returns its length.
func (p Pauser) Begin(now time.Time) time.Duration { return p.p.begin(now) }

// End ends the pause at now; auto tells whether it ended by itself.
func (p Pauser) End(now time.Time, auto bool) { p.p.end(now, auto) }
