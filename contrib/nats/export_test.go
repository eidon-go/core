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
