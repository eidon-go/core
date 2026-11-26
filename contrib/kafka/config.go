package kafka

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// DefaultMaxPollRecords is ConsumerConfig.MaxPollRecords when it is zero.
const DefaultMaxPollRecords = 500

// Config describes the connection to a Kafka cluster; Client and Producer
// take it as is, Consumer as part of ConsumerConfig. Plain fields: fill it
// from any configuration source. TLS, SASL and other client settings go
// through WithTLSConfig and WithKgoOpts.
type Config struct {
	// Brokers is the seed broker list, "host:port" each. Required.
	Brokers []string `json:"brokers" yaml:"brokers"`

	// ClientID identifies the client to the brokers (logs, quotas).
	// Default: franz-go's "kgo".
	ClientID string `json:"clientId" yaml:"clientId"`
}

// Validate reports every invalid field at once. Each error wraps
// ErrInvalidConfig.
func (c Config) Validate() error {
	var errs []error

	if len(c.Brokers) == 0 {
		errs = append(errs, fmt.Errorf("%w: brokers must not be empty", ErrInvalidConfig))
	}

	for i, b := range c.Brokers {
		if strings.TrimSpace(b) == "" {
			errs = append(errs, fmt.Errorf("%w: brokers[%d] is empty", ErrInvalidConfig, i))
		}
	}

	return errors.Join(errs...)
}

// kgoOpts returns the client options derived from the fields.
func (c Config) kgoOpts() []kgo.Opt {
	opts := []kgo.Opt{kgo.SeedBrokers(c.Brokers...)}
	if c.ClientID != "" {
		opts = append(opts, kgo.ClientID(c.ClientID))
	}

	return opts
}

// clone copies the slices, so that a caller changing them after the
// constructor (Start creates the client later) changes nothing.
func (c Config) clone() Config {
	c.Brokers = slices.Clone(c.Brokers)

	return c
}

// Offset selects where a consumer group starts reading a partition that has
// no committed offset yet.
type Offset string

const (
	// OffsetNewest starts at the end of the partition: only records produced
	// after the group joined are read. It is the default ("").
	OffsetNewest Offset = "newest"

	// OffsetOldest starts at the beginning of the partition.
	OffsetOldest Offset = "oldest"
)

// ConsumerConfig describes a consumer group member.
type ConsumerConfig struct {
	Config `json:",inline" yaml:",inline"`

	// Group is the consumer group ID. Required.
	Group string `json:"group" yaml:"group"`

	// Topics are the topics to consume. Required.
	Topics []string `json:"topics" yaml:"topics"`

	// Offset is where to start a partition without a committed offset:
	// OffsetNewest (default, also "") or OffsetOldest, case-insensitive.
	Offset Offset `json:"offset" yaml:"offset"`

	// MaxPollRecords bounds the records taken per poll. Rebalances wait
	// while a poll's records are handled (retries included), so keep the
	// batch small enough to be handled well within the group's rebalance
	// timeout (franz-go default 60s). Default: DefaultMaxPollRecords.
	MaxPollRecords int `json:"maxPollRecords" yaml:"maxPollRecords"`

	// Concurrency is how many partitions of a poll are handled at once, one
	// goroutine each; the records of a partition are always handled in
	// order. A slow partition then no longer holds up the others. Default
	// (0): 1, one partition after another.
	Concurrency int `json:"concurrency" yaml:"concurrency"`
}

// Validate reports every invalid field at once. Each error wraps
// ErrInvalidConfig.
func (c ConsumerConfig) Validate() error {
	errs := []error{c.Config.Validate()}

	if strings.TrimSpace(c.Group) == "" {
		errs = append(errs, fmt.Errorf("%w: group must not be empty", ErrInvalidConfig))
	}

	if len(c.Topics) == 0 {
		errs = append(errs, fmt.Errorf("%w: topics must not be empty", ErrInvalidConfig))
	}

	for i, t := range c.Topics {
		if strings.TrimSpace(t) == "" {
			errs = append(errs, fmt.Errorf("%w: topics[%d] is empty", ErrInvalidConfig, i))
		}
	}

	if _, err := c.Offset.kgo(); err != nil {
		errs = append(errs, err)
	}

	if c.MaxPollRecords < 0 {
		errs = append(errs, fmt.Errorf("%w: maxPollRecords must not be negative, got %d",
			ErrInvalidConfig, c.MaxPollRecords))
	}

	if c.Concurrency < 0 {
		errs = append(errs, fmt.Errorf("%w: concurrency must not be negative, got %d",
			ErrInvalidConfig, c.Concurrency))
	}

	return errors.Join(errs...)
}

func (o Offset) kgo() (kgo.Offset, error) {
	switch Offset(strings.ToLower(string(o))) {
	case "", OffsetNewest:
		return kgo.NewOffset().AtEnd(), nil
	case OffsetOldest:
		return kgo.NewOffset().AtStart(), nil
	default:
		return kgo.Offset{}, fmt.Errorf("%w: offset must be %q or %q, got %q",
			ErrInvalidConfig, OffsetNewest, OffsetOldest, string(o))
	}
}
