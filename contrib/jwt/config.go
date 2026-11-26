package jwt

import (
	"errors"
	"fmt"
	"slices"
	"time"
)

// DefaultLeeway is the clock skew tolerated by default: services on
// different nodes disagree on the time by up to a few hundred milliseconds,
// enough to reject a token as "not valid yet" the moment it is issued.
const DefaultLeeway = 30 * time.Second

// Config configures an AuthService.
type Config struct {
	// Issuer is the "iss" claim of the issued tokens, required in the parsed
	// ones. Required: usually the service name.
	Issuer string `json:"issuer" yaml:"issuer"`

	// Audience is the "aud" claim of the issued tokens; a parsed token must
	// name at least one of them. Default: Issuer alone.
	Audience []string `json:"audience" yaml:"audience"`

	// RequiredSubject, when set, is the "sub" claim of every issued token and
	// required in the parsed ones — for tokens of one kind ("service").
	// Default empty: "sub" comes from a payload that implements Subjecter
	// (the user's ID), and any one is accepted. It cannot be combined with a
	// Subjecter payload.
	RequiredSubject string `json:"requiredSubject" yaml:"requiredSubject"`

	// TTL is how long an issued token is valid ("exp" = now + TTL).
	// Required, at least a second: the time claims are whole seconds
	// (github.com/golang-jwt/jwt/v5's TimePrecision), so a shorter TTL could
	// issue a token that is expired at once.
	TTL time.Duration `json:"ttl" yaml:"ttl"`

	// Leeway is the clock skew tolerated when checking "exp", "nbf" and
	// "iat". Default (0): DefaultLeeway.
	Leeway time.Duration `json:"leeway" yaml:"leeway"`
}

// Validate reports every problem of c at once; each wraps ErrInvalidConfig.
func (c Config) Validate() error {
	var errs []error

	if c.Issuer == "" {
		errs = append(errs, fmt.Errorf("%w: Issuer is required", ErrInvalidConfig))
	}

	if slices.Contains(c.Audience, "") {
		errs = append(errs, fmt.Errorf("%w: Audience must not contain empty strings", ErrInvalidConfig))
	}

	if c.TTL < time.Second {
		errs = append(errs, fmt.Errorf("%w: TTL must be at least 1s", ErrInvalidConfig))
	}

	if c.Leeway < 0 {
		errs = append(errs, fmt.Errorf("%w: Leeway must not be negative", ErrInvalidConfig))
	}

	return errors.Join(errs...)
}

func (c Config) withDefaults() Config {
	if len(c.Audience) == 0 {
		c.Audience = []string{c.Issuer}
	} else {
		c.Audience = slices.Clone(c.Audience)
	}

	if c.Leeway == 0 {
		c.Leeway = DefaultLeeway
	}

	return c
}
