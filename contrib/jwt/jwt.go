// Package jwt issues and parses JSON Web Tokens that carry a typed payload,
// on github.com/golang-jwt/jwt/v5.
//
// An AuthService[T] signs tokens with the registered claims set from its
// Config (iss, sub, aud, iat, nbf, exp, jti) plus the caller's payload T, and
// parses them back, checking the signature, the algorithm, the issuer, the
// audience and the time claims. The algorithm is a Signer and a Verifier from
// a sub-package (jwt/hs256), so it can change without touching the callers.
//
// Signing and parsing do no I/O, so nothing here is a lifecycle component:
// build the AuthService once and share it; it is safe for concurrent use.
package jwt

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	gojwt "github.com/golang-jwt/jwt/v5"
)

var (
	// ErrInvalidConfig is wrapped by every error from Config.Validate and by
	// the constructors for invalid arguments.
	ErrInvalidConfig = errors.New("jwt: invalid config")

	// ErrInvalidToken is wrapped, together with the cause from
	// github.com/golang-jwt/jwt/v5, by every error of a Verifier and of
	// AuthService.ParseToken: a malformed token, a bad signature, an
	// unexpected algorithm, a wrong issuer or audience, an expired token….
	ErrInvalidToken = errors.New("jwt: invalid token")

	// ErrTokenExpired is the cause of ErrInvalidToken for a token past its
	// expiry (plus the leeway): the client should refresh it. It is
	// github.com/golang-jwt/jwt/v5's ErrTokenExpired.
	ErrTokenExpired = gojwt.ErrTokenExpired
)

// Signer signs claims into a compact JWT. Implementations live in the
// algorithm sub-packages (jwt/hs256) and are safe for concurrent use.
type Signer interface {
	// Sign returns the signed token. ctx is for signers that call out (a
	// KMS); local ones ignore it.
	Sign(ctx context.Context, claims gojwt.Claims) (string, error)
}

// Verifier parses a compact JWT into claims, a pointer to a struct that
// implements gojwt.Claims (*AuthClaims[T], *gojwt.RegisteredClaims, …).
// Implementations accept only their own algorithm, apply opts after their
// own parser options, and wrap every error with ErrInvalidToken. They are
// safe for concurrent use. A Verifier answers for the signature and the
// algorithm: AuthService checks the claims again after Verify, whatever the
// Verifier did with opts.
type Verifier interface {
	// Verify checks the token and fills claims. ctx is for verifiers that
	// call out (a JWKS endpoint); local ones ignore it.
	Verify(ctx context.Context, token string, claims gojwt.Claims, opts ...gojwt.ParserOption) error
}

// AuthClaims is the claims set of an AuthService: the registered claims and
// the caller's payload under "authInfo".
type AuthClaims[T any] struct {
	gojwt.RegisteredClaims

	// AuthInfo is the caller's payload, encoded as JSON.
	AuthInfo T `json:"authInfo"`
}

// Subjecter is implemented by a payload that names the subject of its token
// (the user's ID): CreateToken puts Subject() into the "sub" claim, where
// gateways, meshes and audit logs look for it. Either T or *T may implement
// it.
type Subjecter interface {
	Subject() string
}

// subjectOf returns info.Subject() when info or &info is a Subjecter.
func subjectOf[T any](info T) (string, bool) {
	if s, ok := any(info).(Subjecter); ok {
		return s.Subject(), true
	}

	if s, ok := any(&info).(Subjecter); ok {
		return s.Subject(), true
	}

	return "", false
}

// isSubjecter reports whether T or *T implements Subjecter.
func isSubjecter[T any]() bool {
	var zero T
	if _, ok := any(zero).(Subjecter); ok {
		return true
	}

	_, ok := any(&zero).(Subjecter)

	return ok
}

// Option configures NewAuthService.
type Option func(*options)

type options struct {
	now func() time.Time
}

// WithClock replaces time.Now for the time claims set by CreateToken and
// checked by ParseToken (tests, a synchronized clock). A nil clock is
// ignored.
func WithClock(now func() time.Time) Option {
	return func(o *options) {
		if now != nil {
			o.now = now
		}
	}
}

// AuthService issues and parses tokens that carry a payload T. It is safe
// for concurrent use.
type AuthService[T any] struct {
	cfg       Config
	signer    Signer
	verifier  Verifier
	now       func() time.Time
	parse     []gojwt.ParserOption
	validator *gojwt.Validator
}

// NewAuthService validates cfg and builds an AuthService on signer and
// verifier, usually one key of an algorithm sub-package:
//
//	key, err := hs256.New(secret)
//	svc, err := jwt.NewAuthService[Session](cfg, key, key)
func NewAuthService[T any](cfg Config, signer Signer, verifier Verifier, opts ...Option) (*AuthService[T], error) {
	err := cfg.Validate()

	if signer == nil {
		err = errors.Join(err, fmt.Errorf("%w: nil Signer", ErrInvalidConfig))
	}

	if verifier == nil {
		err = errors.Join(err, fmt.Errorf("%w: nil Verifier", ErrInvalidConfig))
	}

	if isSubjecter[T]() && cfg.RequiredSubject != "" {
		err = errors.Join(err, fmt.Errorf("%w: RequiredSubject with a Subjecter payload", ErrInvalidConfig))
	}

	if err != nil {
		return nil, err
	}

	o := options{now: time.Now}

	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}

	cfg = cfg.withDefaults()

	parse := []gojwt.ParserOption{
		gojwt.WithIssuer(cfg.Issuer),
		gojwt.WithAudience(cfg.Audience...),
		gojwt.WithExpirationRequired(),
		gojwt.WithIssuedAt(),
		gojwt.WithLeeway(cfg.Leeway),
		gojwt.WithTimeFunc(o.now),
	}

	if cfg.RequiredSubject != "" {
		parse = append(parse, gojwt.WithSubject(cfg.RequiredSubject))
	}

	return &AuthService[T]{
		cfg:       cfg,
		signer:    signer,
		verifier:  verifier,
		now:       o.now,
		parse:     parse,
		validator: gojwt.NewValidator(parse...),
	}, nil
}

// CreateToken signs a new token carrying info. It sets iss from the Config,
// sub to info.Subject() when info (or &info) is a Subjecter (else Config.RequiredSubject),
// aud to Config.Audience, iat and nbf to now, exp to now + TTL and a random
// jti.
func (s *AuthService[T]) CreateToken(ctx context.Context, info T) (string, error) {
	now := s.now()

	subject := s.cfg.RequiredSubject
	if sub, ok := subjectOf(info); ok {
		subject = sub
	}

	claims := &AuthClaims[T]{
		RegisteredClaims: gojwt.RegisteredClaims{
			Issuer:    s.cfg.Issuer,
			Subject:   subject,
			Audience:  gojwt.ClaimStrings(s.cfg.Audience),
			ExpiresAt: gojwt.NewNumericDate(now.Add(s.cfg.TTL)),
			NotBefore: gojwt.NewNumericDate(now),
			IssuedAt:  gojwt.NewNumericDate(now),
			ID:        rand.Text(),
		},
		AuthInfo: info,
	}

	token, err := s.signer.Sign(ctx, claims)
	if err != nil {
		return "", fmt.Errorf("jwt: sign: %w", err)
	}

	return token, nil
}

// ParseToken verifies token and returns its payload. See ParseClaims.
//
//nolint:ireturn // T is the caller's payload type
func (s *AuthService[T]) ParseToken(ctx context.Context, token string) (T, error) {
	claims, err := s.ParseClaims(ctx, token)
	if err != nil {
		var zero T

		return zero, err
	}

	return claims.AuthInfo, nil
}

// ParseClaims verifies token and returns all of its claims (the jti for a
// revocation list, the expiry for a refresh). The token must be signed by
// the Verifier's algorithm and key, issued by Config.Issuer for one of
// Config.Audience (and for Config.RequiredSubject when set), carry an expiry,
// and be within its iat/nbf/exp window, widened by Config.Leeway. The
// Verifier answers for the signature; the claims are checked here again
// after it, whatever it did with the parser options it was given. Every
// error wraps ErrInvalidToken; an expired token also wraps ErrTokenExpired.
func (s *AuthService[T]) ParseClaims(ctx context.Context, token string) (*AuthClaims[T], error) {
	var claims AuthClaims[T]

	if err := s.verifier.Verify(ctx, token, &claims, s.parse...); err != nil {
		return nil, invalid(err)
	}

	if err := s.validator.Validate(&claims); err != nil {
		return nil, invalid(err)
	}

	return &claims, nil
}

// invalid wraps err with ErrInvalidToken unless it already is one.
func invalid(err error) error {
	if errors.Is(err, ErrInvalidToken) {
		return err
	}

	return fmt.Errorf("%w: %w", ErrInvalidToken, err)
}
