// Package hs256 signs and verifies JSON Web Tokens with HMAC-SHA256 and a
// shared secret: a Key is both the jwt.Signer and the jwt.Verifier of a
// jwt.AuthService.
//
//	key, err := hs256.New(secret)
//	svc, err := jwt.NewAuthService[Session](cfg, key, key)
//
// Every service that verifies the tokens holds the secret and can therefore
// issue them too.
package hs256

import (
	"context"
	"errors"
	"fmt"

	gojwt "github.com/golang-jwt/jwt/v5"

	"github.com/eidon-go/core/contrib/jwt"
)

// MinSecretLength is the shortest secret New accepts, in bytes: the size of
// the SHA-256 output, as RFC 7518 (section 3.2) requires for HS256.
const MinSecretLength = 32

// Key signs and verifies HS256 tokens with one secret. It is safe for
// concurrent use.
type Key struct {
	secret []byte
}

// New returns a Key for secret, which it copies. The secret must be at least
// MinSecretLength bytes of random data (not a password); it is never logged.
func New(secret []byte) (*Key, error) {
	if len(secret) < MinSecretLength {
		return nil, fmt.Errorf("%w: HS256 secret is %d bytes, at least %d required",
			jwt.ErrInvalidConfig, len(secret), MinSecretLength)
	}

	return &Key{secret: append([]byte(nil), secret...)}, nil
}

// Sign signs claims with HS256. ctx is not used. A Key not made by New (a
// zero or nil Key) returns an error. Implements jwt.Signer.
func (k *Key) Sign(_ context.Context, claims gojwt.Claims) (string, error) {
	if !k.valid() {
		return "", fmt.Errorf("hs256: sign: %w", errNoKey)
	}

	token, err := gojwt.NewWithClaims(gojwt.SigningMethodHS256, claims).SignedString(k.secret)
	if err != nil {
		return "", fmt.Errorf("hs256: sign: %w", err)
	}

	return token, nil
}

// Verify parses token into claims, accepting only HS256 signed with this
// key; opts add checks (issuer, audience, leeway, …). Every error wraps
// jwt.ErrInvalidToken and the cause from github.com/golang-jwt/jwt/v5. ctx
// is not used. Implements jwt.Verifier.
func (k *Key) Verify(_ context.Context, token string, claims gojwt.Claims, opts ...gojwt.ParserOption) error {
	parse := append([]gojwt.ParserOption{gojwt.WithValidMethods([]string{gojwt.SigningMethodHS256.Alg()})}, opts...)

	if _, err := gojwt.ParseWithClaims(token, claims, k.keyfunc, parse...); err != nil {
		return fmt.Errorf("%w: %w", jwt.ErrInvalidToken, err)
	}

	return nil
}

var (
	// errAlgorithm rejects a token of another algorithm in keyfunc, so that
	// a WithValidMethods in Verify's opts cannot widen the accepted
	// algorithms.
	errAlgorithm = errors.New("hs256: token is not signed with HS256")

	// errNoKey is returned by a Key not made by New, which would otherwise
	// sign and verify with an empty secret.
	errNoKey = errors.New("hs256: key not created with New")
)

func (k *Key) valid() bool { return k != nil && len(k.secret) >= MinSecretLength }

func (k *Key) keyfunc(token *gojwt.Token) (any, error) {
	if !k.valid() {
		return nil, errNoKey
	}

	if token.Method != gojwt.SigningMethodHS256 {
		return nil, errAlgorithm
	}

	return k.secret, nil
}

// Compile-time checks.
var (
	_ jwt.Signer   = (*Key)(nil)
	_ jwt.Verifier = (*Key)(nil)
)
