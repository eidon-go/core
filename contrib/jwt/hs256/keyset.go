package hs256

import (
	"context"
	"fmt"

	gojwt "github.com/golang-jwt/jwt/v5"

	"github.com/eidon-go/core/contrib/jwt"
)

// Secret is one secret of a KeySet and its ID, the "kid" header of the
// tokens it signs.
type Secret struct {
	// ID names the secret in the "kid" header. The empty ID is the secret of
	// tokens without "kid" — those of a single Key, during a migration to a
	// KeySet.
	ID string
	// Secret is at least MinSecretLength bytes of random data.
	Secret []byte
}

// KeySet signs and verifies HS256 tokens with several secrets, for a rotation
// without downtime: it signs with one secret, naming it in the "kid" header,
// and verifies a token with the secret its "kid" names. Rotate in three
// deploys: add the new secret for verifying; then sign with it; drop the old
// one once the tokens it signed have expired. It is safe for concurrent use.
type KeySet struct {
	signing Secret
	byID    map[string][]byte
}

// NewKeySet returns a KeySet that signs with signing and verifies with it and
// with verifyOnly. It copies the secrets. Every secret must be at least
// MinSecretLength bytes, and the IDs must differ.
func NewKeySet(signing Secret, verifyOnly ...Secret) (*KeySet, error) {
	set := &KeySet{byID: make(map[string][]byte, 1+len(verifyOnly))} //nolint:exhaustruct_v5 // signing below

	for _, secret := range append([]Secret{signing}, verifyOnly...) {
		if len(secret.Secret) < MinSecretLength {
			return nil, fmt.Errorf("%w: HS256 secret %q is %d bytes, at least %d required",
				jwt.ErrInvalidConfig, secret.ID, len(secret.Secret), MinSecretLength)
		}

		if _, dup := set.byID[secret.ID]; dup {
			return nil, fmt.Errorf("%w: duplicate HS256 secret ID %q", jwt.ErrInvalidConfig, secret.ID)
		}

		set.byID[secret.ID] = append([]byte(nil), secret.Secret...)
	}

	set.signing = Secret{ID: signing.ID, Secret: set.byID[signing.ID]}

	return set, nil
}

// Sign signs claims with the signing secret, its ID in the "kid" header
// (none for the empty ID). ctx is not used. Implements jwt.Signer.
func (s *KeySet) Sign(_ context.Context, claims gojwt.Claims) (string, error) {
	if s == nil || s.byID == nil {
		return "", fmt.Errorf("hs256: sign: %w", errNoKey)
	}

	token := gojwt.NewWithClaims(gojwt.SigningMethodHS256, claims)
	if s.signing.ID != "" {
		token.Header["kid"] = s.signing.ID
	}

	signed, err := token.SignedString(s.signing.Secret)
	if err != nil {
		return "", fmt.Errorf("hs256: sign: %w", err)
	}

	return signed, nil
}

// Verify parses token into claims, accepting only HS256 signed with the
// secret its "kid" names; opts add checks. Every error wraps
// jwt.ErrInvalidToken. ctx is not used. Implements jwt.Verifier.
func (s *KeySet) Verify(_ context.Context, token string, claims gojwt.Claims, opts ...gojwt.ParserOption) error {
	return verify(token, claims, s.keyfunc, opts)
}

func (s *KeySet) keyfunc(token *gojwt.Token) (any, error) {
	if s == nil || s.byID == nil {
		return nil, errNoKey
	}

	if token.Method != gojwt.SigningMethodHS256 {
		return nil, errAlgorithm
	}

	kid, _ := token.Header["kid"].(string)

	secret, ok := s.byID[kid]
	if !ok {
		return nil, fmt.Errorf("%w %q", errUnknownKey, kid)
	}

	return secret, nil
}

// Compile-time checks.
var (
	_ jwt.Signer   = (*KeySet)(nil)
	_ jwt.Verifier = (*KeySet)(nil)
)
