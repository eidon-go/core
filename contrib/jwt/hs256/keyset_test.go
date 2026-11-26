package hs256_test

import (
	"strings"
	"testing"

	gojwt "github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/jwt"
	"github.com/eidon-go/core/contrib/jwt/hs256"
)

func secretOf(c string) []byte { return []byte(strings.Repeat(c, hs256.MinSecretLength)) }

func TestNewKeySet_Errors(t *testing.T) {
	t.Parallel()

	_, err := hs256.NewKeySet(hs256.Secret{ID: "a", Secret: []byte("short")})
	require.ErrorIs(t, err, jwt.ErrInvalidConfig)

	_, err = hs256.NewKeySet(hs256.Secret{ID: "a", Secret: secretOf("a")}, hs256.Secret{ID: "a", Secret: secretOf("b")})
	require.ErrorIs(t, err, jwt.ErrInvalidConfig)
	require.ErrorContains(t, err, "duplicate")
}

// The three steps of a rotation from the single Key "old" to "new".
func TestKeySet_Rotation(t *testing.T) {
	t.Parallel()

	claims := func() *gojwt.RegisteredClaims { return &gojwt.RegisteredClaims{Issuer: "auth"} }

	single, err := hs256.New(secretOf("o"))
	require.NoError(t, err)

	legacy, err := single.Sign(t.Context(), claims())
	require.NoError(t, err)

	// 1. Verify with the new secret too; still sign with the old one.
	step1, err := hs256.NewKeySet(
		hs256.Secret{ID: "", Secret: secretOf("o")},
		hs256.Secret{ID: "new", Secret: secretOf("n")},
	)
	require.NoError(t, err)
	require.NoError(t, step1.Verify(t.Context(), legacy, claims()), "tokens without kid")

	// 2. Sign with the new secret; the old tokens stay valid.
	step2, err := hs256.NewKeySet(
		hs256.Secret{ID: "new", Secret: secretOf("n")},
		hs256.Secret{ID: "", Secret: secretOf("o")},
	)
	require.NoError(t, err)

	fresh, err := step2.Sign(t.Context(), claims())
	require.NoError(t, err)

	parsed, _, err := gojwt.NewParser().ParseUnverified(fresh, claims())
	require.NoError(t, err)
	assert.Equal(t, "new", parsed.Header["kid"])

	require.NoError(t, step1.Verify(t.Context(), fresh, claims()), "a pod of step 1 accepts it")
	require.NoError(t, step2.Verify(t.Context(), legacy, claims()))

	// 3. Drop the old secret.
	step3, err := hs256.NewKeySet(hs256.Secret{ID: "new", Secret: secretOf("n")})
	require.NoError(t, err)
	require.NoError(t, step3.Verify(t.Context(), fresh, claims()))

	err = step3.Verify(t.Context(), legacy, claims())
	require.ErrorIs(t, err, jwt.ErrInvalidToken)
	require.ErrorContains(t, err, "unknown key ID")
}

func TestKeySet_RejectsAForgedKid(t *testing.T) {
	t.Parallel()

	set, err := hs256.NewKeySet(
		hs256.Secret{ID: "a", Secret: secretOf("a")},
		hs256.Secret{ID: "b", Secret: secretOf("b")},
	)
	require.NoError(t, err)

	// Signed with "a"'s secret but claiming to be "b".
	token := gojwt.NewWithClaims(gojwt.SigningMethodHS256, gojwt.RegisteredClaims{Issuer: "auth"})
	token.Header["kid"] = "b"

	forged, err := token.SignedString(secretOf("a"))
	require.NoError(t, err)

	require.ErrorIs(t, set.Verify(t.Context(), forged, &gojwt.RegisteredClaims{}), jwt.ErrInvalidToken)
}

func TestKeySet_NotFromNew(t *testing.T) {
	t.Parallel()

	var set *hs256.KeySet

	_, err := set.Sign(t.Context(), gojwt.RegisteredClaims{})
	require.Error(t, err)
	require.ErrorIs(t, set.Verify(t.Context(), "x.y.z", &gojwt.RegisteredClaims{}), jwt.ErrInvalidToken)
}
