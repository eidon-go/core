package hs256_test

import (
	"strings"
	"testing"
	"time"

	gojwt "github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/jwt"
	"github.com/eidon-go/core/contrib/jwt/hs256"
)

func TestNew_ShortSecret(t *testing.T) {
	t.Parallel()

	_, err := hs256.New(make([]byte, hs256.MinSecretLength-1))
	require.ErrorIs(t, err, jwt.ErrInvalidConfig)
	require.ErrorContains(t, err, "31 bytes")
}

func TestKey_SignVerify(t *testing.T) {
	t.Parallel()

	secret := []byte(strings.Repeat("k", hs256.MinSecretLength))

	key, err := hs256.New(secret)
	require.NoError(t, err)

	secret[0] = 'x' // the key holds a copy

	want := gojwt.RegisteredClaims{Issuer: "auth", ExpiresAt: gojwt.NewNumericDate(time.Now().Add(time.Hour))}

	token, err := key.Sign(t.Context(), want)
	require.NoError(t, err)

	var got gojwt.RegisteredClaims

	require.NoError(t, key.Verify(t.Context(), token, &got))
	assert.Equal(t, "auth", got.Issuer)

	err = key.Verify(t.Context(), token, &got, gojwt.WithIssuer("other"))
	require.ErrorIs(t, err, jwt.ErrInvalidToken)
	require.ErrorIs(t, err, gojwt.ErrTokenInvalidIssuer, "options add checks")

	// signed with the key's secret: only the algorithm is wrong
	hs512, err := gojwt.NewWithClaims(gojwt.SigningMethodHS512, want).
		SignedString([]byte(strings.Repeat("k", hs256.MinSecretLength)))
	require.NoError(t, err)

	err = key.Verify(t.Context(), hs512, &got, gojwt.WithValidMethods([]string{"HS512"}))
	require.ErrorIs(t, err, jwt.ErrInvalidToken)
	require.ErrorIs(t, err, gojwt.ErrTokenUnverifiable, "options cannot widen the accepted algorithms")
}
