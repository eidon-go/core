package jwt_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	gojwt "github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eidon-go/core/contrib/jwt"
	"github.com/eidon-go/core/contrib/jwt/hs256"
)

type session struct {
	UserID string   `json:"userId"`
	Roles  []string `json:"roles"`
}

var errSign = errors.New("kms unavailable")

func secret() []byte { return []byte(strings.Repeat("s", hs256.MinSecretLength)) }

func otherSecret() []byte { return []byte(strings.Repeat("o", hs256.MinSecretLength)) }

// start is the time tokens are issued at.
func start() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }

// clock is a settable time source.
type clock struct{ now time.Time }

func (c *clock) Now() time.Time { return c.now }

func newKey(t *testing.T, s []byte) *hs256.Key {
	t.Helper()

	key, err := hs256.New(s)
	require.NoError(t, err)

	return key
}

func newService(t *testing.T, cfg jwt.Config, key *hs256.Key, clk *clock) *jwt.AuthService[session] {
	t.Helper()

	svc, err := jwt.NewAuthService[session](cfg, key, key, jwt.WithClock(clk.Now), jwt.WithClock(nil), nil)
	require.NoError(t, err)

	return svc
}

func baseConfig() jwt.Config { return jwt.Config{Issuer: "auth", TTL: time.Hour} }

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  jwt.Config
		want []string
	}{
		{name: "valid", cfg: baseConfig()},
		{
			name: "valid with all fields",
			cfg:  jwt.Config{Issuer: "auth", Audience: []string{"api"}, Subject: "user", TTL: time.Second, Leeway: 1},
		},
		{name: "empty", cfg: jwt.Config{}, want: []string{"Issuer", "TTL"}},
		{
			name: "TTL below the claims' precision",
			cfg:  jwt.Config{Issuer: "auth", TTL: 999 * time.Millisecond},
			want: []string{"TTL"},
		},
		{
			name: "invalid values",
			cfg:  jwt.Config{Issuer: "auth", Audience: []string{"api", ""}, TTL: -1, Leeway: -1},
			want: []string{"Audience", "TTL", "Leeway"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.Validate()
			if len(tt.want) == 0 {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, jwt.ErrInvalidConfig)

			for _, field := range tt.want {
				assert.ErrorContains(t, err, field)
			}
		})
	}
}

func TestNewAuthService_Errors(t *testing.T) {
	t.Parallel()

	key := newKey(t, secret())

	_, err := jwt.NewAuthService[session](jwt.Config{}, nil, nil)
	require.ErrorIs(t, err, jwt.ErrInvalidConfig)
	require.ErrorContains(t, err, "Issuer")
	require.ErrorContains(t, err, "nil Signer")
	require.ErrorContains(t, err, "nil Verifier")

	_, err = jwt.NewAuthService[session](baseConfig(), key, nil)
	require.ErrorIs(t, err, jwt.ErrInvalidConfig)
}

func TestAuthService_RoundTrip(t *testing.T) {
	t.Parallel()

	clk := &clock{now: start()}
	svc := newService(t, jwt.Config{Issuer: "auth", Subject: "user", TTL: time.Hour}, newKey(t, secret()), clk)
	info := session{UserID: "42", Roles: []string{"admin"}}

	token, err := svc.CreateToken(t.Context(), info)
	require.NoError(t, err)

	got, err := svc.ParseToken(t.Context(), token)
	require.NoError(t, err)
	assert.Equal(t, info, got)

	claims, err := svc.ParseClaims(t.Context(), token)
	require.NoError(t, err)
	assert.Equal(t, "auth", claims.Issuer)
	assert.Equal(t, "user", claims.Subject)
	assert.Equal(t, gojwt.ClaimStrings{"auth"}, claims.Audience, "the audience defaults to the issuer")
	assert.Equal(t, start(), claims.IssuedAt.UTC())
	assert.Equal(t, start(), claims.NotBefore.UTC())
	assert.Equal(t, start().Add(time.Hour), claims.ExpiresAt.UTC())
	assert.NotEmpty(t, claims.ID)

	again, err := svc.CreateToken(t.Context(), info)
	require.NoError(t, err)

	other, err := svc.ParseClaims(t.Context(), again)
	require.NoError(t, err)
	assert.NotEqual(t, claims.ID, other.ID, "every token has its own jti")
}

func TestAuthService_TimeClaims(t *testing.T) {
	t.Parallel()

	key := newKey(t, secret())

	tests := []struct {
		name   string
		leeway time.Duration
		shift  time.Duration // of the clock between issuing and parsing
		want   error
	}{
		{name: "just before expiry", shift: time.Hour - time.Second},
		{name: "expired", shift: time.Hour + time.Second, want: jwt.ErrTokenExpired},
		{name: "expired within leeway", leeway: time.Minute, shift: time.Hour + time.Second},
		{name: "issued in the future", shift: -time.Second, want: gojwt.ErrTokenUsedBeforeIssued},
		{name: "issued in the future within leeway", leeway: time.Minute, shift: -time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			clk := &clock{now: start()}
			cfg := baseConfig()
			cfg.Leeway = tt.leeway
			svc := newService(t, cfg, key, clk)

			token, err := svc.CreateToken(t.Context(), session{UserID: "1"})
			require.NoError(t, err)

			clk.now = start().Add(tt.shift)

			_, err = svc.ParseToken(t.Context(), token)
			if tt.want == nil {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, jwt.ErrInvalidToken)
			require.ErrorIs(t, err, tt.want)
		})
	}
}

func TestAuthService_Rejects(t *testing.T) {
	t.Parallel()

	clk := &clock{now: start()}
	key := newKey(t, secret())
	verifier := newService(t, jwt.Config{
		Issuer: "auth", Audience: []string{"api", "web"}, Subject: "user",
		TTL: time.Hour,
	}, key, clk)

	tests := []struct {
		name   string
		issuer *jwt.AuthService[session]
		token  string // used when issuer is nil
		want   error
	}{
		{
			name: "another issuer",
			issuer: newService(
				t,
				jwt.Config{Issuer: "other", Audience: []string{"api"}, Subject: "user", TTL: time.Hour},
				key,
				clk,
			),
			want: gojwt.ErrTokenInvalidIssuer,
		},
		{
			name: "another audience",
			issuer: newService(
				t,
				jwt.Config{Issuer: "auth", Audience: []string{"admin"}, Subject: "user", TTL: time.Hour},
				key,
				clk,
			),
			want: gojwt.ErrTokenInvalidAudience,
		},
		{
			name: "another subject",
			issuer: newService(
				t,
				jwt.Config{Issuer: "auth", Audience: []string{"web"}, Subject: "robot", TTL: time.Hour},
				key,
				clk,
			),
			want: gojwt.ErrTokenInvalidSubject,
		},
		{
			name: "another key",
			issuer: newService(
				t,
				jwt.Config{Issuer: "auth", Audience: []string{"web"}, Subject: "user", TTL: time.Hour},
				newKey(t, otherSecret()),
				clk,
			),
			want: gojwt.ErrTokenSignatureInvalid,
		},
		{
			name:  "malformed",
			token: "not.a.token",
			want:  gojwt.ErrTokenMalformed,
		},
		{
			name:  "no expiry",
			token: sign(t, gojwt.SigningMethodHS256, claimsWithoutExpiry(), secret()),
			want:  gojwt.ErrTokenRequiredClaimMissing,
		},
		{
			name:  "another HMAC algorithm with the same secret",
			token: sign(t, gojwt.SigningMethodHS512, validClaims(), secret()),
			want:  gojwt.ErrTokenSignatureInvalid,
		},
		{
			name:  "unsigned",
			token: sign(t, gojwt.SigningMethodNone, validClaims(), gojwt.UnsafeAllowNoneSignatureType),
			want:  gojwt.ErrTokenSignatureInvalid,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			token := tt.token

			if tt.issuer != nil {
				var err error

				token, err = tt.issuer.CreateToken(t.Context(), session{UserID: "1"})
				require.NoError(t, err)
			}

			_, err := verifier.ParseToken(t.Context(), token)
			require.ErrorIs(t, err, jwt.ErrInvalidToken)
			require.ErrorIs(t, err, tt.want)
		})
	}

	t.Run("accepts any one of the audiences", func(t *testing.T) {
		t.Parallel()

		issuer := newService(t, jwt.Config{Issuer: "auth", Audience: []string{"web"}, Subject: "user", TTL: time.Hour},
			key, clk)

		token, err := issuer.CreateToken(t.Context(), session{UserID: "1"})
		require.NoError(t, err)

		_, err = verifier.ParseToken(t.Context(), token)
		require.NoError(t, err)
	})
}

func validClaims() *jwt.AuthClaims[session] {
	claims := claimsWithoutExpiry()
	claims.ExpiresAt = gojwt.NewNumericDate(time.Now().Add(time.Hour))

	return claims
}

func claimsWithoutExpiry() *jwt.AuthClaims[session] {
	return &jwt.AuthClaims[session]{
		RegisteredClaims: gojwt.RegisteredClaims{
			Issuer:   "auth",
			Subject:  "user",
			Audience: gojwt.ClaimStrings{"api"},
			IssuedAt: gojwt.NewNumericDate(start()),
		},
	}
}

func sign(t *testing.T, method gojwt.SigningMethod, claims gojwt.Claims, key any) string {
	t.Helper()

	token, err := gojwt.NewWithClaims(method, claims).SignedString(key)
	require.NoError(t, err)

	return token
}

// fakeKey is a Signer and Verifier that fail with err.
type fakeKey struct{ err error }

func (f fakeKey) Sign(context.Context, gojwt.Claims) (string, error) { return "", f.err }

func (f fakeKey) Verify(context.Context, string, gojwt.Claims, ...gojwt.ParserOption) error {
	return f.err
}

func TestAuthService_KeyErrors(t *testing.T) {
	t.Parallel()

	svc, err := jwt.NewAuthService[session](baseConfig(), fakeKey{err: errSign}, fakeKey{err: errSign})
	require.NoError(t, err)

	_, err = svc.CreateToken(t.Context(), session{})
	require.ErrorIs(t, err, errSign)

	_, err = svc.ParseToken(t.Context(), "token")
	require.ErrorIs(t, err, errSign)
	require.ErrorIs(t, err, jwt.ErrInvalidToken, "a Verifier's error is wrapped with ErrInvalidToken")
}

func TestAuthService_ConfigIsCopied(t *testing.T) {
	t.Parallel()

	audience := []string{"api"}
	key := newKey(t, secret())

	svc, err := jwt.NewAuthService[session](jwt.Config{Issuer: "auth", Audience: audience, TTL: time.Hour}, key, key)
	require.NoError(t, err)

	audience[0] = "changed"

	token, err := svc.CreateToken(t.Context(), session{})
	require.NoError(t, err)

	claims, err := svc.ParseClaims(t.Context(), token)
	require.NoError(t, err)
	assert.Equal(t, gojwt.ClaimStrings{"api"}, claims.Audience)
}
