package jwt_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/eidon-go/core/contrib/jwt"
	"github.com/eidon-go/core/contrib/jwt/hs256"
)

func Example() {
	type Session struct {
		UserID string `json:"userId"`
	}

	// 32+ random bytes from a secret store; never a literal in real code.
	key, err := hs256.New([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		fmt.Println(err)

		return
	}

	svc, err := jwt.NewAuthService[Session](jwt.Config{Issuer: "auth", TTL: 15 * time.Minute}, key, key)
	if err != nil {
		fmt.Println(err)

		return
	}

	ctx := context.Background()

	token, err := svc.CreateToken(ctx, Session{UserID: "42"})
	if err != nil {
		fmt.Println(err)

		return
	}

	session, err := svc.ParseToken(ctx, token)
	if err != nil {
		fmt.Println(err)

		return
	}

	fmt.Println(session.UserID)
	// Output: 42
}

func ExampleAuthService_ParseToken_expired() {
	type Session struct {
		UserID string `json:"userId"`
	}

	key, err := hs256.New([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		fmt.Println(err)

		return
	}

	issuedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := issuedAt
	cfg := jwt.Config{Issuer: "auth", TTL: time.Minute}

	svc, err := jwt.NewAuthService[Session](cfg, key, key, jwt.WithClock(func() time.Time { return now }))
	if err != nil {
		fmt.Println(err)

		return
	}

	token, err := svc.CreateToken(context.Background(), Session{UserID: "42"})
	if err != nil {
		fmt.Println(err)

		return
	}

	now = issuedAt.Add(time.Hour)

	_, err = svc.ParseToken(context.Background(), token)

	switch {
	case errors.Is(err, jwt.ErrTokenExpired):
		fmt.Println("expired: refresh")
	case errors.Is(err, jwt.ErrInvalidToken):
		fmt.Println("invalid: reject")
	}
	// Output: expired: refresh
}
