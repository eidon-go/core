# jwt

[![Go Reference](https://pkg.go.dev/badge/github.com/eidon-go/core/contrib/jwt.svg)](https://pkg.go.dev/github.com/eidon-go/core/contrib/jwt)
[![Release](https://img.shields.io/github/v/tag/eidon-go/core?filter=contrib%2Fjwt%2Fv*&sort=semver&label=release)](https://github.com/eidon-go/core/releases)
[![Go Version](https://img.shields.io/github/go-mod/go-version/eidon-go/core?filename=contrib%2Fjwt%2Fgo.mod)](./go.mod)
[![codecov](https://codecov.io/gh/eidon-go/core/graph/badge.svg?flag=contrib-jwt)](https://codecov.io/gh/eidon-go/core?flags%5B0%5D=contrib-jwt)

JSON Web Tokens with a typed payload, on [golang-jwt/jwt/v5](https://github.com/golang-jwt/jwt).
An `AuthService[T]` issues tokens that carry your payload `T` and parses them back with every
check on: signature, algorithm, issuer, audience, expiry. The algorithm is a pluggable
`Signer`/`Verifier`; [`jwt/hs256`](./hs256) provides HMAC-SHA256 with a shared secret.

```sh
go get github.com/eidon-go/core/contrib/jwt
```

```go
import (
    "github.com/eidon-go/core/contrib/jwt"
    "github.com/eidon-go/core/contrib/jwt/hs256"
)

type Session struct {
    UserID string   `json:"userId"`
    Roles  []string `json:"roles"`
}

key, err := hs256.New(secret) // ≥ 32 random bytes
svc, err := jwt.NewAuthService[Session](jwt.Config{
    Issuer: "auth",
    TTL:    15 * time.Minute,
}, key, key)

token, err := svc.CreateToken(ctx, Session{UserID: "42", Roles: []string{"admin"}})

session, err := svc.ParseToken(ctx, token)
switch {
case errors.Is(err, jwt.ErrTokenExpired): // refresh
case errors.Is(err, jwt.ErrInvalidToken): // reject
}
```

## Configuration

| Field | Default | Meaning |
|---|---|---|
| `Issuer` | required | `iss` of issued tokens; parsed tokens must carry it |
| `Audience` | `[Issuer]` | `aud` of issued tokens; parsed tokens must name at least one |
| `RequiredSubject` | empty | `sub` of every issued token, required in parsed ones (tokens of one kind: `"service"`); not with a `Subjecter` payload |
| `TTL` | required, ≥ 1s (time claims are whole seconds) | `exp` = now + TTL |
| `Leeway` | `jwt.DefaultLeeway` (30s) | clock skew tolerated on `exp`, `nbf`, `iat`: a token issued on one node is not "not valid yet" on another whose clock is behind |

`Config.Validate` reports every problem at once (each wraps `ErrInvalidConfig`).

| Option | Default | |
|---|---|---|
| `WithClock(func() time.Time)` | `time.Now` | time source of issued and checked time claims |

## Tokens

- `CreateToken(ctx, info)` sets `iss`, `sub`, `aud`, `iat`, `nbf` (now), `exp` (now + TTL), a
  random `jti`, and the payload under `authInfo` (`AuthClaims[T]`). `sub` is the subject of the
  token — the user's ID, where gateways, meshes and audit logs look for it: a payload that
  implements `jwt.Subjecter` (`Subject() string`) sets it; else it is `RequiredSubject`.

```go
func (s Session) Subject() string { return s.UserID }
```
- `ParseToken(ctx, token)` returns the payload; `ParseClaims` returns all claims (the `jti` for a
  revocation list, `exp` for a refresh). A token must be signed with the key's algorithm, issued
  by `Issuer` for one of `Audience` (and `RequiredSubject` when set), carry `exp`, and be inside its
  `iat`/`nbf`/`exp` window widened by `Leeway`.
- Every parse error wraps `ErrInvalidToken` and the golang-jwt cause, so `errors.Is` works with
  both `jwt.ErrTokenExpired` and golang-jwt's errors (`ErrTokenInvalidAudience`, …).

## Algorithms

`Signer` and `Verifier` are small interfaces over golang-jwt claims. A `Verifier` answers for the
signature and the algorithm; `AuthService` passes its claim checks to `Verify` as parser options
and checks the claims again itself afterwards, so a third-party `Verifier` that ignores the options
(a JWKS verifier passing only its own) cannot let an expired or foreign token through. Both take a `ctx` for implementations that call out (a KMS
signer, a JWKS verifier); the local ones ignore it.

- **`hs256.Key`** — HMAC-SHA256 with a shared secret of at least `hs256.MinSecretLength` (32)
  bytes, copied by `hs256.New`. It is both the `Signer` and the `Verifier`, and accepts only HS256
  (other HMAC variants and `none` are rejected, also when a caller's options would allow them).
  Every service that verifies the tokens can issue them too; use an asymmetric algorithm when that
  is not acceptable.

- **`hs256.KeySet`** — several secrets, for a rotation without downtime: it signs with one, named
  in the `kid` header, and verifies a token with the secret its `kid` names. The secret with the
  empty ID verifies tokens without `kid` (those of a single `Key`). Rotate in three deploys: add the
  new secret for verifying; sign with it; drop the old one once its tokens have expired.

```go
keys, err := hs256.NewKeySet(
    hs256.Secret{ID: "2026-11", Secret: newSecret}, // signs and verifies
    hs256.Secret{ID: "2026-10", Secret: oldSecret}, // verifies only
)
svc, err := jwt.NewAuthService[Session](cfg, keys, keys)
```

A `Key` can also be used on its own, with any golang-jwt claims:

```go
token, err := key.Sign(ctx, gojwt.RegisteredClaims{...})
err = key.Verify(ctx, token, &claims, gojwt.WithIssuer("auth"))
```

## Lifecycle

None: signing and parsing are CPU-only, so nothing here is a lifecycle component and there are no
logger or OpenTelemetry options. Build the `AuthService` once and share it — it is safe for
concurrent use.
