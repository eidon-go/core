# core

[![CI](https://github.com/eidon-go/core/actions/workflows/ci.yml/badge.svg)](https://github.com/eidon-go/core/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/eidon-go/core.svg)](https://pkg.go.dev/github.com/eidon-go/core)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Go framework for bootstrapping production services around one lifecycle contract
(`Resource` / `Runner`) and one option pattern.

The repository holds several Go modules. The **core** module is small, stable and depends only on
`gopkg.in/yaml.v3` and `golang.org/x/sync`. Every integration (etcd, OpenTelemetry, HTTP, gRPC, Kafka,
SQL, …) is a **separate module with its own version**, so a service pulls in only the dependencies of
the integrations it actually imports.

## Modules

| Module | Import path | Purpose |
|---|---|---|
| core | `github.com/eidon-go/core` | [`lifecycle`](./lifecycle/README.md) — `Resource`/`Runner` contracts and groups; [`app`](./app/README.md) — the application runner (signals, ordered startup and shutdown); [`di`](./di/README.md) — configuration (yaml, env, remote KV) and the bootstrap container; `retry` — bounded startup retry |

Integration modules are added one by one after the core API is reviewed.

## Core concepts

`core` revolves around small interfaces in [`lifecycle`](./lifecycle/README.md):

```go
type Resource interface {
    Shutdown(ctx context.Context) error
}

type Runner interface {
    Resource
    Run(ctx context.Context) error
}
```

`app.App` runs every `Runner`, handles SIGINT/SIGTERM and shuts everything down in reverse order of
registration. See the package READMEs for details.

## Versioning

- Every module follows semver independently.
- Core tags: `vX.Y.Z`. Integration tags: `<module>/vX.Y.Z` (e.g. `kafka/v1.4.0`, `di/etcd/v1.0.0`).
  Major versions ≥ 2 use the `/vN` module path suffix.
- An integration's `go.mod` requires the **minimum** core version (and minimum dependency versions)
  it needs, so upgrading an integration never forces upgrades elsewhere.
- A change that needs new core API is released in two steps: core first, then the integration.
- A bad release is withdrawn with `retract` and a new version; tags are never re-published.

## Development

```sh
make work        # generate a local go.work with every module (gitignored)
make test        # unit tests of every published module
make lint        # golangci-lint in every module
make tidy-check  # fail if go mod tidy would change anything
make help        # all targets
```

CI checks every module on its own (`GOWORK=off`, i.e. against the published versions of its
dependencies) at both its declared minimum Go version and the current stable one, plus a job that
builds all modules together through a generated `go.work`.

Releases are cut with the **Release** workflow (`workflow_dispatch`, inputs `module` and `version`):
it validates the version, re-runs the module's checks, renders notes from the commits that touch the
module ([git-cliff](https://git-cliff.org), Conventional Commits) and creates the tag and the GitHub
Release.

## License

[MIT](./LICENSE)
