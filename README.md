# core

[![CI](https://github.com/eidon-go/core/actions/workflows/ci.yml/badge.svg)](https://github.com/eidon-go/core/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/eidon-go/core.svg)](https://pkg.go.dev/github.com/eidon-go/core)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Go framework for bootstrapping production services around one lifecycle contract
and functional options.

The repository holds several Go modules. The **core** module is small, stable and depends only on
`go.yaml.in/yaml/v3` (Go 1.25+). Every integration (etcd, OpenTelemetry, HTTP, gRPC, Kafka, SQL, …)
is a **separate module with its own version**, so a service pulls in only the dependencies of the
integrations it actually imports.

## Modules

| Module | Import path | Packages |
|---|---|---|
| core | `github.com/eidon-go/core` | [`lifecycle`](./lifecycle/README.md) — component contracts (`Resource`, `Starter`, `Runner`, `Healthchecker`, `Namer`); [`app`](./app/README.md) — runs components: ordered start, signals, bounded reverse-order shutdown, health; [`config`](./config/README.md) — layered configuration (defaults, files, KV, env) with hot reload; [`retry`](./retry/README.md) — retry policies with backoff |

Integration modules are added one by one after the core API is reviewed.

## Example

```go
func main() {
    ctx := context.Background()

    cfg, err := config.New(ctx, config.WithFiles("config.yaml"), config.WithEnvPrefix("APP"))
    if err != nil {
        log.Fatal(err)
    }

    a := app.New(app.WithShutdownTimeout(30 * time.Second))
    a.Add(cfg, db, cache)          // dependencies first; db.Start connects with retry
    a.Add(grpcServer, httpServer)  // runners

    if err := a.Run(ctx); err != nil {
        log.Fatal(err)
    }
}
```

`app.App` starts the components in order, runs the runners until SIGINT/SIGTERM (or a runner
fails), then shuts everything down in reverse order under one timeout. See the package READMEs
for the contracts.

## Versioning

- Every module follows semver independently.
- Core tags: `vX.Y.Z`. Integration tags: `<module>/vX.Y.Z` (e.g. `kafka/v1.4.0`, `config/etcd/v1.0.0`).
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
