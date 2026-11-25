# core

[![CI](https://github.com/eidon-go/core/actions/workflows/ci.yml/badge.svg)](https://github.com/eidon-go/core/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/eidon-go/core/graph/badge.svg)](https://codecov.io/gh/eidon-go/core)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=eidon-go_core&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=eidon-go_core)
[![Go Report Card](https://goreportcard.com/badge/github.com/eidon-go/core)](https://goreportcard.com/report/github.com/eidon-go/core)

[![Go Reference](https://pkg.go.dev/badge/github.com/eidon-go/core.svg)](https://pkg.go.dev/github.com/eidon-go/core)
[![Release](https://img.shields.io/github/v/tag/eidon-go/core?filter=v*&sort=semver&label=release)](https://github.com/eidon-go/core/releases)
[![Go Version](https://img.shields.io/github/go-mod/go-version/eidon-go/core)](./go.mod)

[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/eidon-go/core/badge)](https://scorecard.dev/viewer/?uri=github.com/eidon-go/core)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Conventional Commits](https://img.shields.io/badge/Conventional%20Commits-1.0.0-fe5196.svg)](https://www.conventionalcommits.org)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-3.0-4baaaa.svg)](./CODE_OF_CONDUCT.md)

An explicit-wiring, stdlib-first kit for production Go services: one lifecycle contract that every
integration follows, so telemetry, health, retries, logging and graceful shutdown behave the same
across HTTP, gRPC, databases and brokers. No reflection-based wiring, no DI container.

The repository holds several Go modules. The **core** module is small, stable and has no
third-party dependencies (Go 1.25+). Every integration and format (YAML, etcd, OpenTelemetry, HTTP,
gRPC, Kafka, SQL, …) is a **separate module with its own version** under `contrib/`, so a service
pulls in only the dependencies of what it actually imports. Everything outside `contrib/` is the
core.

## Modules

| Module | Import path | Contents |
|---|---|---|
| core | `github.com/eidon-go/core` | [`lifecycle`](./lifecycle/README.md) — component contracts (`Resource`, `Starter`, `Runner`, `Healthchecker`, `Namer`), `Group`, contract tests in `lifecycletest`; [`app`](./app/README.md) — runs components: ordered start, signals, readiness, bounded shutdown (runners together, then the rest in reverse order), lifecycle events; [`config`](./config/README.md) — layered configuration (defaults, files, KV, env) with hot reload and pluggable formats (JSON built in); [`retry`](./retry/README.md) — retry policies with backoff |
| contrib/config/yaml | `github.com/eidon-go/core/contrib/config/yaml` | [YAML parser](./contrib/config/yaml/README.md) for `config` |
| contrib/config/etcd | `github.com/eidon-go/core/contrib/config/etcd` | [etcd v3 backend](./contrib/config/etcd/README.md) for `config` (`config.KV`): watched keys, gap-free hot reload |
| contrib/http2 | `github.com/eidon-go/core/contrib/http2` | [HTTP server](./contrib/http2/README.md): HTTP/1.1 and HTTP/2 (TLS, h2c), graceful drain, probes, OpenTelemetry, panic recovery; [client](./contrib/http2/README.md#client) (`contrib/http2/client`): tuned transport, h2c, OpenTelemetry, RoundTripper wrappers |

```sh
go get github.com/eidon-go/core
go get github.com/eidon-go/core/contrib/config/yaml   # only if you read YAML
go get github.com/eidon-go/core/contrib/config/etcd   # only if you keep configuration in etcd
```

## Status and roadmap

The core API is a **v1 candidate**, released as `v1.0.0-rc.N` while the first integration modules
are written against it. `v1.0.0` for the core and `contrib/config/yaml/v1.0.0` follow once the main
integration modules have validated the API; integration modules are tagged after that.

Integration modules are added one at a time, each as its own module with its own version, written
to the [contrib guidelines](./docs/contrib-guidelines.md) and checked with `lifecycletest`:

| Module | Purpose | State |
|---|---|---|
| `contrib/otel` | OpenTelemetry SDK and exporters setup | planned |
| `contrib/grpc` | gRPC server and client | planned |
| `contrib/jwt` | JWT signing and verification | planned |
| `contrib/kafka` | Kafka producer and consumer | planned |
| `contrib/nats` | NATS client and consumer | planned |
| `contrib/rabbitmq` | RabbitMQ publisher and consumer | planned |
| `contrib/redis` | Redis client | planned |
| `contrib/sql` | `database/sql` and pgx (`contrib/sql/pgx`) with a shared transaction manager, typed `Get`/`Select`/`Exec` helpers and after-commit hooks; starts at v0.x | planned |

Later, on the same scheme: adapters for `sql` such as `river` and `sqlx`.

## Design principles

- **Explicit wiring.** Components are built with constructors and registered with `app.Add`; there
  is no container, no reflection and no global state.
- **Dependency-free core.** Anything that needs a third-party library is a separate module.
- **One lifecycle contract.** Every integration implements the same [`lifecycle`](./lifecycle/README.md)
  interfaces, so start order, readiness, retries and graceful shutdown work the same everywhere.
- **No struct binding.** Configuration is read through typed getters into structs you define;
  integration `Config` structs are plain Go structs you can fill from any source.
- **Constructors do no I/O.** Connecting and pinging happen in `Start`, under `app`'s control and
  with a configurable `retry.Policy`.

## Example

```go
func main() {
    ctx := context.Background()

    cfg, err := config.New(ctx,
        config.WithParser(yaml.Parse, yaml.Extensions()...),
        config.WithFiles("config.yaml"),
        config.WithEnvPrefix("APP"),
    )
    if err != nil {
        log.Fatal(err)
    }

    a := app.New(app.WithShutdownTimeout(30*time.Second), app.WithShutdownDelay(5*time.Second))
    a.Add(cfg, db, cache)          // dependencies first; db.Start connects with retry
    a.Add(grpcServer, httpServer)  // runners

    if err := a.Run(ctx); err != nil {
        log.Fatal(err)
    }
}
```

`app.App` starts the components in order, runs the runners until SIGINT/SIGTERM (or a runner
fails), fails readiness, then under one timeout stops the runners together and shuts the other
components down in reverse order. See the
package READMEs for the contracts.

## Versioning

- Every module follows semver independently.
- Core tags: `vX.Y.Z`. Integration tags: `contrib/<name>/vX.Y.Z` (e.g. `contrib/kafka/v1.4.0`, `contrib/config/etcd/v1.0.0`).
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
