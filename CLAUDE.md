# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`github.com/eidon-go/core` — an explicit-wiring, stdlib-first kit for production Go services. One
lifecycle contract that every integration follows; no reflection-based wiring, no DI container.

It is a **multi-module repo**: every directory with a `go.mod` is a separately versioned module
(tags `<dir>/vX.Y.Z`, core is `vX.Y.Z`). `scripts/modules.sh` lists them (`--published` excludes
anything under `internal/`, e.g. the future `internal/integration` test module).

- `.` (core) — `lifecycle`, `app`, `config`, `retry`. **Standard library only (testify in tests
  only), Go 1.25+. Never add a third-party dependency here** — anything that needs one becomes its
  own module.
- `contrib/<name>` — every other published module: integrations (`contrib/http2`, later
  `contrib/otel`, `contrib/sql`, …) and config formats/backends (`contrib/config/yaml`,
  `contrib/config/etcd`). Module path `github.com/eidon-go/core/contrib/<name>`, tags
  `contrib/<name>/vX.Y.Z`. Everything outside `contrib/` and `internal/` is the core module.

The point of the layout: the core is small and stable (v1), integrations change often and each
has its own version, so a client's module graph contains only the dependencies of the modules it
imports.

## Status and roadmap

1. **Done:** core + `contrib/config/yaml` merged, CI green. Core `v1.0.0-rc.5` is the latest tag.
   Contrib modules are written against rc.N (cut rc.4… when a module needs core API changes —
   breaking changes are still allowed). Core `v1.0.0` + `contrib/config/yaml/v1.0.0` are tagged
   once the main contrib modules (`http2`, `config/etcd`, `otel`, `grpc`) have validated the API;
   contrib modules are tagged only after that, with their `require` bumped to core `v1.0.0`.
2. **Phase 2 — contrib modules** (all under `contrib/`), one module per PR, in this order:
   `http2` (server and `http2/client`; done) → `config/etcd` (done) → `otel` (done) → `grpc` (server and `grpc/client`; done) → `kafka` (done; first
   `lifecycle.Worker` user) → `jwt` → `nats` → `rabbitmq` → `redis` → `sql` (as v0.x). Modules may declare a newer `go` than core
   when their dependencies need it (`contrib/http2` is go 1.26: otel ≥ v1.47 for a reachable
   vulnerability fix). Later, the same way: `river`, `sqlx` (adapters for `sql`). Each module is written to
   `docs/contrib-guidelines.md` and ships with `lifecycletest` checks, a README, `Example*` tests
   and its own tag. The checklist is in `CONTRIBUTING.md` ("Adding an integration module"); when
   a module lands, move it from the roadmap table to the Modules table in `README.md`.
3. `internal/integration` (unpublished, testcontainers, a `replace` per module for local runs;
   created with `config/etcd`) holds the container tests; `make test-integration` and the CI
   `integration` job run them (Docker needed). A module that adds tests there adds its `require`
   and `replace` to `internal/integration/go.mod`.

### Planned `sql` module (key decisions)

- One module `contrib/sql` with two packages: `sql` (database/sql) and `sql/pgx` (pgxpool).
  `sql/pgx` is a package, not a module — scany v2 already pulls pgx/v5. Shared transaction core
  in `contrib/sql/internal/txcore` (not public).
- Scanning: scany v2 behind a public `Scanner` interface (`WithScanner`). No sqlx (separate adapter
  module later), no squirrel dependency (helpers take a structural `Builder`; `sql.Dollar` /
  `sql.Question` structurally satisfy squirrel's `PlaceholderFormat`).
- `Transactor` (`WithTx`, `OnCommit`) and `QuerierProvider` are small interfaces; nested `WithTx`
  defaults to `Join` + rollback-only, other modes via `WithTxMode`. `Get/Select/Exec[T]` helpers
  work in both worlds.
- `New(cfg, opts...)` does no I/O; open + ping with `WithConnectRetry` in `Start`. `DB` is
  `Starter`/`Resource`/`Healthchecker`/`Namer`. Pool users (River) must be registered after the
  DB so they shut down first.

## Settled decisions — don't reopen

- `config` stays in core; only format parsers (and KV backends like etcd) are split out.
- **No reflection-based struct binding (`config.Decode` etc.), ever.** Consumers fill their structs
  with getters / `Reader`; contrib `Config` structs are plain Go structs with no required tags.
- YAML timestamps stay strings (config has no time type; keeps YAML/JSON/env layers consistent).
- No cross-module `internal` packages: shared code that another module needs becomes public in
  core (that is why `retry` is public).
- Core uses `sync.WaitGroup.Go` instead of `x/sync/errgroup`.

## Releases

- Tags: core `vX.Y.Z`, modules `<dir>/vX.Y.Z` (`contrib/http2/v1.0.0`); major ≥ 2 uses a `/vN`
  path suffix.
- Released only via `.github/workflows/release.yml` (`workflow_dispatch` with `module` + `version`,
  from `main`): `scripts/release-check.sh` validates, then `GOWORK=off` tidy/build/test,
  per-module notes via `scripts/release-notes.sh` (git-cliff), `gh release create`.
- A module that needs new core API: tag core first → bump `require` in the module → tag the module
  (two PRs).
- A bad release is withdrawn with `retract` plus a new version; tags are never re-published.
- Dependabot bumps GitHub Actions only; `latest-deps.yml` weekly checks modules against the latest
  dependency versions without committing anything.

## Commands

Make targets iterate over every module (`cd <module> && ...`):

```sh
make work        # generate local go.work with all modules (gitignored) — needed for cross-module edits
make build       # go build ./... in every published module
make test        # go test -race -count=1 -timeout=5m -short ./... in every published module
make lint        # golangci-lint (v2) run ./... in every module
make format      # golangci-lint fmt in every module
make tidy / make tidy-check
make vuln        # govulncheck
make test-integration   # internal/integration, needs Docker (not present yet)
```

Pre-push check: `make format lint test tidy-check`.

Single test (run from the module's directory, since each module is separate):

```sh
go test -race -run 'TestGroup_JoinsErrors' ./lifecycle/
cd contrib/config/yaml && go test -race -run 'TestParse_Errors' ./...
```

CI also builds/tests each published module with `GOWORK=off` against the *published* versions of
its dependencies, on the minimum and current Go versions. A change spanning modules (e.g. core +
contrib/http2) must work both with and without `go.work`. CI rejects `replace` directives in
published `go.mod` files.

## Architecture

- **`lifecycle`** — the central contract every component implements: `Resource` (Shutdown, every
  component), optional `Starter` (Start), `Runner` (long-running Run), `Healthchecker`, `Namer`;
  `Group` / `GroupNamed` combine resources. The precise phase semantics (start order; a failed
  startup shuts down every component, started or not; cancel of Run's ctx + Shutdown = one stop —
  Run may drain, Shutdown bounds it, side effects once; Run may be called after Shutdown began;
  Shutdown idempotent and safe without Start/Run or after a failed Start; 100ms grace after ctx
  deadline; only sources of work are Runners) are documented in the package doc of
  `lifecycle/lifecycle.go` — read it before touching `app` or writing a component. `lifecycle/lifecycletest` provides
  `lifecycletest.Resource` / `lifecycletest.Runner` contract tests that every component must pass.
  Components build on the adapters instead of their own stop state machine: clients on
  `lifecycle.CloseOnce` / `ShutdownOnce` (`*Releaser`), workers (consumers, schedulers) on
  `lifecycle.NewWorker` (`WorkerFuncs{Run func(stop, work context.Context) error}`), servers on `lifecycle.NewServer`
  (`ServerFuncs{Listen, Serve, Drain, Close}`, kept in an unexported field, not embedded).
- **`app`** — drives components: ordered Start, concurrent Run, signals, readiness/health,
  bounded shutdown (Run ctx cancelled, runners shut down concurrently, then the rest in reverse
  order), lifecycle events via `app.WithObserver`
  (metrics/traces come from the observer, not from wrapping components — wrappers hide optional
  interfaces).
- **`config`** — layered config with priority `env > watched KV keys > KV keys > files > defaults`;
  nested documents are flattened to dotted keys; read through generic getters
  (`config.Get[T]`, `GetOrDefault`, `Live[T]` for hot-reloaded values) — no struct binding.
  JSON is built in; other formats plug in via `WithParser(func([]byte) (map[string]any, error), exts...)`.
  KV backends implement `config.KV`.
- **`retry`** — `retry.Policy` with backoff; `OnRetry` hook for logging, `retry.Forever` only
  bounded by ctx; `retry.ConnectPolicy()` is the default connect policy and `Policy.Logged`
  adds the standard retry log.

Dependency rules between packages/modules (from `docs/contrib-guidelines.md`):
`lifecycle` depends only on stdlib. Integration modules may import only `core/lifecycle` and
`core/retry` — never `core/app` or `core/config` (config arrives as a plain `Config` struct).
Exception: `contrib/config/<backend>` modules implement `config.KV` and import `config` only.
Format modules (`contrib/config/yaml`) need not import core at all.

## Conventions

New integration modules must follow `docs/contrib-guidelines.md` (constructor `New(cfg, opts...)`
with no network I/O — I/O goes in `Start`/`Run`; `With*` options; `Config.Validate()` using
`errors.Join`; `WithLogger` defaulting to `slog.Default()` and always `*Context` log methods;
OTel providers as options; sentinel errors `Err<What>` with `"<module>: "` prefix). Read it before
adding a module.

- Public API of published modules is stable: renames/signature/behavior changes are breaking.
- Integration modules `require` minimum versions; don't bump without reason.
- Tests: table-driven where it fits, `t.Parallel()` first, `t.Context()`, no `time.Sleep`
  synchronization. Testable `Example*` functions live in `example_test.go`.
- Exported identifiers need doc comments; update the package/module `README.md` when behavior
  or usage changes.
- Conventional Commits (`feat|fix|perf|refactor|docs|test|ci|chore`), scope = package or module
  without the `contrib/` prefix (`app`, `http2`, `config/yaml`); breaking: `feat(app)!:` + `BREAKING CHANGE:` footer. PRs are
  squash-merged, so the PR title becomes the commit/release-notes entry (`cliff.toml`).
