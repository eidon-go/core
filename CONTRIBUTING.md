# Contributing

Thanks for your interest in improving eidon-go! This document describes how to propose changes.
By participating, you agree to follow the [Code of Conduct](./CODE_OF_CONDUCT.md).

## Before you start

- **Bugs:** open an issue with the module, its version, the Go version, a minimal reproduction,
  and the expected vs. actual behavior.
- **Features and new integration modules:** open an issue first to discuss the design. The core
  module is intentionally small and free of third-party dependencies, so most new functionality
  belongs in a separate module.
- **Security issues:** never open a public issue — follow [SECURITY.md](./SECURITY.md).
- Small fixes (typos, docs, obvious bugs) can go straight to a pull request.

## Repository layout

The repository holds several Go modules, each with its own `go.mod` and its own version:

- `.` — the core module (`github.com/eidon-go/core`): `lifecycle`, `app`, `config`, `retry`;
  standard library only.
- `contrib/<name>` — integration and format modules (`contrib/http2`, `contrib/config/yaml`, …),
  one module per directory. Everything outside `contrib/` (and `internal/`) is the core module.
- `internal/integration` — unpublished module with container-based integration tests.

New integration modules follow the [contrib module guidelines](./docs/contrib-guidelines.md).

### Adding an integration module

The planned modules and their order are listed in the [roadmap](./README.md#status-and-roadmap).
One module per pull request:

1. Create `contrib/<name>/go.mod` with module path `github.com/eidon-go/core/contrib/<name>`, the minimum `go`
   directive and minimum `require` versions; no `replace`. `make work` picks it up automatically.
2. Implement it to the [guidelines](./docs/contrib-guidelines.md) and prove the lifecycle contract
   with `lifecycletest.Resource` / `lifecycletest.Runner`.
3. Container-based tests go to `internal/integration` (create the module if it does not exist yet).
4. Add `contrib/<name>/README.md` and testable `Example*` functions, and move the module from the roadmap
   to the **Modules** table in the root `README.md`.
5. If the module needs core API that is not released yet, land and release that core change in a
   separate pull request first: CI checks every module with `GOWORK=off` against the published
   core.
6. After merge, a maintainer releases the module (`contrib/<name>/vX.Y.Z`) with the Release workflow.

## Development setup

Requirements:

- Go — the version declared in the module's `go.mod` or newer;
- [golangci-lint](https://golangci-lint.run/welcome/install/) v2 (CI uses the version pinned in
  `.github/workflows/ci.yml`);
- Docker — only for integration tests.

```sh
make work      # generate a local go.work with every module (not committed)
make build     # build every published module
make test      # unit tests with the race detector
make lint      # golangci-lint in every module
make format    # format code
make tidy      # go mod tidy in every module
make vuln      # govulncheck
make help      # all targets
```

`go.work` is local only: CI also checks every published module on its own (`GOWORK=off`), against
the published versions of its dependencies, so a change that spans modules must work both ways.

## Making changes

- **Keep the public API stable.** Removing or renaming exported identifiers, changing signatures or
  behavior of published modules is a breaking change and needs prior discussion.
- **No new dependencies in the core module.** Integration modules keep their requirements at the
  minimum versions they need; do not bump them without a reason.
- **No `replace` directives** in published modules' `go.mod` (CI rejects them).
- **Errors** are wrapped with `fmt.Errorf("...: %w", err)` and returned — no panics in library code.
- **Tests** accompany every change: table-driven where it fits, `t.Parallel()` at the top of each
  test, `t.Context()` instead of `context.Background()`, no `time.Sleep`-based synchronization.
- **Docs:** exported identifiers have doc comments; update the module's `README.md` when behavior
  or usage changes.

Before pushing, run:

```sh
make format lint test tidy-check
```

## Commit messages and pull requests

Commits follow [Conventional Commits](https://www.conventionalcommits.org): release notes are
generated from them.

```
<type>(<scope>): <summary>

feat(config): add etcd source
fix(app): stop components in reverse order on start failure
docs(retry): document jitter
```

- Types: `feat`, `fix`, `perf`, `refactor`, `docs`, `test`, `ci`, `chore`.
- Scope: the package or module the change touches (`app`, `config`, `http2`, `config/yaml`, …) — for contrib modules without the `contrib/` prefix.
- Breaking changes: `feat(app)!: ...` and a `BREAKING CHANGE:` footer describing the migration.

Pull requests:

- One logical change per PR; the PR title follows the same Conventional Commits format. PRs are
  squash-merged, so the title becomes the commit message on `main` and the release notes entry.
- Describe *what* changes and *why*; link the related issue.
- CI must pass: build, tests (minimum and current Go versions), lint, `govulncheck`, coverage.
- Automated reviews (CodeRabbit, SonarQube Cloud, Codecov) comment on the PR; address or answer
  their findings. A maintainer review is required to merge.

## Releases

Releases are made by maintainers per module with the *Release* workflow (tags `vX.Y.Z` for the
core module, `contrib/<name>/vX.Y.Z` for the others), following [Semantic Versioning](https://semver.org).

## License

By contributing, you agree that your contributions are licensed under the [MIT License](./LICENSE).
