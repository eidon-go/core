<!--
PR title: Conventional Commits, e.g. "feat(config): add etcd source" or "fix(app)!: ..." for a
breaking change. It becomes the squash commit and the release notes entry.
See CONTRIBUTING.md.
-->

## What and why

<!-- What does this change and why is it needed? -->

Closes #

## Modules affected

<!-- e.g. core (app, config), config/yaml -->

## Checklist

- [ ] Tests added or updated; `make format lint test tidy-check` passes locally
- [ ] Exported identifiers are documented; module README updated if usage changed
- [ ] No breaking change to published modules — or it is marked with `!` and described below
- [ ] No new third-party dependency in the core module

## Breaking changes / migration

<!-- Remove if none. -->
