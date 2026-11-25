# Security Policy

## Supported versions

Every module of this repository (the core module and each integration module) is versioned
separately. Security fixes are released for the **latest minor version** of each module; until a
module reaches v1, only its latest release is supported.

| Version                         | Supported |
|---------------------------------|-----------|
| Latest minor release of a module | ✅        |
| Older releases                  | ❌        |

## Reporting a vulnerability

**Please do not report security vulnerabilities through public issues, pull requests or
discussions.**

Report them privately through GitHub:
[**Report a vulnerability**](https://github.com/eidon-go/core/security/advisories/new)
(the *Security* tab → *Report a vulnerability*).

Please include as much of the following as you can:

- the affected module(s) and version(s);
- a description of the issue and its impact;
- steps to reproduce, or a proof of concept;
- a suggested fix, if you have one.

## What to expect

- An acknowledgement of your report within **3 business days**.
- An initial assessment (confirmed or not, severity) within **10 business days**.
- Updates on the progress towards a fix while it is in work.

Once a fix is released, the vulnerability is disclosed through a
[GitHub Security Advisory](https://github.com/eidon-go/core/security/advisories), and a CVE is
requested where appropriate. Reporters are credited in the advisory unless they prefer otherwise.

## Dependencies

Published modules are checked with [`govulncheck`](https://go.dev/doc/security/vuln/) in CI on
every change. If a vulnerability affects only a third-party dependency, upgrading that dependency
in your own `go.mod` usually fixes it — modules here declare the minimum versions they need and
do not force upgrades.
