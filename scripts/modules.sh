#!/usr/bin/env bash
# Lists the Go modules of this repository, one directory per line, relative to
# the repository root ("." is the core module), sorted.
#
#   scripts/modules.sh              every module
#   scripts/modules.sh --published  only published modules (excludes anything
#                                   under an internal/ directory, e.g. the
#                                   integration-test module)
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

all=$(find . -name go.mod -not -path './.git/*' -not -path '*/vendor/*' -not -path '*/testdata/*' |
	sed -e 's#/go.mod$##' -e 's#^\./##' |
	LC_ALL=C sort)

case "${1:-}" in
"") printf '%s\n' "$all" ;;
--published) printf '%s\n' "$all" | grep -Ev '(^|/)internal(/|$)' || true ;;
*)
	echo "usage: $0 [--published]" >&2
	exit 2
	;;
esac
