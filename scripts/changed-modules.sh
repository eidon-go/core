#!/usr/bin/env bash
# Prints the modules affected by the changes between two commits, one per line.
# A file belongs to the module with the longest directory prefix; anything not
# inside a nested module belongs to the core module ("."). Changes to shared
# build infrastructure affect every module.
#
#   scripts/changed-modules.sh <base> <head>
set -euo pipefail

if [[ $# -ne 2 ]]; then
	echo "usage: $0 <base> <head>" >&2
	exit 2
fi

cd "$(git rev-parse --show-toplevel)"

modules=$(scripts/modules.sh)
changed=$(git diff --name-only "$1" "$2")

if printf '%s\n' "$changed" | grep -Eq '^(\.github/|scripts/|Makefile$|\.golangci\.yml$)'; then
	printf '%s\n' "$modules"
	exit 0
fi

while IFS= read -r file; do
	[[ -z "$file" ]] && continue
	owner="."
	while IFS= read -r m; do
		[[ "$m" == "." ]] && continue
		if [[ "$file" == "$m/"* && ${#m} -gt ${#owner} ]]; then
			owner="$m"
		fi
	done <<<"$modules"
	# A deleted module is no longer in the list; its files fall back to ".".
	echo "$owner"
done <<<"$changed" | LC_ALL=C sort -u
