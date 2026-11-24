#!/usr/bin/env bash
# Renders release notes for one module with git-cliff, limited to commits that
# touch the module's directory and excluding nested modules, ranged from the
# module's previous tag.
#
#   scripts/release-notes.sh <module-dir> <tag>   (notes on stdout)
set -euo pipefail

if [[ $# -ne 2 ]]; then
	echo "usage: $0 <module-dir> <tag>" >&2
	exit 2
fi

module="${1%/}"
tag="$2"

cd "$(git rev-parse --show-toplevel)"

args=(--config cliff.toml --unreleased --tag "$tag" --strip header)

if [[ "$module" == "." ]]; then
	prefix=""
else
	prefix="$module/"
	args+=(--include-path "$module/**")
fi

# Escape regex metacharacters of the module path (only "." can occur).
args+=(--tag-pattern "^${prefix//./\\.}v[0-9]+\\.[0-9]+\\.[0-9]+\$")

while IFS= read -r m; do
	[[ "$m" == "." || "$m" == "$module" ]] && continue
	if [[ "$module" == "." || "$m" == "$module/"* ]]; then
		args+=(--exclude-path "$m/**")
	fi
done < <(scripts/modules.sh)

exec git-cliff "${args[@]}"
