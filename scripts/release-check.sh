#!/usr/bin/env bash
# Validates a release request and prints the git tag to create.
#
#   scripts/release-check.sh <module-dir> <version>
#   scripts/release-check.sh .       v1.2.0   ->  v1.2.0
#   scripts/release-check.sh di/etcd v1.0.0   ->  di/etcd/v1.0.0
#
# Fails if the module is not published, the version is not semver, the major
# version does not match the module path (/vN suffix for N >= 2), the tag
# already exists, or the version is not greater than the module's latest tag.
set -euo pipefail

if [[ $# -ne 2 ]]; then
	echo "usage: $0 <module-dir> <version>" >&2
	exit 2
fi

module="${1%/}"
version="$2"

cd "$(git rev-parse --show-toplevel)"

fail() {
	echo "release-check: $*" >&2
	exit 1
}

scripts/modules.sh --published | grep -qxF -- "$module" ||
	fail "'$module' is not a published module (see scripts/modules.sh --published)"

[[ "$version" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?$ ]] ||
	fail "'$version' is not a semver version like v1.2.3 or v1.2.3-rc.1"
major="${BASH_REMATCH[1]}"

module_path=$(awk '$1 == "module" { print $2; exit }' "$module/go.mod")
if ((major >= 2)); then
	[[ "$module_path" == */v"$major" ]] || fail "v$major requires module path ending in /v$major, got $module_path"
elif [[ "$module_path" =~ /v[0-9]+$ ]]; then
	fail "module path $module_path has a major suffix, but the version is $version"
fi

if [[ "$module" == "." ]]; then
	prefix=""
else
	prefix="$module/"
fi
tag="$prefix$version"

git rev-parse -q --verify "refs/tags/$tag" >/dev/null && fail "tag $tag already exists"

latest=$(git tag --list "${prefix}v*" |
	grep -E "^${prefix//./\\.}v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$" |
	sed "s#^${prefix}##" | sort -V | tail -n1 || true)
if [[ -n "$latest" ]]; then
	highest=$(printf '%s\n%s\n' "$latest" "$version" | sort -V | tail -n1)
	[[ "$highest" == "$version" ]] || fail "$version is not greater than the latest release $latest"
fi

echo "$tag"
