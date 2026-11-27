#!/usr/bin/env bash
# Fails when the copies of a file that several modules must keep identical
# (modules cannot share an internal package) have diverged. Each group lists
# the reference copy first; every other copy of the same path pattern must be
# byte-identical to it.
set -euo pipefail

cd "$(dirname "$0")/.."

status=0

check() {
  local ref=$1
  shift

  for f in "$@"; do
    if [[ "$f" != "$ref" ]] && ! cmp -s "$ref" "$f"; then
      echo "::error file=$f::copy diverged from $ref (keep them byte-identical)"
      status=1
    fi
  done
}

# The default propagator of every module that propagates the trace context.
check contrib/http2/internal/propagator/propagator.go \
  $(find contrib -path '*/internal/propagator/propagator.go' | sort)

exit $status
