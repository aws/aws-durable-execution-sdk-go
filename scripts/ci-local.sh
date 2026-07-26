#!/bin/sh
# Run the same checks locally that CI runs (.github/workflows/ci.yml):
# build, vet, lint, formatting, and race-enabled tests for every module.
#
# Usage:
#   scripts/ci-local.sh [module...]
#
# Modules default to all four: . insight compliance examples
# (e.g. `scripts/ci-local.sh examples` to check only the examples module).
#
# Requires: Go and golangci-lint at the versions pinned in .mise.toml
# (`mise install` sets both up).

set -eu

ROOT=$(cd "$(dirname "$0")/.." && pwd)

if [ $# -gt 0 ]; then
    MODULES=$*
else
    MODULES=". insight compliance examples"
fi

for mod in $MODULES; do
    echo "==> module: $mod"
    cd "$ROOT/$mod"

    echo " -> go build ./..."
    go build ./...

    echo " -> go vet ./..."
    go vet ./...

    if [ "$mod" = "examples" ]; then
        echo " -> go vet -tags cloud ./cloud"
        go vet -tags cloud ./cloud

        echo " -> go test -tags durablecheck -race ./context-validation-..."
        go test -tags durablecheck -race ./context-validation-child ./context-validation-step ./context-validation-wait-condition
    fi

    echo " -> golangci-lint run ./..."
    golangci-lint run ./...

    echo " -> golangci-lint fmt --diff ./..."
    diff=$(golangci-lint fmt --diff ./...)
    if [ -n "$diff" ]; then
        printf '%s\n' "$diff"
        echo "Files are not properly formatted. Run 'golangci-lint fmt ./...'."
        exit 1
    fi

    echo " -> go test -race ./..."
    go test -race ./...
done

echo "All checks passed."
