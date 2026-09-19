#!/bin/sh
# Run the same checks locally that CI runs (.github/workflows/ci.yml):
# build, vet, lint, formatting, and race-enabled tests for every module.
#
# Usage:
#   scripts/ci-local.sh [module...]
#
# Modules default to all five: . insight conformance examples analysis
# (e.g. `scripts/ci-local.sh examples` to check only the examples module).
#
# Requires: Go and golangci-lint at the versions pinned in .mise.toml
# (`mise install` sets both up).

set -eu

ROOT=$(cd "$(dirname "$0")/.." && pwd)

if [ $# -gt 0 ]; then
    MODULES=$*
else
    MODULES=". insight conformance examples analysis"
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

    if [ "$mod" = "analysis" ]; then
        # The determinism analyzer must stay clean on the repository's own
        # examples and conformance handlers.
        bin=$(mktemp -t durablelint.XXXXXX)
        echo " -> go build -o durablelint ./cmd/durablelint"
        go build -o "$bin" ./cmd/durablelint
        for target in examples conformance; do
            echo " -> durablelint ./... (in $target)"
            (cd "$ROOT/$target" && "$bin" ./...)
        done
        rm -f "$bin"
    fi
done

echo "All checks passed."
