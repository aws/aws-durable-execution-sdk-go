#!/bin/sh
# Build script for Go Durable Execution conformance test handlers.
# Compiles each handler into publish/<handler>/bootstrap for SAM deployment
# on the provided.al2023 runtime.
#
# The conformance/go.mod uses a committed `replace` directive pointing at
# the parent SDK (../), so no go.work generation is needed.
#
# Requires:
#   - Go 1.25+ installed locally
#
# Usage:
#   ./build_examples.sh [operation...]
#
# Operations (default: all found in this directory):
#   step, wait, callback, child, invoke, parallel, wait_for_callback,
#   wait_for_condition, map
#
# Examples:
#   ./build_examples.sh step
#   ./build_examples.sh

set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PUBLISH_DIR="$SCRIPT_DIR/publish"

cd "$SCRIPT_DIR"

if [ $# -gt 0 ]; then
    operations=$*
else
    operations=$(find . -maxdepth 1 -mindepth 1 -type d \
        ! -name publish ! -name internal ! -name '.*' -exec basename {} \;)
fi

for op in $operations; do
    [ -d "$op" ] || { echo "Error: unknown operation '$op'." >&2; exit 1; }
    for dir in "$op"/*/; do
        [ -d "$dir" ] || continue
        handler=$(basename "$dir")
        out="$PUBLISH_DIR/$handler"
        echo "Building $op/$handler -> publish/$handler/bootstrap"
        mkdir -p "$out"
        CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
            go build -tags lambda.norpc -o "$out/bootstrap" "./$op/$handler"

        # SAM builds provided.al2023 functions through BuildMethod:
        # makefile, whose target is keyed on the function logical ID.
        # Logical IDs are the PascalCase form of the handler dir name
        # (step_basic -> StepBasic). The Makefile just copies the
        # pre-built bootstrap, mirroring the dotnet examples.
        logical_id=$(echo "$handler" | awk -F_ '{for (i = 1; i <= NF; i++) printf "%s%s", toupper(substr($i, 1, 1)), substr($i, 2)}')
        cat > "$out/Makefile" << MKEOF
.PHONY: build-$logical_id

build-$logical_id:
	cp -r . \$(ARTIFACTS_DIR)/
	rm -f \$(ARTIFACTS_DIR)/Makefile
MKEOF
    done
done

echo "Build complete. Binaries in $PUBLISH_DIR"
