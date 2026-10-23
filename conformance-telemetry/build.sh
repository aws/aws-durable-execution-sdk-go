#!/bin/sh
# Build script for the Go Durable Execution Telemetry conformance handlers.
# Compiles each handler into publish/<handler>/bootstrap for SAM deployment
# on the provided.al2023 runtime.
#
# The go.mod in this directory has committed `replace` directives that point
# at the parent SDK and the telemetry module, so no go.work file is needed.
#
# Requires:
#   - Go 1.24+ installed locally
#
# Usage:
#   ./build.sh [handler...]
#
# Handlers (default: every directory here that contains a main.go):
#   telemetry_1_success
#
# Examples:
#   ./build.sh telemetry_1_success
#   ./build.sh

set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PUBLISH_DIR="$SCRIPT_DIR/publish"

cd "$SCRIPT_DIR"

if [ $# -gt 0 ]; then
    handlers=$*
else
    handlers=$(find . -maxdepth 1 -mindepth 1 -type d \
        ! -name publish ! -name '.*' -exec basename {} \; | sort)
fi

for handler in $handlers; do
    [ -f "$handler/main.go" ] || continue
    out="$PUBLISH_DIR/$handler"
    echo "Building $handler -> publish/$handler/bootstrap"
    mkdir -p "$out"
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
        go build -tags lambda.norpc -o "$out/bootstrap" "./$handler"

    # SAM builds provided.al2023 functions through BuildMethod: makefile.
    # The make target is keyed on the function logical ID, which is the
    # PascalCase form of the handler directory name
    # (telemetry_1_success -> Telemetry1Success). The Makefile copies the
    # pre-built bootstrap.
    logical_id=$(echo "$handler" | awk -F_ '{for (i = 1; i <= NF; i++) printf "%s%s", toupper(substr($i, 1, 1)), substr($i, 2)}')
    cat > "$out/Makefile" << MKEOF
.PHONY: build-$logical_id

build-$logical_id:
	cp -r . \$(ARTIFACTS_DIR)/
	rm -f \$(ARTIFACTS_DIR)/Makefile
MKEOF
done

echo "Build complete. Binaries in $PUBLISH_DIR"
