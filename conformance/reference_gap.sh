#!/bin/sh
# Lists the reference conformance handlers that have no Go counterpart.
#
# The Go suite mirrors the reference suites handler for handler: a reference
# handler handlers/<suite>/<name>.<ext> has its Go counterpart at
# <suite>/<name>/main.go in this directory. This script prints every
# reference handler whose Go counterpart is missing, one <suite>/<name> per
# line, and exits 1 when it printed anything. Run it after the reference
# suite adds a requirement to see what the Go suite still lacks.
#
# Usage:
#   ./reference_gap.sh <reference handlers directory>
#
# The argument is the handlers/ directory of a checked-out reference suite,
# for example packages/aws-durable-execution-sdk-js-conformance-tests/handlers
# in the JavaScript SDK repository or
# packages/aws-durable-execution-sdk-python-conformance-tests/handlers in the
# Python SDK repository. Suite directories are compared by name; a Python
# __init__.py is not a handler. The OpenTelemetry suites live in separate
# packages and are outside this comparison.

set -eu

if [ $# -ne 1 ] || [ ! -d "$1" ]; then
    echo "usage: $0 <reference handlers directory>" >&2
    exit 2
fi

ref=$1
here=$(cd "$(dirname "$0")" && pwd)
missing=0

for suite_dir in "$ref"/*/; do
    [ -d "$suite_dir" ] || continue
    suite=$(basename "$suite_dir")
    for file in "$suite_dir"*.*; do
        [ -f "$file" ] || continue
        name=$(basename "$file")
        name=${name%.*}
        case "$name" in
            __init__) continue ;;
        esac
        if [ ! -f "$here/$suite/$name/main.go" ]; then
            echo "$suite/$name"
            missing=1
        fi
    done
done

exit $missing
