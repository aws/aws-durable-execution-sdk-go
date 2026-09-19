#!/bin/sh
# Reference parity check for the Go examples.
#
# Lists every reference SDK example that has no Go counterpart, so parity can
# be re-checked whenever the reference adds an example. The mapping from
# reference handler to Go example lives in parity-map.txt, because the Go
# examples are matched by behaviour rather than by name.
#
# Usage:
#   ./parity.sh <path>
#
# <path> is a clone of the reference SDK repository or its examples
# directory (the one containing src/examples/). A reference handler is any
# .ts file under src/examples/ that is not a test and does not live under
# the shared/ or utils/ helper directories.
#
# Output, one line per gap:
#   unmapped <reference>        the reference handler has no row in
#                               parity-map.txt; decide on a Go example
#                               (or a deliberate omission) and add one
#   missing  <reference> -> <go> the row exists but examples/<go>/main.go
#                               does not; the Go example is still to be
#                               written
#   stale    <reference> -> <go> the row names a reference handler that no
#                               longer exists; remove or rename the row
#
# Exits 0 when there is no gap of any kind, 1 otherwise.

set -eu

if [ $# -ne 1 ]; then
  echo "usage: $0 <reference-repository-or-examples-dir>" >&2
  exit 2
fi

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
MAP="$SCRIPT_DIR/parity-map.txt"

ref="$1"
for candidate in \
  "$ref/src/examples" \
  "$ref/packages/aws-durable-execution-sdk-js-examples/src/examples"; do
  if [ -d "$candidate" ]; then
    ref_examples="$candidate"
    break
  fi
done
if [ -z "${ref_examples:-}" ]; then
  echo "$0: no src/examples directory under $ref" >&2
  exit 2
fi

# Reference handlers: path under src/examples/ without the .ts extension.
handlers=$(cd "$ref_examples" && find . -name '*.ts' ! -name '*.test.ts' ! -name '*.d.ts' \
  ! -path './shared/*' ! -path './utils/*' | sed 's|^\./||; s|\.ts$||' | sort)

# Mapping rows: "<reference> <go>", comments and blank lines removed.
rows=$(grep -v '^[[:space:]]*#' "$MAP" | grep -v '^[[:space:]]*$' | awk '{print $1, $2}')

gaps=0

for h in $handlers; do
  go=$(printf '%s\n' "$rows" | awk -v h="$h" '$1 == h {print $2; exit}')
  if [ -z "$go" ]; then
    echo "unmapped $h"
    gaps=$((gaps + 1))
  elif [ ! -f "$SCRIPT_DIR/$go/main.go" ]; then
    echo "missing  $h -> $go"
    gaps=$((gaps + 1))
  fi
done

printf '%s\n' "$rows" | while read -r h go; do
  if ! printf '%s\n' "$handlers" | grep -qx "$h"; then
    echo "stale    $h -> $go"
  fi
done > "${TMPDIR:-/tmp}/parity-stale.$$"
if [ -s "${TMPDIR:-/tmp}/parity-stale.$$" ]; then
  cat "${TMPDIR:-/tmp}/parity-stale.$$"
  gaps=$((gaps + $(wc -l < "${TMPDIR:-/tmp}/parity-stale.$$")))
fi
rm -f "${TMPDIR:-/tmp}/parity-stale.$$"

total=$(printf '%s\n' "$handlers" | wc -l | tr -d ' ')
if [ "$gaps" -eq 0 ]; then
  echo "parity: all $total reference examples have a Go counterpart"
  exit 0
fi
echo "parity: $gaps gap(s) across $total reference examples"
exit 1
