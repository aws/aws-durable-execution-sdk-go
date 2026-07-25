#!/bin/sh
# Build script for Go Durable Execution SDK examples.
# Compiles each example into .aws-sam/build-artifacts/<example>/bootstrap
# for SAM deployment on the provided.al2023 runtime.
#
# Usage:
#   ./build.sh
#
# The examples module uses a committed replace directive (go.mod) pointing
# at the parent SDK directory (../), so no external SDK path argument is
# needed.

set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
BUILD_DIR="$SCRIPT_DIR/.aws-sam/build-artifacts"

cd "$SCRIPT_DIR"

# List of examples to build. Each entry is a directory name under examples/.
EXAMPLES="
  order-fulfillment
  simple-step
  named-step
  step-with-retry
  steps-with-retry
  attempt-fallback
  interrupted-no-retry
  step-error-determinism
  retry-exhaustion
  retry-invoke
  retry-invoke-target
  retry-callback
  wait-basic
  wait-named
  wait-configurable
  wait-unawaited
  wait-for-condition
  multiple-waits
  invoke-simple
  invoke-simple-target
  invoke-tenant-id
  invoke-tenant-target
  chained-invoke
  child-context-basic
  child-context-virtual
  child-context-serdes
  child-context-large-data
  child-context-error-propagation
  child-context-failing-step
  child-context-checkpoint-size-limit
  child-ops-preservation
  child-ops-invalid-depth
  callback-sender
  wait-callback-basic
  wait-callback-timeout
  wait-callback-quick-completion
  wait-callback-anonymous
  wait-callback-submitter-failure
  wait-callback-failures
  wait-callback-submitter-retry
  create-callback-simple
  create-callback-timeout
  create-callback-failures
  create-callback-concurrent
  wait-callback-nested
  wait-callback-serdes
  wait-callback-mixed-ops
  wait-callback-multiple-invocations
  wait-callback-child-context
  wait-callback-heartbeat
  wait-callback-error-instance-failure
  wait-callback-error-instance-submitter
  wait-callback-error-instance-timeout
  create-callback-heartbeat
  create-callback-serdes
  create-callback-mixed-ops
  create-callback-error-instance
  map-basic
  map-empty
  map-large-scale
  map-error-preservation
  map-min-successful
  map-tolerated-failure-count
  map-failure-threshold
  map-tolerated-failure-percentage
  map-completion-config-issue
  map-virtual-context
  parallel-basic
  parallel-empty
  parallel-invoke
  parallel-wait
  parallel-error-preservation
  parallel-min-successful
  parallel-tolerated-failure
  parallel-tolerated-failure-percentage
  parallel-virtual-context
  future-all
  future-all-settled
  future-all-wait
  future-any
  future-race
  future-race-wait
  future-combinators-mixed
  future-replay
  future-unhandled-error
  concurrent-wait
  concurrent-operations
  concurrent-callback-submitter
  concurrent-callback-wait
  logger-after-wait
  logger-after-callback
  logger-log-levels
  serde-basic
  serde-custom-config
  context-validation-child
  context-validation-step
  context-validation-wait-condition
  force-checkpoint-callback
  force-checkpoint-invoke
  force-checkpoint-wait
  force-checkpoint-step-retry
  error-determinism
  handler-error
  hello-world
  simple-execution
  non-durable
  no-replay-execution
  large-payload
  undefined-results
  comprehensive-operations
"

for example in $EXAMPLES; do
  out="$BUILD_DIR/$example"
  echo "Building $example -> .aws-sam/build-artifacts/$example/bootstrap"
  mkdir -p "$out"
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
      go build -tags lambda.norpc -o "$out/bootstrap" "./$example"

  # SAM BuildMethod: makefile requires a Makefile target matching the logical
  # resource ID from template.yaml. The target copies the pre-built binary
  # into SAM's artifacts directory.
  resource_id=$(echo "$example" | sed 's/-\([a-z]\)/\U\1/g; s/^\([a-z]\)/\U\1/' | sed 's/$/Function/')
  cat > "$out/Makefile" << MKEOF
.PHONY: build-${resource_id}

build-${resource_id}:
	cp -r . \$(ARTIFACTS_DIR)/
	rm -f \$(ARTIFACTS_DIR)/Makefile
MKEOF
done

echo "Build complete. Artifacts in $BUILD_DIR"
