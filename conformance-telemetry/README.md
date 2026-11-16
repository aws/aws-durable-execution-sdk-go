# Durable Execution Go SDK: Durable Execution Telemetry Conformance Tests

This module answers one question. Does the [`telemetry`](../telemetry) module, with its
`CloudWatchLogsExporter`, deliver a complete `DurableExecutionTelemetry` record to CloudWatch Logs
for a real Lambda execution?

Unit tests can assert that the plugin builds a record. They cannot assert that the record reaches
the configured log group on a real invocation, or that it carries the right identity fields for the
deployed function. Those are properties of a deployed function. So the scenario here is a Lambda
function. It is deployed with the plugin configured and invoked once. The runner then fetches the
record from CloudWatch Logs and checks it against the requirement.

The requirement and the runner that enforces it live in
[`aws/aws-durable-execution-conformance-tests`](https://github.com/aws/aws-durable-execution-conformance-tests),
package `aws-durable-execution-conformance-tests-telemetry`. This module holds the SDK-specific
part: the handler and the SAM template that maps it to the requirement ID.

## Why a separate module

The `conformance` module is run in CI by a job that globs `conformance/template_*.yaml`. That job
uses the core runner, which has no telemetry extension. A telemetry template placed there would be
run with `--suite telemetry` and fail. So the telemetry handlers live in their own module.

## Coverage

| Suite       | Requirement   | What it checks                                                                                                                                  |
| ----------- | ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `telemetry` | `telemetry-1` | One successful single-step execution emits one `SUCCEEDED` record with `input`, `output`, ARN-derived identity fields, and `operationsByName`.  |
| `telemetry` | `telemetry-2` | One execution whose single step fails emits one `FAILED` record with an `error` on the record and on the step, and no `output`.                 |
| `telemetry` | `telemetry-3` | A result override on one step exports exactly one selected key of the step result. Execution `input` and `output` are excluded.                 |
| `telemetry` | `telemetry-4` | With `full-tree` operation detail, the record carries a map, its two named iteration contexts, and the step inside each iteration.              |

## Layout

```
telemetry_1_success/main.go         # single step "increment"; plugin + CloudWatchLogsExporter
telemetry_2_failure/main.go         # single step "charge" that fails with no retry
telemetry_3_result_select/main.go   # single step "fetch-user"; result override keeps one key
telemetry_4_full_tree_map/main.go   # map "process-items" with one step per item; full-tree detail
template.yaml                       # telemetry suite; declares TelemetryLogGroupName and creates the group
build.sh                            # builds publish/<handler>/bootstrap and the SAM Makefile
doc.go                              # second package, so `go build ./...` writes no executable
```

Each handler uses the real SDK API only. It constructs no record and calls no exporter method.
A missing or wrong record is the plugin's defect. That defect is the signal the suite measures.

## Template contract

The runner passes `TelemetryLogGroupName` from the extension. Extensions have no pre-deploy hook.
So the template creates the group with an `AWS::Logs::LogGroup` resource, and the function
depends on it. The group name reaches the handler through `TELEMETRY_LOG_GROUP_NAME`. The handler
exits at start-up if that variable is unset.

The `LambdaExecutionRoleArn` role must allow `logs:CreateLogStream` and `logs:PutLogEvents` on the
group. `AWSLambdaBasicDurableExecutionRolePolicy` grants both on `*`.

## Building locally

Prerequisites: Go 1.24 or later.

```bash
cd conformance-telemetry
go build ./... && go vet ./...
./build.sh        # -> publish/<handler>/bootstrap and Makefile, for every handler
```

Cloud run, from a checkout of the conformance repository with the telemetry package installed:

```bash
durable-execution-conformance \
  --template <this-repo>/conformance-telemetry/template.yaml \
  --language go --region us-west-2 --suite telemetry \
  --report console json \
  --parameter-overrides LambdaExecutionRoleArn=<role-arn>
```

## CI

[`.github/workflows/telemetry-conformance-tests.yml:5–78`](../.github/workflows/telemetry-conformance-tests.yml#L5-L78)
runs on pull requests and pushes to `main` that touch the SDK, the telemetry
module, this module, or related CI and dependency files. It also supports
manual dispatch.

It calls the conformance repository's reusable telemetry workflow at a pinned
SHA. `sdk_ref` selects the PR head commit, and `examples_dir` points to this
module. The reusable workflow installs no Go toolchain, so
[`setup-go.sh`](../.github/workflows/scripts/setup-go.sh) installs a
checksum-verified Go release. CI vets the module and builds the handlers with
`build.sh`, because SAM's makefile build only copies the built `bootstrap`
binary. CI then deploys the template and validates the `telemetry` suite.
Reports and execution histories are uploaded as artifacts.

CI assumes AWS credentials through OIDC using the repository's existing test
secrets. Fork and Dependabot pull requests skip the cloud job.
