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

| Suite       | Requirements  | What it pins down                                                                                                                              |
| ----------- | ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `telemetry` | `telemetry-1` | One successful single-step execution emits one `SUCCEEDED` record with `input`, `output`, ARN-derived identity fields, and `operationsByName`. |

## Layout

```
telemetry_1_success/main.go   # single step "increment"; plugin + CloudWatchLogsExporter
template.yaml                 # telemetry suite; declares TelemetryLogGroupName and creates the group
build.sh                      # builds publish/<handler>/bootstrap and the SAM Makefile
doc.go                        # second package, so `go build ./...` writes no executable
```

The handler uses the real SDK API only. It constructs no record and calls no exporter method.
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
./build.sh        # -> publish/telemetry_1_success/bootstrap and Makefile
```

Cloud run, from a checkout of the conformance repository with the telemetry package installed:

```bash
durable-execution-conformance \
  --template <this-repo>/conformance-telemetry/template.yaml \
  --language go --region us-west-2 --suite telemetry \
  --report console json \
  --parameter-overrides LambdaExecutionRoleArn=<role-arn>
```

A caller GitHub workflow is a follow-on.
