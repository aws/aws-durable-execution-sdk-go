# Durable Execution Telemetry Exporter

Observability plugin for the AWS Durable Execution SDK for Go. Emits execution
snapshot records to configurable destinations as durable workflows run, complete,
or fail. Records capture execution metadata, timing, operation details, and
content fields (inputs, outputs, errors) for diagnostics and monitoring.

> This module is built on the experimental plugin API (`durable.Plugin`,
> `durable.PluginFactory`, `durable.WithPlugins`), so its versions stay
> below v1. Any release may change the `telemetry` package API and the
> record schema. Upgrade this module together with the SDK. See Versioning
> in the [root README](../README.md#versioning).

## Installation

```console
go get github.com/aws/aws-durable-execution-sdk-go/telemetry
```

## Quick Start

```go
package main

import (
    "github.com/aws/aws-durable-execution-sdk-go/durable"
    "github.com/aws/aws-durable-execution-sdk-go/telemetry"
)

func handler(ctx durable.Context, event any) (any, error) {
    // ... your durable workflow ...
    return nil, nil
}

func main() {
    // TelemetryPlugin is a durable.PluginFactory: the handler calls its
    // CreatePlugin once per invocation.
    durable.Start(handler, durable.WithPlugins(telemetry.New(telemetry.Config{})))
}
```

With a zero-value `Config`, the plugin emits a single JSON record to stdout when
the execution completes (succeeded or failed). Lambda captures stdout to the
function's CloudWatch log group automatically, so this requires zero additional
IAM permissions.

## Invocation lifetime and concurrency

`TelemetryPlugin` is a factory shared by the execution environment. Each
`CreatePlugin` call creates a separate record, operation index, sampling decision,
mutex, and on-change dispatch state. Hooks and log enrichment for one invocation
use only that invocation's state. Concurrent operation hooks within an invocation
remain synchronized.

The factory initializes execution metadata, input, and sampling before returning
the hooks. A resumed invocation retains the execution ARN and original execution
start timestamp. Its request ID identifies the new invocation. Sampling uses the
execution ARN, so the decision stays the same across resumes.

At invocation start, the plugin initializes its operation buffer from the full
checkpointed snapshot. This includes unchanged nested operations in completed
child contexts whose bodies do not run again. The root execution operation is
excluded from the operations array. `OperationDetail` and the SDK's
`durable.WithPluginChildOperationsDepth` still limit the operations reported.
Subsequent hooks update the buffer by operation ID.

Exporters and content transforms remain shared. `InputTransform`,
`OutputTransform`, and each `OperationOverride.Result` may be called concurrently
by different invocations. Callers must make these functions and their captured
state safe for concurrent use. Within one invocation, content transforms remain
serialized.

Each invocation waits for its own in-flight on-change dispatches before its final
emission. It does not wait for another invocation's dispatches. Shared exporters
can receive overlapping `Export` and `Flush` calls from different invocations.
This change does not add a shared scheduler or establish a complete exporter
concurrency contract. Invocation-local records alone do not make an exporter or
its dependencies concurrency-safe. Exporter concurrency and scheduling remain
separate follow-up work.

## Configuration

`telemetry.Config` controls all plugin behavior:

```go
type Config struct {
    EmitMode        EmitMode
    OperationDetail OperationDetail
    SamplingRate    float64
    Exporters       []Exporter
    Content         ContentConfig
}
```

### EmitMode

Controls when a record is sent to exporters.

| Value | Behavior |
| --- | --- |
| `EmitOnComplete` | Emit only when the execution reaches a terminal state (succeeded or failed). This is the default. |
| `EmitAlways` | Emit after every invocation regardless of outcome. |
| `EmitOnChange` | Emit a running snapshot on every operation start/end, plus the terminal record. |

### OperationDetail

Controls which operations appear in the record's `Operations` array.

| Value | Behavior |
| --- | --- |
| `OperationDetailTopLevel` | Only top-level operations (those without a ParentID). This is the default. |
| `OperationDetailFullTree` | Every operation, including children of contexts. |

### SamplingRate

Fraction of executions (0.0 to 1.0) that emit records. Defaults to 1.0 (every
execution). The decision is per-execution and deterministic, derived from an
FNV-1a hash of the execution ARN.

```go
plugin := telemetry.New(telemetry.Config{
    SamplingRate: 0.1, // 10% of executions
})
```

### ContentConfig

Controls what content appears in emitted records.

```go
type ContentConfig struct {
    ExcludeInput    bool
    InputTransform  func(any) any
    ExcludeOutput   bool
    OutputTransform func(any) any
    Operations      ContentOperations
}

type ContentOperations struct {
    Overrides     []OperationOverride
    ExcludeErrors bool
}

type OperationOverride struct {
    OperationName string
    Exclude       bool
    Result        func(any) any
}
```

The zero value includes the execution input and output unchanged, omits
operation results, and includes operation and execution errors. Each field
changes one thing, so set only the fields you need.

```go
telemetry.New(telemetry.Config{
    Content: telemetry.ContentConfig{
        // Remove a sensitive field from the recorded input.
        InputTransform: func(v any) any {
            if m, ok := v.(map[string]any); ok {
                delete(m, "password")
            }
            return v
        },
        // Do not record the output at all.
        ExcludeOutput: true,
        Operations: telemetry.ContentOperations{
            Overrides: []telemetry.OperationOverride{
                // Record this operation's result, masking one field.
                {OperationName: "fetch-user", Result: func(v any) any {
                    if m, ok := v.(map[string]any); ok {
                        m["email"] = "***"
                    }
                    return v
                }},
                // Drop this operation from the record.
                {OperationName: "debug-dump", Exclude: true},
            },
        },
    },
})
```

Content fields are JSON values (`json.RawMessage`):

- `Record.Input` and `Record.Output` are the handler's typed input and result,
  marshaled once. A Go `string` becomes a JSON string. A nil pointer, map, or
  slice becomes `null`, the same text the SDK checkpoints for that value.
- `OperationRecord.Result` is the checkpointed result. It is recorded only for
  operations named by an override that sets `Result`.

A transform receives the content decoded as a JSON value: `map[string]any`,
`[]any`, `string`, `json.Number`, `bool`, or `nil`. A struct arrives as a
`map[string]any`, so a field can be deleted or overwritten by key. Numbers
arrive as `json.Number` rather than `float64`. A `float64` holds 53 bits of
mantissa, so an integer above 2^53 would be rounded by the decode alone.
`json.Number` keeps the exact digits; call `Int64()` or `Float64()` on it to do
arithmetic. The return value is what the record carries. `InputTransform` and `OutputTransform` are
not called when the matching `Exclude*` field is true.

`OperationOverride.Result` receives the checkpointed text decoded as a JSON
value, or the text itself as a `string` when it is not valid JSON. The plugin
does not run the operation's own serializer, so an operation with a custom
Serdes delivers its serialized form. To record a result unchanged, return the
argument.

If a transform panics, or returns a value that cannot be marshaled, the field
is omitted. The untransformed value is never recorded in its place.

`Overrides` are matched by `OperationName`. When several entries share a name,
the last one wins. `ExcludeErrors` omits the `error` field from operation
entries; the execution-level `error` on the record is always included.

There is no per-field length cap. An exporter that implements `SizeLimiter`
bounds the whole record instead; see [Size Limits](#size-limits).

Transforms may run concurrently across invocations. Their functions and
captured state must support concurrent use. See
[Invocation lifetime and concurrency](#invocation-lifetime-and-concurrency).

### OperationsFormat

Some exporters support an `OperationsFormat` field that controls how operations
are rendered in the serialized JSON body:

| Value | Behavior |
| --- | --- |
| `OperationsFormatArray` | Standard `operations` JSON array. This is the default. |
| `OperationsFormatByName` | Operations grouped by name (`operationsByName`), no array. |
| `OperationsFormatBoth` | Both the array and the by-name aggregation. |

Exporters that support this: File, HTTP, SQS, EventBridge, Firehose.

The LambdaLog, CloudWatch Logs, and DynamoDB exporters have no
`OperationsFormat` field. They always render `OperationsFormatByName`. See
[LambdaLog](#lambdalog-default), [CloudWatch Logs](#cloudwatch-logs), and
[DynamoDB](#dynamodb).

## Exporters

The plugin dispatches records to all configured exporters concurrently. Each
exporter's errors are swallowed independently (never failing the execution or
affecting other exporters). If no exporters are configured, a single
`LambdaLogExporter` is used by default.

### LambdaLog (default)

Writes records as a single JSON line to stdout. Lambda captures this to the
function's CloudWatch log group automatically.

```go
exporter := telemetry.NewLambdaLogExporter()
```

Each line carries `operationsByName` and no `operations` array. The format is
fixed. CloudWatch Logs Insights selects a field by a dotted path such as
`operationsByName.increment.count`. A query cannot select an element of the
`operations` array by its name. The JavaScript, Python, and Java LambdaLog
exporters also render `operationsByName` and have no format option.

Size limit: 256 KB (configurable via `MaxSizeBytes`). The limit applies to the
`operationsByName` rendering.

### File

Writes each record as a JSON file to a local directory.

```go
exporter := telemetry.NewFileExporter(telemetry.FileExporterConfig{
    Dir:              "/tmp/telemetry-records",
    OperationsFormat: telemetry.OperationsFormatByName, // optional
})
```

A custom `FileNamer` function can override the default filename scheme
(`{hash8}_{timestamp}.json`).

### HTTP

POSTs JSON records to an HTTP endpoint. Retries on 5xx responses with
exponential backoff.

```go
exporter := telemetry.NewHTTPExporter(telemetry.HTTPExporterConfig{
    URL: "https://example.com/ingest",
    Headers: map[string]string{
        "Authorization": "Bearer my-token",
    },
    Timeout:    5 * time.Second,
    MaxRetries: 2,
})
```

### S3

Writes records as JSON objects to an S3 bucket with configurable partitioning.

```go
exporter := &telemetry.S3Exporter{
    API:          s3Client,
    Bucket:       "my-telemetry-bucket",
    Prefix:       "durable-execution-telemetry/", // default
    Partitioning: telemetry.S3PartitioningDate, // default: year=YYYY/month=MM/day=DD/
}
```

Partitioning options: `S3PartitioningDate` (default), `S3PartitioningFunctionName`,
`S3PartitioningNone`.

Size limit: 5 MB (configurable via `MaxSizeBytes`).

### CloudWatch Logs

Writes records to a specific CloudWatch Logs log group via PutLogEvents. Unlike
LambdaLog (which relies on Lambda's stdout capture), this exporter lets you
centralize records from multiple functions into a shared, queryable log group.

```go
exporter := &telemetry.CloudWatchLogsExporter{
    API:             cwlClient,
    LogGroupName:    "/aws/durable-execution/telemetry",
    LogStreamPrefix: "durable-execution-telemetry/", // default
}
```

Log streams are named `{prefix}{YYYY}/{MM}/{DD}` (one per UTC calendar day,
auto-created on first write).

Each log event carries `operationsByName` and no `operations` array, for the
same reason as [LambdaLog](#lambdalog-default). The format is fixed.

Size limit: 256 KB (configurable via `MaxSizeBytes`). The limit applies to the
`operationsByName` rendering.

### SQS

Sends records as SQS messages. FIFO mode is auto-detected from a `.fifo` suffix
on the queue URL.

```go
exporter := &telemetry.SQSExporter{
    API:      sqsClient,
    QueueURL: "https://sqs.us-east-1.amazonaws.com/123456789012/telemetry-queue",
}
```

Message attributes `status` and `functionName` are always set for filtering.
In FIFO mode, `MessageGroupID` defaults to the execution ARN and deduplication
is based on `{ExecutionArn}:{EmittedAt}`.

Size limit: 256 KB (configurable via `MaxSizeBytes`).

### EventBridge

Publishes records as EventBridge events for event-driven reactions.

```go
exporter := &telemetry.EventBridgeExporter{
    API:          ebClient,
    EventBusName: "default", // default
    Source:       "software.amazon.lambda.durable.telemetry", // default
}
```

`DetailType` is the record's status (SUCCEEDED/FAILED/RUNNING). `Detail` is the
full rendered record JSON.

Size limit: 256 KB (configurable via `MaxSizeBytes`).

### Firehose

Sends records to a Kinesis Data Firehose delivery stream for delivery to S3,
Redshift, Splunk, or any configured HTTP endpoint.

```go
exporter := &telemetry.FirehoseExporter{
    API:                firehoseClient,
    DeliveryStreamName: "telemetry-delivery-stream",
}
```

Records are appended with a trailing newline (NDJSON) so they remain individually
parseable when Firehose batches multiple records into a single S3 object.

Size limit: 1 MB (configurable via `MaxSizeBytes`).

### DynamoDB

Writes records as items to a DynamoDB table.

```go
exporter := &telemetry.DynamoDBExporter{
    API:       ddbClient,
    TableName: "durable-execution-telemetry",
}
```

Each item holds the record in the by-name shape: `operationsByName` replaces
the `operations` array (see [OperationsFormat](#operationsformat)). Every
top-level key of that record JSON is its own item attribute. There is no
`recordJson` attribute. The JavaScript, Python, and Java exporters also write
one attribute per top-level key of the by-name record.

Each JSON value becomes the matching DynamoDB attribute type:

| JSON value | Attribute type |
| --- | --- |
| object | `M` |
| array | `L` |
| string | `S` |
| number | `N` |
| boolean | `BOOL` |
| null | `NULL` |

So `input`, `output`, and `operationsByName` are `M` attributes when the value
is an object. A reader can address a nested value by its document path, for
example `operationsByName.increment.count`. A number keeps the digits of the
record JSON. DynamoDB stores at most 38 significant digits, and a non-zero
magnitude between 1E-130 and 9.99...E+125. A record that carries any other
number, for example a `float64` value of `1e200`, fails `PutItem` with a
`ValidationException`.

Partition key (`pk` by default) is the execution ARN. Sort key (`sk` by default)
is the EmittedAt timestamp. Set `SortKeyField` to `telemetry.NoSortKey` to disable
the sort key (each export overwrites the previous record for the same execution).
The key attributes are written after the record attributes. So a key attribute
name that equals a record key, for example `PartitionKeyField: "executionName"`,
holds the key value.

Size limit: 400 KB (configurable via `MaxSizeBytes`). The exporter implements
`RenderingSizeLimiter`, so truncation sizes the by-name record JSON.

### OpenSearch

Indexes records as documents via HTTP POST to the OpenSearch `_doc` API.

```go
exporter := &telemetry.OpenSearchExporter{
    Endpoint:  "https://search-my-domain.us-east-1.es.amazonaws.com",
    IndexName: "durable-execution-telemetry", // default
    Username:  "admin",
    Password:  "secret",
}
```

Supports Basic auth (Username/Password) or Bearer token authentication.

Size limit: 10 MB (configurable via `MaxSizeBytes`).

### Redshift

Writes records to Amazon Redshift via the Redshift Data API (HTTP-based, no VPC
required). Fire-and-forget: does not poll for statement completion.

```go
exporter := &telemetry.RedshiftExporter{
    API:           redshiftDataClient,
    WorkgroupName: "my-workgroup", // serverless mode
    Database:      "analytics",
    TableName:     "durable_execution_telemetry", // default
}
```

Supports two connection modes: serverless (`WorkgroupName`) or provisioned
(`ClusterIdentifier`). SQL is always parameterized.

### RDS/Aurora

Writes records to Aurora/RDS via the RDS Data API (HTTP-based, no VPC required).

```go
exporter := &telemetry.RDSExporter{
    API:         rdsDataClient,
    ResourceArn: "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster",
    SecretArn:   "arn:aws:secretsmanager:us-east-1:123456789012:secret:my-creds",
    Database:    "app_db",
    TableName:   "durable_execution_telemetry", // default
}
```

Size limit: 1 MB (configurable via `MaxSizeBytes`).

## Custom Exporters

Implement the `Exporter` interface to send records to any destination:

```go
type Exporter interface {
    Export(ctx context.Context, record Record) error
}
```

Optional interfaces an exporter may additionally implement:

- `Flusher` - flush buffered data before the invocation returns.
- `SizeLimiter` - opt into size-based truncation with a `MaxRecordSizeBytes() int` method.
- `RenderingSizeLimiter` - `SizeLimiter` plus a `Render(Record) ([]byte, error)` method for format-aware size checking.

```go
type MyExporter struct{}

func (e *MyExporter) Export(ctx context.Context, record telemetry.Record) error {
    // Send record to your destination
    return nil
}

// Optional: implement Flusher
func (e *MyExporter) Flush(ctx context.Context) error {
    return nil
}
```

## Record Structure

Each emitted `Record` contains:

| Field | Type | Description |
| --- | --- | --- |
| `RecordType` | string | Always `"DurableExecutionTelemetry"`. |
| `SchemaVersion` | string | Always `"1.0"`. |
| `ExecutionArn` | string | The durable execution's full ARN. |
| `ExecutionName` | string | Extracted from the execution-name segment of the ARN. |
| `FunctionName` | string | Function name parsed from the execution ARN. Always emitted. |
| `FunctionQualifier` | string | Function qualifier parsed from the execution ARN (`$LATEST`, a version, or an alias). JSON key `functionQualifier`. Always emitted. |
| `Region` | string | Region parsed from the execution ARN. Always emitted. |
| `AccountID` | string | Account ID parsed from the execution ARN. JSON key `accountId`. Always emitted. |
| `Status` | Status | `RUNNING`, `SUCCEEDED`, or `FAILED`. |
| `StartTimestamp` | *time.Time | When the execution started. JSON key `startTime`. Falls back to the oldest operation start, then the invocation time, when the payload omits it. |
| `EndTimestamp` | *time.Time | When the execution ended (terminal records only). JSON key `endTime`. |
| `DurationMs` | *int64 | End minus start in milliseconds. |
| `EmittedAt` | time.Time | When this record was emitted. Strictly increases across the records of one invocation. |
| `Operations` | []OperationRecord | Operation snapshots (filtered by OperationDetail). |
| `Error` | *ErrorRecord | Execution-level error (failed executions only). |
| `Input` | json.RawMessage | Execution input as a JSON value (if ContentConfig allows). |
| `Output` | json.RawMessage | Execution output as a JSON value (if ContentConfig allows). |
| `Custom` | map[string]any | User-defined metadata. |
| `Truncated` | bool | True if the record was truncated for size. |
| `DroppedOperations` | int | Number of operations dropped during truncation. |
| `DroppedInput` | bool | True if input was dropped during truncation. |
| `DroppedOutput` | bool | True if output was dropped during truncation. |

Each `OperationRecord` contains ID, Name, Type, SubType, Status, ParentID,
timestamps (JSON keys `startTime` and `endTime`), DurationMs, Attempt count,
Result (a JSON value), and Error.

Every timestamp in the JSON output (`emittedAt`, `startTime`, `endTime`, at
both the record and operation level) is UTC with exactly three fraction digits
and a `Z` suffix, for example `2026-10-24T02:22:35.088Z`. The Go fields stay
`time.Time`; `Record.MarshalJSON` and `OperationRecord.MarshalJSON` apply the
format. `json.Unmarshal` into `Record` parses it back. The DynamoDB sort key,
the SQS FIFO deduplication ID, and the RDS/Redshift `emitted_at` column use the
same format.

## Size Limits

Exporters that implement `SizeLimiter` trigger automatic record truncation when
the serialized record exceeds their limit. Truncation drops operation results
first (largest to smallest), then input/output fields, and sets the `Truncated`
metadata fields.

| Exporter | Default Limit |
| --- | --- |
| LambdaLog | 256 KB |
| CloudWatch Logs | 256 KB |
| SQS | 256 KB |
| EventBridge | 256 KB |
| DynamoDB | 400 KB |
| Firehose | 1 MB |
| RDS/Aurora | 1 MB |
| S3 | 5 MB |
| OpenSearch | 10 MB |

All limits are configurable via the exporter's `MaxSizeBytes` field.

## License

Apache-2.0. See [LICENSE](../LICENSE).
