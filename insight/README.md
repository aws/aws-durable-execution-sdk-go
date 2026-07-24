# Workflow Insight

Observability plugin for the AWS Durable Execution SDK for Go. Emits execution
snapshot records to configurable destinations as durable workflows run, complete,
or fail. Records capture execution metadata, timing, operation details, and
content fields (inputs, outputs, errors) for diagnostics and monitoring.

> **EXPERIMENTAL.** The plugin instrumentation API (`durable.WithPlugins`) is
> experimental and may change in future releases without a major-version bump.

## Installation

```console
go get github.com/aws/aws-durable-execution-sdk-go/insight
```

## Quick Start

```go
package main

import (
    "github.com/aws/aws-durable-execution-sdk-go/durable"
    "github.com/aws/aws-durable-execution-sdk-go/insight"
)

func handler(ctx durable.Context, event any) (any, error) {
    // ... your durable workflow ...
    return nil, nil
}

func main() {
    plugin := insight.New(insight.Config{})
    durable.Start(handler, durable.WithPlugins(plugin.Plugin()))
}
```

With a zero-value `Config`, the plugin emits a single JSON record to stdout when
the execution completes (succeeded or failed). Lambda captures stdout to the
function's CloudWatch log group automatically, so this requires zero additional
IAM permissions.

## Configuration

`insight.Config` controls all plugin behavior:

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
plugin := insight.New(insight.Config{
    SamplingRate: 0.1, // 10% of executions
})
```

### ContentConfig

Controls what data is included in emitted records and how it is formatted.

```go
type ContentConfig struct {
    IncludeInput            bool
    IncludeOutput           bool
    IncludeOperationResults bool
    IncludeOperationErrors  bool
    IncludeExecutionError   bool
    MaxContentLength        int
    OperationFilter         func(OperationRecord) bool
    Redactor                func(string) string
}
```

All `Include*` fields default to true when the entire struct is left at its zero
value. `MaxContentLength` defaults to 10 KB. Content exceeding that length is
truncated and the resulting `ContentField` has `Truncated: true`.

`DefaultContentConfig()` returns the default configuration explicitly.

### OperationsFormat

Some exporters support an `OperationsFormat` field that controls how operations
are rendered in the serialized JSON body:

| Value | Behavior |
| --- | --- |
| `OperationsFormatArray` | Standard `operations` JSON array. This is the default. |
| `OperationsFormatByName` | Operations grouped by name (`operationsByName`), no array. |
| `OperationsFormatBoth` | Both the array and the by-name aggregation. |

Exporters that support this: File, HTTP, SQS, EventBridge, Firehose.

## Exporters

The plugin dispatches records to all configured exporters concurrently. Each
exporter's errors are swallowed independently (never failing the execution or
affecting other exporters). If no exporters are configured, a single
`LambdaLogExporter` is used by default.

### LambdaLog (default)

Writes records as a single JSON line to stdout. Lambda captures this to the
function's CloudWatch log group automatically.

```go
exporter := insight.NewLambdaLogExporter()
```

Size limit: 256 KB (configurable via `MaxSizeBytes`).

### File

Writes each record as a JSON file to a local directory.

```go
exporter := insight.NewFileExporter(insight.FileExporterConfig{
    Dir:              "/tmp/insight-records",
    OperationsFormat: insight.OperationsFormatByName, // optional
})
```

A custom `FileNamer` function can override the default filename scheme
(`{hash8}_{timestamp}.json`).

### HTTP

POSTs JSON records to an HTTP endpoint. Retries on 5xx responses with
exponential backoff.

```go
exporter := insight.NewHTTPExporter(insight.HTTPExporterConfig{
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
exporter := &insight.S3Exporter{
    API:          s3Client,
    Bucket:       "my-insight-bucket",
    Prefix:       "workflow-insight/", // default
    Partitioning: insight.S3PartitioningDate, // default: year=YYYY/month=MM/day=DD/
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
exporter := &insight.CloudWatchLogsExporter{
    API:             cwlClient,
    LogGroupName:    "/aws/durable-execution/insight",
    LogStreamPrefix: "workflow-insight/", // default
}
```

Log streams are named `{prefix}{YYYY}/{MM}/{DD}` (one per UTC calendar day,
auto-created on first write).

Size limit: 256 KB (configurable via `MaxSizeBytes`).

### SQS

Sends records as SQS messages. FIFO mode is auto-detected from a `.fifo` suffix
on the queue URL.

```go
exporter := &insight.SQSExporter{
    API:      sqsClient,
    QueueURL: "https://sqs.us-east-1.amazonaws.com/123456789012/insight-queue",
}
```

Message attributes `status` and `functionName` are always set for filtering.
In FIFO mode, `MessageGroupID` defaults to the execution ARN and deduplication
is based on `{ExecutionArn}:{EmittedAt}`.

Size limit: 256 KB (configurable via `MaxSizeBytes`).

### EventBridge

Publishes records as EventBridge events for event-driven reactions.

```go
exporter := &insight.EventBridgeExporter{
    API:          ebClient,
    EventBusName: "default", // default
    Source:       "aws.durable-execution.insight", // default
}
```

`DetailType` is the record's status (SUCCEEDED/FAILED/RUNNING). `Detail` is the
full rendered record JSON.

Size limit: 256 KB (configurable via `MaxSizeBytes`).

### Firehose

Sends records to a Kinesis Data Firehose delivery stream for delivery to S3,
Redshift, Splunk, or any configured HTTP endpoint.

```go
exporter := &insight.FirehoseExporter{
    API:                firehoseClient,
    DeliveryStreamName: "insight-delivery-stream",
}
```

Records are appended with a trailing newline (NDJSON) so they remain individually
parseable when Firehose batches multiple records into a single S3 object.

Size limit: 1 MB (configurable via `MaxSizeBytes`).

### DynamoDB

Writes records as items to a DynamoDB table.

```go
exporter := &insight.DynamoDBExporter{
    API:       ddbClient,
    TableName: "workflow-insight",
}
```

Partition key (`pk` by default) is the execution ARN. Sort key (`sk` by default)
is the EmittedAt timestamp. Set `SortKeyField` to `insight.NoSortKey` to disable
the sort key (each export overwrites the previous record for the same execution).

Size limit: 400 KB (configurable via `MaxSizeBytes`).

### OpenSearch

Indexes records as documents via HTTP POST to the OpenSearch `_doc` API.

```go
exporter := &insight.OpenSearchExporter{
    Endpoint:  "https://search-my-domain.us-east-1.es.amazonaws.com",
    IndexName: "workflow-insight", // default
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
exporter := &insight.RedshiftExporter{
    API:           redshiftDataClient,
    WorkgroupName: "my-workgroup", // serverless mode
    Database:      "analytics",
    TableName:     "workflow_insight", // default
}
```

Supports two connection modes: serverless (`WorkgroupName`) or provisioned
(`ClusterIdentifier`). SQL is always parameterized.

### RDS/Aurora

Writes records to Aurora/RDS via the RDS Data API (HTTP-based, no VPC required).

```go
exporter := &insight.RDSExporter{
    API:         rdsDataClient,
    ResourceArn: "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster",
    SecretArn:   "arn:aws:secretsmanager:us-east-1:123456789012:secret:my-creds",
    Database:    "app_db",
    TableName:   "workflow_insight", // default
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

func (e *MyExporter) Export(ctx context.Context, record insight.Record) error {
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
| `RecordType` | string | Always `"WorkflowInsight"`. |
| `SchemaVersion` | string | Always `"1.0"`. |
| `ExecutionArn` | string | The durable execution's full ARN. |
| `ExecutionName` | string | Extracted from the ARN (the execution ID segment). |
| `FunctionName` | string | Extracted from the function ARN. |
| `Status` | Status | `RUNNING`, `SUCCEEDED`, or `FAILED`. |
| `StartTimestamp` | *time.Time | When the execution started. |
| `EndTimestamp` | *time.Time | When the execution ended (terminal records only). |
| `DurationMs` | *int64 | End minus start in milliseconds. |
| `InvocationCount` | int | Number of Lambda invocations for this execution. |
| `EmittedAt` | time.Time | When this record was emitted. |
| `Operations` | []OperationRecord | Operation snapshots (filtered by OperationDetail). |
| `Error` | *ErrorRecord | Execution-level error (failed executions only). |
| `Input` | *ContentField | Execution input (if ContentConfig allows). |
| `Output` | *ContentField | Execution output (if ContentConfig allows). |
| `Custom` | map[string]any | User-defined metadata. |
| `Truncated` | bool | True if the record was truncated for size. |
| `DroppedOperations` | int | Number of operations dropped during truncation. |
| `DroppedInput` | bool | True if input was dropped during truncation. |
| `DroppedOutput` | bool | True if output was dropped during truncation. |

Each `OperationRecord` contains ID, Name, Type, SubType, Status, ParentID,
timestamps, DurationMs, Attempt count, Result, Error, and Input content fields.

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
