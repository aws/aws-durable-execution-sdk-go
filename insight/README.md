# insight — Workflow Insight for the AWS Durable Execution SDK for Go

**Workflow Insight** is an EXPERIMENTAL observability plugin for
[AWS Lambda durable functions](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html),
ported from the JS reference SDK's own
[`@aws/durable-execution-sdk-js-insight`](https://github.com/aws/aws-durable-execution-sdk-js)
package. It automatically captures execution state — status, timing,
operations, input/output, and errors — and exports it to the
destination(s) of your choice. One line of configuration gives you full
visibility into your durable workflows without building custom
instrumentation.

This is its own, separate Go module (`insight/go.mod`), not part of the
core `github.com/aws/aws-durable-execution-sdk-go` module — see
[Why a separate module](#why-a-separate-module) below.

> **EXPERIMENTAL.** This package, and the core SDK's own
> `pkg/durable/plugin` instrumentation system it builds on, may change or
> be removed in a future release without a major-version bump. Everything
> below reflects this Go port's own real, current state — not aspiration.
> See [Scope of this port](#scope-of-this-port) for exactly what is and
> is not implemented.

## What it does

Every time your durable function runs, Workflow Insight builds a
**cumulative snapshot** of the execution (`WorkflowInsightRecord`) and
sends it to one or more exporters. The record includes:

- **Execution identity** — ARN, function name, region, account
- **Status** — `RUNNING`, `SUCCEEDED`, `FAILED`
- **Timing** — start/end time, total duration
- **Input/Output** — the event and result of the execution
- **Operations** — every step, wait, invoke, and callback with individual timing and status
- **Errors** — error name and message when the execution fails

## Record Schema (`WorkflowInsightRecord`)

Every emitted record has this shape (see `record.go` for the real,
authoritative Go struct with full field docs):

```go
type WorkflowInsightRecord struct {
	RecordType    string // fixed: "WorkflowInsight"
	SchemaVersion string // fixed: "1.0"
	EmittedAt     time.Time

	// Execution identity
	ExecutionARN      string
	ExecutionName     string // customer-provided name (--durable-execution-name), omitted if empty
	FunctionName      string
	FunctionQualifier string
	Region            string
	AccountID         string

	// Execution state
	Status     ExecutionStatus // "RUNNING" | "SUCCEEDED" | "FAILED"
	StartTime  time.Time
	EndTime    *time.Time // nil while running
	DurationMs *int64     // nil while running

	// Payload
	Input  any
	Output any
	Error  *ErrorDetail // {Name, Message}

	// Operations (steps, waits, invokes, callbacks, contexts)
	Operations []OperationRecord

	// Truncation markers — present only when the size limiter dropped data
	Truncated         bool
	DroppedOperations int
	DroppedInput      bool
	DroppedOutput     bool
}

type OperationRecord struct {
	ID        string // stable hash ID, same across replays
	Name      string // customer-provided name, e.g. operations.Step's own id argument
	Type      string // STEP | WAIT | CALLBACK | CHAINED_INVOKE | CONTEXT
	SubType   string
	ParentID  string // present for a child-context operation
	Status    string // STARTED | SUCCEEDED | FAILED | PENDING | CANCELLED | ...
	StartTime *time.Time
	EndTime   *time.Time
	DurationMs *int64
	Attempt   int
	Error     *ErrorDetail
	Result    any // excluded (nil) unless opted in via a Content override — see below
	Truncated bool
}
```

## Quick Start

```go
package main

import (
	"context"
	"log"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/awssdk"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/plugin"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
	"github.com/aws/aws-durable-execution-sdk-go/insight"
)

func handler(event map[string]any, dc types.DurableContext) (map[string]any, error) {
	result, err := operations.Step(dc, "process", func(sc types.StepContext) (map[string]any, error) {
		return doWork(event)
	})
	return result, err
}

func main() {
	ctx := context.Background()
	awsClient, err := awssdk.New(ctx)
	if err != nil {
		log.Fatalf("failed to construct awssdk.Client: %v", err)
	}

	insightPlugin := insight.New(insight.Config{}) // zero-config: EmitModeOnComplete, SamplingRate 1.0, LambdaLogExporter

	durableEntry := durable.WithDurableExecution(handler, &durable.Config{
		Client:  awsClient,
		Plugins: []plugin.InstrumentationPlugin{insightPlugin},
	})
	// ... wire durableEntry into the Lambda Runtime API — see
	// examples/simple-step-go/main.go in the root module for a complete,
	// working example of that wiring.
	_ = durableEntry
}
```

That's it. With zero configuration, insight records appear in your
function's own CloudWatch log group as JSON (via the default
`LambdaLogExporter`). No extra IAM permissions, no infrastructure to set
up.

## Configuration (`insight.Config`)

```go
insightPlugin := insight.New(insight.Config{
	EmitMode:        insight.EmitModeOnComplete, // default; also: EmitModeOnFailure, EmitModeOnChange
	OperationDetail: insight.OperationDetailTopLevel, // default; also: OperationDetailFullTree
	SamplingRate:    1.0, // default: every execution: 0.0–1.0
	Exporters:       []insight.Exporter{&insight.LambdaLogExporter{}}, // default
	Content:         insight.ContentConfig{}, // default: include everything
})
```

### `EmitMode`

| Mode | Behavior | Use case |
|---|---|---|
| `EmitModeOnComplete` (default) | Emit one record when execution completes (SUCCEEDED/FAILED) | Low overhead; sufficient for post-hoc analysis |
| `EmitModeOnChange` | Emit on every operation change + at end | Real-time monitoring; see executions as they progress |
| `EmitModeOnFailure` | Emit one record only when execution ends in FAILED | Lowest overhead; error-focused alerting and triage |

`EmitModeOnChange` **coalesces**: at most one dispatch runs at a time per
plugin instance. An operation change that arrives while a dispatch is
already in flight doesn't queue a second one — it marks the plugin dirty,
and the in-flight dispatch's own loop sends one more, always-latest
snapshot once it's free again, then stops. The final terminal record is
always guaranteed to be observed *after* any preceding mid-flight
snapshot.

### `SamplingRate`

A `float64` between 0 and 1 (default `1.0` = every execution). Values
outside `[0, 1]`, or the Go zero value `0` itself (left unset), are
clamped to `1.0` — see `Config.SamplingRate`'s own doc for why Go's zero
value can't mean "sample nothing" the way it might in another language.

The decision is **per-execution and all-or-nothing** (a sampled-in
execution emits every record it would otherwise emit; a sampled-out one
emits none) and **deterministic across replays** (derived from an
FNV-1a hash of the execution ARN, stable across every invocation of the
same execution).

### `OperationDetail`

| Mode | Behavior | Use case |
|---|---|---|
| `OperationDetailTopLevel` (default) | Only top-level operations (anything with a `ParentID` is dropped) | Consistent, resume-independent snapshots |
| `OperationDetailFullTree` | Every operation, including children of contexts (parallel branches, map items, etc.) | Full nested detail (see the caveat below) |

> **⚠️ `OperationDetailFullTree` and suspend/resume:** the backend
> prunes a finished context's children from the state handed to later
> invocations (a performance optimization). For an execution that
> suspends/resumes, a full-tree record emitted from a *later* invocation
> can be missing children of contexts that finished in an *earlier* one.
> The JS SDK's own answer is a separate core-SDK setting
> (`pluginsConfig.childOperationsDepth` on `withDurableExecution`) that
> forces the backend to preserve children across invocations — **this Go
> SDK has no equivalent core-SDK option yet**, so this caveat is
> currently real and unmitigated here, not just documented-but-worked-
> around. See `OperationDetailFullTree`'s own doc comment.

### `Content` (advanced)

```go
Content: insight.ContentConfig{
	Input:  func(v any) any { return redact(v) }, // or insight.ExcludeContent, or nil (include as-is, the default)
	Output: insight.ExcludeContent,
	Operations: insight.OperationsConfig{
		IncludeErrors: boolPtr(false), // default true; *bool, not bool — see its own doc for why
		Overrides: []insight.OperationOverride{
			{OperationName: "internal-log", Exclude: true},
			{OperationName: "charge-payment", Result: func(v any) any {
				m := v.(map[string]any)
				return map[string]any{"amount": m["amount"]} // redact everything except amount
			}},
		},
	},
},
```

Overrides are matched by `OperationName`; the **last** matching entry
wins if more than one matches the same name.

Operation results are **excluded by default** — set `Result` on a
matching override to opt a specific operation's own result in, with an
optional transform (`insight.IncludeAsIs` opts in without transforming
anything). The transform receives the operation's own **checkpointed,
already-serialized** value — JSON-decoded when it parses as JSON,
otherwise the raw string — **not** necessarily your original return
value, and **not** run through your own `Serdes.Deserialize`.

## Exporters

### Comparison Table

| Exporter | Destination | Upsert | Default `MaxRecordSizeBytes` | Setup |
|---|---|---|---|---|
| `LambdaLogExporter` | Function's own CloudWatch log group | No | 256 KB | None |
| `CloudWatchLogsExporter` | Any CloudWatch log group | No | 256 KB | Log group + IAM |
| `S3Exporter` | Amazon S3 | Yes (by key) | 5 MB | Bucket |
| `DynamoDBExporter` | Amazon DynamoDB | Configurable | 400 KB | Table |
| `AuroraExporter` | Aurora MySQL/PostgreSQL | Yes (UPSERT) | 1 MB | Cluster + Data API |
| `RedshiftExporter` | Amazon Redshift | Yes (MERGE) | 1 MB | Cluster/Serverless |
| `OpenSearchExporter` | Amazon OpenSearch | Yes (by `_id`) | 10 MB | Domain |
| `FirehoseExporter` | Kinesis Data Firehose → anywhere | N/A | 1 MB | Delivery stream |
| `EventBridgeExporter` | Amazon EventBridge | N/A | 256 KB | Event bus |
| `SQSExporter` | Amazon SQS | N/A | 256 KB | Queue |
| `OTelExporter` | Any OTLP/HTTP+JSON backend | N/A | 1 MB | Endpoint |
| `HttpExporter` | Any HTTP endpoint | N/A | none¹ | URL |
| `FileExporter` | Filesystem (EFS/mount) | Configurable | none¹ | Directory |

¹ No default — truncation is disabled unless you set `MaxSizeBytes`
explicitly.

### `LambdaLogExporter` (default)

```go
exp := insight.NewLambdaLogExporter() // or &insight.LambdaLogExporter{}
```

Writes one JSON line per record to stdout. Since Lambda captures stdout
into the function's own CloudWatch log group automatically, this
requires **zero IAM permissions** and **zero setup**.

### `CloudWatchLogsExporter`

```go
exp, err := insight.NewCloudWatchLogsExporter(ctx, "/custom/workflow-insight")
```

Writes to a **specific** log group via `PutLogEvents`, for centralizing
records from multiple functions. Daily log streams
(`{LogStreamPrefix}{YYYY}/{MM}/{DD}`, prefix defaulting to
`workflow-insight/`), auto-created on first write.

**IAM required:** `logs:CreateLogStream`, `logs:PutLogEvents` on the
target log group.

### `S3Exporter`

```go
exp, err := insight.NewS3Exporter(ctx, "my-insight-bucket")
exp.Partitioning = insight.S3PartitioningDate // default; also: S3PartitioningFunctionName, S3PartitioningNone
```

Object key: `{Prefix}{partition}{ExecutionName}.json` (Hive-style
`year=YYYY/month=MM/day=DD/` partitioning by default, for Athena/Glue
crawler compatibility). Upserts by key — a later write for the same
execution overwrites the same object.

**IAM required:** `s3:PutObject` on the bucket/prefix.

### `DynamoDBExporter`

```go
exp, err := insight.NewDynamoDBExporter(ctx, "workflow-insight")
// exp.SortKey defaults to nil -> "sk" (full history per execution).
// For upsert-only (overwrite) mode instead:
exp.SortKey = insight.NoSortKey
```

**Upsert behavior:**
- *With* a sort key (the default): each emission creates a new item — full history per execution.
- *Without* one (`SortKey: insight.NoSortKey`): `PutItem` overwrites — only the latest state is kept.

**IAM required:** `dynamodb:PutItem` on the table.

### `AuroraExporter`

```go
exp, err := insight.NewAuroraExporter(ctx, resourceARN, secretARN, "workflows")
exp.Engine = insight.AuroraEnginePostgreSQL // default; also: AuroraEngineMySQL
```

Upserts by `execution_arn` via the RDS Data API (no VPC required) —
`INSERT ... ON CONFLICT DO UPDATE` (PostgreSQL) or
`INSERT ... ON DUPLICATE KEY UPDATE` (MySQL). Every value is a bound SQL
parameter, never string-interpolated.

**Table schema:**
```sql
CREATE TABLE workflow_insight (
  execution_arn VARCHAR(512) PRIMARY KEY,
  execution_name VARCHAR(256),
  function_name VARCHAR(128),
  status VARCHAR(20),
  start_time VARCHAR(30),
  end_time VARCHAR(30),
  duration_ms BIGINT,
  record_json TEXT,
  emitted_at VARCHAR(30)
);
```

**IAM required:** `rds-data:ExecuteStatement`, `secretsmanager:GetSecretValue`.

### `RedshiftExporter`

```go
// Serverless:
exp, err := insight.NewRedshiftServerlessExporter(ctx, "my-workgroup", secretARN, "workflows")
// Provisioned cluster:
exp, err := insight.NewRedshiftClusterExporter(ctx, "my-cluster", secretARN, "workflows")
```

Upserts by `execution_arn` via `MERGE` (real Redshift `MERGE` syntax, not
PostgreSQL's `ON CONFLICT`), using `JSON_PARSE()` for the `record_json`
SUPER column.

**IAM required:** `redshift-data:ExecuteStatement` (+
`redshift-serverless:GetCredentials` or
`redshift:GetClusterCredentialsWithIAM`).

### `OpenSearchExporter`

```go
exp, err := insight.NewOpenSearchExporter(ctx, "https://my-domain.us-east-1.es.amazonaws.com", "us-east-1")
// exp.Auth defaults to OpenSearchAuthSigV4. For self-managed/basic auth:
exp2 := &insight.OpenSearchExporter{Endpoint: "https://opensearch.internal:9200", Auth: insight.OpenSearchAuthBasic, Username: "admin", Password: "..."}
```

Document `_id` = `ExecutionARN` — a later PUT overwrites. SigV4 signing
uses the real AWS SDK's own `aws-sdk-go-v2/aws/signer/v4` package, not
hand-rolled.

**IAM required (SigV4):** `es:ESHttpPut` on the domain.

### `FirehoseExporter`

```go
exp, err := insight.NewFirehoseExporter(ctx, "workflow-insight-stream")
```

NDJSON-formatted (trailing newline), so records stay individually
parseable once Firehose batches them into an S3 object.

**IAM required:** `firehose:PutRecord` on the delivery stream.

### `EventBridgeExporter`

```go
exp, err := insight.NewEventBridgeExporter(ctx)
// exp.EventBusName defaults to "default"; exp.Source defaults to insight.DefaultEventBridgeSource.
```

`DetailType` = the record's own status (`SUCCEEDED`/`FAILED`/`RUNNING`);
`Detail` = the full rendered record.

**IAM required:** `events:PutEvents` on the event bus.

### `SQSExporter`

```go
exp, err := insight.NewSQSExporter(ctx, queueURL)
```

FIFO mode is auto-detected from a `.fifo` `QueueURL` suffix —
`MessageGroupId` defaults to `ExecutionARN`, `MessageDeduplicationId` is
`{ExecutionARN}:{EmittedAt}`. `status`/`functionName` message attributes
are always set (standard or FIFO) for native SQS filtering.

**IAM required:** `sqs:SendMessage` on the queue.

### `OTelExporter`

```go
exp := &insight.OTelExporter{
	Endpoint: "https://otlp.datadoghq.com/v1/logs",
	Headers:  map[string]string{"DD-API-KEY": os.Getenv("DD_API_KEY")},
}
```

Emits records as OpenTelemetry log records via OTLP/HTTP+JSON (only
`http/json` is implemented — see `OTelProtocol`'s own doc). Resource
attributes: `service.name`, `cloud.region`, `cloud.account.id`,
`faas.name`, `faas.version`. Log attributes:
`workflow.execution_arn`, `workflow.status`, `workflow.duration_ms`.
Severity: `ERROR` for `FAILED`, `INFO` otherwise.

**No IAM policy needed** — plain HTTPS to an external endpoint.

### `HttpExporter`

```go
exp := &insight.HttpExporter{
	URL:     "https://my-service.example.com/ingest",
	Headers: map[string]string{"Authorization": "Bearer " + os.Getenv("TOKEN")},
}
```

Generic webhook: POSTs (or PUTs, via `Method`) the **bare** rendered
record JSON — distinct from `OTelExporter`'s own OTLP envelope. Default
timeout 10s (`insight.DefaultHttpExporterTimeout`), only applied when
`HTTPClient` is left nil.

**No IAM policy needed.**

### `FileExporter`

```go
exp := &insight.FileExporter{Directory: "/mnt/efs/workflow-insight"}
// exp.Mode defaults to insight.FileModeNDJSON; also: insight.FileModeJSON
```

`FileModeNDJSON` (default) appends to `{YYYY-MM-DD}.ndjson`.
`FileModeJSON` writes one file per execution
(`{ExecutionName}.json`), overwritten on update. Uses only the standard
library (`os`/`io`) — no AWS SDK dependency.

**Note:** `Directory` must already exist — this exporter never creates
it.

## `OperationsFormat`

`FirehoseExporter`, `EventBridgeExporter`, `SQSExporter`, `OTelExporter`,
`HttpExporter`, and `FileExporter` all support an `OperationsFormat`
field:

| Value | Behavior |
|---|---|
| `insight.OperationsFormatArray` (default) | The canonical `operations` array |
| `insight.OperationsFormatByName` | Only the `operationsByName` map (see below) |
| `insight.OperationsFormatBoth` | Both |

`CloudWatchLogsExporter`/`DynamoDBExporter` don't have this option —
those are always point-access stores that would need it, but this Go
port has not wired `operationsByName` into them specifically (only into
the 6 exporters above); `S3Exporter`/`OpenSearchExporter`/
`AuroraExporter`/`RedshiftExporter` are array-native and always emit the
canonical array only.

### Querying by operation name (`operationsByName`)

Each entry aggregates metrics across every occurrence of a name:

```go
type OperationByName struct {
	Type, SubType   string
	Count           int
	MinDurationMs, MaxDurationMs, TotalDurationMs int64
	FailedCount     int
	MaxAttempt      int
	Status          string       // most recently seen occurrence
	Error           *ErrorDetail // only populated when Count == 1
	Result          any          // only populated when Count == 1 (and only when the underlying operation opted in via Content — see above)
}
```

- **Metrics** (`Count`, min/max/total duration, `FailedCount`,
  `MaxAttempt`) span every occurrence sharing a name.
- **`Type`/`SubType`/`Status`** reflect the most-recently-seen
  occurrence — not aggregated.
- **`Error`/`Result` are included only when the name occurred exactly
  once.** For a repeated name (loops/retries/map items) both are
  dropped — there's no single representative value — but `FailedCount`
  still flags failures.
- Operations **without a name are excluded** (they can't be keyed).

## Record size & truncation

Each exporter carries its own `MaxSizeBytes` (see the comparison table
above for defaults). When a record's rendered size exceeds it, the
plugin truncates a **per-exporter copy** (best-effort) — the same record
can go out full to one exporter and trimmed to another.

**Drop order**, until the record fits:
1. operation `Result` fields, oldest first — only ever has anything to
   drop for an operation whose own name matched a `Result` override
   opt-in in the first place; most operations, most of the time, have
   no `Result` set at all, so this step is frequently a no-op in
   *practice* without being one *by design*.
2. whole operations, oldest first.
3. as a last resort, execution `Input`, then `Output`.


Identity/timeline fields are **never** dropped. When anything is
dropped, the record carries `Truncated: true`; `DroppedOperations`
(count), `DroppedInput`/`DroppedOutput` flags are set as applicable.

For the 6 `OperationsFormat`-supporting exporters (plus `OTelExporter`'s
own OTLP envelope), truncation sizes against the **exact bytes that
exporter would actually send** (via `RenderingSizeLimiter`), not the
canonical array shape — so a record trimmed to its limit reflects what
is actually serialized, matching the JS SDK's own documented behavior.

## Multiple Exporters

Records are sent to **all** configured exporters in parallel. If one
fails, the others still receive the record — matching the JS SDK's own
documented contract exactly.

```go
insightPlugin := insight.New(insight.Config{
	Exporters: []insight.Exporter{
		s3Exporter,       // long-term storage
		dynamoDBExporter, // fast lookups
		eventBridgeExporter, // trigger alerts
	},
	EmitMode: insight.EmitModeOnChange,
})
```

## Custom Exporters

Implement the `Exporter` interface (and, optionally, `Flusher` and/or
`SizeLimiter`/`RenderingSizeLimiter`):

```go
type MyExporter struct{}

func (e *MyExporter) Export(ctx context.Context, record insight.WorkflowInsightRecord) error {
	// send record wherever you want
	return nil
}

// Optional: flush any buffered data before the invocation returns.
func (e *MyExporter) Flush(ctx context.Context) error { return nil }

// Optional: opt into size-based truncation.
func (e *MyExporter) MaxRecordSizeBytes() int { return 256_000 }
```

## Handling Backend-Initiated Events (`STOPPED`, `TIMED_OUT`)

Workflow Insight's plugin runs **inside your Lambda function** — it can
only emit records when the function is invoked. `STOPPED` (manual stop)
and `TIMED_OUT` (execution timeout exceeded) originate from the
**backend**, with no accompanying invocation at all, so this plugin
fundamentally cannot observe them. See
[`docs/lifecycle-events.md`](docs/lifecycle-events.md) for the real gap
and the correct workaround (a separate EventBridge subscription).

## Why a separate module

Workflow Insight's exporters each pull in a different AWS SDK client
(S3, DynamoDB, the Aurora/Redshift Data APIs, OpenSearch, Kinesis
Firehose, EventBridge, SQS, ...) that a plain core-SDK consumer should
never be forced to transitively depend on — the same reason the JS
reference SDK ships this as a separate npm package rather than folding
it into its own core SDK.

## Scope of this port

This is a full, but still explicitly EXPERIMENTAL, port of the JS
reference SDK's own `workflowInsight` plugin. Implemented:

- The full record schema (including per-operation `Result`, opt-in via
  `Content`), all 13 documented exporters, `EmitMode` (including
  `on-change` with coalescing), `SamplingRate`, `OperationDetail`,
  `Content` (input/output transforms, error inclusion, per-operation
  exclusion and result opt-in), `OperationsFormat`/`operationsByName`
  (including its own `Result` aggregation), and size-based truncation
  (including the operation-result drop step).

Deliberately **not** ported (see `record.go`'s own package doc for the
authoritative, up-to-date list):

- OTLP over `http/protobuf` or gRPC (only `http/json` is implemented for
  `OTelExporter`).
- The CDK-equivalent infrastructure-as-code stack.

## License

This project is licensed under the Apache-2.0 License.
