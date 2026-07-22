// Package insight implements Workflow Insight, an EXPERIMENTAL
// observability plugin for the AWS Durable Execution SDK for Go, ported
// from the JS reference SDK's own `@aws/durable-execution-sdk-js-insight`
// package.
//
// Workflow Insight is a consumer of the SDK's own EXPERIMENTAL
// instrumentation-plugin system (github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/plugin,
// see that package's own doc) rather than a core SDK feature - it is
// intentionally published as its OWN Go module (insight/go.mod, sibling
// to the repo's root module, exactly matching this repo's own existing
// conformance/go.mod precedent) rather than a new pkg/durable subpackage,
// for the same reason the JS SDK ships it as a separate npm package: its
// exporters pull in AWS SDK clients for a dozen different services
// (S3, DynamoDB, Aurora Data API, Redshift Data API, OpenSearch, Kinesis
// Firehose, EventBridge, SQS, ...) that a plain core-SDK consumer should
// never be forced to transitively depend on.
//
// # Scope of this initial port
//
// This is a phased, partial port, landed as a first, reviewable
// increment (matching this SDK's own established pattern - see e.g.
// pkg/durable/plugin's own "phased" history):
//
//   - The core WorkflowInsightRecord/OperationRecord schema (this file),
//     matching the JS SDK's own schemaVersion "1.0" shape field-for-field.
//   - The plugin itself (insight.go): hooks into OnInvocationStart/
//     OnInvocationEnd/OnOperationChange/WrapInvocation, builds a
//     cumulative per-execution record, and dispatches it to configured
//     exporters - supporting emitMode "on-complete" (default) and
//     "on-failure" in this initial pass.
//   - LambdaLogExporter - the JS SDK's own zero-config, zero-IAM default
//     (writes to stdout, captured by Lambda's own CloudWatch log group
//     automatically).
//   - CloudWatchLogsExporter - writes to a specific, caller-owned log
//     group (for centralizing records from multiple functions), with
//     the JS SDK's own documented daily-log-stream naming and
//     auto-create-if-missing behavior.
//   - S3Exporter - writes JSON objects to S3 with the JS SDK's own
//     documented Hive-style date partitioning (plus function-name and
//     no-partitioning alternatives), keyed by ExecutionName so repeated
//     writes for the same execution upsert the same object.
//   - DynamoDBExporter - writes items via PutItem, with the JS SDK's own
//     documented dual mode: WITH a sort key (the default), each
//     emission accumulates a new item (full history per execution);
//     WITHOUT one (SortKey: NoSortKey), PutItem overwrites the single
//     item per execution (upsert-only).
//   - AuroraExporter - writes to Aurora MySQL/PostgreSQL via the RDS
//     Data API (no VPC required), upserting by execution_arn via
//     `ON CONFLICT DO UPDATE` (PostgreSQL, the default) or
//     `ON DUPLICATE KEY UPDATE` (MySQL) - matching the JS SDK's own
//     documented schema and upsert behavior exactly, always via bound
//     SQL parameters (never string-interpolated record data).
//   - RedshiftExporter - writes to Amazon Redshift via the Redshift Data
//     API (also no VPC required), supporting both Serverless
//     (WorkgroupName) and provisioned-cluster (ClusterIdentifier +
//     SecretARN) connection modes, upserting by execution_arn via
//     MERGE - matching the JS SDK's own documented schema/behavior, and
//     AuroraExporter's own parameterized-SQL convention exactly.
//   - OpenSearchExporter - indexes to Amazon OpenSearch Service (or a
//     self-managed cluster) via a plain HTTPS PUT to the document API,
//     upserting by document _id = ExecutionARN, supporting both SigV4
//     (IAM, the default - signed via the real AWS SDK's own
//     aws-sdk-go-v2/aws/signer/v4 package, not hand-rolled) and HTTP
//     Basic auth, matching the JS SDK's own documented dual-mode
//     support exactly.
//   - operationsByName (operations_by_name.go: OperationsByName) and the
//     shared OperationsFormat option (operations_format.go: array /
//     by-name / both) FirehoseExporter, EventBridgeExporter,
//     SQSExporter, OTelExporter, HttpExporter, and FileExporter all
//     support - matching the JS SDK's own documented aggregation rules
//     exactly (count/min/max/totalDurationMs/failedCount/maxAttempt
//     aggregated across every occurrence of a name; type/subType/status
//     reflect the most-recently-seen occurrence; result AND error
//     included only when a name occurred exactly once; unnamed
//     operations excluded entirely).
//   - FirehoseExporter - sends each record as a single NDJSON-formatted
//     PutRecord call to a Kinesis Data Firehose delivery stream,
//     supporting OperationsFormat, matching the JS SDK's own
//     FirehoseExporter.
//   - EventBridgeExporter - publishes each record as a single
//     EventBridge event (Source defaulting to
//     DefaultEventBridgeSource, DetailType = the record's own
//     ExecutionStatus, Detail = the rendered record per
//     OperationsFormat), matching the JS SDK's own EventBridgeExporter
//     and detecting PutEvents' own partial-failure response shape
//     (FailedEntryCount > 0 on an HTTP 200) as a real error rather than
//     silently treating it as success.
//   - SQSExporter - sends each record as a single SendMessage call to a
//     standard or FIFO queue (auto-detected from a ".fifo" QueueURL
//     suffix - no separate config flag), setting MessageGroupId
//     (defaulting to ExecutionARN) and MessageDeduplicationId
//     ("{ExecutionARN}:{EmittedAt}") in FIFO mode, and always setting
//     "status"/"functionName" message attributes for SQS-native
//     filtering regardless of queue type - matching the JS SDK's own
//     documented SQSExporter behavior exactly.
//   - OTelExporter - emits each record as a single OTLP/HTTP log-export
//     request (JSON encoding only - see OTelProtocol's own doc for why
//     protobuf is out of scope), matching the JS SDK's own documented
//     resource/log attribute mapping and INFO/ERROR severity mapping
//     exactly, using minimal hand-written structs covering only the
//     OTLP fields this exporter populates rather than a full
//     OpenTelemetry Go SDK dependency for what is otherwise a single
//     HTTP POST.
//   - FileExporter - writes to the filesystem (an EFS mount, an S3 File
//     Gateway-backed NFS share, or a local directory for development),
//     supporting FileModeNDJSON (the default - one line appended per
//     Export to a daily file, relying on POSIX/EFS's own O_APPEND
//     atomicity guarantee for concurrent-invocation safety) and
//     FileModeJSON (one file per execution, overwritten on every
//     Export - upsert-by-filename), matching the JS SDK's own
//     documented FileExporter modes exactly. Uses only the standard
//     library (os/io) - no AWS SDK dependency, matching the JS SDK's
//     own "no AWS SDK dependencies" note for this exporter.
//   - HttpExporter - a generic HTTP/webhook exporter: POSTs (or PUTs,
//     via Method) the bare rendered record JSON to any URL, for custom
//     backends, internal microservices, or prototyping - matching the
//     JS SDK's own HttpExporter exactly, including its documented
//     10-second default timeout, and distinct from OTelExporter's own
//     OTLP-specific envelope.
//
// This completes the full exporter set the JS reference SDK's own
// README documents (13 of 13): LambdaLog, CloudWatchLogs, S3, DynamoDB,
// Aurora, Redshift, OpenSearch, Firehose, EventBridge, SQS, OTel, File,
// Http.
//
// Also implemented: SamplingRate (Config.SamplingRate, insight.go's
// sampledIn - a deterministic, FNV-1a-hash-of-ExecutionARN-based
// per-execution decision, matching the JS SDK's own documented
// "deterministic across replays" and "all-or-nothing" guarantees),
// OperationDetail (Config.OperationDetail, OperationDetailTopLevel -
// the default - vs OperationDetailFullTree, filtering the emitted
// record's own Operations array by ParentID presence - see
// OperationDetailFullTree's own doc for the one real, UNMITIGATED
// caveat this Go port currently has around full-tree mode across
// suspend/resume, which the JS SDK's own childOperationsDepth core-SDK
// setting addresses but this Go SDK has no equivalent for yet),
// Content (Config.Content, content.go: ContentConfig.Input/Output
// value transforms, OperationsConfig.IncludeErrors, and
// OperationOverride.Exclude/Result - the latter opting a specific
// operation's own checkpointed result IN, matching the JS SDK's own
// documented "excluded by default, opt-in per operation" behavior
// exactly, backed by the real OperationRecord.Result/rawResult fields
// - see those fields' own doc), and EmitModeOnChange
// (insight.go's scheduleOnChangeDispatch - emits a RUNNING snapshot on
// every OnOperationStart/OnOperationEnd/OnOperationChange call IN
// ADDITION TO the terminal record, with the JS SDK's own documented
// "coalescing" behavior: at most one dispatch goroutine runs at a time
// per Plugin instance, and an operation change arriving while a
// dispatch is already in flight marks the Plugin dirty rather than
// spawning a second concurrent dispatch, so the loop always sends the
// LATEST snapshot once it's free again rather than an already-stale
// one - see EmitModeOnChange's own doc for the full contract, and
// onChangeWG's own doc for why OnInvocationEnd must wait for any
// in-flight mid-flight dispatch before sending its own final record).
//
// Also implemented: size-based truncation (truncate.go: Truncate,
// SizeLimiter, RenderingSizeLimiter - matching the JS SDK's own
// documented drop order (operation result fields, oldest first -
// dropping only from operations that actually have one set via an
// OperationOverride.Result opt-in, since most operations have none by
// default - then whole operations oldest-first, then Input, then
// Output as a last resort) and per-exporter default MaxRecordSizeBytes tiers exactly. Every one of the 13 exporters
// implements SizeLimiter with its own documented default (LambdaLog/
// CloudWatchLogs/SQS/EventBridge: 256KB; DynamoDB: 400KB; Aurora/
// Redshift/Firehose/OTel: 1MB; S3: 5MB; OpenSearch: 10MB; HTTP/File:
// none, truncation disabled unless explicitly configured, matching the
// JS SDK's own documented lack of a default for those two
// specifically). The 6 exporters with an OperationsFormat option
// additionally implement RenderingSizeLimiter, so Truncate's own
// fits-or-doesn't-fit sizing check is performed against the EXACT
// bytes that exporter would actually send (its own OperationsFormat-
// respecting rendering, or - for OTelExporter specifically - the full
// OTLP envelope, not just the bare record), matching the JS SDK's own
// documented "the same record can go out full to one exporter and
// trimmed to another" and "sizes against what is actually serialized"
// behaviors exactly.
//
// Deliberately NOT yet ported (tracked as follow-on work):
//
//   - OTLP over http/protobuf or gRPC (only http/json is implemented for
//     OTelExporter).
//   - The CDK-equivalent infrastructure-as-code stack.
//
// See docs/lifecycle-events.md for a real, permanent gap the JS SDK
// itself documents (not something this Go port can close with more
// code): an in-process plugin fundamentally cannot observe a
// backend-initiated STOPPED/TIMED_OUT execution outcome, since neither
// one ever triggers a Lambda invocation at all - that document covers
// the correct workaround (a separate EventBridge subscription) instead.
package insight

import "time"

// SchemaVersion is the fixed schema version for WorkflowInsightRecord,
// matching the JS SDK's own "1.0".
const SchemaVersion = "1.0"

// RecordType is the fixed discriminator value on every emitted record,
// matching the JS SDK's own "WorkflowInsight" - use this to filter
// insight records out of a shared log stream/table that may also contain
// unrelated data.
const RecordType = "WorkflowInsight"

// ExecutionStatus mirrors the JS SDK's WorkflowInsightRecord.status.
// Unlike plugin.InvocationStatus (which has a RETRYING state meaningful
// to a single Lambda invocation), this only ever takes one of three
// values, matching the JS SDK's own execution-level (not
// invocation-level) status model: a suspend for a Wait/timer surfaces as
// RUNNING, not a distinct state.
type ExecutionStatus string

const (
	ExecutionStatusRunning   ExecutionStatus = "RUNNING"
	ExecutionStatusSucceeded ExecutionStatus = "SUCCEEDED"
	ExecutionStatusFailed    ExecutionStatus = "FAILED"
)

// ErrorDetail carries an error's name and message, matching the JS SDK's
// own WorkflowInsightRecord.error/OperationRecord.error shape.
type ErrorDetail struct {
	Name    string `json:"name"`
	Message string `json:"message"`
}

// OperationRecord describes a single durable operation (step, wait,
// callback, chained invoke, or context) within a WorkflowInsightRecord,
// matching the JS SDK's own OperationRecord field-for-field.
type OperationRecord struct {
	// ID is this operation's stable hash ID - the SAME value across
	// replays (see the SDK's own SHA-256 operation-ID hashing, which
	// this field surfaces verbatim).
	ID string `json:"id"`

	// Name is the customer-provided name (e.g. the id argument passed to
	// operations.Step), omitted when empty.
	Name string `json:"name,omitempty"`

	// Type is the operation's wire-level type (STEP, WAIT, CALLBACK,
	// CHAINED_INVOKE, CONTEXT), matching plugin.OperationInfo.Type
	// verbatim.
	Type string `json:"type"`

	// SubType is additional categorization (e.g. "Step",
	// "WaitForCondition", "MapIteration", "ParallelBranch",
	// "RunInChildContext"), matching plugin.OperationInfo.SubType
	// verbatim. Omitted when empty.
	SubType string `json:"subType,omitempty"`

	// ParentID is the parent operation's own ID, for a child-context
	// operation. Omitted for a top-level operation.
	ParentID string `json:"parentId,omitempty"`

	// Status is this operation's current status (STARTED, SUCCEEDED,
	// FAILED, PENDING, CANCELLED, ...), matching
	// plugin.OperationInfo.Status verbatim.
	Status string `json:"status"`

	// StartTime/EndTime are ISO-8601 timestamps, omitted when zero.
	StartTime *time.Time `json:"startTime,omitempty"`
	EndTime   *time.Time `json:"endTime,omitempty"`

	// DurationMs is EndTime-StartTime in milliseconds, present only once
	// both timestamps are known (i.e. once the operation has ended).
	DurationMs *int64 `json:"durationMs,omitempty"`

	// Attempt is the current/final retry attempt count, omitted when
	// zero (not applicable to this operation kind).
	Attempt int `json:"attempt,omitempty"`

	// Error is this operation's own failure, present only once the
	// operation has ended in a non-success status.
	Error *ErrorDetail `json:"error,omitempty"`

	// Result is this operation's own checkpointed result, matching the
	// JS SDK's own documented per-operation result field EXACTLY,
	// including its own documented opt-in-only default: `nil` unless
	// explicitly requested via a matching OperationOverride.Result
	// transform in ContentConfig (see that field's own doc) - operation
	// results are NOT included by default, matching the JS SDK's own
	// "By default... operation results are NOT included" note. Also
	// matches the JS SDK's own documented decoding rule: the underlying
	// checkpointed value is JSON-parsed when it parses as JSON, and left
	// as the raw string otherwise - see rawResult's own doc for the
	// unexported field this is actually decoded FROM, and
	// decodeRawResult in content.go for where that decoding happens.
	//
	// # A real, narrow limitation inherited from the core SDK, not
	// # invented here
	//
	// This is the CHECKPOINTED, already-serialized value - NOT
	// necessarily your original return value. It does NOT run your own
	// operation's Serdes.Deserialize. If an operation uses a custom
	// Serdes (one that serializes to a non-JSON format, encrypts, or
	// offloads a large value to external storage and checkpoints only a
	// pointer), an override's own Result transform receives that
	// serialized form or pointer, not the original deserialized Go
	// value - matching the JS SDK's own identical, explicitly documented
	// caveat for this exact scenario. Only enable Result for operations
	// using the default JSON serialization, or whose serialized form
	// your own transform can handle.
	Result any `json:"result,omitempty"`

	// rawResult is the operation's own checkpointed result in its RAW,
	// still-serialized wire form (matching plugin.OperationInfo.Result's
	// own doc exactly - this SDK's plugin dispatch layer has no access
	// to the operation's own Serdes, so it can only ever hand plugins
	// the raw string). Unexported (never marshaled) - Result (above) is
	// the PUBLIC, JSON-decoded-when-possible field a caller/exporter
	// actually sees; rawResult exists purely so applyContent (content.go)
	// can decode it lazily, ONLY when an OperationOverride.Result
	// transform actually requests it for a given operation name - most
	// operations, most of the time, have no matching override at all
	// (Result stays nil, unincluded, matching the JS SDK's own
	// documented default), so this avoids paying a JSON-decode cost for
	// every single operation in every record when nothing ever asked
	// for it.
	rawResult string

	// Truncated is true if the size limiter (see truncate.go) dropped
	// THIS operation's own result while truncating a record to fit an
	// exporter's own MaxRecordSizeBytes - matching the JS SDK's own
	// documented per-operation truncated flag.
	Truncated bool `json:"truncated,omitempty"`
}

// WorkflowInsightRecord is the cumulative snapshot Workflow Insight
// builds for a single durable execution and hands to each configured
// Exporter, matching the JS SDK's own WorkflowInsightRecord
// field-for-field (schemaVersion "1.0").
type WorkflowInsightRecord struct {
	RecordType    string `json:"recordType"`
	SchemaVersion string `json:"schemaVersion"`

	// EmittedAt is the ISO-8601 timestamp of THIS record's own emission -
	// distinct from StartTime/EndTime, which describe the execution
	// itself. A single execution may emit multiple records over its
	// lifetime (e.g. under a future "on-change" emitMode); each one
	// carries its own EmittedAt.
	EmittedAt time.Time `json:"emittedAt"`

	// Execution identity.
	ExecutionARN      string `json:"executionArn"`
	ExecutionName     string `json:"executionName,omitempty"`
	FunctionName      string `json:"functionName,omitempty"`
	FunctionQualifier string `json:"functionQualifier,omitempty"`
	Region            string `json:"region,omitempty"`
	AccountID         string `json:"accountId,omitempty"`

	// Execution state.
	Status     ExecutionStatus `json:"status"`
	StartTime  time.Time       `json:"startTime"`
	EndTime    *time.Time      `json:"endTime,omitempty"`
	DurationMs *int64          `json:"durationMs,omitempty"`

	// Payload.
	Input  any          `json:"input,omitempty"`
	Output any          `json:"output,omitempty"`
	Error  *ErrorDetail `json:"error,omitempty"`

	// Operations (steps, waits, invokes, callbacks, contexts).
	Operations []OperationRecord `json:"operations"`

	// Truncation markers - present only when the size limiter (see
	// truncate.go) dropped data to fit an exporter's own
	// MaxRecordSizeBytes, matching the JS SDK's own documented markers
	// exactly.
	Truncated         bool `json:"truncated,omitempty"`
	DroppedOperations int  `json:"droppedOperations,omitempty"`
	DroppedInput      bool `json:"droppedInput,omitempty"`
	DroppedOutput     bool `json:"droppedOutput,omitempty"`
}
