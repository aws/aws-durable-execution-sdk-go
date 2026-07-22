package insight

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// OTelProtocol selects the OTLP wire encoding OTelExporter uses,
// matching the JS SDK's own OTelExporter protocol option. Only
// "http/json" is supported by this Go port - see OTelExporter's own doc
// for why "http/protobuf" is out of scope for now.
type OTelProtocol string

const (
	// OTelProtocolHTTPJSON sends OTLP/HTTP with a JSON-encoded body -
	// the only protocol this exporter implements. The default.
	OTelProtocolHTTPJSON OTelProtocol = "http/json"
)

// otlpAnyValue/otlpKeyValue/otlpLogRecord/... below are minimal,
// hand-written structs covering exactly the OTLP log data model fields
// this exporter populates (opentelemetry-proto's logs/v1/logs.proto and
// common/v1/common.proto) - NOT a generated or vendored OTLP SDK/schema
// package. There is no lightweight, dependency-free OTLP JSON model
// package in the Go ecosystem the way there is a real generated AWS SDK
// v2 client for every other exporter in this file; pulling in a full
// OpenTelemetry Go SDK (go.opentelemetry.io/otel/...) merely to
// construct a JSON body by hand would be a much heavier dependency than
// this exporter's own actual job (one HTTP POST) requires. Field names
// and the exact nesting (resourceLogs -> scopeLogs -> logRecords) are
// taken directly from the official opentelemetry-proto source, not
// guessed.
type otlpAnyValue struct {
	StringValue string `json:"stringValue"`
}

type otlpKeyValue struct {
	Key   string       `json:"key"`
	Value otlpAnyValue `json:"value"`
}

type otlpLogRecord struct {
	TimeUnixNano   string         `json:"timeUnixNano"`
	SeverityNumber int            `json:"severityNumber"`
	SeverityText   string         `json:"severityText"`
	Body           otlpAnyValue   `json:"body"`
	Attributes     []otlpKeyValue `json:"attributes"`
}

type otlpResource struct {
	Attributes []otlpKeyValue `json:"attributes"`
}

type otlpScopeLogs struct {
	LogRecords []otlpLogRecord `json:"logRecords"`
}

type otlpResourceLogs struct {
	Resource  otlpResource    `json:"resource"`
	ScopeLogs []otlpScopeLogs `json:"scopeLogs"`
}

type otlpExportLogsServiceRequest struct {
	ResourceLogs []otlpResourceLogs `json:"resourceLogs"`
}

// OTel severity numbers, per opentelemetry-proto's SeverityNumber enum -
// this exporter only ever emits INFO (9) or ERROR (17), matching the JS
// SDK's own documented "Severity: ERROR for FAILED, INFO otherwise".
const (
	otlpSeverityInfo  = 9
	otlpSeverityError = 17
)

// OTelExporter emits records as OpenTelemetry log records via OTLP
// HTTP/JSON, compatible with any OTLP-compatible backend (Datadog,
// Grafana, Splunk, New Relic, Honeycomb, etc.), matching the JS SDK's
// own OTelExporter.
//
// # Mapping
//
// Matching the JS SDK's own documented mapping exactly:
//   - Resource attributes: service.name (FunctionName), cloud.region
//     (Region), cloud.account.id (AccountID), faas.name (FunctionName),
//     faas.version (FunctionQualifier).
//   - Log attributes: workflow.execution_arn, workflow.status,
//     workflow.duration_ms.
//   - Log body: the full rendered record JSON (per OperationsFormat) -
//     operations are only ever in the body, never attributes, so
//     OperationsFormat never affects attribute cardinality, matching the
//     JS SDK's own documented note on this.
//   - Severity: ERROR for FAILED, INFO otherwise.
type OTelExporter struct {
	// HTTPClient sends the actual request. Defaults to
	// http.DefaultClient when nil.
	HTTPClient HTTPDoer

	// Endpoint is the OTLP/HTTP logs endpoint URL, e.g.
	// "https://otlp.datadoghq.com/v1/logs". Required.
	Endpoint string

	// Headers are added to every request - typically authentication
	// (an API key or bearer token), since OTLP itself has no universal
	// auth scheme.
	Headers map[string]string

	// Protocol selects the wire encoding. Only OTelProtocolHTTPJSON is
	// currently supported - see that type's own doc. Defaults to
	// OTelProtocolHTTPJSON when left at its zero value.
	Protocol OTelProtocol

	// OperationsFormat controls how the record's own operations are
	// rendered within the log body - see that type's own doc. Defaults
	// to OperationsFormatArray when left at its zero value.
	OperationsFormat OperationsFormat

	// MaxSizeBytes overrides DefaultMaxRecordSizeBytesSQL. Left at its
	// zero value, MaxRecordSizeBytes() returns DefaultMaxRecordSizeBytesSQL
	// instead.
	MaxSizeBytes int
}

// Export sends record as a single OTLP/HTTP log-export request.
func (e *OTelExporter) Export(ctx context.Context, record WorkflowInsightRecord) error {
	if e.Endpoint == "" {
		return fmt.Errorf("insight.OTelExporter: Endpoint is required")
	}
	if e.Protocol != "" && e.Protocol != OTelProtocolHTTPJSON {
		return fmt.Errorf("insight.OTelExporter: unsupported Protocol %q (only %q is implemented)", e.Protocol, OTelProtocolHTTPJSON)
	}

	payload, err := e.Render(record)
	if err != nil {
		return fmt.Errorf("insight.OTelExporter: rendering record: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.Endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("insight.OTelExporter: building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range e.Headers {
		req.Header.Set(k, v)
	}

	client := e.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("insight.OTelExporter: POST: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("insight.OTelExporter: POST returned status %d", resp.StatusCode)
	}
	return nil
}

// Render implements RenderingSizeLimiter, returning the EXACT bytes
// Export would POST as the request body - the full OTLP
// ExportLogsServiceRequest envelope (NOT just the bare rendered record,
// which only ever becomes this envelope's own log Body field) - so
// Truncate's own fits-or-doesn't-fit sizing reflects what is actually
// sent over the wire.
func (e *OTelExporter) Render(record WorkflowInsightRecord) ([]byte, error) {
	bodyJSON, err := renderRecord(record, e.OperationsFormat)
	if err != nil {
		return nil, err
	}

	severity := otlpSeverityInfo
	if record.Status == ExecutionStatusFailed {
		severity = otlpSeverityError
	}
	durationMs := int64(0)
	if record.DurationMs != nil {
		durationMs = *record.DurationMs
	}

	reqBody := otlpExportLogsServiceRequest{
		ResourceLogs: []otlpResourceLogs{{
			Resource: otlpResource{Attributes: []otlpKeyValue{
				strKV("service.name", record.FunctionName),
				strKV("cloud.region", record.Region),
				strKV("cloud.account.id", record.AccountID),
				strKV("faas.name", record.FunctionName),
				strKV("faas.version", record.FunctionQualifier),
			}},
			ScopeLogs: []otlpScopeLogs{{
				LogRecords: []otlpLogRecord{{
					TimeUnixNano:   fmt.Sprintf("%d", record.EmittedAt.UnixNano()),
					SeverityNumber: severity,
					SeverityText:   string(record.Status),
					Body:           otlpAnyValue{StringValue: string(bodyJSON)},
					Attributes: []otlpKeyValue{
						strKV("workflow.execution_arn", record.ExecutionARN),
						strKV("workflow.status", string(record.Status)),
						strKV("workflow.duration_ms", fmt.Sprintf("%d", durationMs)),
					},
				}},
			}},
		}},
	}

	return json.Marshal(reqBody)
}

// MaxRecordSizeBytes implements SizeLimiter, returning e.MaxSizeBytes or
// DefaultMaxRecordSizeBytesSQL when unset.
func (e *OTelExporter) MaxRecordSizeBytes() int {
	if e.MaxSizeBytes > 0 {
		return e.MaxSizeBytes
	}
	return DefaultMaxRecordSizeBytesSQL
}

// strKV builds an OTLP KeyValue with a string value, omitting the value
// entirely being the caller's responsibility if that's ever desired -
// this exporter always includes every attribute it documents, even when
// empty (e.g. Region unset), matching the JS SDK's own unconditional
// attribute-setting behavior rather than silently dropping unset
// fields.
func strKV(key, value string) otlpKeyValue {
	return otlpKeyValue{Key: key, Value: otlpAnyValue{StringValue: value}}
}
