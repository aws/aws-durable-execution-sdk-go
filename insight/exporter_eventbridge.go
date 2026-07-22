package insight

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
)

// EventBridgeAPI is the subset of *eventbridge.Client this package
// depends on, exposed as an interface so tests can substitute a fake
// without needing real AWS credentials or network access - matching the
// exact convention pkg/durable/awssdk.LambdaAPI already established for
// this same reason.
type EventBridgeAPI interface {
	PutEvents(ctx context.Context, params *eventbridge.PutEventsInput, optFns ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error)
}

// DefaultEventBridgeSource is the default EventBridge event Source this
// exporter publishes under, matching the JS SDK's own documented
// default exactly.
const DefaultEventBridgeSource = "aws.durable-execution.insight"

// EventBridgeExporter publishes records to Amazon EventBridge for
// event-driven reactions (triggering notifications on failure, starting
// remediation workflows, fan-out to multiple consumers without coupling
// them directly to the durable function itself), matching the JS SDK's
// own EventBridgeExporter.
//
// # Event structure
//
// Matching the JS SDK's own documented shape exactly: Source defaults to
// DefaultEventBridgeSource; DetailType is the record's own ExecutionStatus
// (SUCCEEDED/FAILED/RUNNING); Detail is the full rendered record JSON
// (per OperationsFormat).
type EventBridgeExporter struct {
	// API is the underlying EventBridge client. If nil,
	// NewEventBridgeExporter must be used to construct one with a real
	// *eventbridge.Client resolved from the standard AWS SDK
	// credential/region chain.
	API EventBridgeAPI

	// EventBusName is the target event bus. Defaults to "default" (the
	// account's own default bus) when empty, matching the JS SDK's own
	// default - no bus creation needed for that case.
	EventBusName string

	// Source is the event Source field. Defaults to
	// DefaultEventBridgeSource when empty.
	Source string

	// OperationsFormat controls how the record's own operations are
	// rendered within Detail - see that type's own doc. Defaults to
	// OperationsFormatArray when left at its zero value.
	OperationsFormat OperationsFormat

	// MaxSizeBytes overrides DefaultMaxRecordSizeBytesLog. Left at its
	// zero value, MaxRecordSizeBytes() returns DefaultMaxRecordSizeBytesLog
	// instead.
	MaxSizeBytes int
}

// NewEventBridgeExporter constructs an EventBridgeExporter backed by a
// real *eventbridge.Client, resolving credentials and region via the
// standard AWS SDK for Go v2 default chain - matching
// pkg/durable/awssdk.New's own established convention.
func NewEventBridgeExporter(ctx context.Context, optFns ...func(*awsconfig.LoadOptions) error) (*EventBridgeExporter, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("insight.NewEventBridgeExporter: loading AWS config: %w", err)
	}
	return &EventBridgeExporter{API: eventbridge.NewFromConfig(cfg)}, nil
}

// Export publishes record as a single EventBridge event.
func (e *EventBridgeExporter) Export(ctx context.Context, record WorkflowInsightRecord) error {
	if e.API == nil {
		return fmt.Errorf("insight.EventBridgeExporter: API is nil (use NewEventBridgeExporter, or set API explicitly for tests)")
	}

	source := e.Source
	if source == "" {
		source = DefaultEventBridgeSource
	}
	eventBusName := e.EventBusName
	if eventBusName == "" {
		eventBusName = "default"
	}

	detail, err := renderRecord(record, e.OperationsFormat)
	if err != nil {
		return fmt.Errorf("insight.EventBridgeExporter: rendering record: %w", err)
	}

	out, err := e.API.PutEvents(ctx, &eventbridge.PutEventsInput{
		Entries: []ebtypes.PutEventsRequestEntry{{
			EventBusName: aws.String(eventBusName),
			Source:       aws.String(source),
			DetailType:   aws.String(string(record.Status)),
			Detail:       aws.String(string(detail)),
		}},
	})
	if err != nil {
		return fmt.Errorf("insight.EventBridgeExporter: PutEvents: %w", err)
	}
	// PutEvents can report a PARTIAL failure (HTTP 200, but an individual
	// entry failed) - FailedEntryCount is the documented way to detect
	// this, since a single-entry PutEvents call (this exporter always
	// sends exactly one entry) makes it unambiguous which entry failed.
	if out != nil && out.FailedEntryCount > 0 && len(out.Entries) > 0 {
		entry := out.Entries[0]
		return fmt.Errorf("insight.EventBridgeExporter: PutEvents entry failed: %s: %s", aws.ToString(entry.ErrorCode), aws.ToString(entry.ErrorMessage))
	}
	return nil
}

// MaxRecordSizeBytes implements SizeLimiter, returning e.MaxSizeBytes or
// DefaultMaxRecordSizeBytesLog when unset.
func (e *EventBridgeExporter) MaxRecordSizeBytes() int {
	if e.MaxSizeBytes > 0 {
		return e.MaxSizeBytes
	}
	return DefaultMaxRecordSizeBytesLog
}

// Render implements RenderingSizeLimiter, returning the EXACT bytes
// Export would send as the event's own Detail field, so Truncate's own
// fits-or-doesn't-fit sizing reflects the real serialized form under e's
// own OperationsFormat.
func (e *EventBridgeExporter) Render(record WorkflowInsightRecord) ([]byte, error) {
	return renderRecord(record, e.OperationsFormat)
}
