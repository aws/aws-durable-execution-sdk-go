package insight

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
)

// EventBridgeAPI is the subset of *eventbridge.Client this package
// depends on, exposed as an interface so tests can substitute a fake
// without needing real AWS credentials or network access.
type EventBridgeAPI interface {
	PutEvents(ctx context.Context, params *eventbridge.PutEventsInput, optFns ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error)
}

// DefaultEventBridgeSource is the default EventBridge event Source.
const DefaultEventBridgeSource = "aws.durable-execution.insight"

// EventBridgeExporter publishes records to Amazon EventBridge for
// event-driven reactions.
//
// Source defaults to DefaultEventBridgeSource; DetailType is the
// record's Status (SUCCEEDED/FAILED/RUNNING); Detail is the full
// rendered record JSON per OperationsFormat.
type EventBridgeExporter struct {
	// API is the underlying EventBridge client. Required.
	API EventBridgeAPI

	// EventBusName is the target event bus. Defaults to "default" when
	// empty.
	EventBusName string

	// Source is the event Source field. Defaults to
	// DefaultEventBridgeSource when empty.
	Source string

	// OperationsFormat controls how operations are rendered within
	// Detail. Defaults to OperationsFormatArray when left empty.
	OperationsFormat OperationsFormat

	// MaxSizeBytes overrides DefaultMaxRecordSizeBytesLog. Left at its
	// zero value, MaxRecordSizeBytes() returns DefaultMaxRecordSizeBytesLog
	// instead.
	MaxSizeBytes int
}

// Export publishes record as a single EventBridge event.
func (e *EventBridgeExporter) Export(ctx context.Context, record Record) error {
	if e.API == nil {
		return fmt.Errorf("insight.EventBridgeExporter: API is nil")
	}

	source := e.Source
	if source == "" {
		source = DefaultEventBridgeSource
	}
	eventBusName := e.EventBusName
	if eventBusName == "" {
		eventBusName = "default"
	}

	detail, err := RenderRecord(record, e.OperationsFormat)
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

// Render implements RenderingSizeLimiter, returning the exact bytes
// Export would send as the event's Detail field.
func (e *EventBridgeExporter) Render(record Record) ([]byte, error) {
	return RenderRecord(record, e.OperationsFormat)
}
