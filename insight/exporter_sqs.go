package insight

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// rfc3339Milli is the time format used for SQS deduplication IDs.
const rfc3339Milli = "2006-01-02T15:04:05.000Z07:00"

// SQSAPI is the subset of *sqs.Client this package depends on, exposed
// as an interface so tests can substitute a fake without needing real
// AWS credentials or network access.
type SQSAPI interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

// SQSExporter sends records to Amazon SQS (standard or FIFO) via
// SendMessage.
//
// FIFO mode is auto-detected from a ".fifo" suffix on QueueURL. In FIFO
// mode, MessageGroupID defaults to the record's ExecutionArn and
// MessageDeduplicationId is set to "{ExecutionArn}:{EmittedAt}".
//
// Message attributes "status" and "functionName" are always set,
// enabling SQS message filtering without body parsing.
type SQSExporter struct {
	// API is the underlying SQS client. Required.
	API SQSAPI

	// QueueURL is the target queue. Required. A ".fifo" suffix switches
	// this exporter into FIFO mode automatically.
	QueueURL string

	// MessageGroupID overrides the FIFO message group ID. Only
	// meaningful when QueueURL ends in ".fifo". Defaults to the
	// record's ExecutionArn when empty.
	MessageGroupID string

	// OperationsFormat controls how operations are rendered within the
	// message body. Defaults to OperationsFormatArray when left empty.
	OperationsFormat OperationsFormat

	// MaxSizeBytes overrides DefaultMaxRecordSizeBytesLog. Left at its
	// zero value, MaxRecordSizeBytes() returns DefaultMaxRecordSizeBytesLog
	// instead.
	MaxSizeBytes int
}

// Export sends record as a single SQS message.
func (e *SQSExporter) Export(ctx context.Context, record Record) error {
	if e.API == nil {
		return fmt.Errorf("insight.SQSExporter: API is nil")
	}
	if e.QueueURL == "" {
		return fmt.Errorf("insight.SQSExporter: QueueURL is required")
	}

	body, err := RenderRecord(record, e.OperationsFormat)
	if err != nil {
		return fmt.Errorf("insight.SQSExporter: rendering record: %w", err)
	}

	input := &sqs.SendMessageInput{
		QueueUrl:    aws.String(e.QueueURL),
		MessageBody: aws.String(string(body)),
		MessageAttributes: map[string]sqstypes.MessageAttributeValue{
			"status":       sqsStrAttr(string(record.Status)),
			"functionName": sqsStrAttr(record.FunctionName),
		},
	}

	if strings.HasSuffix(e.QueueURL, ".fifo") {
		groupID := e.MessageGroupID
		if groupID == "" {
			groupID = record.ExecutionArn
		}
		input.MessageGroupId = aws.String(groupID)
		input.MessageDeduplicationId = aws.String(fmt.Sprintf("%s:%s", record.ExecutionArn, record.EmittedAt.Format(rfc3339Milli)))
	}

	if _, err := e.API.SendMessage(ctx, input); err != nil {
		return fmt.Errorf("insight.SQSExporter: SendMessage: %w", err)
	}
	return nil
}

// MaxRecordSizeBytes implements SizeLimiter, returning e.MaxSizeBytes or
// DefaultMaxRecordSizeBytesLog when unset.
func (e *SQSExporter) MaxRecordSizeBytes() int {
	if e.MaxSizeBytes > 0 {
		return e.MaxSizeBytes
	}
	return DefaultMaxRecordSizeBytesLog
}

// Render implements RenderingSizeLimiter, returning the exact bytes
// Export would send as MessageBody.
func (e *SQSExporter) Render(record Record) ([]byte, error) {
	return RenderRecord(record, e.OperationsFormat)
}

// sqsStrAttr builds an SQS string-valued message attribute.
func sqsStrAttr(value string) sqstypes.MessageAttributeValue {
	return sqstypes.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String(value),
	}
}
