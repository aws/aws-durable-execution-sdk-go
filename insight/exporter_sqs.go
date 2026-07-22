package insight

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// SQSAPI is the subset of *sqs.Client this package depends on, exposed
// as an interface so tests can substitute a fake without needing real
// AWS credentials or network access - matching the exact convention
// pkg/durable/awssdk.LambdaAPI already established for this same reason.
type SQSAPI interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

// SQSExporter sends records to Amazon SQS (standard or FIFO) via
// SendMessage, matching the JS SDK's own SQSExporter.
//
// # FIFO support
//
// Matching the JS SDK's own documented behavior: FIFO mode is
// auto-detected from a ".fifo" suffix on QueueURL - no separate config
// flag needed. In FIFO mode, MessageGroupID defaults to the record's own
// ExecutionARN (so all messages for one execution stay ordered relative
// to each other, while different executions can be processed in
// parallel by different consumers) and MessageDeduplicationId is set to
// "{ExecutionARN}:{EmittedAt}" - matching the JS SDK's own documented
// dedup ID exactly.
//
// Message attributes "status" and "functionName" are always set
// (standard or FIFO), enabling SQS message filtering by either
// attribute without consumers needing to parse the message body first.
type SQSExporter struct {
	// API is the underlying SQS client. If nil, NewSQSExporter must be
	// used to construct one with a real *sqs.Client resolved from the
	// standard AWS SDK credential/region chain.
	API SQSAPI

	// QueueURL is the target queue. Required. A ".fifo" suffix switches
	// this exporter into FIFO mode automatically - see this type's own
	// doc.
	QueueURL string

	// MessageGroupID overrides the FIFO message group ID. Only
	// meaningful when QueueURL ends in ".fifo". Defaults to the record's
	// own ExecutionARN when empty.
	MessageGroupID string

	// OperationsFormat controls how the record's own operations are
	// rendered within the message body - see that type's own doc.
	// Defaults to OperationsFormatArray when left at its zero value.
	OperationsFormat OperationsFormat

	// MaxSizeBytes overrides DefaultMaxRecordSizeBytesLog. Left at its
	// zero value, MaxRecordSizeBytes() returns DefaultMaxRecordSizeBytesLog
	// instead.
	MaxSizeBytes int
}

// NewSQSExporter constructs an SQSExporter backed by a real *sqs.Client,
// resolving credentials and region via the standard AWS SDK for Go v2
// default chain - matching pkg/durable/awssdk.New's own established
// convention.
func NewSQSExporter(ctx context.Context, queueURL string, optFns ...func(*awsconfig.LoadOptions) error) (*SQSExporter, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("insight.NewSQSExporter: loading AWS config: %w", err)
	}
	return &SQSExporter{
		API:      sqs.NewFromConfig(cfg),
		QueueURL: queueURL,
	}, nil
}

// Export sends record as a single SQS message.
func (e *SQSExporter) Export(ctx context.Context, record WorkflowInsightRecord) error {
	if e.API == nil {
		return fmt.Errorf("insight.SQSExporter: API is nil (use NewSQSExporter, or set API explicitly for tests)")
	}
	if e.QueueURL == "" {
		return fmt.Errorf("insight.SQSExporter: QueueURL is required")
	}

	body, err := renderRecord(record, e.OperationsFormat)
	if err != nil {
		return fmt.Errorf("insight.SQSExporter: rendering record: %w", err)
	}

	input := &sqs.SendMessageInput{
		QueueUrl:    aws.String(e.QueueURL),
		MessageBody: aws.String(string(body)),
		MessageAttributes: map[string]sqstypes.MessageAttributeValue{
			"status":       strAttr(string(record.Status)),
			"functionName": strAttr(record.FunctionName),
		},
	}

	if strings.HasSuffix(e.QueueURL, ".fifo") {
		groupID := e.MessageGroupID
		if groupID == "" {
			groupID = record.ExecutionARN
		}
		input.MessageGroupId = aws.String(groupID)
		input.MessageDeduplicationId = aws.String(fmt.Sprintf("%s:%s", record.ExecutionARN, record.EmittedAt.Format(rfc3339)))
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

// Render implements RenderingSizeLimiter, returning the EXACT bytes
// Export would send as MessageBody, so Truncate's own
// fits-or-doesn't-fit sizing reflects the real serialized form under e's
// own OperationsFormat.
func (e *SQSExporter) Render(record WorkflowInsightRecord) ([]byte, error) {
	return renderRecord(record, e.OperationsFormat)
}

// strAttr builds an SQS string-valued message attribute.
func strAttr(value string) sqstypes.MessageAttributeValue {
	return sqstypes.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String(value),
	}
}
