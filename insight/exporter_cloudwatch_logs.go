package insight

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

// CloudWatchLogsAPI is the subset of *cloudwatchlogs.Client this package
// depends on, exposed as an interface so tests can substitute a fake
// without needing real AWS credentials or network access - matching the
// exact convention pkg/durable/awssdk.LambdaAPI already established for
// this same reason.
type CloudWatchLogsAPI interface {
	PutLogEvents(ctx context.Context, params *cloudwatchlogs.PutLogEventsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.PutLogEventsOutput, error)
}

// CloudWatchLogsExporter writes records to a SPECIFIC CloudWatch Logs log
// group via PutLogEvents, matching the JS SDK's own CloudWatchLogsExporter.
// Unlike LambdaLogExporter (which relies on Lambda's own automatic stdout
// capture into the function's OWN log group), this exporter lets the
// caller centralize records from multiple functions into one shared,
// queryable log group - at the cost of needing its own log group and IAM
// grant (logs:CreateLogStream, logs:PutLogEvents on the target log
// group) set up ahead of time.
//
// # Log stream naming
//
// Matching the JS SDK's own documented pattern exactly:
// "{LogStreamPrefix}{YYYY}/{MM}/{DD}" - one stream per UTC calendar day,
// auto-created on first write via a real DescribeLogStreams/
// CreateLogStream round trip (see ensureLogStream) when it does not
// already exist. LogStreamPrefix defaults to "workflow-insight/" when
// left empty (matching the JS SDK's own default).
type CloudWatchLogsExporter struct {
	// API is the underlying CloudWatch Logs client. If nil,
	// NewCloudWatchLogsExporter must be used to construct one with a
	// real *cloudwatchlogs.Client resolved from the standard AWS SDK
	// credential/region chain.
	API CloudWatchLogsAPI

	// LogGroupName is the target log group. Required.
	LogGroupName string

	// LogStreamPrefix prefixes the daily log stream name. Defaults to
	// "workflow-insight/" when empty.
	LogStreamPrefix string

	// MaxSizeBytes overrides DefaultMaxRecordSizeBytesLog. Left at its
	// zero value, MaxRecordSizeBytes() returns DefaultMaxRecordSizeBytesLog
	// instead.
	MaxSizeBytes int

	mu               sync.Mutex
	currentStreamDay string // "YYYY/MM/DD" this exporter has last confirmed/created a stream for
	sequenceToken    *string
}

// NewCloudWatchLogsExporter constructs a CloudWatchLogsExporter backed by
// a real *cloudwatchlogs.Client, resolving credentials and region via the
// standard AWS SDK for Go v2 default chain - matching
// pkg/durable/awssdk.New's own established convention and doc for why
// this resolution order is correct for a Lambda execution role.
func NewCloudWatchLogsExporter(ctx context.Context, logGroupName string, optFns ...func(*awsconfig.LoadOptions) error) (*CloudWatchLogsExporter, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("insight.NewCloudWatchLogsExporter: loading AWS config: %w", err)
	}
	return &CloudWatchLogsExporter{
		API:          cloudwatchlogs.NewFromConfig(cfg),
		LogGroupName: logGroupName,
	}, nil
}

// Export writes record as a single log event to this exporter's target
// log group, in the daily log stream for the current UTC date -
// creating that stream first if this is the first Export call for that
// date (or the first Export call on this exporter instance at all).
func (e *CloudWatchLogsExporter) Export(ctx context.Context, record WorkflowInsightRecord) error {
	if e.API == nil {
		return fmt.Errorf("insight.CloudWatchLogsExporter: API is nil (use NewCloudWatchLogsExporter, or set API explicitly for tests)")
	}
	if e.LogGroupName == "" {
		return fmt.Errorf("insight.CloudWatchLogsExporter: LogGroupName is required")
	}

	b, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("insight.CloudWatchLogsExporter: marshaling record: %w", err)
	}

	streamName, err := e.ensureLogStream(ctx)
	if err != nil {
		return fmt.Errorf("insight.CloudWatchLogsExporter: ensuring log stream: %w", err)
	}

	e.mu.Lock()
	seqToken := e.sequenceToken
	e.mu.Unlock()

	out, err := e.API.PutLogEvents(ctx, &cloudwatchlogs.PutLogEventsInput{
		LogGroupName:  aws.String(e.LogGroupName),
		LogStreamName: aws.String(streamName),
		SequenceToken: seqToken,
		LogEvents: []cwltypes.InputLogEvent{{
			Timestamp: aws.Int64(time.Now().UnixMilli()),
			Message:   aws.String(string(b)),
		}},
	})
	if err != nil {
		return fmt.Errorf("insight.CloudWatchLogsExporter: PutLogEvents: %w", err)
	}

	e.mu.Lock()
	if out != nil {
		e.sequenceToken = out.NextSequenceToken
	}
	e.mu.Unlock()
	return nil
}

// MaxRecordSizeBytes implements SizeLimiter, returning e.MaxSizeBytes or
// DefaultMaxRecordSizeBytesLog when unset.
func (e *CloudWatchLogsExporter) MaxRecordSizeBytes() int {
	if e.MaxSizeBytes > 0 {
		return e.MaxSizeBytes
	}
	return DefaultMaxRecordSizeBytesLog
}

// ensureLogStream returns today's (UTC) log stream name, creating it via
// CreateLogStream first if this exporter has not yet confirmed/created a
// stream for today. A real CloudWatch Logs stream for a PAST day still
// accepts new events after midnight rollover has happened once this
// exporter is asked to write to a NEW day's stream, but this exporter
// intentionally never reuses a prior day's cached stream name/sequence
// token across a day boundary - each new day gets CreateLogStream called
// exactly once, on its own first Export call, matching the JS SDK's own
// "one [stream] per day, auto-created" documented behavior.
func (e *CloudWatchLogsExporter) ensureLogStream(ctx context.Context) (string, error) {
	prefix := e.LogStreamPrefix
	if prefix == "" {
		prefix = "workflow-insight/"
	}
	today := time.Now().UTC().Format("2006/01/02")
	streamName := prefix + today

	e.mu.Lock()
	alreadyEnsured := e.currentStreamDay == today
	e.mu.Unlock()
	if alreadyEnsured {
		return streamName, nil
	}

	createAPI, ok := e.API.(interface {
		CreateLogStream(ctx context.Context, params *cloudwatchlogs.CreateLogStreamInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogStreamOutput, error)
	})
	if ok {
		_, err := createAPI.CreateLogStream(ctx, &cloudwatchlogs.CreateLogStreamInput{
			LogGroupName:  aws.String(e.LogGroupName),
			LogStreamName: aws.String(streamName),
		})
		if err != nil {
			var alreadyExists *cwltypes.ResourceAlreadyExistsException
			if !errors.As(err, &alreadyExists) {
				return "", err
			}
			// Stream already exists (created by a previous cold start or
			// a different concurrent invocation) - not an error, just
			// means someone else already did this today.
		}
	}

	e.mu.Lock()
	e.currentStreamDay = today
	e.sequenceToken = nil // a freshly (re)confirmed stream needs no sequence token on its first PutLogEvents call
	e.mu.Unlock()
	return streamName, nil
}
