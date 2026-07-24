package insight

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

// CloudWatchLogsAPI is the subset of *cloudwatchlogs.Client this package
// depends on, exposed as an interface so tests can substitute a fake
// without needing real AWS credentials or network access.
type CloudWatchLogsAPI interface {
	PutLogEvents(ctx context.Context, params *cloudwatchlogs.PutLogEventsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.PutLogEventsOutput, error)
}

// CloudWatchLogsExporter writes records to a specific CloudWatch Logs
// log group via PutLogEvents. Unlike LambdaLogExporter (which relies on
// Lambda's automatic stdout capture), this exporter lets the caller
// centralize records from multiple functions into one shared, queryable
// log group.
//
// Log stream naming: "{LogStreamPrefix}{YYYY}/{MM}/{DD}" — one stream per
// UTC calendar day, auto-created on first write.
type CloudWatchLogsExporter struct {
	// API is the underlying CloudWatch Logs client. Required.
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
	currentStreamDay string
	sequenceToken    *string
}

// Export writes record as a single log event to this exporter's target
// log group, in the daily log stream for the current UTC date — creating
// that stream first if this is the first Export call for that date.
func (e *CloudWatchLogsExporter) Export(ctx context.Context, record Record) error {
	if e.API == nil {
		return fmt.Errorf("insight.CloudWatchLogsExporter: API is nil")
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
// stream for today.
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
		}
	}

	e.mu.Lock()
	e.currentStreamDay = today
	e.sequenceToken = nil
	e.mu.Unlock()
	return streamName, nil
}
