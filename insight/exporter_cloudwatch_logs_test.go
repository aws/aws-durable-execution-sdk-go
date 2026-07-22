package insight

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

// fakeCloudWatchLogsAPI is a test double implementing both
// CloudWatchLogsAPI and the optional CreateLogStream interface
// ensureLogStream type-asserts for, letting these tests verify the
// exporter's own request construction without real AWS credentials or
// network access - matching pkg/durable/awssdk's own fakeLambdaAPI
// convention.
type fakeCloudWatchLogsAPI struct {
	putEventsCalls    []*cloudwatchlogs.PutLogEventsInput
	createStreamCalls []*cloudwatchlogs.CreateLogStreamInput
	createStreamErr   error
	putEventsErr      error
	nextSeqToken      *string
}

func (f *fakeCloudWatchLogsAPI) PutLogEvents(_ context.Context, params *cloudwatchlogs.PutLogEventsInput, _ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.PutLogEventsOutput, error) {
	f.putEventsCalls = append(f.putEventsCalls, params)
	if f.putEventsErr != nil {
		return nil, f.putEventsErr
	}
	return &cloudwatchlogs.PutLogEventsOutput{NextSequenceToken: f.nextSeqToken}, nil
}

func (f *fakeCloudWatchLogsAPI) CreateLogStream(_ context.Context, params *cloudwatchlogs.CreateLogStreamInput, _ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogStreamOutput, error) {
	f.createStreamCalls = append(f.createStreamCalls, params)
	if f.createStreamErr != nil {
		return nil, f.createStreamErr
	}
	return &cloudwatchlogs.CreateLogStreamOutput{}, nil
}

func TestCloudWatchLogsExporter_CreatesStreamOnceThenReuses(t *testing.T) {
	fake := &fakeCloudWatchLogsAPI{}
	exp := &CloudWatchLogsExporter{API: fake, LogGroupName: "/test/insight"}

	rec := WorkflowInsightRecord{RecordType: RecordType, SchemaVersion: SchemaVersion, ExecutionARN: "arn:test"}
	for i := 0; i < 3; i++ {
		if err := exp.Export(context.Background(), rec); err != nil {
			t.Fatalf("Export %d: %v", i, err)
		}
	}

	if len(fake.createStreamCalls) != 1 {
		t.Fatalf("expected CreateLogStream to be called exactly once (same UTC day across all 3 exports), got %d", len(fake.createStreamCalls))
	}
	if len(fake.putEventsCalls) != 3 {
		t.Fatalf("expected PutLogEvents to be called once per Export, got %d", len(fake.putEventsCalls))
	}
	for i, call := range fake.putEventsCalls {
		if aws.ToString(call.LogGroupName) != "/test/insight" {
			t.Errorf("call %d: expected LogGroupName=/test/insight, got %q", i, aws.ToString(call.LogGroupName))
		}
		if len(call.LogEvents) != 1 {
			t.Errorf("call %d: expected exactly 1 log event, got %d", i, len(call.LogEvents))
		}
	}
}

func TestCloudWatchLogsExporter_LogEventContainsValidJSON(t *testing.T) {
	fake := &fakeCloudWatchLogsAPI{}
	exp := &CloudWatchLogsExporter{API: fake, LogGroupName: "/test/insight"}

	rec := WorkflowInsightRecord{RecordType: RecordType, SchemaVersion: SchemaVersion, ExecutionARN: "arn:aws:lambda:us-east-1:123456789012:function:f:1"}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	msg := aws.ToString(fake.putEventsCalls[0].LogEvents[0].Message)
	var decoded WorkflowInsightRecord
	if err := json.Unmarshal([]byte(msg), &decoded); err != nil {
		t.Fatalf("expected the log event message to be valid JSON: %v", err)
	}
	if decoded.ExecutionARN != rec.ExecutionARN {
		t.Errorf("expected round-tripped ExecutionARN %q, got %q", rec.ExecutionARN, decoded.ExecutionARN)
	}
}

func TestCloudWatchLogsExporter_SequenceTokenCarriesForward(t *testing.T) {
	tok1 := "seq-1"
	fake := &fakeCloudWatchLogsAPI{nextSeqToken: &tok1}
	exp := &CloudWatchLogsExporter{API: fake, LogGroupName: "/test/insight"}

	rec := WorkflowInsightRecord{RecordType: RecordType, SchemaVersion: SchemaVersion}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("first Export: %v", err)
	}
	if aws.ToString(fake.putEventsCalls[0].SequenceToken) != "" {
		t.Errorf("expected no SequenceToken on the FIRST call, got %q", aws.ToString(fake.putEventsCalls[0].SequenceToken))
	}

	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("second Export: %v", err)
	}
	if aws.ToString(fake.putEventsCalls[1].SequenceToken) != tok1 {
		t.Errorf("expected the SECOND call to carry forward the token returned by the first call (%q), got %q", tok1, aws.ToString(fake.putEventsCalls[1].SequenceToken))
	}
}

func TestCloudWatchLogsExporter_ResourceAlreadyExists_IsNotAnError(t *testing.T) {
	fake := &fakeCloudWatchLogsAPI{createStreamErr: &cwltypes.ResourceAlreadyExistsException{Message: aws.String("already exists")}}
	exp := &CloudWatchLogsExporter{API: fake, LogGroupName: "/test/insight"}

	rec := WorkflowInsightRecord{RecordType: RecordType, SchemaVersion: SchemaVersion}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("expected ResourceAlreadyExistsException from CreateLogStream to be treated as success, got error: %v", err)
	}
	if len(fake.putEventsCalls) != 1 {
		t.Errorf("expected PutLogEvents to still be called despite the stream already existing, got %d calls", len(fake.putEventsCalls))
	}
}

func TestCloudWatchLogsExporter_MissingLogGroupName_ReturnsError(t *testing.T) {
	exp := &CloudWatchLogsExporter{API: &fakeCloudWatchLogsAPI{}}
	err := exp.Export(context.Background(), WorkflowInsightRecord{})
	if err == nil {
		t.Fatal("expected an error when LogGroupName is unset")
	}
}

func TestCloudWatchLogsExporter_PutLogEventsError_Propagates(t *testing.T) {
	wantErr := errors.New("throttled")
	fake := &fakeCloudWatchLogsAPI{putEventsErr: wantErr}
	exp := &CloudWatchLogsExporter{API: fake, LogGroupName: "/test/insight"}

	err := exp.Export(context.Background(), WorkflowInsightRecord{})
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected the underlying PutLogEvents error to propagate (wrapped), got %v", err)
	}
}
