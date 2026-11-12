package telemetry

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// nonUTCEmittedAt is 2026-06-16T17:00:27.500Z expressed in a -05:00 zone.
// Exporters that key on emittedAt must normalize it to UTC with three
// fraction digits, so the key agrees with the emittedAt field in the body.
var nonUTCEmittedAt = time.Date(2026, 6, 16, 12, 0, 27, 500_000_000, time.FixedZone("EST", -5*60*60))

const wantNonUTCEmittedAt = "2026-06-16T17:00:27.500Z"

func TestDynamoDBExporter_TimestampAttributes_UTCMilliseconds(t *testing.T) {
	fake := &fakeDynamoDBAPI{}
	exp := &DynamoDBExporter{API: fake, TableName: "telemetry"}

	start := nonUTCEmittedAt.Add(-time.Second)
	end := nonUTCEmittedAt.Add(-500 * time.Millisecond)
	rec := Record{ExecutionArn: "arn:test", EmittedAt: nonUTCEmittedAt, StartTimestamp: &start, EndTimestamp: &end}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	item := fake.calls[0].Item
	if got := attrS(t, item, "sk"); got != wantNonUTCEmittedAt {
		t.Errorf("sk = %q, want %q", got, wantNonUTCEmittedAt)
	}
	if got := attrS(t, item, "emittedAt"); got != wantNonUTCEmittedAt {
		t.Errorf("emittedAt = %q, want %q", got, wantNonUTCEmittedAt)
	}
	if got := attrS(t, item, "startTime"); got != "2026-06-16T17:00:26.500Z" {
		t.Errorf("startTime = %q, want UTC millisecond form", got)
	}
	if got := attrS(t, item, "endTime"); got != "2026-06-16T17:00:27.000Z" {
		t.Errorf("endTime = %q, want UTC millisecond form", got)
	}
}

func TestSQSExporter_DeduplicationID_UTCMilliseconds(t *testing.T) {
	fake := &fakeSQSAPI{}
	exp := &SQSExporter{API: fake, QueueURL: "https://sqs.us-east-1.amazonaws.com/123456789012/q.fifo"}

	rec := Record{ExecutionArn: "arn:test", EmittedAt: nonUTCEmittedAt}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	want := "arn:test:" + wantNonUTCEmittedAt
	if got := aws.ToString(fake.calls[0].MessageDeduplicationId); got != want {
		t.Errorf("MessageDeduplicationId = %q, want %q", got, want)
	}
}

func TestRDSExporter_EmittedAt_UTCMilliseconds(t *testing.T) {
	fake := &fakeRDSDataAPI{}
	exp := &RDSExporter{API: fake, ResourceArn: "r", SecretArn: "s", Database: "d"}

	if err := exp.Export(context.Background(), Record{ExecutionArn: "arn:test", EmittedAt: nonUTCEmittedAt}); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if got := rdsParamValue(t, fake.calls[0].Parameters, "ts"); got != wantNonUTCEmittedAt {
		t.Errorf("ts = %q, want %q", got, wantNonUTCEmittedAt)
	}
}

func TestRedshiftExporter_EmittedAt_UTCMilliseconds(t *testing.T) {
	fake := &fakeRedshiftDataAPI{}
	exp := &RedshiftExporter{API: fake, WorkgroupName: "wg", Database: "d"}

	if err := exp.Export(context.Background(), Record{ExecutionArn: "arn:test", EmittedAt: nonUTCEmittedAt}); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if got := rsParamValue(t, fake.calls[0].Parameters, "ts"); got != wantNonUTCEmittedAt {
		t.Errorf("ts = %q, want %q", got, wantNonUTCEmittedAt)
	}
}
