package insight

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata"
	rdsdatatypes "github.com/aws/aws-sdk-go-v2/service/rdsdata/types"
)

// fakeRDSDataAPI is a test double for RDSDataAPI, matching
// pkg/durable/awssdk's own fakeLambdaAPI convention.
type fakeRDSDataAPI struct {
	calls []*rdsdata.ExecuteStatementInput
	err   error
}

func (f *fakeRDSDataAPI) ExecuteStatement(_ context.Context, params *rdsdata.ExecuteStatementInput, _ ...func(*rdsdata.Options)) (*rdsdata.ExecuteStatementOutput, error) {
	f.calls = append(f.calls, params)
	if f.err != nil {
		return nil, f.err
	}
	return &rdsdata.ExecuteStatementOutput{}, nil
}

func paramValue(t *testing.T, params []rdsdatatypes.SqlParameter, name string) string {
	t.Helper()
	for _, p := range params {
		if aws.ToString(p.Name) != name {
			continue
		}
		switch v := p.Value.(type) {
		case *rdsdatatypes.FieldMemberStringValue:
			return v.Value
		case *rdsdatatypes.FieldMemberLongValue:
			return strconv.FormatInt(v.Value, 10)
		}
	}
	t.Fatalf("expected a parameter named %q, params: %+v", name, params)
	return ""
}

func TestAuroraExporter_PostgreSQL_UsesOnConflictUpsert(t *testing.T) {
	fake := &fakeRDSDataAPI{}
	exp := &AuroraExporter{API: fake, ResourceARN: "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster", SecretARN: "arn:aws:secretsmanager:us-east-1:123456789012:secret:creds", Database: "workflows"}

	rec := WorkflowInsightRecord{ExecutionARN: "arn:test", Status: ExecutionStatusSucceeded}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	if len(fake.calls) != 1 {
		t.Fatalf("expected exactly 1 ExecuteStatement call, got %d", len(fake.calls))
	}
	sql := aws.ToString(fake.calls[0].Sql)
	if !strings.Contains(sql, "ON CONFLICT") {
		t.Errorf("expected PostgreSQL (default engine) to use ON CONFLICT, got SQL: %s", sql)
	}
	if strings.Contains(sql, "ON DUPLICATE KEY") {
		t.Errorf("expected PostgreSQL SQL to NOT contain MySQL's ON DUPLICATE KEY, got: %s", sql)
	}
	if !strings.Contains(sql, "workflow_insight") {
		t.Errorf("expected the default table name workflow_insight in SQL, got: %s", sql)
	}
}

func TestAuroraExporter_MySQL_UsesOnDuplicateKeyUpsert(t *testing.T) {
	fake := &fakeRDSDataAPI{}
	exp := &AuroraExporter{API: fake, ResourceARN: "arn:test-cluster", SecretARN: "arn:test-secret", Database: "workflows", Engine: AuroraEngineMySQL}

	if err := exp.Export(context.Background(), WorkflowInsightRecord{ExecutionARN: "arn:test"}); err != nil {
		t.Fatalf("Export: %v", err)
	}

	sql := aws.ToString(fake.calls[0].Sql)
	if !strings.Contains(sql, "ON DUPLICATE KEY UPDATE") {
		t.Errorf("expected MySQL engine to use ON DUPLICATE KEY UPDATE, got SQL: %s", sql)
	}
}

func TestAuroraExporter_CustomTableName(t *testing.T) {
	fake := &fakeRDSDataAPI{}
	exp := &AuroraExporter{API: fake, ResourceARN: "arn:test-cluster", SecretARN: "arn:test-secret", Database: "workflows", Table: "my_custom_table"}

	if err := exp.Export(context.Background(), WorkflowInsightRecord{ExecutionARN: "arn:test"}); err != nil {
		t.Fatalf("Export: %v", err)
	}

	sql := aws.ToString(fake.calls[0].Sql)
	if !strings.Contains(sql, "my_custom_table") {
		t.Errorf("expected the custom table name in SQL, got: %s", sql)
	}
}

func TestAuroraExporter_ParametersAreCorrectlyBound(t *testing.T) {
	fake := &fakeRDSDataAPI{}
	exp := &AuroraExporter{API: fake, ResourceARN: "arn:c", SecretARN: "arn:s", Database: "db"}

	dur := int64(1234)
	end := time.Date(2026, 6, 16, 17, 0, 27, 0, time.UTC)
	rec := WorkflowInsightRecord{
		ExecutionARN: "arn:aws:lambda:us-east-1:123456789012:function:f:1",
		Status:       ExecutionStatusSucceeded,
		EndTime:      &end,
		DurationMs:   &dur,
	}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	params := fake.calls[0].Parameters
	if got := paramValue(t, params, "execution_arn"); got != rec.ExecutionARN {
		t.Errorf("expected execution_arn=%q, got %q", rec.ExecutionARN, got)
	}
	if got := paramValue(t, params, "status"); got != "SUCCEEDED" {
		t.Errorf("expected status=SUCCEEDED, got %q", got)
	}
	if got := paramValue(t, params, "duration_ms"); got != "1234" {
		t.Errorf("expected duration_ms=1234, got %q", got)
	}
	// Every value must be passed as a PARAMETER, never interpolated
	// directly into the SQL string - confirm the SQL text itself
	// contains no record data at all (only column names, table name,
	// and :named placeholders).
	sql := aws.ToString(fake.calls[0].Sql)
	if strings.Contains(sql, rec.ExecutionARN) {
		t.Errorf("expected the SQL string to NEVER contain raw record data (SQL injection risk) - found ExecutionARN embedded directly in: %s", sql)
	}
}

func TestAuroraExporter_MissingRequiredFields_ReturnsError(t *testing.T) {
	tests := []struct {
		name string
		exp  *AuroraExporter
	}{
		{"missing ResourceARN", &AuroraExporter{API: &fakeRDSDataAPI{}, SecretARN: "s", Database: "d"}},
		{"missing SecretARN", &AuroraExporter{API: &fakeRDSDataAPI{}, ResourceARN: "r", Database: "d"}},
		{"missing Database", &AuroraExporter{API: &fakeRDSDataAPI{}, ResourceARN: "r", SecretARN: "s"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.exp.Export(context.Background(), WorkflowInsightRecord{}); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestAuroraExporter_ExecuteStatementError_Propagates(t *testing.T) {
	wantErr := errors.New("connection timed out")
	fake := &fakeRDSDataAPI{err: wantErr}
	exp := &AuroraExporter{API: fake, ResourceARN: "r", SecretARN: "s", Database: "d"}

	err := exp.Export(context.Background(), WorkflowInsightRecord{})
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected the underlying ExecuteStatement error to propagate (wrapped), got %v", err)
	}
}
