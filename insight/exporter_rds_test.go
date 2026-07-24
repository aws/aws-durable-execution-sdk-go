package insight

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata"
	rdsdatatypes "github.com/aws/aws-sdk-go-v2/service/rdsdata/types"
)

// fakeRDSDataAPI is a test double for RDSDataAPI.
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

func rdsParamValue(t *testing.T, params []rdsdatatypes.SqlParameter, name string) string {
	t.Helper()
	for _, p := range params {
		if aws.ToString(p.Name) != name {
			continue
		}
		switch v := p.Value.(type) {
		case *rdsdatatypes.FieldMemberStringValue:
			return v.Value
		case *rdsdatatypes.FieldMemberLongValue:
			return fmt.Sprintf("%d", v.Value)
		}
	}
	t.Fatalf("expected a parameter named %q", name)
	return ""
}

func TestRDSExporter_InsertsWithParameterizedSQL(t *testing.T) {
	fake := &fakeRDSDataAPI{}
	exp := &RDSExporter{
		API:         fake,
		ResourceArn: "arn:aws:rds:us-east-1:111122223333:cluster:my-cluster",
		SecretArn:   "arn:aws:secretsmanager:us-east-1:111122223333:secret:creds",
		Database:    "workflows",
	}

	emittedAt := time.Date(2026, 6, 16, 17, 0, 27, 0, time.UTC)
	rec := Record{
		ExecutionArn: "arn:aws:lambda:us-east-1:111122223333:function:f:1",
		Status:       StatusSucceeded,
		EmittedAt:    emittedAt,
	}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	if len(fake.calls) != 1 {
		t.Fatalf("expected exactly 1 ExecuteStatement call, got %d", len(fake.calls))
	}
	call := fake.calls[0]

	if aws.ToString(call.ResourceArn) != exp.ResourceArn {
		t.Errorf("expected ResourceArn=%q, got %q", exp.ResourceArn, aws.ToString(call.ResourceArn))
	}
	if aws.ToString(call.SecretArn) != exp.SecretArn {
		t.Errorf("expected SecretArn=%q, got %q", exp.SecretArn, aws.ToString(call.SecretArn))
	}
	if aws.ToString(call.Database) != "workflows" {
		t.Errorf("expected Database=workflows, got %q", aws.ToString(call.Database))
	}

	sql := aws.ToString(call.Sql)
	if !strings.Contains(sql, "INSERT INTO") {
		t.Errorf("expected INSERT INTO statement, got: %s", sql)
	}
	if !strings.Contains(sql, "workflow_insight") {
		t.Errorf("expected default table workflow_insight, got: %s", sql)
	}

	// SQL must use named parameters, not interpolated values.
	if strings.Contains(sql, rec.ExecutionArn) {
		t.Errorf("expected SQL to NEVER contain raw record data (SQL injection risk), found ExecutionArn in: %s", sql)
	}

	params := call.Parameters
	if got := rdsParamValue(t, params, "arn"); got != rec.ExecutionArn {
		t.Errorf("expected arn=%q, got %q", rec.ExecutionArn, got)
	}
	if got := rdsParamValue(t, params, "status"); got != "SUCCEEDED" {
		t.Errorf("expected status=SUCCEEDED, got %q", got)
	}
}

func TestRDSExporter_CustomTableName(t *testing.T) {
	fake := &fakeRDSDataAPI{}
	exp := &RDSExporter{API: fake, ResourceArn: "r", SecretArn: "s", Database: "d", TableName: "custom_table"}

	if err := exp.Export(context.Background(), Record{ExecutionArn: "arn:test"}); err != nil {
		t.Fatalf("Export: %v", err)
	}

	sql := aws.ToString(fake.calls[0].Sql)
	if !strings.Contains(sql, "custom_table") {
		t.Errorf("expected custom table name in SQL, got: %s", sql)
	}
}

func TestRDSExporter_MissingRequiredFields_ReturnsError(t *testing.T) {
	tests := []struct {
		name string
		exp  *RDSExporter
	}{
		{"missing ResourceArn", &RDSExporter{API: &fakeRDSDataAPI{}, SecretArn: "s", Database: "d"}},
		{"missing SecretArn", &RDSExporter{API: &fakeRDSDataAPI{}, ResourceArn: "r", Database: "d"}},
		{"missing Database", &RDSExporter{API: &fakeRDSDataAPI{}, ResourceArn: "r", SecretArn: "s"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.exp.Export(context.Background(), Record{}); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestRDSExporter_ExecuteStatementError_Propagates(t *testing.T) {
	wantErr := errors.New("connection timed out")
	fake := &fakeRDSDataAPI{err: wantErr}
	exp := &RDSExporter{API: fake, ResourceArn: "r", SecretArn: "s", Database: "d"}

	err := exp.Export(context.Background(), Record{})
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected the underlying ExecuteStatement error to propagate (wrapped), got %v", err)
	}
}

func TestRDSExporter_MaxRecordSizeBytes(t *testing.T) {
	exp := &RDSExporter{}
	if got := exp.MaxRecordSizeBytes(); got != DefaultMaxRecordSizeBytesRDS {
		t.Errorf("expected %d, got %d", DefaultMaxRecordSizeBytesRDS, got)
	}
	exp.MaxSizeBytes = 200
	if got := exp.MaxRecordSizeBytes(); got != 200 {
		t.Errorf("expected 200, got %d", got)
	}
}
