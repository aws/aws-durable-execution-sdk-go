package insight

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	rsdatatypes "github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
)

// fakeRedshiftDataAPI is a test double for RedshiftDataAPI.
type fakeRedshiftDataAPI struct {
	calls []*redshiftdata.ExecuteStatementInput
	err   error
}

func (f *fakeRedshiftDataAPI) ExecuteStatement(_ context.Context, params *redshiftdata.ExecuteStatementInput, _ ...func(*redshiftdata.Options)) (*redshiftdata.ExecuteStatementOutput, error) {
	f.calls = append(f.calls, params)
	if f.err != nil {
		return nil, f.err
	}
	return &redshiftdata.ExecuteStatementOutput{}, nil
}

func rsParamValue(t *testing.T, params []rsdatatypes.SqlParameter, name string) string {
	t.Helper()
	for _, p := range params {
		if aws.ToString(p.Name) == name {
			return aws.ToString(p.Value)
		}
	}
	t.Fatalf("expected a parameter named %q", name)
	return ""
}

func TestRedshiftExporter_Serverless_UsesWorkgroupName(t *testing.T) {
	fake := &fakeRedshiftDataAPI{}
	exp := &RedshiftExporter{API: fake, WorkgroupName: "my-workgroup", Database: "workflows"}

	if err := exp.Export(context.Background(), Record{ExecutionArn: "arn:test"}); err != nil {
		t.Fatalf("Export: %v", err)
	}

	if len(fake.calls) != 1 {
		t.Fatalf("expected exactly 1 ExecuteStatement call, got %d", len(fake.calls))
	}
	call := fake.calls[0]
	if aws.ToString(call.WorkgroupName) != "my-workgroup" {
		t.Errorf("expected WorkgroupName=my-workgroup, got %q", aws.ToString(call.WorkgroupName))
	}
	if call.ClusterIdentifier != nil {
		t.Errorf("expected no ClusterIdentifier in Serverless mode, got %q", aws.ToString(call.ClusterIdentifier))
	}
}

func TestRedshiftExporter_Cluster_UsesClusterIdentifier(t *testing.T) {
	fake := &fakeRedshiftDataAPI{}
	exp := &RedshiftExporter{API: fake, ClusterIdentifier: "my-cluster", Database: "workflows"}

	if err := exp.Export(context.Background(), Record{ExecutionArn: "arn:test"}); err != nil {
		t.Fatalf("Export: %v", err)
	}

	call := fake.calls[0]
	if aws.ToString(call.ClusterIdentifier) != "my-cluster" {
		t.Errorf("expected ClusterIdentifier=my-cluster, got %q", aws.ToString(call.ClusterIdentifier))
	}
}

func TestRedshiftExporter_UsesParameterizedInsert(t *testing.T) {
	fake := &fakeRedshiftDataAPI{}
	exp := &RedshiftExporter{API: fake, WorkgroupName: "wg", Database: "d"}

	rec := Record{ExecutionArn: "arn:aws:lambda:us-east-1:111122223333:function:f:1", Status: StatusFailed}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	sql := aws.ToString(fake.calls[0].Sql)
	if !strings.Contains(sql, "INSERT INTO") {
		t.Errorf("expected an INSERT INTO statement, got SQL: %s", sql)
	}
	if !strings.Contains(sql, "workflow_insight") {
		t.Errorf("expected the default table name workflow_insight, got SQL: %s", sql)
	}
	// SQL must use named parameters, not interpolated values.
	if strings.Contains(sql, rec.ExecutionArn) {
		t.Errorf("expected SQL to NEVER contain raw record data (SQL injection risk), found ExecutionArn in: %s", sql)
	}

	params := fake.calls[0].Parameters
	if got := rsParamValue(t, params, "arn"); got != rec.ExecutionArn {
		t.Errorf("expected arn=%q, got %q", rec.ExecutionArn, got)
	}
	if got := rsParamValue(t, params, "status"); got != "FAILED" {
		t.Errorf("expected status=FAILED, got %q", got)
	}
}

func TestRedshiftExporter_CustomTableName(t *testing.T) {
	fake := &fakeRedshiftDataAPI{}
	exp := &RedshiftExporter{API: fake, WorkgroupName: "wg", Database: "d", TableName: "my_table"}

	if err := exp.Export(context.Background(), Record{ExecutionArn: "arn:test"}); err != nil {
		t.Fatalf("Export: %v", err)
	}

	sql := aws.ToString(fake.calls[0].Sql)
	if !strings.Contains(sql, "my_table") {
		t.Errorf("expected custom table name in SQL, got: %s", sql)
	}
}

// TestRedshiftExporter_InvalidTableName_Rejected verifies that a table
// name that is not a plain identifier is rejected before any statement is
// sent.
func TestRedshiftExporter_InvalidTableName_Rejected(t *testing.T) {
	for _, table := range []string{
		"t; DROP TABLE x",
		"t (execution_arn) VALUES ('x'); --",
		`t"`,
		"t name",
		"1t",
		"a.b.c",
	} {
		fake := &fakeRedshiftDataAPI{}
		exp := &RedshiftExporter{API: fake, WorkgroupName: "wg", Database: "d", TableName: table}

		if err := exp.Export(context.Background(), Record{ExecutionArn: "arn:test"}); err == nil {
			t.Errorf("Export with table name %q: expected error, got nil", table)
		}
		if len(fake.calls) != 0 {
			t.Errorf("Export with table name %q: statement was sent", table)
		}
	}
}

func TestRedshiftExporter_MutuallyExclusiveWorkgroupAndCluster_ReturnsError(t *testing.T) {
	exp := &RedshiftExporter{API: &fakeRedshiftDataAPI{}, WorkgroupName: "wg", ClusterIdentifier: "cluster", Database: "d"}
	if err := exp.Export(context.Background(), Record{}); err == nil {
		t.Fatal("expected an error when both WorkgroupName and ClusterIdentifier are set")
	}
}

func TestRedshiftExporter_NeitherWorkgroupNorCluster_ReturnsError(t *testing.T) {
	exp := &RedshiftExporter{API: &fakeRedshiftDataAPI{}, Database: "d"}
	if err := exp.Export(context.Background(), Record{}); err == nil {
		t.Fatal("expected an error when neither WorkgroupName nor ClusterIdentifier is set")
	}
}

func TestRedshiftExporter_MissingDatabase_ReturnsError(t *testing.T) {
	exp := &RedshiftExporter{API: &fakeRedshiftDataAPI{}, WorkgroupName: "wg"}
	if err := exp.Export(context.Background(), Record{}); err == nil {
		t.Fatal("expected an error when Database is unset")
	}
}

func TestRedshiftExporter_ExecuteStatementError_Propagates(t *testing.T) {
	wantErr := errors.New("query failed")
	fake := &fakeRedshiftDataAPI{err: wantErr}
	exp := &RedshiftExporter{API: fake, WorkgroupName: "wg", Database: "d"}

	err := exp.Export(context.Background(), Record{})
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected the underlying ExecuteStatement error to propagate (wrapped), got %v", err)
	}
}
