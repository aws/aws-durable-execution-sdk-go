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

// fakeRedshiftDataAPI is a test double for RedshiftDataAPI, matching
// pkg/durable/awssdk's own fakeLambdaAPI convention.
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
	t.Fatalf("expected a parameter named %q, params: %+v", name, params)
	return ""
}

func TestRedshiftExporter_Serverless_UsesWorkgroupName(t *testing.T) {
	fake := &fakeRedshiftDataAPI{}
	exp := &RedshiftExporter{API: fake, WorkgroupName: "my-workgroup", SecretARN: "arn:secret", Database: "workflows"}

	if err := exp.Export(context.Background(), WorkflowInsightRecord{ExecutionARN: "arn:test"}); err != nil {
		t.Fatalf("Export: %v", err)
	}

	if len(fake.calls) != 1 {
		t.Fatalf("expected exactly 1 ExecuteStatement call, got %d", len(fake.calls))
	}
	call := fake.calls[0]
	if aws.ToString(call.WorkgroupName) != "my-workgroup" {
		t.Errorf("expected WorkgroupName=my-workgroup, got %q", aws.ToString(call.WorkgroupName))
	}
	if aws.ToString(call.ClusterIdentifier) != "" {
		t.Errorf("expected no ClusterIdentifier set in Serverless mode, got %q", aws.ToString(call.ClusterIdentifier))
	}
}

func TestRedshiftExporter_Cluster_UsesClusterIdentifierAndSecretARN(t *testing.T) {
	fake := &fakeRedshiftDataAPI{}
	exp := &RedshiftExporter{API: fake, ClusterIdentifier: "my-cluster", SecretARN: "arn:secret", Database: "workflows"}

	if err := exp.Export(context.Background(), WorkflowInsightRecord{ExecutionARN: "arn:test"}); err != nil {
		t.Fatalf("Export: %v", err)
	}

	call := fake.calls[0]
	if aws.ToString(call.ClusterIdentifier) != "my-cluster" {
		t.Errorf("expected ClusterIdentifier=my-cluster, got %q", aws.ToString(call.ClusterIdentifier))
	}
	if aws.ToString(call.SecretArn) != "arn:secret" {
		t.Errorf("expected SecretArn=arn:secret, got %q", aws.ToString(call.SecretArn))
	}
}

func TestRedshiftExporter_UsesMergeStatement(t *testing.T) {
	fake := &fakeRedshiftDataAPI{}
	exp := &RedshiftExporter{API: fake, WorkgroupName: "wg", SecretARN: "s", Database: "d"}

	if err := exp.Export(context.Background(), WorkflowInsightRecord{ExecutionARN: "arn:test"}); err != nil {
		t.Fatalf("Export: %v", err)
	}

	sql := aws.ToString(fake.calls[0].Sql)
	if !strings.Contains(sql, "MERGE INTO") {
		t.Errorf("expected a MERGE INTO statement, got SQL: %s", sql)
	}
	if !strings.Contains(sql, "public.workflow_insight") {
		t.Errorf("expected the default schema.table public.workflow_insight, got SQL: %s", sql)
	}
}

func TestRedshiftExporter_CustomSchemaAndTable(t *testing.T) {
	fake := &fakeRedshiftDataAPI{}
	exp := &RedshiftExporter{API: fake, WorkgroupName: "wg", SecretARN: "s", Database: "d", Schema: "analytics", Table: "insights"}

	if err := exp.Export(context.Background(), WorkflowInsightRecord{ExecutionARN: "arn:test"}); err != nil {
		t.Fatalf("Export: %v", err)
	}

	sql := aws.ToString(fake.calls[0].Sql)
	if !strings.Contains(sql, "analytics.insights") {
		t.Errorf("expected the custom schema.table analytics.insights, got SQL: %s", sql)
	}
}

func TestRedshiftExporter_ParametersAreCorrectlyBound(t *testing.T) {
	fake := &fakeRedshiftDataAPI{}
	exp := &RedshiftExporter{API: fake, WorkgroupName: "wg", SecretARN: "s", Database: "d"}

	rec := WorkflowInsightRecord{ExecutionARN: "arn:aws:lambda:us-east-1:123456789012:function:f:1", Status: ExecutionStatusFailed}
	if err := exp.Export(context.Background(), rec); err != nil {
		t.Fatalf("Export: %v", err)
	}

	params := fake.calls[0].Parameters
	if got := rsParamValue(t, params, "execution_arn"); got != rec.ExecutionARN {
		t.Errorf("expected execution_arn=%q, got %q", rec.ExecutionARN, got)
	}
	if got := rsParamValue(t, params, "status"); got != "FAILED" {
		t.Errorf("expected status=FAILED, got %q", got)
	}

	sql := aws.ToString(fake.calls[0].Sql)
	if strings.Contains(sql, rec.ExecutionARN) {
		t.Errorf("expected the SQL string to NEVER contain raw record data (SQL injection risk), found ExecutionARN embedded in: %s", sql)
	}
}

func TestRedshiftExporter_MutuallyExclusiveWorkgroupAndCluster_ReturnsError(t *testing.T) {
	exp := &RedshiftExporter{API: &fakeRedshiftDataAPI{}, WorkgroupName: "wg", ClusterIdentifier: "cluster", Database: "d"}
	if err := exp.Export(context.Background(), WorkflowInsightRecord{}); err == nil {
		t.Fatal("expected an error when both WorkgroupName and ClusterIdentifier are set")
	}
}

func TestRedshiftExporter_NeitherWorkgroupNorCluster_ReturnsError(t *testing.T) {
	exp := &RedshiftExporter{API: &fakeRedshiftDataAPI{}, Database: "d"}
	if err := exp.Export(context.Background(), WorkflowInsightRecord{}); err == nil {
		t.Fatal("expected an error when neither WorkgroupName nor ClusterIdentifier is set")
	}
}

func TestRedshiftExporter_ClusterWithoutSecretARN_ReturnsError(t *testing.T) {
	exp := &RedshiftExporter{API: &fakeRedshiftDataAPI{}, ClusterIdentifier: "cluster", Database: "d"}
	if err := exp.Export(context.Background(), WorkflowInsightRecord{}); err == nil {
		t.Fatal("expected an error when ClusterIdentifier is set without SecretARN")
	}
}

func TestRedshiftExporter_MissingDatabase_ReturnsError(t *testing.T) {
	exp := &RedshiftExporter{API: &fakeRedshiftDataAPI{}, WorkgroupName: "wg"}
	if err := exp.Export(context.Background(), WorkflowInsightRecord{}); err == nil {
		t.Fatal("expected an error when Database is unset")
	}
}

func TestRedshiftExporter_ExecuteStatementError_Propagates(t *testing.T) {
	wantErr := errors.New("query failed")
	fake := &fakeRedshiftDataAPI{err: wantErr}
	exp := &RedshiftExporter{API: fake, WorkgroupName: "wg", SecretARN: "s", Database: "d"}

	err := exp.Export(context.Background(), WorkflowInsightRecord{})
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected the underlying ExecuteStatement error to propagate (wrapped), got %v", err)
	}
}
