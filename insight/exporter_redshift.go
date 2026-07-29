package insight

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	rsdatatypes "github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
)

// RedshiftDataAPI is the subset of *redshiftdata.Client this package
// depends on, exposed as an interface so tests can substitute a fake
// without needing real AWS credentials or network access.
type RedshiftDataAPI interface {
	ExecuteStatement(ctx context.Context, params *redshiftdata.ExecuteStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.ExecuteStatementOutput, error)
}

// RedshiftExporter writes records to Amazon Redshift via the Redshift
// Data API's ExecuteStatement (HTTP-based — no VPC required).
//
// Supports two connection modes:
//   - Serverless: set WorkgroupName.
//   - Provisioned cluster: set ClusterIdentifier.
//
// Record values are always bound via SqlParameters. The table name is the
// one value embedded in the statement text (identifiers cannot be bound as
// parameters) and is validated as a plain SQL identifier before use.
//
// Fire-and-forget: does NOT poll for statement completion.
type RedshiftExporter struct {
	// API is the underlying Redshift Data API client. Required.
	API RedshiftDataAPI

	// WorkgroupName selects Serverless mode. Mutually exclusive with
	// ClusterIdentifier — set exactly one.
	WorkgroupName string

	// ClusterIdentifier selects provisioned-cluster mode. Mutually
	// exclusive with WorkgroupName — set exactly one.
	ClusterIdentifier string

	// Database is the target database name. Required.
	Database string

	// TableName is the target table name. Defaults to
	// "workflow_insight" when empty.
	TableName string
}

// Export inserts record into the target Redshift table via a
// parameterized INSERT statement.
func (e *RedshiftExporter) Export(ctx context.Context, record Record) error {
	if e.API == nil {
		return fmt.Errorf("insight.RedshiftExporter: API is nil")
	}
	if e.WorkgroupName == "" && e.ClusterIdentifier == "" {
		return fmt.Errorf("insight.RedshiftExporter: exactly one of WorkgroupName or ClusterIdentifier is required")
	}
	if e.WorkgroupName != "" && e.ClusterIdentifier != "" {
		return fmt.Errorf("insight.RedshiftExporter: WorkgroupName and ClusterIdentifier are mutually exclusive")
	}
	if e.Database == "" {
		return fmt.Errorf("insight.RedshiftExporter: Database is required")
	}

	recordJSON, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("insight.RedshiftExporter: marshaling record: %w", err)
	}

	table := e.TableName
	if table == "" {
		table = "workflow_insight"
	}
	if err := validateTableName(table); err != nil {
		return fmt.Errorf("insight.RedshiftExporter: %w", err)
	}

	emittedAt := record.EmittedAt.Format(dynamoDBTimeFormat)

	sql := fmt.Sprintf(
		"INSERT INTO %s (execution_arn, emitted_at, status, record_json) VALUES (:arn, :ts, :status, :json)",
		table,
	)

	params := []rsdatatypes.SqlParameter{
		{Name: aws.String("arn"), Value: aws.String(record.ExecutionArn)},
		{Name: aws.String("ts"), Value: aws.String(emittedAt)},
		{Name: aws.String("status"), Value: aws.String(string(record.Status))},
		{Name: aws.String("json"), Value: aws.String(string(recordJSON))},
	}

	input := &redshiftdata.ExecuteStatementInput{
		Database:   aws.String(e.Database),
		Sql:        aws.String(sql),
		Parameters: params,
	}
	if e.WorkgroupName != "" {
		input.WorkgroupName = aws.String(e.WorkgroupName)
	} else {
		input.ClusterIdentifier = aws.String(e.ClusterIdentifier)
	}

	if _, err := e.API.ExecuteStatement(ctx, input); err != nil {
		return fmt.Errorf("insight.RedshiftExporter: ExecuteStatement: %w", err)
	}
	return nil
}
