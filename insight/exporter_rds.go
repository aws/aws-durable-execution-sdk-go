package insight

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata"
	rdsdatatypes "github.com/aws/aws-sdk-go-v2/service/rdsdata/types"
)

// DefaultMaxRecordSizeBytesRDS is the default maximum record size in
// bytes for the RDSExporter (Data API 1 MB request limit).
const DefaultMaxRecordSizeBytesRDS = 1 * 1024 * 1024

// RDSDataAPI is the subset of *rdsdata.Client this package depends on,
// exposed as an interface so tests can substitute a fake without needing
// real AWS credentials or network access.
type RDSDataAPI interface {
	ExecuteStatement(ctx context.Context, params *rdsdata.ExecuteStatementInput, optFns ...func(*rdsdata.Options)) (*rdsdata.ExecuteStatementOutput, error)
}

// RDSExporter writes records to an Aurora/RDS cluster via the RDS Data
// API's ExecuteStatement (HTTP-based — no VPC required).
//
// Record values are always bound via SqlParameters. The table name is the
// one value embedded in the statement text (identifiers cannot be bound as
// parameters) and is validated as a plain SQL identifier before use. Uses
// INSERT INTO with named parameters for the four columns: execution_arn,
// emitted_at, status, record_json.
type RDSExporter struct {
	// API is the underlying RDS Data API client. Required.
	API RDSDataAPI

	// ResourceArn is the target Aurora cluster's ARN. Required.
	ResourceArn string

	// SecretArn is the Secrets Manager secret ARN holding the database
	// credentials. Required.
	SecretArn string

	// Database is the target database name. Required.
	Database string

	// TableName is the target table name. Defaults to
	// "workflow_insight" when empty.
	TableName string

	// MaxSizeBytes overrides DefaultMaxRecordSizeBytesRDS. Left at its
	// zero value, MaxRecordSizeBytes() returns
	// DefaultMaxRecordSizeBytesRDS instead.
	MaxSizeBytes int
}

// Export inserts record into the target RDS/Aurora table via a
// parameterized INSERT statement.
func (e *RDSExporter) Export(ctx context.Context, record Record) error {
	if e.API == nil {
		return fmt.Errorf("insight.RDSExporter: API is nil")
	}
	if e.ResourceArn == "" || e.SecretArn == "" || e.Database == "" {
		return fmt.Errorf("insight.RDSExporter: ResourceArn, SecretArn, and Database are all required")
	}

	recordJSON, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("insight.RDSExporter: marshaling record: %w", err)
	}

	table := e.TableName
	if table == "" {
		table = "workflow_insight"
	}
	if err := validateTableName(table); err != nil {
		return fmt.Errorf("insight.RDSExporter: %w", err)
	}

	emittedAt := record.EmittedAt.Format(dynamoDBTimeFormat)

	sql := fmt.Sprintf(
		"INSERT INTO %s (execution_arn, emitted_at, status, record_json) VALUES (:arn, :ts, :status, :json)",
		table,
	)

	params := []rdsdatatypes.SqlParameter{
		{Name: aws.String("arn"), Value: &rdsdatatypes.FieldMemberStringValue{Value: record.ExecutionArn}},
		{Name: aws.String("ts"), Value: &rdsdatatypes.FieldMemberStringValue{Value: emittedAt}},
		{Name: aws.String("status"), Value: &rdsdatatypes.FieldMemberStringValue{Value: string(record.Status)}},
		{Name: aws.String("json"), Value: &rdsdatatypes.FieldMemberStringValue{Value: string(recordJSON)}},
	}

	_, err = e.API.ExecuteStatement(ctx, &rdsdata.ExecuteStatementInput{
		ResourceArn: aws.String(e.ResourceArn),
		SecretArn:   aws.String(e.SecretArn),
		Database:    aws.String(e.Database),
		Sql:         aws.String(sql),
		Parameters:  params,
	})
	if err != nil {
		return fmt.Errorf("insight.RDSExporter: ExecuteStatement: %w", err)
	}
	return nil
}

// MaxRecordSizeBytes implements SizeLimiter, returning e.MaxSizeBytes or
// DefaultMaxRecordSizeBytesRDS when unset.
func (e *RDSExporter) MaxRecordSizeBytes() int {
	if e.MaxSizeBytes > 0 {
		return e.MaxSizeBytes
	}
	return DefaultMaxRecordSizeBytesRDS
}
