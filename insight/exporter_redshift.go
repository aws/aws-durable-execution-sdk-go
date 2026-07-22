package insight

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	rsdatatypes "github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
)

// RedshiftDataAPI is the subset of *redshiftdata.Client this package
// depends on, exposed as an interface so tests can substitute a fake
// without needing real AWS credentials or network access - matching the
// exact convention pkg/durable/awssdk.LambdaAPI already established for
// this same reason.
type RedshiftDataAPI interface {
	ExecuteStatement(ctx context.Context, params *redshiftdata.ExecuteStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.ExecuteStatementOutput, error)
}

// RedshiftExporter writes records to Amazon Redshift via the Redshift
// Data API (HTTP-based - no VPC required), matching the JS SDK's own
// RedshiftExporter. Supports both connection modes the JS SDK itself
// documents:
//
//   - Serverless: set WorkgroupName (SecretARN optional - IAM-based
//     temporary credentials via GetCredentials are also supported by the
//     Data API, but this exporter's own minimal-config default is
//     Secrets-Manager-based, matching WithSecretARN's own doc).
//   - Provisioned cluster: set ClusterIdentifier AND SecretARN (both
//     required together for a provisioned cluster).
//
// # Upsert behavior
//
// Matching the JS SDK's own documented behavior exactly: a `MERGE`
// statement, upserting by execution_arn. Table schema matches the JS
// SDK's own documented schema column-for-column (see AuroraExporter's
// own doc - identical column set): execution_arn (primary key),
// execution_name, function_name, status, start_time, end_time,
// duration_ms, record_json, emitted_at.
type RedshiftExporter struct {
	// API is the underlying Redshift Data API client. If nil,
	// NewRedshiftExporter must be used to construct one with a real
	// *redshiftdata.Client resolved from the standard AWS SDK
	// credential/region chain.
	API RedshiftDataAPI

	// WorkgroupName selects Serverless mode. Mutually exclusive with
	// ClusterIdentifier - set exactly one.
	WorkgroupName string

	// ClusterIdentifier selects provisioned-cluster mode. Mutually
	// exclusive with WorkgroupName - set exactly one. Requires SecretARN
	// also be set.
	ClusterIdentifier string

	// SecretARN is the Secrets Manager secret ARN holding the database
	// credentials. Required when ClusterIdentifier is set; optional (but
	// recommended, and required by this exporter's own minimal-config
	// default) when WorkgroupName is set.
	SecretARN string

	// Database is the target database name. Required.
	Database string

	// Table is the target table name. Defaults to "workflow_insight"
	// when empty, matching the JS SDK's own default.
	Table string

	// Schema is the target schema name. Defaults to "public" when empty,
	// matching the JS SDK's own default.
	Schema string

	// MaxSizeBytes overrides DefaultMaxRecordSizeBytesSQL. Left at its
	// zero value, MaxRecordSizeBytes() returns DefaultMaxRecordSizeBytesSQL
	// instead.
	MaxSizeBytes int
}

// NewRedshiftServerlessExporter constructs a RedshiftExporter for
// Serverless mode, backed by a real *redshiftdata.Client, resolving
// credentials/region via the standard AWS SDK for Go v2 default chain -
// matching pkg/durable/awssdk.New's own established convention.
func NewRedshiftServerlessExporter(ctx context.Context, workgroupName, secretARN, database string, optFns ...func(*awsconfig.LoadOptions) error) (*RedshiftExporter, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("insight.NewRedshiftServerlessExporter: loading AWS config: %w", err)
	}
	return &RedshiftExporter{
		API:           redshiftdata.NewFromConfig(cfg),
		WorkgroupName: workgroupName,
		SecretARN:     secretARN,
		Database:      database,
	}, nil
}

// NewRedshiftClusterExporter constructs a RedshiftExporter for a
// provisioned cluster, backed by a real *redshiftdata.Client, resolving
// credentials/region via the standard AWS SDK for Go v2 default chain.
func NewRedshiftClusterExporter(ctx context.Context, clusterIdentifier, secretARN, database string, optFns ...func(*awsconfig.LoadOptions) error) (*RedshiftExporter, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("insight.NewRedshiftClusterExporter: loading AWS config: %w", err)
	}
	return &RedshiftExporter{
		API:               redshiftdata.NewFromConfig(cfg),
		ClusterIdentifier: clusterIdentifier,
		SecretARN:         secretARN,
		Database:          database,
	}, nil
}

// Export upserts record into this exporter's target table via MERGE.
func (e *RedshiftExporter) Export(ctx context.Context, record WorkflowInsightRecord) error {
	if e.API == nil {
		return fmt.Errorf("insight.RedshiftExporter: API is nil (use NewRedshiftServerlessExporter/NewRedshiftClusterExporter, or set API explicitly for tests)")
	}
	if e.WorkgroupName == "" && e.ClusterIdentifier == "" {
		return fmt.Errorf("insight.RedshiftExporter: exactly one of WorkgroupName or ClusterIdentifier is required")
	}
	if e.WorkgroupName != "" && e.ClusterIdentifier != "" {
		return fmt.Errorf("insight.RedshiftExporter: WorkgroupName and ClusterIdentifier are mutually exclusive - set only one")
	}
	if e.ClusterIdentifier != "" && e.SecretARN == "" {
		return fmt.Errorf("insight.RedshiftExporter: SecretARN is required when ClusterIdentifier is set")
	}
	if e.Database == "" {
		return fmt.Errorf("insight.RedshiftExporter: Database is required")
	}

	recordJSON, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("insight.RedshiftExporter: marshaling record: %w", err)
	}

	sql, params := e.buildMerge(record, string(recordJSON))

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
	if e.SecretARN != "" {
		input.SecretArn = aws.String(e.SecretARN)
	}

	if _, err := e.API.ExecuteStatement(ctx, input); err != nil {
		return fmt.Errorf("insight.RedshiftExporter: ExecuteStatement: %w", err)
	}
	return nil
}

// MaxRecordSizeBytes implements SizeLimiter, returning e.MaxSizeBytes or
// DefaultMaxRecordSizeBytesSQL when unset.
func (e *RedshiftExporter) MaxRecordSizeBytes() int {
	if e.MaxSizeBytes > 0 {
		return e.MaxSizeBytes
	}
	return DefaultMaxRecordSizeBytesSQL
}

// buildMerge constructs a parameterized MERGE statement upserting by
// execution_arn - always parameterized (never string-interpolated SQL),
// matching AuroraExporter's own SQL-injection-safety reasoning exactly
// (see that type's own buildUpsert doc).
func (e *RedshiftExporter) buildMerge(record WorkflowInsightRecord, recordJSON string) (string, []rsdatatypes.SqlParameter) {
	schema := e.Schema
	if schema == "" {
		schema = "public"
	}
	table := e.Table
	if table == "" {
		table = "workflow_insight"
	}

	endTime := ""
	if record.EndTime != nil {
		endTime = record.EndTime.Format(rfc3339)
	}
	var durationMs int64
	if record.DurationMs != nil {
		durationMs = *record.DurationMs
	}

	params := []rsdatatypes.SqlParameter{
		rsStrParam("execution_arn", record.ExecutionARN),
		rsStrParam("execution_name", record.ExecutionName),
		rsStrParam("function_name", record.FunctionName),
		rsStrParam("status", string(record.Status)),
		rsStrParam("start_time", record.StartTime.Format(rfc3339)),
		rsStrParam("end_time", endTime),
		rsStrParam("duration_ms", fmt.Sprintf("%d", durationMs)),
		rsStrParam("record_json", recordJSON),
		rsStrParam("emitted_at", record.EmittedAt.Format(rfc3339)),
	}

	// Redshift's own MERGE syntax (distinct from PostgreSQL's/standard
	// SQL's own MERGE): USING a single-row VALUES-derived source table,
	// matched ON the primary key, matching the JS SDK's own documented
	// "uses MERGE to insert or update by execution_arn" behavior.
	sql := fmt.Sprintf(`MERGE INTO %s.%s USING (
  SELECT
    :execution_arn AS execution_arn,
    :execution_name AS execution_name,
    :function_name AS function_name,
    :status AS status,
    :start_time AS start_time,
    :end_time AS end_time,
    CAST(:duration_ms AS BIGINT) AS duration_ms,
    JSON_PARSE(:record_json) AS record_json,
    :emitted_at AS emitted_at
) AS src
ON %s.%s.execution_arn = src.execution_arn
WHEN MATCHED THEN UPDATE SET
  execution_name = src.execution_name,
  function_name = src.function_name,
  status = src.status,
  start_time = src.start_time,
  end_time = src.end_time,
  duration_ms = src.duration_ms,
  record_json = src.record_json,
  emitted_at = src.emitted_at
WHEN NOT MATCHED THEN INSERT (execution_arn, execution_name, function_name, status, start_time, end_time, duration_ms, record_json, emitted_at)
VALUES (src.execution_arn, src.execution_name, src.function_name, src.status, src.start_time, src.end_time, src.duration_ms, src.record_json, src.emitted_at)`,
		schema, table, schema, table)

	return sql, params
}

// rsStrParam builds a Redshift Data API string-valued named parameter.
func rsStrParam(name, value string) rsdatatypes.SqlParameter {
	return rsdatatypes.SqlParameter{
		Name:  aws.String(name),
		Value: aws.String(value),
	}
}
