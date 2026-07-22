package insight

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata"
	rdsdatatypes "github.com/aws/aws-sdk-go-v2/service/rdsdata/types"
)

// AuroraEngine selects the SQL dialect AuroraExporter generates its
// upsert statement in, matching the JS SDK's own AuroraExporter engine
// option.
type AuroraEngine string

const (
	AuroraEnginePostgreSQL AuroraEngine = "postgresql"
	AuroraEngineMySQL      AuroraEngine = "mysql"
)

// RDSDataAPI is the subset of *rdsdata.Client this package depends on,
// exposed as an interface so tests can substitute a fake without needing
// real AWS credentials or network access - matching the exact convention
// pkg/durable/awssdk.LambdaAPI already established for this same reason.
type RDSDataAPI interface {
	ExecuteStatement(ctx context.Context, params *rdsdata.ExecuteStatementInput, optFns ...func(*rdsdata.Options)) (*rdsdata.ExecuteStatementOutput, error)
}

// AuroraExporter writes records to an Aurora MySQL or PostgreSQL cluster
// via the RDS Data API (no VPC required - the Data API is a plain HTTPS
// endpoint), matching the JS SDK's own AuroraExporter.
//
// # Upsert behavior
//
// Matching the JS SDK's own documented behavior exactly: an
// `INSERT ... ON CONFLICT (execution_arn) DO UPDATE` (PostgreSQL) or
// `INSERT ... ON DUPLICATE KEY UPDATE` (MySQL) keyed by execution_arn -
// a repeated Export for the same execution updates the same row rather
// than accumulating duplicates.
//
// # Table schema
//
// Matches the JS SDK's own documented schema exactly (column names and
// order): execution_arn (primary key), execution_name, function_name,
// status, start_time, end_time, duration_ms, record_json, emitted_at.
// Creating this table ahead of time is the caller's own responsibility
// (see the JS SDK's own README for the exact CREATE TABLE statements per
// engine) - this exporter never attempts DDL.
type AuroraExporter struct {
	// API is the underlying RDS Data API client. If nil, NewAuroraExporter
	// must be used to construct one with a real *rdsdata.Client resolved
	// from the standard AWS SDK credential/region chain.
	API RDSDataAPI

	// ResourceARN is the target Aurora cluster's ARN. Required.
	ResourceARN string

	// SecretARN is the Secrets Manager secret ARN holding the database
	// credentials the Data API uses to connect. Required.
	SecretARN string

	// Database is the target database name. Required.
	Database string

	// Table is the target table name. Defaults to "workflow_insight"
	// when empty, matching the JS SDK's own default.
	Table string

	// Engine selects the upsert SQL dialect. Defaults to
	// AuroraEnginePostgreSQL when left at its zero value.
	Engine AuroraEngine

	// MaxSizeBytes overrides DefaultMaxRecordSizeBytesSQL. Left at its
	// zero value, MaxRecordSizeBytes() returns DefaultMaxRecordSizeBytesSQL
	// instead.
	MaxSizeBytes int
}

// NewAuroraExporter constructs an AuroraExporter backed by a real
// *rdsdata.Client, resolving credentials and region via the standard AWS
// SDK for Go v2 default chain - matching pkg/durable/awssdk.New's own
// established convention. Note this ONLY resolves the Lambda execution
// role's own AWS credentials (for calling the RDS Data API itself); the
// DATABASE credentials the Data API then uses internally come from
// SecretARN, set separately on the returned *AuroraExporter.
func NewAuroraExporter(ctx context.Context, resourceARN, secretARN, database string, optFns ...func(*awsconfig.LoadOptions) error) (*AuroraExporter, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("insight.NewAuroraExporter: loading AWS config: %w", err)
	}
	return &AuroraExporter{
		API:         rdsdata.NewFromConfig(cfg),
		ResourceARN: resourceARN,
		SecretARN:   secretARN,
		Database:    database,
	}, nil
}

// Export upserts record into this exporter's target table.
func (e *AuroraExporter) Export(ctx context.Context, record WorkflowInsightRecord) error {
	if e.API == nil {
		return fmt.Errorf("insight.AuroraExporter: API is nil (use NewAuroraExporter, or set API explicitly for tests)")
	}
	if e.ResourceARN == "" || e.SecretARN == "" || e.Database == "" {
		return fmt.Errorf("insight.AuroraExporter: ResourceARN, SecretARN, and Database are all required")
	}

	recordJSON, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("insight.AuroraExporter: marshaling record: %w", err)
	}

	table := e.Table
	if table == "" {
		table = "workflow_insight"
	}

	sql, params := e.buildUpsert(table, record, string(recordJSON))

	_, err = e.API.ExecuteStatement(ctx, &rdsdata.ExecuteStatementInput{
		ResourceArn: aws.String(e.ResourceARN),
		SecretArn:   aws.String(e.SecretARN),
		Database:    aws.String(e.Database),
		Sql:         aws.String(sql),
		Parameters:  params,
	})
	if err != nil {
		return fmt.Errorf("insight.AuroraExporter: ExecuteStatement: %w", err)
	}
	return nil
}

// MaxRecordSizeBytes implements SizeLimiter, returning e.MaxSizeBytes or
// DefaultMaxRecordSizeBytesSQL when unset.
func (e *AuroraExporter) MaxRecordSizeBytes() int {
	if e.MaxSizeBytes > 0 {
		return e.MaxSizeBytes
	}
	return DefaultMaxRecordSizeBytesSQL
}

// buildUpsert constructs the engine-specific upsert SQL and its
// parameterized values - always parameterized (never string-interpolated
// SQL), matching the JS SDK's own use of the Data API's own named
// parameter binding to avoid SQL injection from record content (e.g. a
// handler result or error message that happens to contain SQL
// metacharacters).
func (e *AuroraExporter) buildUpsert(table string, record WorkflowInsightRecord, recordJSON string) (string, []rdsdatatypes.SqlParameter) {
	params := []rdsdatatypes.SqlParameter{
		strParam("execution_arn", record.ExecutionARN),
		strParam("execution_name", record.ExecutionName),
		strParam("function_name", record.FunctionName),
		strParam("status", string(record.Status)),
		strParam("start_time", record.StartTime.Format(rfc3339)),
		strParam("record_json", recordJSON),
		strParam("emitted_at", record.EmittedAt.Format(rfc3339)),
	}
	endTime := ""
	if record.EndTime != nil {
		endTime = record.EndTime.Format(rfc3339)
	}
	params = append(params, strParam("end_time", endTime))
	var durationMs int64
	if record.DurationMs != nil {
		durationMs = *record.DurationMs
	}
	params = append(params, longParam("duration_ms", durationMs))

	columns := "execution_arn, execution_name, function_name, status, start_time, end_time, duration_ms, record_json, emitted_at"
	values := ":execution_arn, :execution_name, :function_name, :status, :start_time, :end_time, :duration_ms, :record_json, :emitted_at"

	if e.Engine == AuroraEngineMySQL {
		sql := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE execution_name=:execution_name, function_name=:function_name, status=:status, start_time=:start_time, end_time=:end_time, duration_ms=:duration_ms, record_json=:record_json, emitted_at=:emitted_at",
			table, columns, values,
		)
		return sql, params
	}

	// PostgreSQL (the default).
	sql := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (execution_arn) DO UPDATE SET execution_name=:execution_name, function_name=:function_name, status=:status, start_time=:start_time, end_time=:end_time, duration_ms=:duration_ms, record_json=:record_json, emitted_at=:emitted_at",
		table, columns, values,
	)
	return sql, params
}

// strParam builds an RDS Data API string-valued named parameter.
func strParam(name, value string) rdsdatatypes.SqlParameter {
	return rdsdatatypes.SqlParameter{
		Name:  aws.String(name),
		Value: &rdsdatatypes.FieldMemberStringValue{Value: value},
	}
}

// longParam builds an RDS Data API integer-valued named parameter.
func longParam(name string, value int64) rdsdatatypes.SqlParameter {
	return rdsdatatypes.SqlParameter{
		Name:  aws.String(name),
		Value: &rdsdatatypes.FieldMemberLongValue{Value: value},
	}
}
