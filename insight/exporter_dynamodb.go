package insight

import (
	"context"
	"encoding/json"
	"fmt"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// DynamoDBAPI is the subset of *dynamodb.Client this package depends on,
// exposed as an interface so tests can substitute a fake without needing
// real AWS credentials or network access - matching the exact convention
// pkg/durable/awssdk.LambdaAPI already established for this same reason.
type DynamoDBAPI interface {
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
}

// DynamoDBExporter writes records to Amazon DynamoDB via PutItem,
// matching the JS SDK's own DynamoDBExporter.
//
// # Upsert behavior
//
// Matching the JS SDK's own documented behavior exactly:
//
//   - WITH a sort key (the default - SortKey left nil): each Export call
//     creates a NEW item, keyed by (PartitionKey=ExecutionARN,
//     SortKey=EmittedAt) - so a full history of every record ever
//     emitted for a given execution accumulates as separate items
//     sharing the same partition key.
//   - WITHOUT a sort key (SortKey set to NoSortKey): PutItem OVERWRITES
//     the single item for a given PartitionKey value on every call -
//     only the latest state is kept.
type DynamoDBExporter struct {
	// API is the underlying DynamoDB client. If nil, NewDynamoDBExporter
	// must be used to construct one with a real *dynamodb.Client
	// resolved from the standard AWS SDK credential/region chain.
	API DynamoDBAPI

	// TableName is the target table. Required.
	TableName string

	// PartitionKey is the partition-key attribute name. Defaults to "pk"
	// when empty. Its value is always the record's own ExecutionARN.
	PartitionKey string

	// SortKey is the sort-key attribute name, or nil to accept the
	// default ("sk") - see this type's own doc for the "full history"
	// mode this produces. Set to NoSortKey (a non-nil pointer to "") to
	// explicitly select the upsert-only (no sort key) mode instead. A
	// plain, unset (nil) SortKey and an explicit NoSortKey are
	// deliberately DISTINGUISHABLE (unlike a bare string field, whose
	// zero value "" cannot be told apart from an explicit empty string)
	// - Export needs that distinction to know whether "no value
	// provided, use the sensible default" or "the caller explicitly
	// wants no sort key at all" was intended.
	SortKey *string

	// MaxSizeBytes overrides DefaultMaxRecordSizeBytesDynamoDB. Left at
	// its zero value, MaxRecordSizeBytes() returns
	// DefaultMaxRecordSizeBytesDynamoDB instead.
	MaxSizeBytes int
}

// NoSortKey is a *string pointing at the empty string, for use as
// DynamoDBExporter.SortKey to explicitly select upsert-only (no sort
// key) mode - see that field's own doc for why a plain string field
// could not express this distinction. Assign as `exp.SortKey =
// insight.NoSortKey`.
var NoSortKey = new(string)

// NewDynamoDBExporter constructs a DynamoDBExporter backed by a real
// *dynamodb.Client, resolving credentials and region via the standard
// AWS SDK for Go v2 default chain - matching
// pkg/durable/awssdk.New's own established convention. PartitionKey
// defaults to "pk" and SortKey is left nil (accepting Export's own "sk"
// default - the JS SDK's own "full history per execution" mode) unless
// set explicitly afterward on the returned *DynamoDBExporter, e.g. to
// insight.NoSortKey for upsert-only mode.
func NewDynamoDBExporter(ctx context.Context, tableName string, optFns ...func(*awsconfig.LoadOptions) error) (*DynamoDBExporter, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("insight.NewDynamoDBExporter: loading AWS config: %w", err)
	}
	return &DynamoDBExporter{
		API:          dynamodb.NewFromConfig(cfg),
		TableName:    tableName,
		PartitionKey: "pk",
	}, nil
}

// Export writes record as a DynamoDB item.
func (e *DynamoDBExporter) Export(ctx context.Context, record WorkflowInsightRecord) error {
	if e.API == nil {
		return fmt.Errorf("insight.DynamoDBExporter: API is nil (use NewDynamoDBExporter, or set API explicitly for tests)")
	}
	if e.TableName == "" {
		return fmt.Errorf("insight.DynamoDBExporter: TableName is required")
	}

	recordJSON, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("insight.DynamoDBExporter: marshaling record: %w", err)
	}

	pk := e.PartitionKey
	if pk == "" {
		pk = "pk"
	}

	item := map[string]ddbtypes.AttributeValue{
		pk:              &ddbtypes.AttributeValueMemberS{Value: record.ExecutionARN},
		"executionName": &ddbtypes.AttributeValueMemberS{Value: record.ExecutionName},
		"functionName":  &ddbtypes.AttributeValueMemberS{Value: record.FunctionName},
		"status":        &ddbtypes.AttributeValueMemberS{Value: string(record.Status)},
		"startTime":     &ddbtypes.AttributeValueMemberS{Value: record.StartTime.Format(rfc3339)},
		"recordJson":    &ddbtypes.AttributeValueMemberS{Value: string(recordJSON)},
		"emittedAt":     &ddbtypes.AttributeValueMemberS{Value: record.EmittedAt.Format(rfc3339)},
	}
	if record.EndTime != nil {
		item["endTime"] = &ddbtypes.AttributeValueMemberS{Value: record.EndTime.Format(rfc3339)}
	}
	if record.DurationMs != nil {
		item["durationMs"] = &ddbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", *record.DurationMs)}
	}

	if e.SortKey == nil || *e.SortKey != "" {
		sk := "sk"
		if e.SortKey != nil {
			sk = *e.SortKey
		}
		item[sk] = &ddbtypes.AttributeValueMemberS{Value: record.EmittedAt.Format(rfc3339)}
	}

	_, err = e.API.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: strPtr(e.TableName),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("insight.DynamoDBExporter: PutItem: %w", err)
	}
	return nil
}

// MaxRecordSizeBytes implements SizeLimiter, returning e.MaxSizeBytes or
// DefaultMaxRecordSizeBytesDynamoDB when unset.
func (e *DynamoDBExporter) MaxRecordSizeBytes() int {
	if e.MaxSizeBytes > 0 {
		return e.MaxSizeBytes
	}
	return DefaultMaxRecordSizeBytesDynamoDB
}

const rfc3339 = "2006-01-02T15:04:05.000Z07:00"

// strPtr is a tiny helper avoiding an aws.String import purely for this
// one call site (dynamodb.PutItemInput.TableName is *string, same as
// every other AWS SDK v2 input's optional-string fields).
func strPtr(s string) *string { return &s }
