package insight

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// DefaultMaxRecordSizeBytesDynamoDB is the default maximum record size
// in bytes for the DynamoDBExporter (DynamoDB 400 KB item limit).
const DefaultMaxRecordSizeBytesDynamoDB = 400 * 1024

// DynamoDBAPI is the subset of *dynamodb.Client this package depends on,
// exposed as an interface so tests can substitute a fake without needing
// real AWS credentials or network access.
type DynamoDBAPI interface {
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
}

// DynamoDBExporter writes records to Amazon DynamoDB via PutItem.
//
// Partition key value is the record's ExecutionArn. Sort key value (when
// present) is the record's EmittedAt timestamp in ISO 8601 format — so
// a full history of every record ever emitted for a given execution
// accumulates as separate items sharing the same partition key.
//
// Set SortKey to NoSortKey to explicitly disable the sort key — PutItem
// then overwrites the single item for a given partition key on every
// call (only the latest state is kept).
type DynamoDBExporter struct {
	// API is the underlying DynamoDB client. Required.
	API DynamoDBAPI

	// TableName is the target table. Required.
	TableName string

	// PartitionKeyField is the partition-key attribute name. Defaults to
	// "pk" when empty.
	PartitionKeyField string

	// SortKeyField is the sort-key attribute name, or nil to accept the
	// default ("sk"). Set to NoSortKey (a non-nil pointer to "") to
	// explicitly disable the sort key.
	SortKeyField *string

	// MaxSizeBytes overrides DefaultMaxRecordSizeBytesDynamoDB. Left at
	// its zero value, MaxRecordSizeBytes() returns
	// DefaultMaxRecordSizeBytesDynamoDB instead.
	MaxSizeBytes int
}

// NoSortKey is a *string pointing at the empty string, for use as
// DynamoDBExporter.SortKeyField to explicitly disable the sort key.
var NoSortKey = new(string)

// Export writes record as a DynamoDB item.
func (e *DynamoDBExporter) Export(ctx context.Context, record Record) error {
	if e.API == nil {
		return fmt.Errorf("insight.DynamoDBExporter: API is nil")
	}
	if e.TableName == "" {
		return fmt.Errorf("insight.DynamoDBExporter: TableName is required")
	}

	pk := e.PartitionKeyField
	if pk == "" {
		pk = "pk"
	}

	item := map[string]ddbtypes.AttributeValue{
		pk: &ddbtypes.AttributeValueMemberS{Value: record.ExecutionArn},
	}

	// Marshal all record fields to DynamoDB attribute values manually.
	if record.ExecutionName != "" {
		item["executionName"] = &ddbtypes.AttributeValueMemberS{Value: record.ExecutionName}
	}
	if record.FunctionName != "" {
		item["functionName"] = &ddbtypes.AttributeValueMemberS{Value: record.FunctionName}
	}
	item["status"] = &ddbtypes.AttributeValueMemberS{Value: string(record.Status)}

	if record.StartTimestamp != nil {
		item["startTimestamp"] = &ddbtypes.AttributeValueMemberS{Value: record.StartTimestamp.Format(dynamoDBTimeFormat)}
	}
	if record.EndTimestamp != nil {
		item["endTimestamp"] = &ddbtypes.AttributeValueMemberS{Value: record.EndTimestamp.Format(dynamoDBTimeFormat)}
	}
	if record.DurationMs != nil {
		item["durationMs"] = &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(*record.DurationMs, 10)}
	}

	item["emittedAt"] = &ddbtypes.AttributeValueMemberS{Value: record.EmittedAt.Format(dynamoDBTimeFormat)}

	// Full record as JSON for downstream consumers.
	recordJSON, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("insight.DynamoDBExporter: marshaling record: %w", err)
	}
	item["recordJson"] = &ddbtypes.AttributeValueMemberS{Value: string(recordJSON)}

	// Sort key: default "sk" = EmittedAt, NoSortKey = omit entirely.
	if e.SortKeyField == nil || *e.SortKeyField != "" {
		sk := "sk"
		if e.SortKeyField != nil {
			sk = *e.SortKeyField
		}
		item[sk] = &ddbtypes.AttributeValueMemberS{Value: record.EmittedAt.Format(dynamoDBTimeFormat)}
	}

	_, err = e.API.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(e.TableName),
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

// dynamoDBTimeFormat is the ISO 8601 format used for DynamoDB timestamp
// attributes.
const dynamoDBTimeFormat = "2006-01-02T15:04:05.000Z07:00"
