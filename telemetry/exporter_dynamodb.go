package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
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
// Each item holds the record in the by-name shape (see
// OperationsFormatByName): every top-level key of the rendered record JSON
// is its own item attribute, and operationsByName replaces the operations
// array. A JSON object becomes an M attribute, an array an L, a string an S,
// a number an N, a boolean a BOOL, and null a NULL. So input, output, and
// operationsByName are M attributes when the value is an object, and a
// reader can address a nested value by its document path, for example
// operationsByName.increment.count. The JavaScript, Python, and Java
// exporters write the same item.
//
// Partition key value is the record's ExecutionArn. Sort key value (when
// present) is the record's EmittedAt timestamp in the record's own
// timestamp format (UTC, millisecond precision, Z suffix) — so
// a full history of every record ever emitted for a given execution
// accumulates as separate items sharing the same partition key. The key
// attributes are set after the record attributes, so a key attribute name
// that equals a record key replaces that record attribute.
//
// Set SortKeyField to NoSortKey to explicitly disable the sort key —
// PutItem then overwrites the single item for a given partition key on
// every call (only the latest state is kept).
//
// DynamoDB stores a number with at most 38 significant digits. The
// magnitude of a non-zero number must be between 1E-130 and
// 9.99...E+125. A record that carries any other number, for example a
// float64 input value of 1e200, fails PutItem with a ValidationException,
// and Export returns that error.
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
		return fmt.Errorf("telemetry.DynamoDBExporter: API is nil")
	}
	if e.TableName == "" {
		return fmt.Errorf("telemetry.DynamoDBExporter: TableName is required")
	}

	pk := e.PartitionKeyField
	if pk == "" {
		pk = "pk"
	}

	item, err := dynamoDBItem(record)
	if err != nil {
		return fmt.Errorf("telemetry.DynamoDBExporter: %w", err)
	}

	item[pk] = &ddbtypes.AttributeValueMemberS{Value: record.ExecutionArn}

	// Sort key: default "sk" = EmittedAt, NoSortKey = omit entirely.
	if e.SortKeyField == nil || *e.SortKeyField != "" {
		sk := "sk"
		if e.SortKeyField != nil {
			sk = *e.SortKeyField
		}
		item[sk] = &ddbtypes.AttributeValueMemberS{Value: formatTimestamp(record.EmittedAt)}
	}

	_, err = e.API.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(e.TableName),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("telemetry.DynamoDBExporter: PutItem: %w", err)
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

// Render implements RenderingSizeLimiter. It returns the by-name record
// JSON that Export converts into item attributes, so truncation sizes the
// record in the shape the item carries. The size is of the JSON text, not
// of DynamoDB's item size accounting, which counts attribute names and
// values differently; the JavaScript exporter sizes the same JSON text.
func (e *DynamoDBExporter) Render(record Record) ([]byte, error) {
	return RenderRecord(record, OperationsFormatByName)
}

// dynamoDBItem converts record into one item attribute per top-level key
// of its by-name JSON rendering.
//
// The rendering is decoded with UseNumber. So every number stays a
// json.Number with the exact digits of the record JSON, and attributevalue
// writes those digits as the N value. Decoding into float64 instead would
// round an integer above 2^53.
func dynamoDBItem(record Record) (map[string]ddbtypes.AttributeValue, error) {
	rendered, err := RenderRecord(record, OperationsFormatByName)
	if err != nil {
		return nil, fmt.Errorf("rendering record: %w", err)
	}

	dec := json.NewDecoder(bytes.NewReader(rendered))
	dec.UseNumber()
	var fields map[string]any
	if err := dec.Decode(&fields); err != nil {
		return nil, fmt.Errorf("decoding rendered record: %w", err)
	}

	item, err := attributevalue.MarshalMap(fields)
	if err != nil {
		return nil, fmt.Errorf("converting record to item attributes: %w", err)
	}
	return item, nil
}
