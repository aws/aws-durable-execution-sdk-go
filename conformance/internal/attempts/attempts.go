// Package attempts tracks step attempt counts across invocations in a
// DynamoDB table, letting conformance handlers fail deterministically on
// specific attempts.
package attempts

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var newClient = sync.OnceValues(func() (*dynamodb.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("attempts: load AWS config: %w", err)
	}
	return dynamodb.NewFromConfig(cfg), nil
})

// Increment atomically increments and returns the attempt counter for the
// given execution. The table name comes from ATTEMPTS_TABLE_NAME, with a
// default of "Attempts".
func Increment(ctx context.Context, executionID string) (int, error) {
	client, err := newClient()
	if err != nil {
		return 0, err
	}
	table := os.Getenv("ATTEMPTS_TABLE_NAME")
	if table == "" {
		table = "Attempts"
	}
	out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(table),
		Key: map[string]types.AttributeValue{
			"executionId": &types.AttributeValueMemberS{Value: executionID},
		},
		UpdateExpression: aws.String("SET attemptCount = if_not_exists(attemptCount, :zero) + :inc"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":zero": &types.AttributeValueMemberN{Value: "0"},
			":inc":  &types.AttributeValueMemberN{Value: "1"},
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	})
	if err != nil {
		return 0, fmt.Errorf("attempts: increment counter: %w", err)
	}
	n, ok := out.Attributes["attemptCount"].(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("attempts: unexpected attribute shape %T", out.Attributes["attemptCount"])
	}
	count, err := strconv.Atoi(n.Value)
	if err != nil {
		return 0, fmt.Errorf("attempts: parse counter: %w", err)
	}
	return count, nil
}
