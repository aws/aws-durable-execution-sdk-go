# Handling Backend-Initiated Lifecycle Events (STOPPED, TIMED_OUT)

Workflow Insight's own `Plugin` runs **inside your Lambda function** - it
can only emit a record when the function is actually invoked. Two real,
permanent execution outcomes originate from the Lambda Durable Functions
**backend** instead, with **no accompanying Lambda invocation at all**:

- `STOPPED` - a durable execution was stopped via the backend's own
  manual-stop API.
- `TIMED_OUT` - a durable execution's configured `ExecutionTimeout`
  (`durable.Config`'s own execution-level timeout, distinct from a
  single Lambda invocation's own timeout) was exceeded.

Because neither of these ever triggers a Lambda invocation, there is
**no lifecycle hook** in `pkg/durable/plugin`'s own
`InstrumentationPlugin` interface that this (or any) plugin could
observe them through - `OnInvocationStart`/`OnInvocationEnd` are only
ever called when the Lambda runtime actually invokes the function, and
a `STOPPED`/`TIMED_OUT` outcome is recorded by the backend independently
of any invocation. This is a genuine, permanent architectural gap in an
in-process instrumentation plugin's own design, not a bug or a
missing-feature gap in this Go port specifically - it applies identically
to the JS/Python/Java reference SDKs' own equivalent Insight-style
plugins, and is documented as such in the JS reference SDK's own
`aws-durable-execution-sdk-js-insight` README, which this document
mirrors.

## The fix: a separate EventBridge subscription

Lambda emits durable-execution lifecycle state changes to EventBridge
independently of any SDK or plugin - subscribing to those events
directly, via a second, small Lambda function of your own, is the
correct way to get complete lifecycle coverage for a destination
Workflow Insight already writes to.

> **Note on the exact event shape below:** the `source` value
> (`aws.lambda`) and `detail-type` (`Lambda Durable Execution State
> Change`) shown here are taken from the JS reference SDK's own
> published documentation for this exact scenario, not independently
> re-verified against a live event in this Go port's own development
> session - if you don't see events matching this pattern once you set
> up the rule below, check the **EventBridge default event bus's own
> event history** (or CloudTrail) for the actual `source`/`detail-type`
> your account's Lambda service is really emitting, and adjust the rule
> accordingly. The underlying MECHANISM (Lambda emits durable-execution
> lifecycle events to the default EventBridge bus; you subscribe with a
> rule; a small Lambda function updates your own destination) is real
> and backend-level, independent of any SDK language - only the exact
> field names shown are carried over from the JS SDK's own docs without
> independent re-verification here.

### Step 1: write a handler that updates your destination

```go
package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-lambda-go/lambda"
)

// LifecycleEventDetail mirrors the JS SDK's own documented event detail
// shape for this scenario - see this file's own top-level note on why
// the exact field names here are inherited from that documentation, not
// independently re-verified in this Go port.
type LifecycleEventDetail struct {
	ExecutionARN string `json:"executionArn"`
	Status       string `json:"status"` // "STOPPED" | "TIMED_OUT"
	Timestamp    string `json:"timestamp"`
}

type LifecycleEvent struct {
	Detail LifecycleEventDetail `json:"detail"`
}

var (
	ddbClient *dynamodb.Client
	tableName = "workflow-insight" // matches insight.DynamoDBExporter's own default TableName
)

func init() {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		panic(err)
	}
	ddbClient = dynamodb.NewFromConfig(cfg)
}

func handler(ctx context.Context, event LifecycleEvent) error {
	_, err := ddbClient.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: event.Detail.ExecutionARN},
		},
		UpdateExpression: aws.String("SET #s = :status, end_time = :ts"),
		ExpressionAttributeNames: map[string]string{
			"#s": "status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":status": &types.AttributeValueMemberS{Value: event.Detail.Status},
			":ts":     &types.AttributeValueMemberS{Value: event.Detail.Timestamp},
		},
	})
	if err != nil {
		return fmt.Errorf("updating insight record for %s: %w", event.Detail.ExecutionARN, err)
	}
	return nil
}

func main() {
	lambda.Start(handler)
}
```

This example targets `insight.DynamoDBExporter`'s own default table
shape (see `exporter_dynamodb.go`'s own doc) - the same pattern applies
to any other exporter's own destination: for `S3Exporter`, overwrite the
execution's own JSON object at its existing key; for
`AuroraExporter`/`RedshiftExporter`, run an `UPDATE` on the `status` and
`end_time` columns of the row already keyed by `execution_arn`.

### Step 2: create the EventBridge rule

```bash
aws events put-rule \
  --name durable-execution-terminal-events \
  --event-pattern '{
    "source": ["aws.lambda"],
    "detail-type": ["Lambda Durable Execution State Change"],
    "detail": {
      "status": ["STOPPED", "TIMED_OUT"]
    }
  }'
```

### Step 3: point the rule at your handler

```bash
aws events put-targets \
  --rule durable-execution-terminal-events \
  --targets Id=1,Arn=arn:aws:lambda:us-east-1:123456789012:function:lifecycle-handler
```

Grant EventBridge permission to invoke it:

```bash
aws lambda add-permission \
  --function-name lifecycle-handler \
  --statement-id eventbridge-lifecycle \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn arn:aws:events:us-east-1:123456789012:rule/durable-execution-terminal-events
```

> **Security note:** the `add-permission` call above is scoped to
> exactly one EventBridge rule ARN as its `--source-arn` (least
> privilege - only that specific rule may invoke this function), and the
> `UpdateItem`/equivalent call in Step 1's handler needs only the same
> narrow write permission on the SAME table/bucket/cluster Workflow
> Insight's own exporter already writes to. Review both against your
> own account's actual IAM policies before deploying to a production
> account - this is a new, real IAM grant and a new, real Lambda
> function, both genuinely deployed resources, not a local-only change.

## What each status means

| Status | Origin | Captured by the Plugin? | Captured by this EventBridge rule? |
|---|---|---|---|
| `RUNNING` | Lambda invocation (including a wait/suspend for a timer or external event) | Yes, under `EmitModeOnChange` | No |
| `SUCCEEDED` | Lambda invocation | Yes | Yes |
| `FAILED` | Lambda invocation | Yes | Yes |
| `STOPPED` | Backend (manual stop) | No | Yes |
| `TIMED_OUT` | Backend (`ExecutionTimeout` exceeded) | No | Yes |

If you also configure `insight.EventBridgeExporter` as one of your own
`Config.Exporters`, its own plugin-emitted events (`SUCCEEDED`/`FAILED`,
under `Source: insight.DefaultEventBridgeSource`) and this separate
rule's own backend-emitted events (`STOPPED`/`TIMED_OUT`, under
`Source: "aws.lambda"`) can share the same downstream consumer - just
make sure any single EventBridge rule you write matches on `source`
specifically if you need to distinguish which of the two actually
produced a given event, since the two use different `source` values by
construction (`EventBridgeExporter`'s own `Source` is caller-configured;
this backend-level event's own `source` is fixed by the Lambda service
itself, not by anything this SDK or plugin controls).
