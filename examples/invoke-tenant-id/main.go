// Command invoke-tenant-id demonstrates [durable.Invoke] with
// [durable.WithTenantID] for tenant-isolated function invocation.
package main

import (
	"encoding/json"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Input carries the target function, tenant identifier, and payload.
type Input struct {
	FunctionName string          `json:"functionName"`
	TenantID     string          `json:"tenantId"`
	Payload      json.RawMessage `json:"payload"`
}

func handler(ctx durable.Context, event Input) (json.RawMessage, error) {
	return durable.Invoke[json.RawMessage](ctx, "invoke-tenant",
		event.FunctionName, event.Payload,
		durable.WithTenantID(event.TenantID))
}

func main() { durable.Start(handler) }
