// Command invoke-tenant-id demonstrates [durable.Invoke] with
// [durable.WithTenantID] for tenant-isolated function invocation.
package main

import (
	"encoding/json"
	"os"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Input carries the target function, tenant identifier, and payload.
type Input struct {
	FunctionName string          `json:"functionName"`
	TenantID     string          `json:"tenantId"`
	Payload      json.RawMessage `json:"payload"`
}

func handler(ctx durable.Context, event Input) (json.RawMessage, error) {
	functionName := event.FunctionName
	if functionName == "" {
		prefix := os.Getenv("FUNCTION_NAME_PREFIX")
		if prefix == "" {
			prefix = "v2-"
		}
		functionName = prefix + "go-invoke-tenant-target-v2:$LATEST"
	}
	tenantID := event.TenantID
	if tenantID == "" {
		tenantID = "tenant-001"
	}
	return durable.Invoke[json.RawMessage](ctx, "invoke-tenant",
		functionName, event.Payload,
		durable.WithTenantID(tenantID))
}

func main() { durable.Start(handler) }
