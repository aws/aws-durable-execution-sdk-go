// Command invoke_with_tenant_id implements conformance requirement 5-8:
// invoke a target function with a tenant-isolation configuration.
package main

import (
	"os"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type input struct {
	TenantID string `json:"tenantId"`
	Payload  any    `json:"payload"`
}

func handler(ctx durable.Context, event input) (any, error) {
	return durable.Invoke[any](ctx, "", os.Getenv("TARGET_FUNCTION_NAME"), event.Payload,
		durable.WithTenantID(event.TenantID))
}

func main() {
	durable.Start(handler)
}
