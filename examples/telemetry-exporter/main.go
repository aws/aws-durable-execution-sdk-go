// Command telemetry-exporter demonstrates configuring and registering the
// Durable Execution Telemetry plugin via [telemetry.New] and
// [durable.WithPlugins]. The plugin observes execution lifecycle events and emits structured records
// to configured exporters.
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/telemetry"
)

// newTelemetryPlugin creates a TelemetryPlugin configured for this example.
// When called with no exporters it uses the default (LambdaLogExporter);
// tests may pass a capturing exporter to observe emitted records.
func newTelemetryPlugin(exporters ...telemetry.Exporter) *telemetry.TelemetryPlugin {
	cfg := telemetry.Config{
		EmitMode:        telemetry.EmitOnComplete,
		OperationDetail: telemetry.OperationDetailTopLevel,
		SamplingRate:    1.0,
	}
	if len(exporters) > 0 {
		cfg.Exporters = exporters
	}
	return telemetry.New(cfg)
}

// telemetryPlugin is the configured Durable Execution Telemetry plugin
// instance.
var telemetryPlugin = newTelemetryPlugin()

// Output is the handler result.
type Output struct {
	Message string `json:"message"`
	OrderID string `json:"orderId"`
}

func handler(ctx durable.Context, input map[string]any) (Output, error) {
	orderID, _ := input["orderId"].(string)
	if orderID == "" {
		orderID = "unknown"
	}

	msg, err := durable.Step(ctx, "process-order", func(_ durable.StepContext) (string, error) {
		return fmt.Sprintf("processed order %s", orderID), nil
	})
	if err != nil {
		return Output{}, fmt.Errorf("process-order step failed: %w", err)
	}

	return Output{Message: msg, OrderID: orderID}, nil
}

func main() { durable.Start(handler, durable.WithPlugins(telemetryPlugin.Plugin())) }
