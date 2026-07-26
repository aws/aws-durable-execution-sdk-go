// Command insight-plugin demonstrates configuring and registering the
// Workflow Insight plugin via [insight.New] and [durable.WithPlugins]. The
// plugin observes execution lifecycle events and emits structured records
// to configured exporters.
package main

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/insight"
)

// newInsightPlugin creates an InsightPlugin configured for this example.
// When called with no exporters it uses the default (LambdaLogExporter);
// tests may pass a capturing exporter to observe emitted records.
func newInsightPlugin(exporters ...insight.Exporter) *insight.InsightPlugin {
	cfg := insight.Config{
		EmitMode:        insight.EmitOnComplete,
		OperationDetail: insight.OperationDetailTopLevel,
		SamplingRate:    1.0,
	}
	if len(exporters) > 0 {
		cfg.Exporters = exporters
	}
	return insight.New(cfg)
}

// insightPlugin is the configured Workflow Insight plugin instance.
var insightPlugin = newInsightPlugin()

// Output is the handler result.
type Output struct {
	Message string `json:"message"`
	OrderID string `json:"orderId"`
}

func handler(ctx durable.Context, input map[string]string) (Output, error) {
	orderID := input["orderId"]
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

func main() { durable.Start(handler, durable.WithPlugins(insightPlugin.Plugin())) }
