// Command telemetry_1_success implements conformance requirement telemetry-1:
// a successful single-step execution emits one Durable Execution Telemetry
// record to CloudWatch Logs.
//
// The handler configures the telemetry plugin with one
// [telemetry.CloudWatchLogsExporter]. It constructs no record and calls no
// exporter method. Delivery relies on the plugin, so a missing or wrong record
// is the plugin's defect, not the handler's.
package main

import (
	"context"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/telemetry"
)

// Input is the handler event.
type Input struct {
	Value int `json:"value"`
}

// Output is the handler result.
type Output struct {
	Value int `json:"value"`
}

func handler(ctx durable.Context, in Input) (Output, error) {
	v, err := durable.Step(ctx, "increment", func(_ durable.StepContext) (int, error) {
		return in.Value + 1, nil
	})
	if err != nil {
		return Output{}, err
	}
	return Output{Value: v}, nil
}

func main() {
	name := os.Getenv("TELEMETRY_LOG_GROUP_NAME")
	if name == "" {
		log.Fatal("TELEMETRY_LOG_GROUP_NAME must be set")
	}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("load AWS config: %v", err)
	}

	exporter := &telemetry.CloudWatchLogsExporter{
		API:          cloudwatchlogs.NewFromConfig(cfg),
		LogGroupName: name,
	}
	plugin := telemetry.New(telemetry.Config{
		EmitMode:        telemetry.EmitOnComplete,
		OperationDetail: telemetry.OperationDetailTopLevel,
		SamplingRate:    1,
		Exporters:       []telemetry.Exporter{exporter},
		Content:         telemetry.ContentConfig{IncludeInput: true, IncludeOutput: true},
	})

	durable.Start(handler, durable.WithPlugins(plugin.Plugin()))
}
