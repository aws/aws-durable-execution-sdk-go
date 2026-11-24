// Command telemetry_4_full_tree_map implements conformance requirement
// telemetry-4: with full-tree operation detail, the record carries a map, its
// named iteration contexts, and the step inside each iteration.
//
// The handler constructs no record and calls no exporter method, so a missing
// or wrong record is the plugin's defect, not the handler's.
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/telemetry"
)

// handler maps over the input items one at a time. Each iteration is named
// item-<item> and runs one step named greet-<item>. The requirement keys the
// record's operations by these names. Do not change them.
func handler(ctx durable.Context, items []int) ([]string, error) {
	result, err := durable.Map(ctx, "process-items", items,
		func(itemCtx durable.Context, item int, _ int) (string, error) {
			return durable.Step(itemCtx, fmt.Sprintf("greet-%d", item), func(_ durable.StepContext) (string, error) {
				return fmt.Sprintf("Hello, %d!", item), nil
			})
		},
		durable.WithMaxConcurrency(1),
		durable.WithItemNamer(func(i int) string {
			return fmt.Sprintf("item-%d", items[i])
		}),
	)
	if err != nil {
		return nil, err
	}
	return result.Results(), nil
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
	// OperationDetailFullTree keeps the operations that have a parent: the
	// two iteration contexts and the step inside each one.
	plugin := telemetry.New(telemetry.Config{
		EmitMode:        telemetry.EmitOnComplete,
		OperationDetail: telemetry.OperationDetailFullTree,
		SamplingRate:    1,
		Exporters:       []telemetry.Exporter{exporter},
	})

	durable.Start(handler, durable.WithPlugins(plugin))
}
