// Command telemetry_3_result_select implements conformance requirement
// telemetry-3: the record carries exactly one selected key of a step result.
//
// The handler constructs no record and calls no exporter method, so a missing
// or wrong record is the plugin's defect, not the handler's.
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
	UserID string `json:"userId"`
}

// User is the step result and the handler result.
type User struct {
	UserID string `json:"userId"`
	Email  string `json:"email"`
	Token  string `json:"token"`
}

// handler returns the step result unchanged. The execution result must carry
// all three keys, so the run shows that the telemetry transform does not
// alter the workflow value.
func handler(ctx durable.Context, in Input) (User, error) {
	return durable.Step(ctx, "fetch-user", func(_ durable.StepContext) (User, error) {
		return User{UserID: in.UserID, Email: "test@example.com", Token: "tok-456"}, nil
	})
}

// selectUserID is the result transform for the fetch-user step. The plugin
// passes the checkpointed result decoded as a JSON value, so a JSON object
// arrives as a map[string]any. The transform reads userId from that map and
// unwraps nothing. If the plugin passed any other shape, the lookup on the
// nil map would yield nil, the record would carry {"userId": null}, and the
// requirement's exact match would fail.
func selectUserID(result any) any {
	object, _ := result.(map[string]any)
	return map[string]any{"userId": object["userId"]}
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
	// Execution input and output are excluded. So the step result can reach
	// the record only through the fetch-user override.
	plugin := telemetry.New(telemetry.Config{
		EmitMode:        telemetry.EmitOnComplete,
		OperationDetail: telemetry.OperationDetailTopLevel,
		SamplingRate:    1,
		Exporters:       []telemetry.Exporter{exporter},
		Content: telemetry.ContentConfig{
			ExcludeInput:  true,
			ExcludeOutput: true,
			Operations: telemetry.ContentOperations{
				Overrides: []telemetry.OperationOverride{
					{OperationName: "fetch-user", Result: selectUserID},
				},
			},
		},
	})

	durable.Start(handler, durable.WithPlugins(plugin))
}
