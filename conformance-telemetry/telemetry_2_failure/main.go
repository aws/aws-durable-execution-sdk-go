// Command telemetry_2_failure implements conformance requirement telemetry-2:
// a single-step execution whose step fails emits one FAILED Durable Execution
// Telemetry record to CloudWatch Logs.
//
// The plugin configuration is the same as telemetry_1_success. The handler
// constructs no record and calls no exporter method, so a missing or wrong
// record is the plugin's defect, not the handler's.
package main

import (
	"context"
	"errors"
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

// paymentError is the error the handler returns. The requirement expects
// the execution error message to be exactly "payment failed". An error
// built with fmt.Errorf("payment failed: %w", cause) would append the cause's
// message. So Error returns the fixed text, and Unwrap exposes the step
// error as the cause.
type paymentError struct {
	cause error
}

func (e *paymentError) Error() string { return "payment failed" }

func (e *paymentError) Unwrap() error { return e.cause }

// handler runs one step that fails on its first attempt and is not retried.
// The requirement's execution result and history sections check the messages
// "card declined" and "payment failed". Do not change them.
func handler(ctx durable.Context, _ Input) (string, error) {
	_, err := durable.Step(ctx, "charge", func(_ durable.StepContext) (string, error) {
		return "", errors.New("card declined")
	}, durable.WithRetry(durable.NoRetry()))
	if err != nil {
		return "", &paymentError{cause: err}
	}
	return "", nil
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
	})

	durable.Start(handler, durable.WithPlugins(plugin))
}
