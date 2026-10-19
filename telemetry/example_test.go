package telemetry_test

import (
	"fmt"

	"github.com/aws/aws-durable-execution-sdk-go/telemetry"
)

// ExampleNew demonstrates creating a telemetry plugin with default
// configuration: emits on completion, 100% sampling, stdout JSON via
// LambdaLogExporter.
func ExampleNew() {
	plugin := telemetry.New(telemetry.Config{})

	// Register with durable.Start via durable.WithPlugins(plugin.Plugin())
	p := plugin.Plugin()
	fmt.Println(p.OnInvocationStart != nil)
	// Output:
	// true
}

// ExampleNew_withS3Exporter demonstrates configuring an S3 exporter with
// date-based partitioning.
func ExampleNew_withS3Exporter() {
	// In production, use a real *s3.Client from aws-sdk-go-v2/service/s3.
	// nil is used here to show the configuration shape without making
	// AWS calls.
	exporter := &telemetry.S3Exporter{
		API:          nil, // replace with s3.NewFromConfig(cfg)
		Bucket:       "my-telemetry-bucket",
		Prefix:       "durable-execution-telemetry/",
		Partitioning: telemetry.S3PartitioningDate,
	}

	plugin := telemetry.New(telemetry.Config{
		Exporters: []telemetry.Exporter{exporter},
	})

	p := plugin.Plugin()
	fmt.Println(p.OnInvocationEnd != nil)
	// Output:
	// true
}

// ExampleNew_withMultipleExporters demonstrates combining a file exporter
// (for local debugging) with an S3 exporter (for durable storage).
func ExampleNew_withMultipleExporters() {
	fileExp := telemetry.NewFileExporter(telemetry.FileExporterConfig{
		Dir: "/tmp/telemetry-records",
	})

	s3Exp := &telemetry.S3Exporter{
		API:    nil, // replace with s3.NewFromConfig(cfg)
		Bucket: "my-telemetry-bucket",
	}

	plugin := telemetry.New(telemetry.Config{
		Exporters: []telemetry.Exporter{fileExp, s3Exp},
	})

	p := plugin.Plugin()
	fmt.Println(p.OnOperationStart != nil)
	// Output:
	// true
}

// ExampleNew_withSampling demonstrates configuring the plugin to emit
// records for only 10% of executions. The sampling decision is
// deterministic per execution ARN.
func ExampleNew_withSampling() {
	plugin := telemetry.New(telemetry.Config{
		SamplingRate: 0.1, // 10% of executions
		EmitMode:     telemetry.EmitOnComplete,
	})

	p := plugin.Plugin()
	fmt.Println(p.OnInvocationStart != nil)
	// Output:
	// true
}

// ExampleNew_withContentConfig demonstrates filtering record content to
// exclude operation results and apply a custom redactor.
func ExampleNew_withContentConfig() {
	plugin := telemetry.New(telemetry.Config{
		Content: telemetry.ContentConfig{
			IncludeInput:            true,
			IncludeOutput:           true,
			IncludeOperationResults: false, // omit operation results
			IncludeOperationErrors:  true,
			IncludeExecutionError:   true,
			MaxContentLength:        4096, // 4 KB max per content field
			Redactor: func(s string) string {
				// Replace sensitive patterns before storing
				return s
			},
		},
	})

	p := plugin.Plugin()
	fmt.Println(p.OnOperationEnd != nil)
	// Output:
	// true
}
