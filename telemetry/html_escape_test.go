package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// htmlText holds the three characters encoding/json escapes by default.
const htmlText = "<a&b>"

// htmlTestRecord is a record whose input, output, operation result, and
// both error messages contain htmlText.
func htmlTestRecord() Record {
	rec := contentTestRecord()
	rec.Input = json.RawMessage(`{"in":"<a&b>"}`)
	rec.Output = json.RawMessage(`"<a&b>"`)
	rec.Error = &ErrorRecord{Type: "E", Message: htmlText}
	rec.Operations[0].Result = json.RawMessage(`{"res":"<a&b>"}`)
	rec.Operations[0].Error = &ErrorRecord{Type: "E", Message: htmlText}
	return rec
}

// assertHTMLLiteral checks that body is valid JSON, carries htmlText
// literally the wanted number of times, and has no \u003c, \u003e or
// \u0026 escape.
func assertHTMLLiteral(t *testing.T, body []byte, want int) {
	t.Helper()
	if !json.Valid(bytes.TrimSpace(body)) {
		t.Fatalf("body is not valid JSON:\n%s", body)
	}
	if got := strings.Count(string(body), htmlText); got != want {
		t.Errorf("body has %d literal %q, want %d:\n%s", got, htmlText, want, body)
	}
	for _, esc := range []string{`\u003c`, `\u003e`, `\u0026`} {
		if strings.Contains(string(body), esc) {
			t.Errorf("body contains the escape %s:\n%s", esc, body)
		}
	}
}

// TestContentValues_KeepHTMLCharactersLiteral covers the content fields
// the plugin encodes itself: the typed input and output, with and without
// a transform, and an operation result.
func TestContentValues_KeepHTMLCharactersLiteral(t *testing.T) {
	identity := func(v any) any { return v }
	value := map[string]string{"k": htmlText}
	const want = `{"k":"<a&b>"}`

	plain := ContentConfig{}
	if got := plain.inputValue(value); string(got) != want {
		t.Errorf("inputValue without a transform = %s, want %s", got, want)
	}
	if got := plain.outputValue(value); string(got) != want {
		t.Errorf("outputValue without a transform = %s, want %s", got, want)
	}

	transformed := ContentConfig{
		InputTransform:  identity,
		OutputTransform: identity,
		Operations: ContentOperations{
			Overrides: []OperationOverride{{OperationName: "step", Result: identity}},
		},
	}
	if got := transformed.inputValue(value); string(got) != want {
		t.Errorf("inputValue with a transform = %s, want %s", got, want)
	}
	if got := transformed.outputValue(value); string(got) != want {
		t.Errorf("outputValue with a transform = %s, want %s", got, want)
	}
	if got := transformed.resultValue("step", want); string(got) != want {
		t.Errorf("resultValue = %s, want %s", got, want)
	}
	// Text that is not valid JSON reaches the transform as a string.
	if got := transformed.resultValue("step", htmlText); string(got) != `"<a&b>"` {
		t.Errorf("resultValue of non-JSON text = %s, want %q", got, `"<a&b>"`)
	}
}

// TestRenderers_KeepHTMLCharactersLiteral covers RenderRecord in every
// format and the default Truncate renderer. The by-name summary of a
// single occurrence carries the result and the error, so every format
// holds htmlText five times; OperationsFormatBoth holds the operation's
// two values twice.
func TestRenderers_KeepHTMLCharactersLiteral(t *testing.T) {
	rec := htmlTestRecord()
	for _, tt := range []struct {
		format OperationsFormat
		want   int
	}{
		{"", 5},
		{OperationsFormatArray, 5},
		{OperationsFormatByName, 5},
		{OperationsFormatBoth, 7},
	} {
		t.Run("RenderRecord/"+string(tt.format), func(t *testing.T) {
			body, err := RenderRecord(rec, tt.format)
			if err != nil {
				t.Fatalf("RenderRecord: %v", err)
			}
			assertHTMLLiteral(t, body, tt.want)
		})
	}
	t.Run("defaultRenderer", func(t *testing.T) {
		body, err := defaultRenderer(rec)
		if err != nil {
			t.Fatalf("defaultRenderer: %v", err)
		}
		assertHTMLLiteral(t, body, 5)
	})
}

// TestExporters_KeepHTMLCharactersLiteral covers every exporter that
// serializes the record itself, plus SQS for the exporters that go
// through RenderRecord.
func TestExporters_KeepHTMLCharactersLiteral(t *testing.T) {
	ctx := context.Background()
	rec := htmlTestRecord()

	t.Run("LambdaLog", func(t *testing.T) {
		var buf bytes.Buffer
		if err := (&LambdaLogExporter{Writer: &buf}).Export(ctx, rec); err != nil {
			t.Fatalf("Export: %v", err)
		}
		assertHTMLLiteral(t, buf.Bytes(), 5)
	})
	t.Run("CloudWatchLogs", func(t *testing.T) {
		fake := &fakeCloudWatchLogsAPI{}
		exp := &CloudWatchLogsExporter{API: fake, LogGroupName: "/test/telemetry"}
		if err := exp.Export(ctx, rec); err != nil {
			t.Fatalf("Export: %v", err)
		}
		assertHTMLLiteral(t, []byte(aws.ToString(fake.putEventsCalls[0].LogEvents[0].Message)), 5)
	})
	t.Run("S3", func(t *testing.T) {
		fake := &fakeS3API{}
		if err := (&S3Exporter{API: fake, Bucket: "b"}).Export(ctx, rec); err != nil {
			t.Fatalf("Export: %v", err)
		}
		assertHTMLLiteral(t, fake.lastBody, 5)
	})
	t.Run("DynamoDB", func(t *testing.T) {
		fake := &fakeDynamoDBAPI{}
		if err := (&DynamoDBExporter{API: fake, TableName: "t"}).Export(ctx, rec); err != nil {
			t.Fatalf("Export: %v", err)
		}
		assertHTMLLiteral(t, []byte(attrS(t, fake.calls[0].Item, "recordJson")), 5)
	})
	t.Run("RDS", func(t *testing.T) {
		fake := &fakeRDSDataAPI{}
		exp := &RDSExporter{API: fake, ResourceArn: "arn:r", SecretArn: "arn:s", Database: "d"}
		if err := exp.Export(ctx, rec); err != nil {
			t.Fatalf("Export: %v", err)
		}
		assertHTMLLiteral(t, []byte(rdsParamValue(t, fake.calls[0].Parameters, "json")), 5)
	})
	t.Run("Redshift", func(t *testing.T) {
		fake := &fakeRedshiftDataAPI{}
		exp := &RedshiftExporter{API: fake, WorkgroupName: "w", Database: "d"}
		if err := exp.Export(ctx, rec); err != nil {
			t.Fatalf("Export: %v", err)
		}
		assertHTMLLiteral(t, []byte(rsParamValue(t, fake.calls[0].Parameters, "json")), 5)
	})
	t.Run("OpenSearch", func(t *testing.T) {
		var body []byte
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusCreated)
		}))
		defer srv.Close()
		exp := &OpenSearchExporter{Client: srv.Client(), Endpoint: srv.URL, Username: "u", Password: "p"}
		if err := exp.Export(ctx, rec); err != nil {
			t.Fatalf("Export: %v", err)
		}
		assertHTMLLiteral(t, body, 5)
	})
	t.Run("SQS", func(t *testing.T) {
		fake := &fakeSQSAPI{}
		if err := (&SQSExporter{API: fake, QueueURL: "https://sqs.example.com/q"}).Export(ctx, rec); err != nil {
			t.Fatalf("Export: %v", err)
		}
		assertHTMLLiteral(t, []byte(aws.ToString(fake.calls[0].MessageBody)), 5)
	})
}
