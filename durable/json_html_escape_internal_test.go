package durable

import (
	"strings"
	"testing"
)

const htmlLiteral = `<b>a</b> & c >`

func assertHTMLLiteral(t *testing.T, where, got string) {
	t.Helper()
	for _, esc := range []string{`\u003c`, `\u003e`, `\u0026`} {
		if strings.Contains(got, esc) {
			t.Errorf("%s: output HTML-escapes %q: %s", where, esc, got)
		}
	}
	if !strings.Contains(got, htmlLiteral) {
		t.Errorf("%s: output does not keep %q literal: %s", where, htmlLiteral, got)
	}
}

// TestBatchSummaryRecordKeepsHTMLLiteral covers the batch summary record a
// batch stores when its full result does not fit a checkpoint, both the
// record with a fitting summary and the record whose summary is shortened.
func TestBatchSummaryRecordKeepsHTMLLiteral(t *testing.T) {
	base := batchSummaryRecord{
		Type: batchSummaryTypeMap, TotalCount: 1, SuccessCount: 1,
		CompletionReason: CompletionAllCompleted, Status: BatchItemSucceeded, ItemStatuses: "S",
	}
	t.Run("fits", func(t *testing.T) {
		record := base
		record.Summary = htmlLiteral
		b, err := marshalBatchSummaryRecord(record)
		if err != nil {
			t.Fatal(err)
		}
		assertHTMLLiteral(t, "summary record", string(b))
	})
	t.Run("shortened", func(t *testing.T) {
		record := base
		record.Summary = strings.Repeat(htmlLiteral, checkpointSizeLimitBytes/len(htmlLiteral)+1)
		b := marshalWithinLimit(t, record)
		assertHTMLLiteral(t, "shortened summary record", string(b))
	})
}

// TestChildErrorDataKeepsHTMLLiteral covers the error data a batch item
// failure stores.
func TestChildErrorDataKeepsHTMLLiteral(t *testing.T) {
	got := encodeChildErrorData(&StepError{ErrorType: "Error", Message: htmlLiteral, ErrorData: htmlLiteral})
	if got == nil {
		t.Fatal("no error data encoded")
	}
	assertHTMLLiteral(t, "child error data", *got)
}
