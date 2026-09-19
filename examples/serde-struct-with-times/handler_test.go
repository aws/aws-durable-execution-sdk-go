// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
	"github.com/aws/aws-durable-execution-sdk-go/examples/internal/extest"
)

// stepPayload returns the checkpointed result of the named succeeded step.
func stepPayload(t *testing.T, result *durabletest.TestResult, name string) string {
	t.Helper()
	op := result.Operation(name)
	if op == nil || op.StepDetails == nil {
		t.Fatalf("%s step not found", name)
	}
	if op.Status != "SUCCEEDED" {
		t.Fatalf("%s status = %s, want SUCCEEDED", name, op.Status)
	}
	return op.StepDetails.Result
}

func TestHandler(t *testing.T) {
	runner := durabletest.NewLocalRunner(handler)
	result := runner.RunUntilComplete(t, event{Title: "Durable Functions 101"})

	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s (%v)", result.Status, result.Error)
	}
	// The wait suspends the first invocation; the second decodes both
	// articles from their checkpoints.
	if got := len(result.Invocations); got != 2 {
		t.Fatalf("expected 2 invocations, got %d", got)
	}

	out, err := durabletest.ResultAs[output](result)
	if err != nil {
		t.Fatalf("deserialize result: %v", err)
	}
	want := inspection{
		Title:           "Durable Functions 101",
		CreatedAt:       "2020-01-01T00:00:00Z",
		PublishedAt:     "2020-01-02T00:00:00Z",
		IsPublished:     true,
		AgeHours:        48,
		ArchivedAtIsNil: true,
		EqualsOriginal:  true,
	}
	if out.DefaultSerdes != want {
		t.Errorf("defaultSerdes = %+v, want %+v", out.DefaultSerdes, want)
	}
	if out.EpochSerdes != want {
		t.Errorf("epochSerdes = %+v, want %+v", out.EpochSerdes, want)
	}

	// The two serdes produce different checkpoint payloads for the same
	// value. The default writes RFC 3339 strings and a JSON null for the
	// nil pointer.
	var byDefault struct {
		CreatedAt  string `json:"createdAt"`
		Metadata   struct{ PublishedAt string }
		ArchivedAt *string `json:"archivedAt"`
	}
	payload := stepPayload(t, result, "create-article")
	if err := json.Unmarshal([]byte(payload), &byDefault); err != nil {
		t.Fatalf("decode default payload %q: %v", payload, err)
	}
	if byDefault.CreatedAt != "2020-01-01T00:00:00Z" || byDefault.Metadata.PublishedAt != "2020-01-02T00:00:00Z" {
		t.Errorf("default payload timestamps = %q / %q, want RFC 3339 strings", byDefault.CreatedAt, byDefault.Metadata.PublishedAt)
	}
	if byDefault.ArchivedAt != nil {
		t.Errorf("default payload archivedAt = %q, want null", *byDefault.ArchivedAt)
	}

	// The epoch serdes writes millisecond counts instead.
	var byEpoch articleWire
	payload = stepPayload(t, result, "create-article-epoch")
	if err := json.Unmarshal([]byte(payload), &byEpoch); err != nil {
		t.Fatalf("decode epoch payload %q: %v", payload, err)
	}
	if byEpoch.CreatedAtMs != createdAt.UnixMilli() || byEpoch.PublishedAtMs != publishedAt.UnixMilli() {
		t.Errorf("epoch payload = %+v, want %d / %d", byEpoch, createdAt.UnixMilli(), publishedAt.UnixMilli())
	}
	if byEpoch.ArchivedAtMs != nil {
		t.Errorf("epoch payload archivedAtMs = %d, want null", *byEpoch.ArchivedAtMs)
	}

	// Every operation runs sequentially on the handler goroutine, so the
	// sequence is deterministic.
	extest.AssertSignature(t, result, extest.Ordered)
}

func TestEpochSerdesKeepsArchivedAt(t *testing.T) {
	archived := time.Date(2021, time.June, 1, 12, 30, 0, 0, time.UTC)
	in := newArticle("archived")
	in.ArchivedAt = &archived

	data, err := epochSerdes.Marshal(t.Context(), durable.SerdesContext{}, in)
	if err != nil {
		t.Fatal(err)
	}
	var out Article
	if err := epochSerdes.Unmarshal(t.Context(), durable.SerdesContext{}, data, &out); err != nil {
		t.Fatal(err)
	}
	if out.ArchivedAt == nil || !out.ArchivedAt.Equal(archived) {
		t.Fatalf("ArchivedAt = %v, want %v", out.ArchivedAt, archived)
	}
	if !out.CreatedAt.Equal(in.CreatedAt) || !out.Metadata.PublishedAt.Equal(in.Metadata.PublishedAt) {
		t.Fatalf("round trip changed timestamps: %+v", out)
	}
}
