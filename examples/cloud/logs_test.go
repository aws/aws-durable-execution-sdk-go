// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build cloud

package cloud

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
)

// logSettleTimeout bounds how long assertLogLines waits for the records
// of an execution to reach the log group.
const logSettleTimeout = 3 * time.Minute

// logSettleQuiet is how long the counts must stay the same, once every
// expected line has arrived, before they are compared.
const logSettleQuiet = 20 * time.Second

// assertLogLines checks that the execution arn of functionName wrote each
// message in want exactly as many times as want says, counting the
// records without the replay attribute, and wrote no other message.
// Records reach CloudWatch Logs some seconds after they are written, so
// the counts are read until they stop changing.
func assertLogLines(ctx context.Context, t *testing.T, logs *cloudwatchlogs.Client, functionName, arn string, want map[string]int) {
	t.Helper()
	group := "/aws/lambda/" + functionName
	deadline := time.Now().Add(logSettleTimeout)
	var got map[string]int
	var stableSince time.Time
	for {
		counts, err := countExecutionLines(ctx, logs, group, arn)
		if err != nil {
			t.Fatalf("read log records of %s: %v", arn, err)
		}
		arrived := true
		for msg, n := range want {
			if counts[msg] < n {
				arrived = false
			}
		}
		switch {
		case !maps.Equal(counts, got):
			got, stableSince = counts, time.Now()
		case arrived && time.Since(stableSince) >= logSettleQuiet:
			if !maps.Equal(got, want) {
				t.Fatalf("log lines of %s = %v, want %v", arn, got, want)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("log lines of %s = %v after %s, want %v", arn, got, logSettleTimeout, want)
		}
		time.Sleep(5 * time.Second)
	}
}

// countExecutionLines counts, per message, the records in group that the
// SDK logger wrote for the execution arn without the replay attribute.
func countExecutionLines(ctx context.Context, logs *cloudwatchlogs.Client, group, arn string) (map[string]int, error) {
	counts := map[string]int{}
	in := &cloudwatchlogs.FilterLogEventsInput{
		LogGroupName:  aws.String(group),
		FilterPattern: aws.String(fmt.Sprintf("%q", arn)),
		StartTime:     aws.Int64(time.Now().Add(-2 * executionTimeout).UnixMilli()),
	}
	p := cloudwatchlogs.NewFilterLogEventsPaginator(logs, in)
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, ev := range page.Events {
			var rec struct {
				Message      string `json:"message"`
				ExecutionArn string `json:"executionArn"`
				Replay       *bool  `json:"replay"`
			}
			if json.Unmarshal([]byte(aws.ToString(ev.Message)), &rec) != nil {
				continue
			}
			if rec.ExecutionArn != arn || rec.Replay != nil {
				continue
			}
			counts[rec.Message]++
		}
	}
	return counts, nil
}
