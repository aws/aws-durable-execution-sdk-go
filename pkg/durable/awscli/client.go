// Package awscli provides a checkpoint.Client implementation that shells
// out to the AWS CLI (`aws lambda checkpoint-durable-execution`) instead
// of using the AWS SDK for Go v2 directly.
//
// # Why this exists
//
// This is a stopgap, not the intended production implementation - see
// docs/checkpoint-replay-design.md. Two alternatives were tried before
// this one:
//
//  1. AWS SDK for Go v2: cannot be fetched, no outbound network access to
//     the Go module proxy in this development environment.
//  2. Hand-rolled SigV4 signing (see pkg/durable/sigv4lambda): built and
//     tested against a real deployed Lambda, but produces a "signature
//     we calculated does not match" 403 - a subtle, currently
//     unresolved bug in the canonical-request construction. Not worth
//     further debugging time for a component this SDK will replace with
//     a real SDK client anyway; abandoning it in favor of this CLI shim
//     to unblock verifying the SDK's actual logic (step execution,
//     checkpointing sequence, output envelope) against the real backend.
//
// Shelling out to the CLI has real costs a production implementation
// must not have: it requires the `aws` binary installed in the Lambda
// container image, adds a process-spawn per API call, and depends on the
// CLI's own credential resolution matching the Lambda execution role
// (works via the standard AWS_* environment variables the Lambda runtime
// sets, which the CLI reads automatically).
package awscli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"

	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// Client shells out to the AWS CLI for each checkpoint call.
type Client struct {
	Region string
}

// cliCheckpointUpdate/cliOperationError/etc. mirror the CLI's --updates
// JSON syntax exactly (see `aws lambda checkpoint-durable-execution
// help`), which uses the same field names as types.OperationUpdate/
// types.ErrorObject, so this is effectively a direct re-encode rather
// than a shape translation - kept as distinct types only so this
// package's JSON tags are independent of types.go's.
func toCLIUpdates(updates []types.OperationUpdate) []byte {
	b, _ := json.Marshal(updates)
	return b
}

type cliCheckpointOutput struct {
	CheckpointToken   string `json:"CheckpointToken"`
	NewExecutionState *struct {
		Operations []types.Operation `json:"Operations"`
	} `json:"NewExecutionState,omitempty"`
}

// Checkpoint shells out to `aws lambda checkpoint-durable-execution`.
func (c *Client) Checkpoint(ctx context.Context, req types.CheckpointDurableExecutionRequest) (*types.CheckpointDurableExecutionResponse, error) {
	args := []string{"lambda", "checkpoint-durable-execution",
		"--durable-execution-arn", req.DurableExecutionArn,
		"--checkpoint-token", req.CheckpointToken,
		"--output", "json",
	}
	if c.Region != "" {
		args = append(args, "--region", c.Region)
	}
	if len(req.Updates) > 0 {
		args = append(args, "--updates", string(toCLIUpdates(req.Updates)))
	}
	if req.ClientToken != nil {
		args = append(args, "--client-token", *req.ClientToken)
	}

	out, err := runAWSCLI(ctx, args)
	if err != nil {
		return nil, fmt.Errorf("awscli.Client.Checkpoint: %w", err)
	}

	var parsed cliCheckpointOutput
	if err := json.Unmarshal(out, &parsed); err != nil {
		return nil, fmt.Errorf("awscli.Client.Checkpoint: parsing CLI output %q: %w", string(out), err)
	}

	resp := &types.CheckpointDurableExecutionResponse{}
	if parsed.CheckpointToken != "" {
		resp.NextCheckpointToken = &parsed.CheckpointToken
	}
	if parsed.NewExecutionState != nil {
		resp.UpdatedOperations = parsed.NewExecutionState.Operations
	}
	return resp, nil
}

func runAWSCLI(ctx context.Context, args []string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "aws", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("aws %v: %w (stderr: %s)", args, err, stderr.String())
	}
	return stdout.Bytes(), nil
}
