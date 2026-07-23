// Package main's handler.go isolates this example's business logic from
// main.go's Lambda Runtime API plumbing, so it can be exercised by
// handler_test.go via the SDK's testing.LocalTestRunner without needing a
// real Lambda execution environment - mirroring this repo's other
// examples' handler.go/handler_test.go split.
//
// This example closes docs/ts-sdk-examples-comparison.md's gap 6/8
// (large-payload/Serdes-overflow): "no Go example demonstrates even the
// conservative behavior" *operations.ResultTooLargeError* provides.
//
// # Context this example is built on (see errors.go's own extensive doc
// for the full research)
//
// docs/remaining-work.md §6 task 16 found that the automatic
// backend-offload mechanism this SDK's own comparison table originally
// assumed existed (mirroring what the JS/Java columns implied) does NOT
// exist on the real wire shape - neither the official
// CheckpointDurableExecution API Reference nor the AWS SDK for Go v2's
// independently-generated Lambda types model any S3/pointer/offload
// field on OperationUpdate/Operation. The REAL, confirmed pattern (the
// official Developer Guide's "Manage state"/"Serialization" pages) is
// client-side and OPT-IN: an application stages large data itself in
// external storage (S3, DynamoDB) and returns only a small reference, or
// supplies a custom types.Serdes that does the same thing transparently.
// operations.ResultTooLargeError is this SDK's own conservative
// fallback for the case where a caller does NEITHER of those: rather
// than silently sending an oversized payload (which would presumably
// fail unhelpfully at the network/backend layer - unconfirmed, since
// this session did not trigger a real oversized request against a live
// endpoint, per errors.go's own honesty note), the SDK rejects it
// client-side, before checkpointing, with a clear, actionable, typed
// error.
//
// This example demonstrates BOTH the failure this conservative check
// produces AND the recommended pattern a real caller should use instead
// to avoid it in the first place - see handler's own doc below for how
// the two scenarios are split.
package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// oversizedPayloadSizeBytes is a KNOWN, deterministic size comfortably
// over operations' own 750KB (750*1024 bytes) single-operation-result
// checkpoint threshold (see pkg/durable/operations/errors.go's
// resultTooLargeThresholdBytes) - chosen as exactly 800*1024 bytes (800KB,
// ~50KB/~6.8% over the threshold) rather than something size-fuzzy (e.g.
// "a few paragraphs of lorem ipsum" or "N repeated JSON records"), so the
// test asserting on this scenario is precise about genuinely crossing the
// threshold, not merely "probably large enough." A repeated-character
// string is used instead of e.g. random bytes or a marshaled struct
// specifically so the EXACT byte length is trivial to reason about and
// verify (len(strings.Repeat(s, n)) == len(s)*n, no JSON-encoding
// overhead or escaping to account for).
const oversizedPayloadSizeBytes = 800 * 1024

// generateOversizedPayload returns a deterministic string of EXACTLY
// oversizedPayloadSizeBytes bytes - a single repeated ASCII character, so
// its serialized (JSON string) length is trivially len(payload) plus the
// two quote-byte JSON overhead, with no multi-byte-rune or escaping
// surprises to account for when reasoning about whether it crosses
// operations.checkResultSize's threshold.
func generateOversizedPayload() string {
	buf := make([]byte, oversizedPayloadSizeBytes)
	for i := range buf {
		buf[i] = 'x'
	}
	return string(buf)
}

// externalStore simulates what a real implementation would stage in
// actual external storage (e.g. an S3 bucket via s3.PutObject, or a
// DynamoDB table) - see StageLargeResultExternally's doc for the full
// disclaimer. This is a package-level, in-memory, mutex-guarded map ONLY
// because this is a local, single-process example with no real AWS
// infrastructure of its own to stage data in; it is NOT a substitute for
// real external storage, and would not survive a fresh Lambda execution
// environment (a new cold-start container has its own, empty
// externalStore), would not be shared across concurrent invocations
// running in different execution environments, and would not survive a
// suspend/resume cycle onto a different container. A real implementation
// MUST use genuine external storage (S3/DynamoDB) - this map exists
// purely so this LOCAL example can demonstrate the calling pattern
// (return a small reference instead of the large payload) without
// requiring real AWS infrastructure to run its tests.
var (
	externalStoreMu sync.Mutex
	externalStore   = map[string]string{}
)

// StageLargeResultExternally simulates staging a large value in external
// storage and returns a small reference key standing in for what a real
// S3 object key/DynamoDB item ID would be. See externalStore's doc for
// the explicit "this is simulated, not real S3" disclaimer - do not treat
// this function as demonstrating anything about REAL external-storage
// integration (retry behavior, IAM permissions, eventual consistency,
// etc.), only the SHAPE of the calling pattern: a Step stages data
// externally and returns a reference instead of the data itself, which is
// the recommended, real, currently-available way to avoid
// ResultTooLargeError (see docs/remaining-work.md §6 task 16's research
// and errors.go/ResultTooLargeError's own doc for the two real,
// AWS-documented mitigations this mirrors).
func StageLargeResultExternally(value string) (referenceKey string, err error) {
	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		return "", fmt.Errorf("generating external storage reference key: %w", err)
	}
	referenceKey = "s3://simulated-large-payload-bucket/" + hex.EncodeToString(keyBytes)

	externalStoreMu.Lock()
	externalStore[referenceKey] = value
	externalStoreMu.Unlock()

	return referenceKey, nil
}

// FetchFromExternalStore is the read-side counterpart to
// StageLargeResultExternally, provided only so this example's test suite
// can independently confirm the staged value really was stored (round-
// tripping through the reference key) - a real implementation would call
// s3.GetObject/dynamodb.GetItem here instead. Not called by handler
// itself - a durable handler that needed the large value back would call
// this (or its real equivalent) from a LATER step, not as part of this
// example's own scope.
func FetchFromExternalStore(referenceKey string) (string, bool) {
	externalStoreMu.Lock()
	defer externalStoreMu.Unlock()
	value, ok := externalStore[referenceKey]
	return value, ok
}

// LargePayloadEvent is this example's input shape. Scenario selects which
// of the two demonstrated behaviors to run - modeled as a string enum
// rather than two separate handlers/booleans, since the two scenarios are
// mutually exclusive alternatives a caller picks between (Go has no
// union-type input shape to express "one of these two event shapes"
// without a discriminant field).
type LargePayloadEvent struct {
	// Scenario selects the behavior: "oversized" runs the Step that
	// returns a result exceeding the 750KB threshold directly (failing
	// clearly with *operations.ResultTooLargeError); "reference" runs
	// the Step that stages the same size of data externally (simulated)
	// and returns only a small reference key (succeeding).
	Scenario string `json:"scenario"`
}

// LargePayloadResult is this example's output shape for BOTH scenarios.
// For "oversized", the handler never reaches a point where it can return
// a populated result (operations.Step's call itself fails before ever
// returning to the handler) - the zero value is returned alongside the
// propagated error, matching every other example's error-path
// convention (see e.g. error-handling-go's ChargeCardResult doc).
type LargePayloadResult struct {
	// ReferenceKey is populated only by the "reference" scenario - the
	// small key/pointer string StageLargeResultExternally returned,
	// standing in for a real S3 key/DynamoDB item ID. Deliberately NOT
	// the large payload itself - that is the entire point of this
	// scenario.
	ReferenceKey string `json:"referenceKey,omitempty"`
	// PayloadSizeBytes records the size of the large value that was
	// EITHER rejected (oversized scenario, though the handler never
	// actually gets to populate this field for that scenario - see
	// LargePayloadResult's own doc) or staged externally (reference
	// scenario) - included for the reference scenario so a caller/test
	// can confirm the size that was actually staged matches
	// oversizedPayloadSizeBytes, without needing to fetch the staged
	// value back just to check its length.
	PayloadSizeBytes int `json:"payloadSizeBytes,omitempty"`
}

// errUnknownScenario is returned for any Scenario value other than the
// two this handler recognizes - a plain, non-operation-produced error
// (matching e.g. simple-step-go's ValidatingHandler's errMissingMessage:
// a pre-operation validation failure, not itself a durable-operation
// error), kept as a named sentinel purely for a clear, greppable error
// message rather than any errors.Is/As-based dispatch need.
var errUnknownScenario = errors.New("unknown scenario: must be \"oversized\" or \"reference\"")

// handler demonstrates operations.ResultTooLargeError
// (pkg/durable/operations/errors.go, docs/remaining-work.md §6 task 16)
// via two mutually exclusive Step scenarios, selected by
// LargePayloadEvent.Scenario:
//
//  1. "oversized": a Step ("generate-oversized-report") returns a
//     deterministic oversizedPayloadSizeBytes (800KB)-sized string
//     DIRECTLY as its result. operations.checkResultSize rejects this
//     BEFORE it is ever checkpointed (see errors.go's own doc for
//     exactly where this check runs relative to Checkpoint().Enqueue),
//     so the Step call itself returns a *operations.ResultTooLargeError,
//     which propagates up through this handler (wrapped with %w, per
//     this repo's established errors.As-preserving convention) and
//     fails the whole execution clearly - rather than either silently
//     corrupting/truncating the oversized data, or letting it reach the
//     network layer and fail there unhelpfully (an opaque low-level
//     error from checkpoint.Manager/the real backend, which this
//     conservative client-side check exists specifically to avoid - see
//     errors.go's research notes).
//
//  2. "reference": a DIFFERENT Step ("stage-report-externally") is
//     given the EXACT SAME SIZE of large data (oversizedPayloadSizeBytes)
//     to work with, but instead of returning it directly, it calls
//     StageLargeResultExternally (a SIMULATED external-storage stand-in
//     - see that function's doc for the explicit "not real S3, an
//     in-memory map" disclaimer) and returns only the small reference
//     key StageLargeResultExternally produced as its actual Step
//     result. This is the RECOMMENDED pattern the real reference SDKs
//     actually use for large results (per docs/remaining-work.md §6 task
//     16's research into the TS/Python/Java SDKs' real large-payload
//     mechanisms: "store references, not payloads" - stage large data
//     externally and return a reference, or use a custom Serdes that
//     does the same). Because the CHECKPOINTED result is just the small
//     reference string, it is comfortably under the 750KB threshold and
//     the Step succeeds normally.
//
// Both scenarios generate the SAME oversizedPayloadSizeBytes-sized value
// (via generateOversizedPayload, a deterministic repeated-character
// string of a known exact byte length - not something size-fuzzy) so the
// comparison between "returned directly: fails" and "staged + reference
// returned: succeeds" is a direct, apples-to-apples contrast of the two
// approaches to the identical underlying data, not two differently-sized
// scenarios that might coincidentally behave differently for unrelated
// reasons.
func handler(event LargePayloadEvent, dc types.DurableContext) (LargePayloadResult, error) {
	dc.Logger().Info("handler started", map[string]any{"scenario": event.Scenario})

	switch event.Scenario {
	case "oversized":
		_, err := operations.Step(dc, "generate-oversized-report", func(sc types.StepContext) (string, error) {
			// Returns the large payload DIRECTLY as the step's result -
			// this is the anti-pattern operations.checkResultSize exists
			// to catch before it is ever checkpointed. See handler's own
			// doc above for why this specific size/scenario pairing was
			// chosen.
			return generateOversizedPayload(), nil
		})
		if err != nil {
			// The core point of this scenario: use errors.As to recover
			// the structured *operations.ResultTooLargeError (mirroring
			// error-handling-go's identical errors.As-inspection
			// pattern for *operations.StepFailedError) and report on
			// its SizeBytes/ThresholdBytes fields specifically, rather
			// than letting an opaque error propagate - proving the
			// failure is the SPECIFIC, clearly-typed rejection this
			// example exists to demonstrate, not some other unrelated
			// Step failure that happens to also return a non-nil error.
			var tooLarge *operations.ResultTooLargeError
			if errors.As(err, &tooLarge) {
				return LargePayloadResult{}, fmt.Errorf(
					"generate-oversized-report step result was too large (%d bytes, exceeds %d-byte threshold, operation id %s): %w",
					tooLarge.SizeBytes, tooLarge.ThresholdBytes, tooLarge.ID, err,
				)
			}
			// Not a ResultTooLargeError - some other failure. Propagate
			// as-is rather than pretending every error from this Step
			// call is necessarily a size-limit rejection.
			return LargePayloadResult{}, fmt.Errorf("generate-oversized-report step failed: %w", err)
		}
		// Unreachable in practice (the Step above always exceeds the
		// threshold, by construction - oversizedPayloadSizeBytes >
		// operations' 750KB threshold), but Go requires a return on
		// every path; this only executes if a future change shrinks
		// oversizedPayloadSizeBytes below the threshold by mistake, in
		// which case returning a zero-value result with no error is the
		// correct behavior for what would then be a genuinely-succeeding
		// step.
		return LargePayloadResult{}, nil

	case "reference":
		referenceKey, err := operations.Step(dc, "stage-report-externally", func(sc types.StepContext) (string, error) {
			largeValue := generateOversizedPayload()
			// This is the RECOMMENDED pattern (see handler's own doc
			// above): stage the large value externally (SIMULATED here
			// - see StageLargeResultExternally's doc for the explicit
			// "in-memory map, not real S3" disclaimer) and return only
			// the small reference key as this Step's actual result,
			// which is what gets checkpointed - never the large value
			// itself.
			return StageLargeResultExternally(largeValue)
		})
		if err != nil {
			return LargePayloadResult{}, fmt.Errorf("stage-report-externally step failed: %w", err)
		}

		dc.Logger().Info("handler completed", map[string]any{"referenceKey": referenceKey})
		return LargePayloadResult{
			ReferenceKey:     referenceKey,
			PayloadSizeBytes: oversizedPayloadSizeBytes,
		}, nil

	default:
		return LargePayloadResult{}, fmt.Errorf("large-payload-go: %w (got %q)", errUnknownScenario, event.Scenario)
	}
}
