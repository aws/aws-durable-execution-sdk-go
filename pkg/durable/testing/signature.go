// signature.go implements docs/remaining-work.md §7 task 17c:
// event-history/signature snapshot assertions, porting the JS SDK's
// assertEventSignatures / .history.json golden-file pattern (see that
// document's "Completion criteria" section and task 17c's own
// description) to Go.
//
// # What a "signature" is, and why
//
// A TestResult's raw operation log (TestResult.GetOperations()) is not
// directly diffable against a committed golden file: it carries
// timestamps (StartTimestamp/EndTimestamp/StepDetails.NextAttemptTimestamp/
// WaitDetails.ScheduledEndTimestamp), operation IDs that are opaque
// SHA-256 hashes (see context.Context.NextStepID's doc) carrying no
// meaningful structure of their own, and (on a CloudTestRunner run)
// request IDs / execution ARNs unique to that one real invocation - none
// of which are meant to be stable across repeated test runs, and
// asserting on any of them would make a "golden file" fail on every
// single run through no real behavior change at all.
//
// An EventSignature strips all of that down to exactly the fields the
// task's own framing calls out as the meaningful, deterministic subset:
// Type, SubType, Status, and Name - the shape of the operation log
// itself (what kinds of operations ran, in what order, nested how
// deeply, and whether each succeeded or failed), not any particular
// run's identifiers or wall-clock timing. This is deliberately a
// SUBSET, not a full re-serialization of Operation - the whole point of
// this task, per docs/remaining-work.md's own framing, is "catches
// accidental non-determinism, dropped checkpoints, or unintended
// changes to the operation log that a bare result comparison would
// miss," which the four chosen fields are exactly sufficient for
// without also making the golden file brittle against every unrelated
// change (a different step's timestamp, a different execution's ARN).
package testing

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	dcontext "github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/context"
	"github.com/aws/aws-durable-execution-sdk-csharp/pkg/durable/types"
)

// EventSignature is the deterministic, comparable subset of a single
// checkpointed operation's identity used for golden-file history
// assertions (see this file's doc comment for what's deliberately
// excluded and why).
//
// Depth and Index (both new relative to the JS reference's flatter
// {type, subType, status, name} tuple - see AssertEventSignatures' doc
// for why they were added) make the signature capture STRUCTURE
// (nesting, ordering) as well as identity, since Go's map-based
// TestResult.operations (unlike the JS SDK's own naturally
// insertion-ordered array) has no inherent order to preserve otherwise -
// see EventSignatures' doc for how these are derived from each
// operation's ParentID and checkpoint-arrival sequence (previously
// derived from each operation's own hierarchical ID string, before this
// SDK's SHA-256 operation-ID-hashing change made that approach
// impossible - see EventSignatures' doc for the full writeup).
type EventSignature struct {
	// Depth is 0 for a top-level operation, 1 for a direct child of a
	// CONTEXT operation, 2 for a grandchild, and so on - derived by
	// walking the operation's Operation.ParentID chain up to the root
	// (see EventSignatures' doc): each hop up a ParentID link adds one to
	// Depth, matching TestResult.GetChildOperations' own ParentID-based
	// convention for the same structural information.
	Depth int `json:"depth"`
	// Index is this operation's position among its siblings (other
	// operations sharing the same ParentID), 1-based, ordered by
	// checkpoint arrival - see EventSignatures' doc (siblingOrder) for
	// exactly what "arrival order" means and why it produces identical
	// values to this field's pre-hashing derivation (the operation ID's
	// own trailing counter segment, back when IDs were plain
	// hierarchical strings) - NOT the order operations happen to be
	// range-iterated from TestResult's underlying map, which Go does not
	// guarantee is stable across runs at all. This is what makes
	// EventSignatures' output deterministic across repeated runs of the
	// SAME execution, independent of map iteration order.
	Index int `json:"index"`
	// Type is the operation's wire Type (STEP, WAIT, CALLBACK,
	// CHAINED_INVOKE, CONTEXT, EXECUTION).
	Type types.OperationType `json:"type"`
	// SubType distinguishes operations that share a wire Type (e.g.
	// CONTEXT/RUN_IN_CHILD_CONTEXT vs. CONTEXT/MAP vs. CONTEXT/PARALLEL vs.
	// CONTEXT/MAP_ITERATION vs. CONTEXT/PARALLEL_BRANCH; STEP vs.
	// STEP/WAIT_FOR_CONDITION) - see operations/errors.go's
	// checkReplayConsistency for the same Type+SubType pairing used
	// elsewhere in this SDK to distinguish operation kinds. Omitted from
	// the golden JSON when empty, matching every operation that doesn't
	// use SubType at all (Step, Wait, CreateCallback, Invoke).
	SubType string `json:"subType,omitempty"`
	// Status is the operation's terminal (or last-observed) status.
	Status types.OperationStatus `json:"status"`
	// Name is the operation's user-assigned name, empty for the root
	// EXECUTION operation and for unnamed Map/Parallel branches (see
	// context.Context.NewChild vs. NewChildWithName's doc).
	Name string `json:"name,omitempty"`
}

// EventSignatures computes the deterministic, ordered signature slice
// for result's full operation log - both top-level operations AND every
// nested child-context operation, recursively - matching what a golden
// history file is meant to capture: the WHOLE shape of what ran, not
// just the top level.
//
// # ParentID-based derivation (post SHA-256-hashing)
//
// Ordering and hierarchy are both derived from Operation.ParentID (see
// dcontext.Context.ParentStepID's doc), NOT from splitting each
// operation's own ID string on "-" and sorting by its trailing numeric
// counter segment, which is how this function worked before this SDK's
// SHA-256 operation-ID-hashing change (see
// docs/remaining-work.md's "SHA-256 operation ID hashing" section for
// the full writeup). That ID-string-parsing approach is now IMPOSSIBLE:
// every ID minted by context.Context.NextStepID is a SHA-256 hash, and a
// hash reveals nothing about the string it was computed from - there is
// no way to recover "this ID's parent is that ID" or "this ID was the
// Nth minted in its context" by inspecting the hash bytes themselves.
// This is a forced redesign, not a style preference - see
// result.go's GetChildOperations doc for the identical situation there.
//
// Hierarchy (Depth, and which operations are some operation's children)
// now comes directly from walking the ParentID chain (buildSignatures,
// below) - straightforward, since ParentID is a real, always-populated
// field (docs/remaining-work.md §0's session made sure of that) that
// directly encodes exactly this relationship, arguably more directly
// than the old ID-prefix convention did.
//
// Ordering (Index, this operation's position among its siblings) is
// harder, since a SHA-256 hash carries no numeric "this was checkpoint
// N" signal the way a plain hierarchical ID string's trailing counter
// segment did. This uses dcontext.MintOrder (see that function's doc) -
// context.Context.NextStepID itself records each hashed ID's GLOBAL
// mint order (the order NextStepID's own internal counter produced it
// in, process-wide) the instant it's minted, which is proven to produce
// the IDENTICAL Index values the old counter-order derivation did for
// every case that matters: mint order strictly precedes checkpoint
// order for every ID this SDK ever produces (NextStepID is always
// called before that same ID's START checkpoint is enqueued, at every
// call site - step.go, wait.go, callback.go, invoke.go, batch.go,
// wait_for_condition.go), and - critically for Map/Parallel - is
// GUARANTEED to be assigned in deterministic index order even though
// checkpoint ARRIVAL is not: batch.go's runBatch claims every branch/
// item's step ID up front, by index, on the single parent goroutine
// BEFORE spawning any branch goroutine (see that function's own doc,
// bug #3), specifically for production replay-safety reasons unrelated
// to this testing package - but each branch's START checkpoint is still
// enqueued later, from INSIDE that branch's own concurrently-running
// goroutine (runBatchItem). An earlier draft of this fix tried stamping
// an ordinal at CHECKPOINT-ARRIVAL time instead (in
// testing/inmemory_client.go) and was caught immediately by this exact
// concurrency gap: two PARALLEL_BRANCH siblings' checkpoints legitimately
// arrived out of index order under real goroutine scheduling, producing
// a golden-file mismatch in examples/map-parallel-go's own pre-existing
// test - proof mint order, not arrival order, is the only signal that is
// actually deterministic here. For a CloudTestRunner result, where no
// same-process mint-order bookkeeping exists at all (a real backend
// process, not this one, minted whatever IDs it used), this falls back
// to sorting by Operation.StartTimestamp - the best remaining real
// signal - and, if that too is unavailable or ties, preserves whatever
// order the input operations were provided in (sort.SliceStable),
// matching this function's own pre-existing, honestly-documented "not
// confirmed to matter in practice" fallback philosophy for
// cloud-observed operations (see the historical directChildIndex
// numeric-parse-failure fallback this replaces, which had the identical
// caveat for the identical reason: operations this SDK cannot fully
// order with 100% confidence from available signals alone).
//
// The root EXECUTION operation itself is excluded from the returned
// slice: it is not something a handler's own code chose to run (every
// OTHER operation, STEP/WAIT/CALLBACK/CHAINED_INVOKE/CONTEXT, is a
// direct or indirect consequence of a call the handler body made), and
// including it would add a fixed, uninteresting {EXECUTION, "", status,
// ""} entry to every single golden file for no diagnostic value - a
// golden-file diff catching "the handler's OWN operation sequence
// changed" is the entire point of this feature (see this file's doc
// comment), and the root EXECUTION operation never varies in a way any
// test would want a diff to flag.
func EventSignatures(result TestResult) []EventSignature {
	return buildSignatures(result.operations, "")
}

// buildSignatures recursively derives signatures for every operation
// whose ParentID equals parentID (parentID=="" selects top-level
// operations - i.e. operations with no parent, matching
// dcontext.Context.ParentStepID's "empty for the root Context" - exactly
// mirroring TestResult.GetChildOperations' own ParentID-based
// convention, see that method's doc), sorts them by their mint order
// (siblingOrder, below), and appends each operation's own children
// (recursively, at parentID = op.ID) immediately after it - a
// depth-first, pre-order walk, matching the natural reading order of a
// nested operation log (a CONTEXT operation's own signature line,
// immediately followed by everything that ran inside it).
func buildSignatures(ops map[string]types.Operation, parentID string) []EventSignature {
	return buildSignaturesAtDepth(ops, parentID, 0)
}

func buildSignaturesAtDepth(ops map[string]types.Operation, parentID string, depth int) []EventSignature {
	type indexed struct {
		op    types.Operation
		order int64
	}
	var children []indexed
	for _, op := range ops {
		if op.Type == types.OperationTypeExecution {
			continue // see EventSignatures' doc: the root operation is deliberately excluded
		}
		if op.ParentID != parentID {
			continue
		}
		children = append(children, indexed{op: op, order: siblingOrder(op)})
	}
	// Stable sort by each child's own order (mint order, or
	// StartTimestamp-derived fallback - see siblingOrder's doc) - stable
	// so that a genuine tie (two children siblingOrder cannot fully
	// distinguish, e.g. both missing a recorded mint order AND a
	// StartTimestamp, on a CloudTestRunner result) preserves the input
	// map's own iteration order rather than reshuffling unpredictably
	// run to run, matching this function's pre-existing fallback
	// philosophy (see EventSignatures' doc).
	sort.SliceStable(children, func(i, j int) bool { return children[i].order < children[j].order })

	var out []EventSignature
	for i, c := range children {
		out = append(out, EventSignature{
			Depth:   depth,
			Index:   i + 1, // 1-based, matching this field's pre-existing convention (see EventSignature.Index's doc)
			Type:    c.op.Type,
			SubType: c.op.SubType,
			Status:  c.op.Status,
			Name:    c.op.Name,
		})
		out = append(out, buildSignaturesAtDepth(ops, c.op.ID, depth+1)...)
	}
	return out
}

// siblingOrder returns the value used to sort op among its siblings: its
// global mint order from dcontext.MintOrder (see that function's doc)
// when recorded - which it always is for any operation this SDK's own
// context.Context.NextStepID minted, i.e. every LocalTestRunner result -
// or, for a CloudTestRunner result (a real backend's own IDs, never
// minted by this process at all), a value derived from
// op.StartTimestamp (unix nanoseconds, so earlier operations sort
// first), or, failing both, 0 (grouping all such operations together for
// buildSignaturesAtDepth's sort.SliceStable to leave in their existing
// relative order - see that function's doc for why this remains an
// honest "not confirmed to matter in practice" fallback, not a real
// ordering guarantee, exactly mirroring the pre-hashing implementation's
// identical caveat for operations whose ID didn't parse as a plain
// numeric segment).
func siblingOrder(op types.Operation) int64 {
	if seq, ok := dcontext.MintOrder(op.ID); ok {
		return seq
	}
	if op.StartTimestamp != nil {
		return op.StartTimestamp.UnixNano()
	}
	return 0
}

// AssertEventSignatures computes result's EventSignatures and compares
// them against the JSON golden file at goldenFilePath, failing t via
// t.Fatalf/t.Errorf on any mismatch - the Go port of the JS SDK's
// assertEventSignatures + committed .history.json snapshot pattern (see
// this file's doc comment, and docs/remaining-work.md task 17c).
//
// # Regenerating the golden file
//
// Set the UPDATE_GOLDEN=1 environment variable (a common Go testing
// idiom - mirroring the standard library's own -update flag convention
// used by, e.g., golang.org/x/tools' own testdata-golden-file tests, and
// the same idea the task's own instructions asked for explicitly) to
// (re)write goldenFilePath from result's actual current signatures
// instead of comparing against it:
//
//	UPDATE_GOLDEN=1 go test ./examples/simple-step-go/... -run TestHandler
//
// This intentionally still requires deleting/regenerating a file under
// version control (the golden file itself, which a reviewer sees diffed
// in the same commit as whatever behavior change necessitated it) rather
// than providing any way to edit expectations without a visible file
// change - matching the JS SDK's own committed-.history.json convention
// exactly, and this task's own explicit instruction to support
// regeneration "rather than requiring manual JSON editing," not "rather
// than requiring the file to change at all."
//
// The golden file's parent directory is created automatically when
// regenerating (os.MkdirAll) so a brand-new example's first golden file
// doesn't require a manual mkdir step.
func AssertEventSignatures(t *testing.T, result TestResult, goldenFilePath string) {
	t.Helper()

	actual := EventSignatures(result)

	if os.Getenv("UPDATE_GOLDEN") != "" {
		writeGoldenFile(t, goldenFilePath, actual)
		return
	}

	expected, err := readGoldenFile(goldenFilePath)
	if err != nil {
		t.Fatalf("testing.AssertEventSignatures: reading golden file %q: %v (run with UPDATE_GOLDEN=1 to create it)", goldenFilePath, err)
	}

	if !equalSignatures(expected, actual) {
		expectedJSON, _ := json.MarshalIndent(expected, "", "  ")
		actualJSON, _ := json.MarshalIndent(actual, "", "  ")
		t.Fatalf(
			"testing.AssertEventSignatures: operation-log signature mismatch against golden file %q.\n\nExpected (golden):\n%s\n\nActual:\n%s\n\nIf this change is intentional, regenerate the golden file with:\n  UPDATE_GOLDEN=1 go test -run %s ./...\n",
			goldenFilePath, expectedJSON, actualJSON, t.Name(),
		)
	}
}

func equalSignatures(a, b []EventSignature) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func readGoldenFile(path string) ([]EventSignature, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var sigs []EventSignature
	if err := json.Unmarshal(b, &sigs); err != nil {
		return nil, fmt.Errorf("parsing golden file JSON: %w", err)
	}
	return sigs, nil
}

func writeGoldenFile(t *testing.T, path string, sigs []EventSignature) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("testing.AssertEventSignatures: creating golden file directory for %q: %v", path, err)
	}
	b, err := json.MarshalIndent(sigs, "", "  ")
	if err != nil {
		t.Fatalf("testing.AssertEventSignatures: marshaling golden file for %q: %v", path, err)
	}
	b = append(b, '\n')
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatalf("testing.AssertEventSignatures: writing golden file %q: %v", path, err)
	}
	t.Logf("testing.AssertEventSignatures: wrote golden file %q (UPDATE_GOLDEN=1 was set)", path)
}
