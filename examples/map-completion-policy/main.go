// Command map-completion-policy demonstrates a custom completion policy
// for [durable.Map] composed from small rules. A
// [durable.CompletionConfig] ShouldComplete callback is mutually exclusive
// with MinSuccessful and the failure tolerances, so a batch that needs a
// quorum together with a rule of its own writes the quorum as a rule too.
// firstOf combines the rules by reading each rule's
// [durable.CompletionDecision] with Complete and Outcome.
//
// The handler writes a record to five replicas and commits once three
// acknowledge. Three rules decide:
//
//   - quorum: three replicas acknowledged, so the batch succeeds.
//   - primaryMustAck: the primary (replica 0) failed, so the batch fails.
//   - quorumUnreachable: too few replicas are left to reach the quorum,
//     so the batch fails without trying the rest.
//
// The writes run one at a time ([durable.WithMaxConcurrency] 1), so the
// rules see the replicas finish in index order and the outcome of each
// input is exact. The policy itself works at any concurrency. Replicas
// the batch never started are omitted from the result.
//
// The input names the replicas that are unavailable. The default input
// has none, so replicas 0 to 2 acknowledge and replicas 3 and 4 are never
// written.
package main

import (
	"errors"
	"fmt"
	"slices"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Input names the replicas whose writes fail.
type Input struct {
	Unavailable []string `json:"unavailable"`
}

type Output struct {
	Committed    bool     `json:"committed"`
	Reason       string   `json:"reason"`
	Acknowledged []string `json:"acknowledged"`
	Failed       []string `json:"failed"`
	NotAttempted int      `json:"notAttempted"`
}

var replicas = []string{"replica-0", "replica-1", "replica-2", "replica-3", "replica-4"}

// writeQuorum is the number of acknowledgements that commits the write.
const writeQuorum = 3

// rule is one completion rule. Like every ShouldComplete callback it must
// depend only on the progress it is given.
type rule func(durable.BatchProgress) durable.CompletionDecision

// quorum completes the batch as succeeded once n items have succeeded.
func quorum(n int) rule {
	return func(p durable.BatchProgress) durable.CompletionDecision {
		if p.SuccessCount >= n {
			return durable.CompleteBatch(durable.CompletionOutcomeSucceeded)
		}
		return durable.ContinueBatch()
	}
}

// primaryMustAck completes the batch as failed once the primary, the item
// at index 0, has failed.
func primaryMustAck(p durable.BatchProgress) durable.CompletionDecision {
	if p.Items[0].Status == durable.BatchItemFailed {
		return durable.CompleteBatch(durable.CompletionOutcomeFailed)
	}
	return durable.ContinueBatch()
}

// quorumUnreachable completes the batch as failed once the items that
// have not finished cannot bring the successes up to n.
func quorumUnreachable(n int) rule {
	return func(p durable.BatchProgress) durable.CompletionDecision {
		if p.SuccessCount+(p.TotalCount-p.CompletedCount) < n {
			return durable.CompleteBatch(durable.CompletionOutcomeFailed)
		}
		return durable.ContinueBatch()
	}
}

// firstOf completes the batch when any rule completes it. When several
// rules complete it on the same snapshot, a failed outcome takes
// precedence over a succeeded one, so a failing rule cannot be outvoted.
// With these rules and one write at a time that never happens. With
// concurrent writes one snapshot can show several items that finished
// since the last, such as a failed primary and a third acknowledgement.
func firstOf(rules ...rule) rule {
	return func(p durable.BatchProgress) durable.CompletionDecision {
		decision := durable.ContinueBatch()
		for _, r := range rules {
			d := r(p)
			if !d.Complete() {
				continue
			}
			if d.Outcome() == durable.CompletionOutcomeFailed {
				return d
			}
			decision = d
		}
		return decision
	}
}

func handler(ctx durable.Context, in Input) (Output, error) {
	result, err := durable.Map(ctx, "replicate", replicas,
		func(_ durable.Context, replica string, _ int) (string, error) {
			// The write is simulated. A real write would run in a Step.
			if slices.Contains(in.Unavailable, replica) {
				return "", fmt.Errorf("%s unavailable", replica)
			}
			return "ack", nil
		},
		durable.WithItemNamer(func(i int) string { return replicas[i] }),
		durable.WithMaxConcurrency(1),
		durable.WithCompletion(durable.CompletionConfig{
			ShouldComplete: firstOf(quorum(writeQuorum), primaryMustAck, quorumUnreachable(writeQuorum)),
		}),
	)
	// A failed outcome is reported as a *durable.BatchError alongside the
	// populated result; this handler reports the result. Any other error
	// is an SDK failure and propagates.
	var berr *durable.BatchError
	if err != nil && !errors.As(err, &berr) {
		return Output{}, err
	}

	out := Output{
		// The policy's outcome is authoritative: a quorum commits even
		// when some replicas failed.
		Committed:    result.Reason == durable.CompletionCustomSucceeded,
		Reason:       result.Reason.String(),
		Acknowledged: []string{},
		Failed:       []string{},
		NotAttempted: len(replicas) - result.TotalCount(),
	}
	for _, item := range result.Succeeded() {
		out.Acknowledged = append(out.Acknowledged, item.Name)
	}
	for _, item := range result.Failed() {
		out.Failed = append(out.Failed, item.Name)
	}
	return out, nil
}

func main() { durable.Start(handler) }
