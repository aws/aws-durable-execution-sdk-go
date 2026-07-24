package dag

// TriggerRule determines whether a task runs based on the terminal
// statuses of its upstream dependencies. It is an open string-typed enum
// (Go cannot express a closed union); unknown values are rejected at
// validation time (see DagInvalidTriggerRuleError). The empty value is
// treated as the default, AllSuccess.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type TriggerRule string

const (
	// AllSuccess (the default) runs the task only if every upstream
	// dependency succeeded. With no upstreams it is satisfied.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	AllSuccess TriggerRule = "ALL_SUCCESS"

	// AllFailed runs the task only if there is at least one upstream and
	// every upstream failed.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	AllFailed TriggerRule = "ALL_FAILED"

	// AllDone runs the task once every upstream is terminal, regardless of
	// their statuses. With no upstreams it is satisfied.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	AllDone TriggerRule = "ALL_DONE"

	// AnySuccess runs the task if at least one upstream succeeded.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	AnySuccess TriggerRule = "ANY_SUCCESS"

	// AnyFailed runs the task if at least one upstream failed.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	AnyFailed TriggerRule = "ANY_FAILED"

	// NoneFailed runs the task if no upstream failed (successes and skips
	// are allowed). With no upstreams it is satisfied.
	//
	// Experimental: This API is experimental and may be changed or removed
	// in future releases.
	NoneFailed TriggerRule = "NONE_FAILED"
)

// knownTriggerRules is the set of valid rules, used by validation.
var knownTriggerRules = map[TriggerRule]struct{}{
	AllSuccess: {}, AllFailed: {}, AllDone: {},
	AnySuccess: {}, AnyFailed: {}, NoneFailed: {},
}

// allAre reports whether every status equals want.
func allAre(statuses []TaskStatus, want TaskStatus) bool {
	for _, s := range statuses {
		if s != want {
			return false
		}
	}
	return true
}

func anyIs(statuses []TaskStatus, want TaskStatus) bool {
	for _, s := range statuses {
		if s == want {
			return true
		}
	}
	return false
}

func noneIs(statuses []TaskStatus, want TaskStatus) bool {
	return !anyIs(statuses, want)
}

// triggerRuleEvaluators maps each rule to a predicate over the terminal
// statuses of a task's upstream dependencies. Semantics (including
// empty-upstream rows and the len>0 guard on AllFailed) port verbatim from
// the canonical JS spec (§5.3). SKIPPED counts as neither success nor
// failure.
var triggerRuleEvaluators = map[TriggerRule]func([]TaskStatus) bool{
	AllSuccess: func(s []TaskStatus) bool { return allAre(s, StatusSucceeded) },
	AllFailed:  func(s []TaskStatus) bool { return len(s) > 0 && allAre(s, StatusFailed) },
	AllDone:    func(s []TaskStatus) bool { return true },
	AnySuccess: func(s []TaskStatus) bool { return anyIs(s, StatusSucceeded) },
	AnyFailed:  func(s []TaskStatus) bool { return anyIs(s, StatusFailed) },
	NoneFailed: func(s []TaskStatus) bool { return noneIs(s, StatusFailed) },
}

// evaluateTrigger reports whether a task with the given trigger rule should
// run given its upstreams' terminal statuses. An empty rule defaults to
// AllSuccess. An unknown rule (which validation should have rejected)
// conservatively returns false.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func evaluateTrigger(rule TriggerRule, upstream []TaskStatus) bool {
	if rule == "" {
		rule = AllSuccess
	}
	eval, ok := triggerRuleEvaluators[rule]
	if !ok {
		return false
	}
	return eval(upstream)
}
