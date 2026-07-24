package dag

import (
	"regexp"
	"strings"
)

var taskNameRe = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

const maxTaskNameLen = 100

// validateName checks a single task name against the naming rules and
// returns a non-nil error describing the first violation.
func validateName(name string) error {
	if name == "" {
		return &DagInvalidTaskNameError{Name: name, Reason: "name must be non-empty"}
	}
	if len(name) > maxTaskNameLen {
		return &DagInvalidTaskNameError{Name: name, Reason: "name exceeds 100 characters"}
	}
	if !taskNameRe.MatchString(name) {
		return &DagInvalidTaskNameError{Name: name, Reason: "name must match ^[a-zA-Z0-9_]+$ (no dashes)"}
	}
	if strings.Contains(name, "DAG_NODE_T_") {
		return &DagInvalidTaskNameError{Name: name, Reason: `name must not contain the reserved substring "DAG_NODE_T_"`}
	}
	return nil
}

// validate runs the full validation pass over a registered Context and DAG
// config: config guards, name rules, duplicates (already accumulated in
// regErrs), missing/foreign deps, unknown trigger rules, and cycle
// detection. It aggregates everything into a *DagValidationError, or returns
// nil if the graph is valid. Nothing is scheduled if this returns non-nil.
func validate(d *Context, cfg config) error {
	var errs []error

	// Config guards (return immediately as they are DAG-wide).
	if cfg.maxConcurrency != nil && *cfg.maxConcurrency <= 0 {
		return &DagInvalidConfigError{Reason: "maxConcurrency must be positive"}
	}
	if cfg.completion != nil && cfg.completion.isCustom() && cfg.completion.isThreshold() {
		return &DagInvalidConfigError{Reason: "completion config: custom ShouldComplete and threshold fields are mutually exclusive"}
	}

	// Carry over duplicate/registration errors accumulated during
	// registration.
	errs = append(errs, d.regErrs...)

	// Unknown DAG-level default trigger rule (validated once, not per task).
	if cfg.defaultTrigger != "" {
		if _, ok := knownTriggerRules[cfg.defaultTrigger]; !ok {
			errs = append(errs, &DagInvalidTriggerRuleError{Rule: cfg.defaultTrigger})
		}
	}

	// Name rules.
	for _, t := range d.tasks {
		if err := validateName(t.name); err != nil {
			errs = append(errs, err)
		}
		// Unknown trigger rule.
		if t.hasTrigger && t.trigger != "" {
			if _, ok := knownTriggerRules[t.trigger]; !ok {
				errs = append(errs, &DagInvalidTriggerRuleError{Rule: t.trigger})
			}
		}
	}

	// Missing / foreign-scope dependencies.
	for _, t := range d.tasks {
		for _, dep := range t.allDeps() {
			if _, ok := d.byName[dep]; !ok {
				errs = append(errs, &DagInvalidDependencyError{Task: t.name, Dep: dep})
			}
		}
	}

	// Cycle detection (Kahn's algorithm over allDeps edges). Only run if no
	// missing-dep errors would make the graph ill-defined; but running it
	// anyway is safe because we only traverse known nodes.
	if cycle := detectCycle(d); len(cycle) > 0 {
		errs = append(errs, &DagCyclicDependencyError{Cycle: cycle})
	}

	if len(errs) == 0 {
		return nil
	}
	return &DagValidationError{Errs: errs}
}

// detectCycle returns a slice of task names forming a cycle, or nil if the
// graph is acyclic. Uses Kahn's algorithm: if not all nodes can be removed
// by repeatedly deleting zero-in-degree nodes, the remainder contains a
// cycle.
func detectCycle(d *Context) []string {
	// Build adjacency over known nodes only (dep -> dependents), and
	// in-degree per node.
	indeg := make(map[string]int, len(d.tasks))
	adj := make(map[string][]string, len(d.tasks))
	for _, t := range d.tasks {
		if _, ok := indeg[t.name]; !ok {
			indeg[t.name] = 0
		}
	}
	for _, t := range d.tasks {
		for _, dep := range t.allDeps() {
			if _, known := d.byName[dep]; !known {
				continue // missing deps handled elsewhere
			}
			adj[dep] = append(adj[dep], t.name)
			indeg[t.name]++
		}
	}

	// Queue of zero-in-degree nodes.
	var queue []string
	for name, deg := range indeg {
		if deg == 0 {
			queue = append(queue, name)
		}
	}
	removed := 0
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		removed++
		for _, m := range adj[n] {
			indeg[m]--
			if indeg[m] == 0 {
				queue = append(queue, m)
			}
		}
	}
	if removed == len(indeg) {
		return nil
	}
	// Collect the remaining (cyclic) nodes in registration order for a
	// stable, readable message.
	var cycle []string
	for _, t := range d.tasks {
		if indeg[t.name] > 0 {
			cycle = append(cycle, t.name)
		}
	}
	return cycle
}
