package durable

import "time"

// ── task-fn shapes (deps-first, uniform) ──────────────────────────────────

// DagStepFunc is a step task body: it receives resolved [Deps] and a
// [StepContext].
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagStepFunc[T any] func(deps Deps, sctx StepContext) (T, error)

// DagPayloadFunc produces an Invoke task's payload from resolved [Deps].
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagPayloadFunc[In any] func(deps Deps) (In, error)

// DagSubmitterFunc submits a callback for a Callback task.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagSubmitterFunc func(deps Deps, sctx StepContext, callbackID string) error

// DagCheckFunc is a WaitForCondition task's poll body.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagCheckFunc[S any] func(deps Deps, state S, sctx StepContext) (S, error)

// DagChildFunc is a Child (RunInChildContext) task body.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagChildFunc[T any] func(deps Deps, cctx Context) (T, error)

// DagItemsFunc produces a Map task's input items from resolved [Deps].
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagItemsFunc[In any] func(deps Deps) []In

// DagMapFunc maps a single item within a Map task.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagMapFunc[In, Out any] func(cctx Context, item In, index int) (Out, error)

// ── config / options ──────────────────────────────────────────────────────

// dagConfig carries both task-level and DAG-level settings; which fields
// apply depends on whether the option is passed to a registration function
// or to [Dag].
type dagConfig struct {
	// task-level
	trigger       TriggerRule
	hasTrigger    bool
	runIf         func(Deps) bool
	retry         RetryStrategy
	serdes        Serdes
	timeout       *time.Duration
	conditionPred any // func(S) bool, erased

	// task-level, batch-only (Map/Parallel inner fan-out).
	batchMaxConcurrency *int

	// dag-level
	maxConcurrency *int
	defaultTrigger TriggerRule
	defaultRetry   RetryStrategy
	dagSerdes      Serdes
	completion     *DagCompletionConfig
	summaryGen     func(*DagResult) string

	// applied records, in application order, which option builders set this
	// config, so registration can reject options that do not apply to the
	// target operation.
	applied []dagOptionID
}

// dagOptionID identifies a functional [DagOption] builder.
type dagOptionID int

const (
	optTrigger dagOptionID = iota
	optRunIf
	optRetry
	optSerdes
	optTimeout
	optCondition
	optBatchMaxConcurrency
	optMaxConcurrency
	optDefaultTrigger
	optDefaultRetry
	optDagSerdes
	optCompletion
	optSummaryGen
)

func optionName(id dagOptionID) string {
	switch id {
	case optTrigger:
		return "WithTriggerRule"
	case optRunIf:
		return "WithRunIf"
	case optRetry:
		return "WithTaskRetry"
	case optSerdes:
		return "WithDagTaskSerdes"
	case optTimeout:
		return "WithTaskTimeout"
	case optCondition:
		return "WithCondition"
	case optBatchMaxConcurrency:
		return "WithBatchMaxConcurrency"
	case optMaxConcurrency:
		return "WithDagMaxConcurrency"
	case optDefaultTrigger:
		return "WithDefaultTriggerRule"
	case optDefaultRetry:
		return "WithDefaultRetry"
	case optDagSerdes:
		return "WithDagSerdes"
	case optCompletion:
		return "WithDagCompletion"
	case optSummaryGen:
		return "WithSummaryGenerator"
	default:
		return "unknown"
	}
}

// DagOption configures a DAG task or a whole DAG (functional options).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagOption func(*dagConfig)

// WithTriggerRule sets a task's trigger rule.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithTriggerRule(r TriggerRule) DagOption {
	return func(c *dagConfig) { c.trigger = r; c.hasTrigger = true; c.applied = append(c.applied, optTrigger) }
}

// WithRunIf sets a task's conditional-execution predicate. If it returns
// false the task is skipped with [SkipRunIf]. The predicate must be
// deterministic.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithRunIf(pred func(deps Deps) bool) DagOption {
	return func(c *dagConfig) { c.runIf = pred; c.applied = append(c.applied, optRunIf) }
}

// WithTaskRetry sets a task's retry strategy (applied to kinds that support
// one: Step, Callback submitter, WaitForCondition).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithTaskRetry(s RetryStrategy) DagOption {
	return func(c *dagConfig) { c.retry = s; c.applied = append(c.applied, optRetry) }
}

// WithDagTaskSerdes sets a task's custom result serializer.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithDagTaskSerdes(s Serdes) DagOption {
	return func(c *dagConfig) { c.serdes = s; c.applied = append(c.applied, optSerdes) }
}

// WithTaskTimeout sets a callback/condition task's timeout.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithTaskTimeout(d time.Duration) DagOption {
	return func(c *dagConfig) { c.timeout = &d; c.applied = append(c.applied, optTimeout) }
}

// WithCondition sets a WaitForCondition task's completion predicate. It is
// REQUIRED for a WaitForCondition task; omitting it is a registration
// error.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithCondition[S any](pred func(S) bool) DagOption {
	return func(c *dagConfig) { c.conditionPred = pred; c.applied = append(c.applied, optCondition) }
}

// WithBatchMaxConcurrency bounds the inner fan-out of a Map or Parallel
// task (how many items/branches run concurrently within that one task). It
// is a task-level option accepted only by Map and Parallel; it is distinct
// from the DAG-level [WithDagMaxConcurrency].
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithBatchMaxConcurrency(n int) DagOption {
	return func(c *dagConfig) { c.batchMaxConcurrency = &n; c.applied = append(c.applied, optBatchMaxConcurrency) }
}

// WithDagMaxConcurrency bounds how many top-level DAG tasks run
// concurrently (the DAG fan-out limit). It is a DAG-level option: pass it
// to [Dag], not to a task registration. A value <= 0 is a configuration
// error. When this option is not set, the DAG defaults to
// [DefaultDagMaxConcurrency] (40) top-level tasks rather than running
// unbounded. Since a value <= 0 is rejected, the API can no longer express
// a genuinely unbounded scheduler; pass a bound at least as large as the
// task count (e.g. math.MaxInt32) for effectively-unbounded behavior. To
// bound the inner fan-out of a Map/Parallel task, use
// [WithBatchMaxConcurrency].
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithDagMaxConcurrency(n int) DagOption {
	return func(c *dagConfig) { c.maxConcurrency = &n; c.applied = append(c.applied, optMaxConcurrency) }
}

// WithDefaultTriggerRule sets the default trigger rule for tasks that do
// not set their own.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithDefaultTriggerRule(r TriggerRule) DagOption {
	return func(c *dagConfig) { c.defaultTrigger = r; c.applied = append(c.applied, optDefaultTrigger) }
}

// WithDefaultRetry sets the default retry strategy for tasks that do not
// set their own.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithDefaultRetry(s RetryStrategy) DagOption {
	return func(c *dagConfig) { c.defaultRetry = s; c.applied = append(c.applied, optDefaultRetry) }
}

// WithDagSerdes sets the DAG-level default result serializer applied to
// tasks that do not set their own via [WithDagTaskSerdes]. It is a
// DAG-level option: pass it to [Dag].
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithDagSerdes(s Serdes) DagOption {
	return func(c *dagConfig) { c.dagSerdes = s; c.applied = append(c.applied, optDagSerdes) }
}

// WithDagCompletion sets the DAG's completion configuration (threshold or
// custom predicate; mutually exclusive).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithDagCompletion(cc DagCompletionConfig) DagOption {
	return func(c *dagConfig) { c.completion = &cc; c.applied = append(c.applied, optCompletion) }
}

// WithSummaryGenerator sets an observability-only summary generator whose
// output rides along on the result and is never read on replay.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithSummaryGenerator(f func(*DagResult) string) DagOption {
	return func(c *dagConfig) { c.summaryGen = f; c.applied = append(c.applied, optSummaryGen) }
}

func buildDagConfig(opts []DagOption) dagConfig {
	var c dagConfig
	for _, o := range opts {
		if o != nil {
			o(&c)
		}
	}
	return c
}

// ── registration handle ────────────────────────────────────────────────

// DagBuilder is the registration handle threaded into the register callback
// of [Dag]. It carries the ordered task registry, name set, accumulated
// registration errors, and DAG-level defaults.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DagBuilder struct {
	tasks         []*dagTaskDef
	byName        map[string]*dagTaskDef
	regErrs       []error
	defaultRetry  RetryStrategy
	defaultSerdes Serdes
}

func newDagBuilder() *DagBuilder {
	return &DagBuilder{byName: map[string]*dagTaskDef{}}
}

// dagOpKind identifies the concrete DAG operation a task registers.
type dagOpKind int

const (
	opStep dagOpKind = iota
	opInvoke
	opCallback
	opWait
	opConditionOp
	opChild
	opMap
	opParallel
	opSubDag
)

func (k dagOpKind) String() string {
	switch k {
	case opStep:
		return "Step"
	case opInvoke:
		return "Invoke"
	case opCallback:
		return "Callback"
	case opWait:
		return "Wait"
	case opConditionOp:
		return "WaitForCondition"
	case opChild:
		return "Child"
	case opMap:
		return "Map"
	case opParallel:
		return "Parallel"
	case opSubDag:
		return "SubDag"
	default:
		return "unknown"
	}
}

// optionApplicable reports whether a given option applies to a given
// operation. SubDag intentionally accepts every option: its opts slice is
// forwarded to both the task level and the nested DAG level.
func optionApplicable(op dagOpKind, id dagOptionID) bool {
	if op == opSubDag {
		return true
	}
	switch id {
	case optTrigger, optRunIf:
		return true // common to all task operations
	case optRetry:
		return op == opStep || op == opCallback || op == opConditionOp
	case optSerdes:
		return op == opStep || op == opInvoke || op == opCallback || op == opConditionOp || op == opChild
	case optTimeout:
		return op == opCallback || op == opConditionOp
	case optCondition:
		return op == opConditionOp
	case optBatchMaxConcurrency:
		return op == opMap || op == opParallel
	default:
		// DAG-level options: not applicable to any task operation.
		return false
	}
}

func (d *DagBuilder) validateTaskOptions(name string, op dagOpKind, applied []dagOptionID) {
	for _, id := range applied {
		if !optionApplicable(op, id) {
			d.regErrs = append(d.regErrs, &DagInapplicableOptionError{
				Task: name, Option: optionName(id), Op: op.String(),
			})
		}
	}
}

// register creates and records a task definition, applying task-level
// options and detecting duplicate names.
func (d *DagBuilder) register(name string, deps []AnyHandle, kind dagResultKind, op dagOpKind, opts []DagOption) (*dagTaskDef, dagConfig) {
	cfg := buildDagConfig(opts)
	d.validateTaskOptions(name, op, cfg.applied)
	// Apply DAG-level defaults when the task set none of its own.
	if cfg.retry == nil {
		cfg.retry = d.defaultRetry
	}
	if cfg.serdes == nil {
		cfg.serdes = d.defaultSerdes
	}
	def := &dagTaskDef{
		name:       name,
		id:         name,
		kind:       kind,
		trigger:    cfg.trigger,
		hasTrigger: cfg.hasTrigger,
		runIf:      cfg.runIf,
	}
	for _, dep := range deps {
		def.inlineDeps = append(def.inlineDeps, dep.taskName())
	}
	if _, exists := d.byName[name]; exists {
		d.regErrs = append(d.regErrs, &DagDuplicateTaskError{Name: name})
	} else {
		d.byName[name] = def
	}
	d.tasks = append(d.tasks, def)
	return def, cfg
}
