package dag

import (
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/operations"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// Base-SDK type aliases. DurableContext and StepContext are interfaces (never
// pointers). See DAG_SPEC_GO.md §2.3.

// DurableContext is the base SDK's full, operation-issuing context.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type DurableContext = types.DurableContext

// StepContext is the base SDK's step-body context (Logger/Attempt only).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type StepContext = types.StepContext

// Duration is the base SDK's duration type used by Wait/timeouts.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type Duration = types.Duration

// Serdes is the base SDK's (non-generic) serializer interface.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type Serdes = types.Serdes

// RetryStrategy is the base SDK's retry-decision function shape.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type RetryStrategy = func(err error, attempt int) types.RetryDecision

// BatchResult is the base SDK's batch result value type (returned by Map
// and Parallel tasks).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type BatchResult[T any] = operations.BatchResult[T]

// Void is the (empty) result type of a Wait task.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type Void = struct{}

// Branch is a named parallel branch for a Parallel task (names aid
// observability and result access).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type Branch[Out any] struct {
	Name string
	Func func(cctx DurableContext) (Out, error)
}

// ── task-fn shapes (deps-first, uniform) ──────────────────────────────────

// StepFunc is a step task body: it receives resolved Deps and a StepContext.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type StepFunc[T any] func(deps Deps, sctx StepContext) (T, error)

// PayloadFunc produces an Invoke task's payload from resolved Deps.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type PayloadFunc[In any] func(deps Deps) (In, error)

// SubmitterFunc submits a callback for a Callback task.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type SubmitterFunc func(deps Deps, sctx StepContext, callbackID string) error

// CheckFunc is a WaitForCondition task's poll body.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type CheckFunc[S any] func(deps Deps, state S, sctx StepContext) (S, error)

// ChildFunc is a Child (runInChildContext) task body.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type ChildFunc[T any] func(deps Deps, cctx DurableContext) (T, error)

// ItemsFunc produces a Map task's input items from resolved Deps.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type ItemsFunc[In any] func(deps Deps) []In

// MapFunc maps a single item within a Map task.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type MapFunc[In, Out any] func(cctx DurableContext, item In, index int) (Out, error)

// ── config / options ──────────────────────────────────────────────────────

// config carries both task-level and DAG-level settings; which fields apply
// depends on whether the option is passed to a registration function or to
// Dag(...).
type config struct {
	// task-level
	trigger       TriggerRule
	hasTrigger    bool
	runIf         func(Deps) bool
	retry         RetryStrategy
	serdes        Serdes
	timeout       *Duration
	conditionPred any // func(S) bool, erased

	// task-level, batch-only (Map/Parallel inner fan-out). Kept distinct
	// from the DAG-level maxConcurrency below so the two concurrency
	// meanings can never be confused (see WithBatchMaxConcurrency).
	batchMaxConcurrency *int

	// dag-level
	maxConcurrency *int
	defaultTrigger TriggerRule
	defaultRetry   RetryStrategy
	completion     *DagCompletionConfig
	summaryGen     func(*DagResult) string

	// applied records, in application order, which Option builders set this
	// config, so registration can reject options that do not apply to the
	// target operation (see validateTaskOptions).
	applied []optionID
}

// optionID identifies a functional Option builder so a task registration can
// validate that only options applicable to its operation were supplied.
type optionID int

const (
	optTrigger optionID = iota
	optRunIf
	optRetry
	optSerdes
	optTimeout
	optCondition
	optBatchMaxConcurrency
	optMaxConcurrency
	optDefaultTrigger
	optDefaultRetry
	optCompletion
	optSummaryGen
)

// optionName returns the customer-facing builder name for an option, for
// error messages.
func optionName(id optionID) string {
	switch id {
	case optTrigger:
		return "WithTriggerRule"
	case optRunIf:
		return "WithRunIf"
	case optRetry:
		return "WithRetry"
	case optSerdes:
		return "WithSerdes"
	case optTimeout:
		return "WithTimeout"
	case optCondition:
		return "WithCondition"
	case optBatchMaxConcurrency:
		return "WithBatchMaxConcurrency"
	case optMaxConcurrency:
		return "WithMaxConcurrency"
	case optDefaultTrigger:
		return "WithDefaultTriggerRule"
	case optDefaultRetry:
		return "WithDefaultRetry"
	case optCompletion:
		return "WithCompletion"
	case optSummaryGen:
		return "WithSummaryGenerator"
	default:
		return "unknown"
	}
}

// Option configures a task or a DAG (functional options).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type Option func(*config)

// WithTriggerRule sets a task's trigger rule.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithTriggerRule(r TriggerRule) Option {
	return func(c *config) { c.trigger = r; c.hasTrigger = true; c.applied = append(c.applied, optTrigger) }
}

// WithRunIf sets a task's conditional-execution predicate. If it returns
// false the task is skipped with SkipRunIf. The predicate must be
// deterministic.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithRunIf(pred func(deps Deps) bool) Option {
	return func(c *config) { c.runIf = pred; c.applied = append(c.applied, optRunIf) }
}

// WithRetry sets a task's retry strategy (applied to kinds that support one:
// Step, Callback submitter, WaitForCondition).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithRetry(s RetryStrategy) Option {
	return func(c *config) { c.retry = s; c.applied = append(c.applied, optRetry) }
}

// WithSerdes sets a task's custom result serializer.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithSerdes(s Serdes) Option {
	return func(c *config) { c.serdes = s; c.applied = append(c.applied, optSerdes) }
}

// WithTimeout sets a callback/condition task's timeout.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithTimeout(d Duration) Option {
	return func(c *config) { c.timeout = &d; c.applied = append(c.applied, optTimeout) }
}

// WithCondition sets a WaitForCondition task's completion predicate. It is
// REQUIRED for a WaitForCondition task; omitting it is a registration error
// (otherwise the poll would complete after a single iteration).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithCondition[S any](pred func(S) bool) Option {
	return func(c *config) { c.conditionPred = pred; c.applied = append(c.applied, optCondition) }
}

// WithMaxConcurrency bounds how many top-level DAG tasks run concurrently
// (the DAG fan-out limit). It is a DAG-level option: pass it to Dag(...),
// not to a task registration. A value <= 0 is a configuration error
// surfaced by Dag(...). To bound the inner fan-out of a Map/Parallel task,
// use WithBatchMaxConcurrency instead.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithMaxConcurrency(n int) Option {
	return func(c *config) { c.maxConcurrency = &n; c.applied = append(c.applied, optMaxConcurrency) }
}

// WithBatchMaxConcurrency bounds the inner fan-out of a Map or Parallel
// task (how many items/branches run concurrently within that one task). It
// is a task-level option accepted only by Map and Parallel; it is distinct
// from the DAG-level WithMaxConcurrency so the two concurrency meanings are
// never conflated.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithBatchMaxConcurrency(n int) Option {
	return func(c *config) { c.batchMaxConcurrency = &n; c.applied = append(c.applied, optBatchMaxConcurrency) }
}

// WithDefaultTriggerRule sets the default trigger rule for tasks that do not
// set their own.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithDefaultTriggerRule(r TriggerRule) Option {
	return func(c *config) { c.defaultTrigger = r; c.applied = append(c.applied, optDefaultTrigger) }
}

// WithDefaultRetry sets the default retry strategy for tasks that do not set
// their own.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithDefaultRetry(s RetryStrategy) Option {
	return func(c *config) { c.defaultRetry = s; c.applied = append(c.applied, optDefaultRetry) }
}

// WithCompletion sets the DAG's completion configuration (threshold or
// custom predicate; mutually exclusive).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithCompletion(cc DagCompletionConfig) Option {
	return func(c *config) { c.completion = &cc; c.applied = append(c.applied, optCompletion) }
}

// WithSummaryGenerator sets an observability-only summary generator whose
// output rides along on the serialized result and is never read on replay.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithSummaryGenerator(f func(*DagResult) string) Option {
	return func(c *config) { c.summaryGen = f; c.applied = append(c.applied, optSummaryGen) }
}

func buildConfig(opts []Option) config {
	var c config
	for _, o := range opts {
		if o != nil {
			o(&c)
		}
	}
	return c
}

// ── registration context ──────────────────────────────────────────────────

// Context is the opaque registration handle threaded into the registration
// free functions. It carries the ordered task registry, name set,
// accumulated registration errors, DAG-level config, and the name-derived
// prefix under which task IDs are minted.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type Context struct {
	tasks   []*taskDef
	byName  map[string]*taskDef
	regErrs []error
	prefix  string // parent prefix for name-based task IDs (set at run time)
	// defaultRetry is the DAG-level fallback retry strategy applied to
	// retry-supporting task kinds that set none of their own
	// (WithDefaultRetry). Set from the DAG-level config before register runs.
	defaultRetry RetryStrategy
}

func newContext(prefix string) *Context {
	return &Context{byName: map[string]*taskDef{}, prefix: prefix}
}

// opKind identifies the concrete DAG operation a task registers, so
// registration can validate that only options applicable to that operation
// were supplied (see validateTaskOptions).
type opKind int

const (
	opStep opKind = iota
	opInvoke
	opCallback
	opWait
	opCondition
	opChild
	opMap
	opParallel
	opSubDag
)

func (k opKind) String() string {
	switch k {
	case opStep:
		return "Step"
	case opInvoke:
		return "Invoke"
	case opCallback:
		return "Callback"
	case opWait:
		return "Wait"
	case opCondition:
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
// operation. Trigger/RunIf are common to every task; the rest are per-op.
// DAG-level options (maxConcurrency/default*/completion/summaryGen) apply to
// no task operation. SubDag intentionally accepts every option: the spec
// forwards its opts slice to both the task level and the nested DAG level,
// so validating it would break that documented dual-level behavior.
func optionApplicable(op opKind, id optionID) bool {
	if op == opSubDag {
		return true
	}
	switch id {
	case optTrigger, optRunIf:
		return true // common to all task operations
	case optRetry:
		return op == opStep || op == opCallback || op == opCondition
	case optSerdes:
		return op == opStep || op == opInvoke || op == opCallback || op == opCondition || op == opChild
	case optTimeout:
		return op == opCallback || op == opCondition
	case optCondition:
		return op == opCondition
	case optBatchMaxConcurrency:
		return op == opMap || op == opParallel
	default:
		// DAG-level options: not applicable to any task operation.
		return false
	}
}

// validateTaskOptions appends a DagInapplicableOptionError for each supplied
// option that does not apply to the target operation.
func (d *Context) validateTaskOptions(name string, op opKind, applied []optionID) {
	for _, id := range applied {
		if !optionApplicable(op, id) {
			d.regErrs = append(d.regErrs, &DagInapplicableOptionError{
				Task: name, Option: optionName(id), Op: op.String(),
			})
		}
	}
}

// register creates and records a task definition, applying task-level
// options and detecting duplicate names. op identifies the concrete
// operation so misapplied options are rejected at registration.
func (d *Context) register(name string, deps []AnyHandle, kind resultKind, op opKind, opts []Option) (*taskDef, config) {
	cfg := buildConfig(opts)
	d.validateTaskOptions(name, op, cfg.applied)
	// Apply the DAG-level default retry when the task set none of its own.
	if cfg.retry == nil {
		cfg.retry = d.defaultRetry
	}
	def := &taskDef{
		name:       name,
		id:         name, // name-based identity; hashed IDs are derived at run time
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
