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
	trigger      TriggerRule
	hasTrigger   bool
	runIf        func(Deps) bool
	retry        RetryStrategy
	serdes       Serdes
	timeout      *Duration
	initialState any
	conditionPred any // func(S) bool, erased

	// dag-level
	maxConcurrency  *int
	defaultTrigger  TriggerRule
	defaultRetry    RetryStrategy
	completion      *DagCompletionConfig
	summaryGen      func(*DagResult) string
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
	return func(c *config) { c.trigger = r; c.hasTrigger = true }
}

// WithRunIf sets a task's conditional-execution predicate. If it returns
// false the task is skipped with SkipRunIf. The predicate must be
// deterministic.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithRunIf(pred func(deps Deps) bool) Option {
	return func(c *config) { c.runIf = pred }
}

// WithRetry sets a task's retry strategy (applied to kinds that support one:
// Step, Callback submitter, WaitForCondition).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithRetry(s RetryStrategy) Option { return func(c *config) { c.retry = s } }

// WithSerdes sets a task's custom result serializer.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithSerdes(s Serdes) Option { return func(c *config) { c.serdes = s } }

// WithTimeout sets a callback/condition task's timeout.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithTimeout(d Duration) Option { return func(c *config) { c.timeout = &d } }

// WithInitialState sets a WaitForCondition task's initial state. It is a
// generic free function (returning an Option) because Go methods cannot be
// generic; the value is stored type-erased and recovered by the
// WaitForCondition registration function.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithInitialState[S any](s S) Option {
	return func(c *config) { c.initialState = s }
}

// WithCondition sets a WaitForCondition task's completion predicate.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithCondition[S any](pred func(S) bool) Option {
	return func(c *config) { c.conditionPred = pred }
}

// WithMaxConcurrency bounds how many top-level DAG tasks run concurrently.
// A value <= 0 is a configuration error surfaced by Dag(...).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithMaxConcurrency(n int) Option { return func(c *config) { c.maxConcurrency = &n } }

// WithDefaultTriggerRule sets the default trigger rule for tasks that do not
// set their own.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithDefaultTriggerRule(r TriggerRule) Option {
	return func(c *config) { c.defaultTrigger = r }
}

// WithDefaultRetry sets the default retry strategy for tasks that do not set
// their own.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithDefaultRetry(s RetryStrategy) Option { return func(c *config) { c.defaultRetry = s } }

// WithCompletion sets the DAG's completion configuration (threshold or
// custom predicate; mutually exclusive).
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithCompletion(cc DagCompletionConfig) Option {
	return func(c *config) { c.completion = &cc }
}

// WithSummaryGenerator sets an observability-only summary generator whose
// output rides along on the serialized result and is never read on replay.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func WithSummaryGenerator(f func(*DagResult) string) Option {
	return func(c *config) { c.summaryGen = f }
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
}

func newContext(prefix string) *Context {
	return &Context{byName: map[string]*taskDef{}, prefix: prefix}
}

// register creates and records a task definition, applying task-level
// options and detecting duplicate names.
func (d *Context) register(name string, deps []AnyHandle, kind resultKind, opts []Option) (*taskDef, config) {
	cfg := buildConfig(opts)
	def := &taskDef{
		name:    name,
		id:      name, // name-based identity; hashed IDs are derived at run time
		kind:    kind,
		trigger: cfg.trigger,
		hasTrigger: cfg.hasTrigger,
		runIf:   cfg.runIf,
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
