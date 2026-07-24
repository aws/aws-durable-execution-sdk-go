package durable

// AnyHandle is a sealed, heterogeneous reference to a registered DAG task,
// used in dependency lists ([]AnyHandle) and internal storage. Its methods
// are unexported so the interface cannot be implemented outside this
// package.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type AnyHandle interface {
	taskName() string
	taskID() string
	kindOf() dagResultKind
}

// TaskHandle is a typed reference to a registered DAG task producing a
// value of type T. Registration free functions ([DagStep], [DagInvoke],
// ...) mint a handle; the builder methods [TaskHandle.After] and
// [TaskHandle.WithTrigger] mutate the underlying task definition and return
// the handle for chaining. The type parameter T is carried as a phantom so
// that [Get] / [Result] can return the task's value with its concrete type.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type TaskHandle[T any] struct {
	name string
	id   string
	kind dagResultKind
	def  *dagTaskDef
}

func (h TaskHandle[T]) taskName() string      { return h.name }
func (h TaskHandle[T]) taskID() string        { return h.id }
func (h TaskHandle[T]) kindOf() dagResultKind { return h.kind }

// After adds ordering-only dependency edges to this task (the upstream
// results are not injected into [Deps], unlike the deps passed at
// registration). Returns the handle for chaining.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (h TaskHandle[T]) After(deps ...AnyHandle) TaskHandle[T] {
	if h.def != nil {
		for _, d := range deps {
			h.def.orderDeps = append(h.def.orderDeps, d.taskName())
		}
	}
	return h
}

// WithTrigger sets this task's trigger rule, overriding the default.
// Returns the handle for chaining.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (h TaskHandle[T]) WithTrigger(rule TriggerRule) TaskHandle[T] {
	if h.def != nil {
		h.def.trigger = rule
		h.def.hasTrigger = true
	}
	return h
}

// dagTaskDef is the internal, kind-erased definition of a registered task.
type dagTaskDef struct {
	name       string
	id         string
	kind       dagResultKind
	inlineDeps []string // results injected into Deps (from the deps arg)
	orderDeps  []string // ordering-only edges (from After)
	trigger    TriggerRule
	hasTrigger bool
	runIf      func(Deps) bool
	// run executes the task body against its own child context, returning
	// the kind-erased result value or an error.
	run func(taskCtx Context, deps Deps) (any, error)
}

// allDeps returns the union of inline and ordering-only dependency names,
// de-duplicated, preserving first-seen order.
func (t *dagTaskDef) allDeps() []string {
	seen := make(map[string]struct{}, len(t.inlineDeps)+len(t.orderDeps))
	out := make([]string, 0, len(t.inlineDeps)+len(t.orderDeps))
	for _, d := range append(append([]string{}, t.inlineDeps...), t.orderDeps...) {
		if _, ok := seen[d]; ok {
			continue
		}
		seen[d] = struct{}{}
		out = append(out, d)
	}
	return out
}
