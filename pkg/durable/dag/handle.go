package dag

// AnyHandle is a sealed, heterogeneous reference to a registered task, used
// in dependency lists ([]AnyHandle) and internal storage. Its methods are
// unexported so the interface cannot be implemented outside this package.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type AnyHandle interface {
	taskName() string
	taskID() string
	kindOf() resultKind
}

// TaskHandle is a typed reference to a registered task producing a value of
// type T. Registration free functions (Step, Invoke, ...) mint a handle;
// the builder methods DependsOn and WithTrigger mutate the underlying task
// definition and return the handle for chaining. The type parameter T is
// carried as a phantom so that Get[T]/Result[T] can return the task's value
// with its concrete type.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
type TaskHandle[T any] struct {
	name string
	id   string
	kind resultKind
	def  *taskDef
}

func (h TaskHandle[T]) taskName() string  { return h.name }
func (h TaskHandle[T]) taskID() string    { return h.id }
func (h TaskHandle[T]) kindOf() resultKind { return h.kind }

// DependsOn adds ordering-only dependency edges to this task (the upstream
// results are not injected into Deps, unlike the deps passed at
// registration). Returns the handle for chaining.
//
// Experimental: This API is experimental and may be changed or removed in
// future releases.
func (h TaskHandle[T]) DependsOn(deps ...AnyHandle) TaskHandle[T] {
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

// taskDef is the internal, kind-erased definition of a registered task.
type taskDef struct {
	name       string
	id         string
	kind       resultKind
	inlineDeps []string // results injected into Deps (from the deps arg)
	orderDeps  []string // ordering-only edges (from DependsOn)
	trigger    TriggerRule
	hasTrigger bool
	runIf      func(Deps) bool
	// run executes the task body against its own (name-derived) child
	// context, returning the kind-erased result value or an error.
	run func(taskCtx DurableContext, deps Deps) (any, error)
}

// allDeps returns the union of inline and ordering-only dependency names,
// de-duplicated, preserving first-seen order.
func (t *taskDef) allDeps() []string {
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
