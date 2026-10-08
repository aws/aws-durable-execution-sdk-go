package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// AnyBranch is one branch of a [ParallelMixed] call. It hides the branch
// result type so branches of different types share one slice. The
// interface is sealed; [NewTypedBranch] is the only constructor.
type AnyBranch interface {
	// mixedBranch returns the type-erased view of the branch. Its
	// unexported name seals the interface.
	mixedBranch() mixedBranch

	// isNil reports whether the branch is a nil *TypedBranch. Such a
	// value makes the interface non-nil, so ParallelMixed asks it.
	isNil() bool
}

// mixedBranch is the type-erased view of a [TypedBranch] that
// [ParallelMixed] runs.
type mixedBranch struct {
	name string

	// fn is the branch body. It returns the branch value as any, so one
	// batch runs branches of different result types.
	fn func(Context) (any, error)

	// userFn is the user's function value. A failure's stack trace names
	// it as its origin frame, as it would name a [Branch] Func.
	userFn any

	// serdes is the branch serdes, or nil for the batch item serdes.
	serdes Serdes

	// bind records the call the branch was passed to and its index.
	bind func(call *mixedCall, index int)
}

// TypedBranch is a branch whose body returns a value of type T. Build one
// with [NewTypedBranch], pass it to [ParallelMixed], and read its value
// from the returned [BatchResult] with [TypedBranch.Result].
type TypedBranch[T any] struct {
	name   string
	fn     func(Context) (T, error)
	serdes Serdes

	// mu guards bound. ParallelMixed writes it and Result reads it, and
	// the two may run on different goroutines.
	mu    sync.Mutex
	bound *mixedBinding
}

// mixedBinding ties a branch to one ParallelMixed call and to its
// zero-based position in that call's branches.
type mixedBinding struct {
	call  *mixedCall
	index int
}

// TypedBranchOption configures a [TypedBranch] at construction.
type TypedBranchOption interface {
	applyTypedBranch(*typedBranchConfig)
}

type typedBranchConfig struct {
	serdes Serdes
}

type typedBranchOptionFunc func(*typedBranchConfig)

func (f typedBranchOptionFunc) applyTypedBranch(c *typedBranchConfig) { f(c) }

// WithTypedBranchSerdes sets the serializer for one branch's value. It
// marshals the value the branch body returns, and [TypedBranch.Result]
// unmarshals the stored bytes with it. Without it the branch uses the
// batch item serdes: the [WithBatchSerdes] serdes when set, else the
// handler serdes. The SDK stores the bytes the serdes returns unchanged,
// so they need not be JSON.
func WithTypedBranchSerdes(s Serdes) TypedBranchOption {
	return typedBranchOptionFunc(func(c *typedBranchConfig) { c.serdes = s })
}

// NewTypedBranch builds a branch. name is recorded in the history. fn is the
// branch body and receives the branch's own Context. WithTypedBranchSerdes
// overrides the handler serdes for this branch.
//
// An empty name names the branch "parallel-branch-<index>", as in
// [Parallel].
func NewTypedBranch[T any](name string, fn func(Context) (T, error), opts ...TypedBranchOption) *TypedBranch[T] {
	var cfg typedBranchConfig
	for _, o := range opts {
		if o != nil {
			o.applyTypedBranch(&cfg)
		}
	}
	return &TypedBranch[T]{name: name, fn: fn, serdes: cfg.serdes}
}

func (b *TypedBranch[T]) isNil() bool { return b == nil }

func (b *TypedBranch[T]) mixedBranch() mixedBranch {
	return mixedBranch{
		name:   b.name,
		userFn: b.fn,
		serdes: b.serdes,
		fn: func(ctx Context) (any, error) {
			if b.fn == nil {
				return nil, fmt.Errorf("durable: branch %q has a nil function", b.name)
			}
			v, err := b.fn(ctx)
			if err != nil {
				return nil, err
			}
			return v, nil
		},
		bind: func(call *mixedCall, index int) {
			b.mu.Lock()
			b.bound = &mixedBinding{call: call, index: index}
			b.mu.Unlock()
		},
	}
}

// Result returns this branch's value from res. It returns the decoded value
// on success, the branch's own item error on failure, and a
// *BranchNotCompletedError when the branch produced no result (it never
// started, or it was abandoned on early completion). It returns an error
// when res did not come from the ParallelMixed call this branch was passed
// to, or the branch was never passed to a ParallelMixed call.
//
// The value is decoded from the item's stored bytes into T with the
// branch serdes. T is the branch's static type, so the first run and a
// replay return the same concrete type and the same value. A decode
// failure returns a [*SerdesError].
//
// When a branch is passed to several ParallelMixed calls, it belongs to
// the most recent one.
func (b *TypedBranch[T]) Result(res BatchResult[json.RawMessage]) (T, error) {
	var zero T
	b.mu.Lock()
	bound := b.bound
	b.mu.Unlock()
	if bound == nil {
		return zero, fmt.Errorf("durable: branch %q was never passed to a ParallelMixed call", b.name)
	}
	if res.mixed != bound.call {
		return zero, fmt.Errorf("durable: branch %q read with the result of a different ParallelMixed call", b.name)
	}
	call := bound.call
	name := call.itemName(bound.index)

	// Items that never started are omitted from the result, so the lookup
	// is by input index, not by slice position.
	var item *BatchItem[json.RawMessage]
	for i := range res.Items {
		if res.Items[i].Index == bound.index {
			item = &res.Items[i]
			break
		}
	}
	if item == nil {
		return zero, &BranchNotCompletedError{Name: name, Index: bound.index, Status: BatchItemNotStarted}
	}
	switch item.Status {
	case BatchItemSucceeded:
		var out T
		s := call.branchSerdes(bound.index)
		if err := s.Unmarshal(call.ec.Context, call.sctx(bound.index), item.Result, &out); err != nil {
			return zero, call.ec.serdesFailure(batchItemOpName(item.Name, item.Index), serdesDirectionUnmarshal, err)
		}
		return out, nil
	case BatchItemFailed:
		return zero, item.Err
	default:
		return zero, &BranchNotCompletedError{Name: name, Index: bound.index, Status: item.Status}
	}
}

// BranchNotCompletedError is returned by TypedBranch.Result when the branch
// produced no result: it never started, or it started and was abandoned
// when the batch completed early. It is distinct from the branch's own item
// error, which Result returns directly.
type BranchNotCompletedError struct {
	Name   string          // the branch name
	Index  int             // the branch's zero-based position in the input slice
	Status BatchItemStatus // BatchItemNotStarted or BatchItemStarted
}

// Error returns `durable: branch %q (index %d) produced no result: %s`,
// where the final verb phrase is "the branch never started" when Status is
// BatchItemNotStarted and "the branch was abandoned when the batch
// completed early" when Status is BatchItemStarted.
func (e *BranchNotCompletedError) Error() string {
	reason := "the branch never started"
	if e.Status == BatchItemStarted {
		reason = "the branch was abandoned when the batch completed early"
	}
	return fmt.Sprintf("durable: branch %q (index %d) produced no result: %s", e.Name, e.Index, reason)
}

// ParallelMixed runs branches of different result types concurrently. Every
// BatchOption applies unchanged. The returned BatchResult carries an
// unexported identity of this call, which TypedBranch.Result checks. The
// error is the Parallel error unchanged: a *BatchError or
// *BatchCompletionError when the batch fails as a unit, or a suspension or
// SDK error to propagate. A branch failure within the completion
// tolerance returns a nil error; read it with TypedBranch.Result.
//
// ParallelMixed records the same operations and stores the same payloads
// as a [Parallel] over the branch values. Each branch value is marshaled
// once, by the branch serdes, with the branch's own [SerdesContext], and
// the batch stores those bytes. Each item of the returned result holds
// its branch's stored bytes in Result. Read a branch value with
// [TypedBranch.Result], which decodes those bytes into the branch type:
//
//	user := durable.NewTypedBranch("user", fetchUser)    // *TypedBranch[User]
//	count := durable.NewTypedBranch("count", countItems) // *TypedBranch[int]
//	res, err := durable.ParallelMixed(ctx, "load", []durable.AnyBranch{user, count})
//	if err != nil {
//		return err
//	}
//	u, err := user.Result(res)  // User
//	n, err := count.Result(res) // int
//
// Completion and failure follow [Parallel]. A [WithBatchSummary] function
// for a ParallelMixed call has the type func(BatchResult[json.RawMessage])
// string; it receives each branch's stored bytes. The identity of the
// call is never checkpointed, so it does not change the stored record and
// takes no part in replay.
//
// A nil branch, including a nil *TypedBranch, or a branch passed twice in
// one call, is an error, and
// ParallelMixed then records nothing.
func ParallelMixed(ctx Context, name string, branches []AnyBranch, opts ...BatchOption) (BatchResult[json.RawMessage], error) {
	ec, ok := ctx.(*execContext)
	if !ok {
		return BatchResult[json.RawMessage]{}, fmt.Errorf("durable: ParallelMixed %q: Context was not created by the SDK", name)
	}

	views := make([]mixedBranch, len(branches))
	seen := make(map[AnyBranch]bool, len(branches))
	for i, b := range branches {
		if b == nil || b.isNil() {
			return BatchResult[json.RawMessage]{}, fmt.Errorf("durable: ParallelMixed %q: branch %d is nil", name, i)
		}
		if seen[b] {
			return BatchResult[json.RawMessage]{}, fmt.Errorf("durable: ParallelMixed %q: branch %d is passed more than once", name, i)
		}
		seen[b] = true
		views[i] = b.mixedBranch()
	}

	options := resolveBatchOptions(ec, opts)
	options.defaultNamePrefix = defaultParallelBranchNamePrefix
	if options.itemNamer == nil {
		options.itemNamer = func(index int) string {
			if index < len(views) {
				return views[index].name
			}
			return ""
		}
	}

	if options.maxConcurrencySet && options.maxConcurrency <= 0 {
		return BatchResult[json.RawMessage]{}, fmt.Errorf("durable: ParallelMixed %q: max concurrency must be positive, got %d", name, options.maxConcurrency)
	}
	if err := options.completion.validate(); err != nil {
		return BatchResult[json.RawMessage]{}, fmt.Errorf("durable: ParallelMixed %q: %w", name, err)
	}
	if _, err := batchSummaryFunc[json.RawMessage](options); err != nil {
		return BatchResult[json.RawMessage]{}, fmt.Errorf("durable: ParallelMixed %q: %w", name, err)
	}

	id, err := ec.claimOperation(name)
	if err != nil {
		return BatchResult[json.RawMessage]{}, err
	}
	options.itemID = batchItemIDs(ec, id, options.nesting)

	call := &mixedCall{
		ec:       ec,
		options:  options,
		base:     options.itemSerdes,
		branches: views,
		byOpID:   make(map[string]int, len(views)),
		values:   make(map[int]any),
	}
	for i := range views {
		call.byOpID[options.itemID(i)] = i
	}
	options.itemSerdes = mixedItemSerdes{call: call}
	options.itemUserFn = func(index int) any { return views[index].userFn }

	sdkBranches := make([]Branch[json.RawMessage], len(views))
	for i := range views {
		sdkBranches[i] = Branch[json.RawMessage]{
			Name: views[i].name,
			Func: func(c Context) (json.RawMessage, error) {
				v, err := views[i].fn(c)
				if err != nil {
					return nil, err
				}
				call.store(i, v)
				return nil, nil
			},
		}
	}
	for i := range views {
		views[i].bind(call, i)
	}

	result, err := runClaimedParallel(ec, id, name, sdkBranches, options)
	ec.observeOutcome(id, err)
	result.mixed = call
	return result, err
}

// mixedCall is the identity of one ParallelMixed call. It is the value
// [BatchResult] carries so [TypedBranch.Result] can check that a branch is
// read with the result of the call it was passed to. It also holds the
// typed value of each branch from the moment the branch body returns
// until the batch marshals it.
type mixedCall struct {
	ec       *execContext
	options  batchOptions
	base     Serdes
	branches []mixedBranch

	// byOpID maps an item's operation ID to its input index. The batch
	// keys every serdes call for an item on the item's operation ID, so
	// mixedItemSerdes finds the branch from the SerdesContext.
	byOpID map[string]int

	mu     sync.Mutex
	values map[int]any
}

// store records the value the body of branch index returned. The batch
// marshals it next, on the same goroutine.
func (c *mixedCall) store(index int, v any) {
	c.mu.Lock()
	c.values[index] = v
	c.mu.Unlock()
}

// take returns and forgets the value stored for branch index.
func (c *mixedCall) take(index int) (any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.values[index]
	delete(c.values, index)
	return v, ok
}

// branchSerdes returns the serdes of branch index: its own, else the
// batch item serdes.
func (c *mixedCall) branchSerdes(index int) Serdes {
	if s := c.branches[index].serdes; s != nil {
		return s
	}
	return c.base
}

// sctx returns the SerdesContext of branch index.
func (c *mixedCall) sctx(index int) SerdesContext {
	return c.ec.serdesCtx(c.options.itemID(index))
}

// itemName returns the recorded name of branch index.
func (c *mixedCall) itemName(index int) string {
	return itemNameForIndex(c.options, index)
}

// mixedItemSerdes is the item serdes of a ParallelMixed batch. Marshal
// encodes the typed value the branch body returned with the branch
// serdes, so the batch stores the branch serdes bytes once. Unmarshal
// keeps the stored bytes as the item's json.RawMessage result unchanged;
// [TypedBranch.Result] decodes them into the branch type.
type mixedItemSerdes struct {
	call *mixedCall
}

// errMixedNoValue reports a marshal of a branch item whose body produced
// no value in this invocation. The batch marshals an item only right after
// its body returns, so it indicates an SDK defect.
var errMixedNoValue = errors.New("durable: ParallelMixed: no branch value to marshal")

func (s mixedItemSerdes) Marshal(ctx context.Context, sctx SerdesContext, _ any) ([]byte, error) {
	index, ok := s.call.byOpID[sctx.OperationID]
	if !ok {
		return nil, fmt.Errorf("durable: ParallelMixed: no branch has operation ID %q", sctx.OperationID)
	}
	v, ok := s.call.take(index)
	if !ok {
		return nil, errMixedNoValue
	}
	return s.call.branchSerdes(index).Marshal(ctx, sctx, v)
}

func (s mixedItemSerdes) Unmarshal(_ context.Context, _ SerdesContext, data []byte, v any) error {
	out, ok := v.(*json.RawMessage)
	if !ok {
		return fmt.Errorf("durable: ParallelMixed: cannot decode into %T", v)
	}
	if data == nil {
		*out = nil
		return nil
	}
	*out = append(json.RawMessage{}, data...)
	return nil
}
