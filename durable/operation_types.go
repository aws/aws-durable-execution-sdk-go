package durable

// Operation subtypes.
//
// Every durable operation the SDK records carries a type and a subtype.
// The type is one of the [OperationType] constants and says what kind of
// record the service keeps: a step, a wait, a callback, a chained invoke,
// or a context. The subtype names the SDK function that created the
// operation and distinguishes functions that share a type: [Map],
// [Parallel], [RunInChildContext], and [WaitForCallback] all record
// [OperationTypeContext] operations, and each Map item or Parallel branch
// is itself a context operation with its own subtype. A child context is
// the one operation whose subtype the caller may set, with
// [WithChildSubType].
//
// These constants are the values the SDK writes to the SubType field of
// each checkpointed operation and reports in [OperationHookInfo].SubType.
// They are untyped string constants so they compare directly with those
// string fields. Plugins should compare against these constants rather
// than string literals.
const (
	// OperationSubTypeStep is the subtype of a [Step] or [StepAsync]
	// operation. Its type is [OperationTypeStep].
	OperationSubTypeStep = "Step"

	// OperationSubTypeWait is the subtype of a [Wait] or [WaitAsync]
	// operation. Its type is [OperationTypeWait].
	OperationSubTypeWait = "Wait"

	// OperationSubTypeCallback is the subtype of a [CreateCallback]
	// operation, including the callback that [WaitForCallback] creates
	// inside its context. Its type is [OperationTypeCallback].
	OperationSubTypeCallback = "Callback"

	// OperationSubTypeChainedInvoke is the subtype of an [Invoke] or
	// [InvokeAsync] operation. Its type is [OperationTypeChainedInvoke].
	OperationSubTypeChainedInvoke = "ChainedInvoke"

	// OperationSubTypeRunInChildContext is the default subtype of a
	// [RunInChildContext], [RunInChildContextAsync], or [Go] operation;
	// [WithChildSubType] records a caller-defined subtype instead. Its
	// type is [OperationTypeContext].
	OperationSubTypeRunInChildContext = "RunInChildContext"

	// OperationSubTypeWaitForCallback is the subtype of the context
	// operation that [WaitForCallback] records around its callback and
	// submitter step. Its type is [OperationTypeContext].
	OperationSubTypeWaitForCallback = "WaitForCallback"

	// OperationSubTypeWaitForCondition is the subtype of a
	// [WaitForCondition] operation. Its type is [OperationTypeStep].
	OperationSubTypeWaitForCondition = "WaitForCondition"

	// OperationSubTypeMap is the subtype of the parent operation a [Map]
	// call records. Its type is [OperationTypeContext].
	OperationSubTypeMap = "Map"

	// OperationSubTypeMapIteration is the subtype of the child operation
	// recorded for each item of a [Map]. Its type is
	// [OperationTypeContext], and its parent is the Map operation.
	OperationSubTypeMapIteration = "MapIteration"

	// OperationSubTypeParallel is the subtype of the parent operation a
	// [Parallel] call records. Its type is [OperationTypeContext].
	OperationSubTypeParallel = "Parallel"

	// OperationSubTypeParallelBranch is the subtype of the child operation
	// recorded for each branch of a [Parallel]. Its type is
	// [OperationTypeContext], and its parent is the Parallel operation.
	OperationSubTypeParallelBranch = "ParallelBranch"
)
