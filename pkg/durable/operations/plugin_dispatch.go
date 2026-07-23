package operations

import (
	"time"

	dcontext "github.com/aws/aws-durable-execution-sdk-go/pkg/durable/context"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/plugin"
	"github.com/aws/aws-durable-execution-sdk-go/pkg/durable/types"
)

// This file centralizes the EXPERIMENTAL plugin.InstrumentationPlugin
// dispatch helpers shared across every operation kind in this package
// (Step, Wait, CreateCallback, Invoke, RunInChildContext, and the
// Map/Parallel batch item runner) - see pkg/durable/plugin's package doc
// for the full hook surface and scope. Centralizing these here (rather
// than duplicating a slightly different version per operation file, the
// way this file's predecessor lived only in step.go before this package's
// hook coverage was extended to every operation kind) keeps the
// info-struct construction and IsReplay/timestamp conventions consistent
// across every call site.

// operationKindDispatchInfo is the small, per-call set of identifying
// fields every dispatch helper below needs, factored into one struct
// purely to keep each helper's own parameter list short and consistent -
// not a checkpointed or wire type of any kind.
type operationKindDispatchInfo struct {
	ID       string
	ParentID string
	Name     string
	Type     types.OperationType
	SubType  string
}

// dispatchOperationStart fires OnOperationStart for an operation that is
// genuinely (non-replay-skip) beginning execution. attempt is 0 for
// operation kinds with no retry concept (Wait, CreateCallback, Invoke,
// RunInChildContext, Map/Parallel items); Step is currently the only
// kind that passes a nonzero attempt. A no-op when c has no configured
// plugins (see plugin.Dispatch).
func dispatchOperationStart(c *dcontext.Context, info operationKindDispatchInfo, attempt int) {
	plugins := c.Plugins()
	if len(plugins) == 0 {
		return
	}
	pinfo := plugin.OperationInfo{
		ID:             info.ID,
		Name:           info.Name,
		Type:           string(info.Type),
		SubType:        info.SubType,
		ParentID:       info.ParentID,
		Status:         plugin.OperationStatusStarted,
		StartTimestamp: time.Now(),
		Attempt:        attempt,
	}
	plugin.Dispatch(plugins, func(p plugin.InstrumentationPlugin) {
		p.OnOperationStart(c.Context(), pinfo)
	})
}

// dispatchOperationEnd fires OnOperationEnd for an operation that has
// genuinely (non-replay-skip) reached its FINAL outcome - either
// plugin.OperationStatusSucceeded (result is the checkpointed, serialized
// wire-form result; opErr is nil) or a non-success terminal status
// (opErr is the operation's own failure; result is empty). A no-op when
// c has no configured plugins.
//
// For a retryable operation kind (currently only Step), this must only
// be called for the FINAL outcome, never an intermediate retryable
// failure that will re-attempt - a retry is not an operation "end" from a
// plugin's perspective, matching the JS SDK's own event model where a
// retried step only fires onOperationEnd once, for its final outcome.
// See dispatchOperationAttemptEnd for the per-attempt equivalent every
// individual attempt (including retried ones) DOES fire.
func dispatchOperationEnd(c *dcontext.Context, info operationKindDispatchInfo, attempt int, status plugin.OperationStatus, result string, opErr error) {
	plugins := c.Plugins()
	if len(plugins) == 0 {
		return
	}
	pinfo := plugin.OperationEndInfo{OperationInfo: plugin.OperationInfo{
		ID:           info.ID,
		Name:         info.Name,
		Type:         string(info.Type),
		SubType:      info.SubType,
		ParentID:     info.ParentID,
		Status:       status,
		EndTimestamp: time.Now(),
		Result:       result,
		Error:        opErr,
		Attempt:      attempt,
	}}
	plugin.Dispatch(plugins, func(p plugin.InstrumentationPlugin) {
		p.OnOperationEnd(c.Context(), pinfo)
	})
}

// dispatchOperationAttemptStart fires OnOperationAttemptStart for a
// single attempt of a retryable operation (currently: Step) that is
// genuinely beginning execution. A no-op when c has no configured
// plugins.
func dispatchOperationAttemptStart(c *dcontext.Context, info operationKindDispatchInfo, attempt int) {
	plugins := c.Plugins()
	if len(plugins) == 0 {
		return
	}
	ainfo := plugin.AttemptInfo{OperationInfo: plugin.OperationInfo{
		ID:             info.ID,
		Name:           info.Name,
		Type:           string(info.Type),
		SubType:        info.SubType,
		ParentID:       info.ParentID,
		Status:         plugin.OperationStatusStarted,
		StartTimestamp: time.Now(),
		Attempt:        attempt,
	}}
	plugin.Dispatch(plugins, func(p plugin.InstrumentationPlugin) {
		p.OnOperationAttemptStart(c.Context(), ainfo)
	})
}

// dispatchOperationAttemptEnd fires OnOperationAttemptEnd for a single
// attempt of a retryable operation that has ended - EVERY attempt fires
// this, including one that failed but will be retried (unlike
// dispatchOperationEnd, which only fires for the operation's overall
// final outcome). A no-op when c has no configured plugins.
func dispatchOperationAttemptEnd(c *dcontext.Context, info operationKindDispatchInfo, attempt int, outcome plugin.AttemptOutcome, result string, attemptErr error) {
	plugins := c.Plugins()
	if len(plugins) == 0 {
		return
	}
	status := plugin.OperationStatusSucceeded
	if outcome == plugin.AttemptOutcomeFailed {
		status = plugin.OperationStatusFailed
	}
	ainfo := plugin.AttemptEndInfo{
		AttemptInfo: plugin.AttemptInfo{OperationInfo: plugin.OperationInfo{
			ID:           info.ID,
			Name:         info.Name,
			Type:         string(info.Type),
			SubType:      info.SubType,
			ParentID:     info.ParentID,
			Status:       status,
			EndTimestamp: time.Now(),
			Result:       result,
			Error:        attemptErr,
			Attempt:      attempt,
		}},
		Outcome: outcome,
		Error:   attemptErr,
	}
	plugin.Dispatch(plugins, func(p plugin.InstrumentationPlugin) {
		p.OnOperationAttemptEnd(c.Context(), ainfo)
	})
}

// wrapChildContextFn runs fn through every configured plugin's
// WrapChildContextFn hook (outermost-first, per plugin.WrapChain's own
// ordering contract), for a child-context-shaped operation
// (RunInChildContext, a Map item, or a Parallel branch) that is
// genuinely beginning execution. When c has no configured plugins, this
// simply calls fn() directly with no wrapping overhead at all - not even
// plugin.WrapChain's own trivial loop.
//
// fn's signature is the plugin package's own `func() (any, error)`
// (plugin.WrapFn) rather than a generic `func() (T, error)`, since
// InstrumentationPlugin is a single, non-generic interface - callers
// type-assert the returned any back to T themselves (see this function's
// own call sites in invoke.go/batch.go for the exact pattern).
func wrapChildContextFn(c *dcontext.Context, info operationKindDispatchInfo, fn plugin.WrapFn) (any, error) {
	plugins := c.Plugins()
	if len(plugins) == 0 {
		return fn()
	}
	pinfo := plugin.OperationInfo{
		ID:       info.ID,
		Name:     info.Name,
		Type:     string(info.Type),
		SubType:  info.SubType,
		ParentID: info.ParentID,
		Status:   plugin.OperationStatusStarted,
	}
	return plugin.WrapChain(plugins, func(p plugin.InstrumentationPlugin, next plugin.WrapFn) (any, error) {
		return p.WrapChildContextFn(c.Context(), pinfo, next)
	}, fn)
}

// wrapOperationAttemptFn is wrapChildContextFn's counterpart for a single
// Step attempt, running fn through every configured plugin's
// WrapOperationAttemptFn hook.
func wrapOperationAttemptFn(c *dcontext.Context, info operationKindDispatchInfo, attempt int, fn plugin.WrapFn) (any, error) {
	plugins := c.Plugins()
	if len(plugins) == 0 {
		return fn()
	}
	ainfo := plugin.AttemptInfo{OperationInfo: plugin.OperationInfo{
		ID:       info.ID,
		Name:     info.Name,
		Type:     string(info.Type),
		SubType:  info.SubType,
		ParentID: info.ParentID,
		Status:   plugin.OperationStatusStarted,
		Attempt:  attempt,
	}}
	return plugin.WrapChain(plugins, func(p plugin.InstrumentationPlugin, next plugin.WrapFn) (any, error) {
		return p.WrapOperationAttemptFn(c.Context(), ainfo, next)
	}, fn)
}
