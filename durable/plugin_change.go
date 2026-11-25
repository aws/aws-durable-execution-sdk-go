package durable

import (
	"context"
	"sync"
)

// operationChangeQueue delivers OnOperationChange reports for one
// invocation. The checkpoint response path queues one report per response
// that changed an operation's status, and one goroutine dispatches the
// reports in the order they were queued. So a slow hook delays later
// reports and the end of the invocation, never a checkpoint call.
//
// The goroutine exists only while a report is queued or being dispatched:
// enqueue starts it when none runs, and it exits when the queue is empty.
// An invocation whose checkpoint responses change nothing starts none.
//
// A nil *operationChangeQueue is valid and does nothing; the handler holds
// one when no plugin implements OnOperationChange.
type operationChangeQueue struct {
	// ctx is the context the hooks receive: the invocation's context, as
	// for the invocation-level hooks.
	ctx context.Context
	d   *pluginDispatcher

	mu      sync.Mutex
	pending []OperationChangeHookInfo
	// idle is non-nil while the dispatch goroutine runs, and is closed
	// when it exits.
	idle chan struct{}
	// closed is set by close. A closed queue accepts no report.
	closed bool
}

// newOperationChangeQueue returns the queue for one invocation, or nil
// when no plugin of d implements OnOperationChange.
func newOperationChangeQueue(ctx context.Context, d *pluginDispatcher) *operationChangeQueue {
	if !d.hasOperationChangeHook() {
		return nil
	}
	return &operationChangeQueue{ctx: ctx, d: d}
}

// enqueue queues info for dispatch and returns without waiting for the
// hooks. A report queued after close is dropped: the invocation has
// dispatched, or is about to dispatch, OnInvocationEnd, and no hook may
// follow it.
func (q *operationChangeQueue) enqueue(info OperationChangeHookInfo) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.pending = append(q.pending, info)
	if q.idle == nil {
		q.idle = make(chan struct{})
		go q.run(q.idle)
	}
}

// run dispatches the queued reports one at a time, in queue order, and
// exits when none is left. Each dispatch waits for every plugin's hook, so
// one plugin's OnOperationChange calls never overlap one another.
func (q *operationChangeQueue) run(idle chan struct{}) {
	for {
		q.mu.Lock()
		if len(q.pending) == 0 {
			q.idle = nil
			close(idle)
			q.mu.Unlock()
			return
		}
		info := q.pending[0]
		q.pending[0] = OperationChangeHookInfo{}
		q.pending = q.pending[1:]
		q.mu.Unlock()

		dispatchNotification(q.d, func(p *Plugin) {
			if p.OnOperationChange != nil {
				p.OnOperationChange(q.ctx, info)
			}
		})
	}
}

// close stops the queue accepting reports and waits until every report
// queued so far is dispatched, so that OnInvocationEnd follows them. The
// handler calls it before each OnInvocationEnd dispatch.
//
// ctx bounds the wait. When ctx ends first, the reports not yet dispatched
// are dropped, so that no hook starts after OnInvocationEnd. A hook call
// already running cannot be interrupted and may still be running when
// close returns.
func (q *operationChangeQueue) close(ctx context.Context) {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.closed = true
	idle := q.idle
	q.mu.Unlock()
	if idle == nil {
		return
	}
	select {
	case <-idle:
	case <-ctx.Done():
		q.mu.Lock()
		q.pending = nil
		q.mu.Unlock()
	}
}

// hasOperationChangeHook reports whether at least one registered plugin
// implements OnOperationChange. False for a nil dispatcher.
func (d *pluginDispatcher) hasOperationChangeHook() bool {
	if d == nil {
		return false
	}
	for i := range d.plugins {
		if d.plugins[i].OnOperationChange != nil {
			return true
		}
	}
	return false
}

// operationChangeInfo builds the OnOperationChange report for the
// operations a checkpoint response changed. It reads state after the
// response was merged, on the goroutine that merged it, so Operations
// holds the state that response produced.
//
// Both maps omit the operations beyond the plugin depth bound, as the maps
// of [InvocationHookInfo] do. ok is false when the bound omits every
// changed operation; no hook is dispatched then.
//
// An entry has IsReplay false when the operation's status changed during
// this invocation, and true when its record is unchanged since the
// invocation started.
func operationChangeInfo(executionArn string, state *executionState, bound int, changed []*operation) (info OperationChangeHookInfo, ok bool) {
	updated := make(map[string]OperationHookInfo, len(changed))
	for _, op := range changed {
		addCheckpointedOperationInfo(updated, executionArn, state, bound, op)
	}
	if len(updated) == 0 {
		return OperationChangeHookInfo{}, false
	}
	ops, changedIDs := state.snapshot()
	all := make(map[string]OperationHookInfo, len(ops))
	for _, op := range ops {
		addCheckpointedOperationInfo(all, executionArn, state, bound, op)
	}
	for id, opInfo := range all {
		if _, live := changedIDs[id]; live {
			opInfo.IsReplay = false
			all[id] = opInfo
		}
	}
	for id, opInfo := range updated {
		opInfo.IsReplay = false
		updated[id] = opInfo
	}
	return OperationChangeHookInfo{
		ExecutionArn:      executionArn,
		UpdatedOperations: updated,
		Operations:        all,
	}, true
}
