package durable

import (
	"context"
	"sync"
)

// operationChange is what the checkpoint response path records for one
// response that changed an operation's status. The records are shared with
// the state, not copied: a record is never modified after it enters the
// state.
type operationChange struct {
	// changed holds the operations whose status the response changed.
	changed []*operation
	// operations holds every operation in the state once the response was
	// merged.
	operations []*operation
	// changedIDs holds the wire IDs of the operations whose status a
	// response changed during this invocation, up to and including this
	// response.
	changedIDs map[string]struct{}
}

// operationChangeQueue delivers OnOperationChange reports for one
// invocation. The checkpoint response path queues one change per response
// that changed an operation's status. One goroutine builds the report of
// each change and dispatches it, in the order the changes were queued. So
// the checkpoint path only copies the operation pointers, and a slow hook
// delays later reports and the end of the invocation, never a checkpoint
// call.
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
	// state is the invocation's operation state. enqueue reads it.
	state *executionState
	// report builds the hook info of a queued change. It returns false
	// when the change dispatches no hook. See operationChangeInfo.
	report func(operationChange) (OperationChangeHookInfo, bool)

	mu      sync.Mutex
	pending []operationChange
	// idle is non-nil while the dispatch goroutine runs, and is closed
	// when it exits.
	idle chan struct{}
	// closed is set by close. A closed queue accepts no report.
	closed bool
}

// newOperationChangeQueue returns the queue for one invocation, or nil
// when no plugin of d implements OnOperationChange. bound is the plugin
// depth bound of the invocation.
func newOperationChangeQueue(ctx context.Context, d *pluginDispatcher, executionArn string, state *executionState, bound int) *operationChangeQueue {
	if !d.hasOperationChangeHook() {
		return nil
	}
	return &operationChangeQueue{
		ctx:   ctx,
		d:     d,
		state: state,
		report: func(c operationChange) (OperationChangeHookInfo, bool) {
			return operationChangeInfo(executionArn, state, bound, c)
		},
	}
}

// enqueue queues the change that one checkpoint response made and returns
// without waiting for the hooks. changed holds the operations whose status
// the response changed. The checkpoint response path calls enqueue after
// it merged the response, on the goroutine that merged it. So the state
// enqueue reads is the state that response produced.
//
// A change queued after close is dropped: the invocation has dispatched,
// or is about to dispatch, OnInvocationEnd, and no hook may follow it.
func (q *operationChangeQueue) enqueue(changed []*operation) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	c := operationChange{changed: changed}
	if q.state != nil {
		c.operations, c.changedIDs = q.state.snapshot()
	}
	q.pending = append(q.pending, c)
	if q.idle == nil {
		q.idle = make(chan struct{})
		go q.run(q.idle)
	}
}

// run builds and dispatches the report of each queued change, one at a
// time, in queue order, and exits when none is left. Each dispatch waits
// for every plugin's hook, so one plugin's OnOperationChange calls never
// overlap one another.
func (q *operationChangeQueue) run(idle chan struct{}) {
	for {
		q.mu.Lock()
		if len(q.pending) == 0 {
			q.idle = nil
			close(idle)
			q.mu.Unlock()
			return
		}
		c := q.pending[0]
		q.pending[0] = operationChange{}
		q.pending = q.pending[1:]
		q.mu.Unlock()

		info, ok := q.report(c)
		if !ok {
			continue
		}
		dispatchNotification(q.d, func(p *Plugin) {
			if p.OnOperationChange != nil {
				p.OnOperationChange(q.ctx, info)
			}
		})
	}
}

// close stops the queue accepting changes and waits until every change
// queued so far is dispatched, so that OnInvocationEnd follows them. The
// handler calls it before each OnInvocationEnd dispatch.
//
// ctx bounds the wait. When ctx ends first, the changes not yet dispatched
// are dropped, so that no hook starts after OnInvocationEnd. A hook call
// already running cannot be interrupted. So it may still be running when
// close returns, and therefore while OnInvocationEnd runs. The Plugin
// documentation states this exception.
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

// operationChangeInfo builds the OnOperationChange report for the change
// one checkpoint response made. Operations is built from the operations
// that enqueue copied when the response was merged, so it holds the state
// that response produced, whatever later responses changed. state is read
// only to find the depth of an operation; an operation's parent never
// changes.
//
// Both maps omit the operations beyond the plugin depth bound, as the maps
// of [InvocationHookInfo] do. ok is false when the bound omits every
// changed operation; no hook is dispatched then.
//
// An entry has IsReplay false when the operation's status changed during
// this invocation, and true when its record is unchanged since the
// invocation started.
func operationChangeInfo(executionArn string, state *executionState, bound int, c operationChange) (info OperationChangeHookInfo, ok bool) {
	updated := make(map[string]OperationHookInfo, len(c.changed))
	for _, op := range c.changed {
		addCheckpointedOperationInfo(updated, executionArn, state, bound, op)
	}
	if len(updated) == 0 {
		return OperationChangeHookInfo{}, false
	}
	all := make(map[string]OperationHookInfo, len(c.operations))
	for _, op := range c.operations {
		addCheckpointedOperationInfo(all, executionArn, state, bound, op)
	}
	for id, opInfo := range all {
		if _, live := c.changedIDs[id]; live {
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
