package telemetry

import (
	"context"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// opsByID returns the entries as the Operations map of a hook info: keyed
// by operation ID.
func opsByID(ops ...durable.OperationHookInfo) map[string]durable.OperationHookInfo {
	m := make(map[string]durable.OperationHookInfo, len(ops))
	for _, op := range ops {
		m[op.ID] = op
	}
	return m
}

// reportChange calls the plugin's OnOperationChange with ops as the full
// set of checkpointed operations. The SDK calls the hook this way after a
// checkpoint response changed the status of an operation. The SDK calls
// only the hooks that a plugin registered. So reportChange does nothing when
// the plugin registered no OnOperationChange.
func reportChange(ctx context.Context, plugin *durable.Plugin, ops ...durable.OperationHookInfo) {
	if plugin.OnOperationChange == nil {
		return
	}
	all := opsByID(ops...)
	plugin.OnOperationChange(ctx, durable.OperationChangeHookInfo{UpdatedOperations: all, Operations: all})
}

// checkpointedError returns the Error of a snapshot entry for a failed
// operation whose checkpoint holds the error type and message.
func checkpointedError(name, errType, message string) error {
	return &durable.OperationError{Name: name, ErrorType: errType, Message: message}
}
