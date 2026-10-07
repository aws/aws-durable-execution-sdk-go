package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// This file holds the stored form of a batch result: the payload the
// parent Map or Parallel context records when it succeeds.
//
// Two forms exist.
//
//  1. The full payload, batchCheckpointPayload, holds every admitted item:
//     {"all":[<item>,...],"completionReason":"<REASON>"}. A succeeded item
//     is {"result":<value>,"index":<n>,"status":"SUCCEEDED"}, a failed item
//     is {"error":<error object>,"index":<n>,"status":"FAILED"}, and an
//     item abandoned on early completion is {"index":<n>,"status":"STARTED"}.
//  2. The summary record, batchSummaryRecord, replaces the full payload when
//     the full payload is larger than the checkpoint size limit. The
//     checkpoint then also sets ReplayChildren, and replay rebuilds each
//     item from its own recorded operations.
//
// The other Durable Execution SDKs store the same two forms, so a batch
// payload reads the same whichever SDK recorded it.

// batchCheckpointPayload is the full stored form of a batch result.
type batchCheckpointPayload struct {
	All              []batchCheckpointItem `json:"all"`
	CompletionReason CompletionReason      `json:"completionReason"`
}

// batchCheckpointItem is one item of a [batchCheckpointPayload]. The field
// order gives the stored key order: result or error first, then index and
// status. The item name is not stored; replay derives it from the index.
type batchCheckpointItem struct {
	// Result is the output of the item serdes, stored as a JSON value. It
	// is absent when the item did not succeed or the serdes produced no
	// bytes.
	Result json.RawMessage `json:"result,omitempty"`

	// Error is the item's failure. It is absent unless Status is FAILED.
	Error *batchErrorObject `json:"error,omitempty"`

	Index  int             `json:"index"`
	Status BatchItemStatus `json:"status"`
}

// batchErrorObject is the stored form of one level of a failed item's
// error chain. The outermost object describes the item error itself. Cause
// describes the error it wraps, and so on down the chain.
type batchErrorObject struct {
	ErrorType    string            `json:"ErrorType"`
	ErrorMessage string            `json:"ErrorMessage"`
	ErrorData    string            `json:"ErrorData,omitempty"`
	StackTrace   []string          `json:"StackTrace,omitempty"`
	Cause        *batchErrorObject `json:"Cause,omitempty"`
}

// maxBatchErrorDepth bounds how many levels of an error chain are stored.
// A chain longer than this is cut at the bound.
const maxBatchErrorDepth = 32

// newBatchErrorObject builds the stored form of err by walking its Unwrap
// chain from the outermost error inward.
//
// Each level records its wire ErrorType, its message, the ErrorData found
// in the chain from that level inward, and its recorded stack trace.
//
//  1. An SDK error type records the message it carries for the error it
//     wraps, and its Cause is the stored form of that wrapped error.
//  2. A stand-in rebuilt from a record records its type and message, and
//     its Cause is the stored form of the sentinel it unwraps to, if any.
//  3. Any other error records its Error() text, and its Cause is the stored
//     form of errors.Unwrap(err).
//
// The transparent wrappers from [WithErrorData] and FLAT-mode item traces
// add no level. A trace carried by the FLAT-mode wrapper is recorded on the
// level it wraps when that level has no trace of its own.
func newBatchErrorObject(err error) *batchErrorObject {
	return newBatchErrorObjectAt(err, 0)
}

func newBatchErrorObjectAt(err error, depth int) *batchErrorObject {
	if err == nil || depth >= maxBatchErrorDepth {
		return nil
	}
	wrapperTrace := headTraceOf(err)
	data := errorDataOf(err)
	err = unwrapErrorData(err)

	if re, ok := err.(*replayedError); ok {
		errType := re.errType
		if errType == "" {
			errType = "Error"
		}
		return &batchErrorObject{
			ErrorType: errType, ErrorMessage: re.message, ErrorData: data, StackTrace: wrapperTrace,
			Cause: newBatchErrorObjectAt(re.sentinel, depth+1),
		}
	}

	if name, ok := sdkWireErrorType(err); ok {
		obj := &batchErrorObject{ErrorType: name, ErrorData: data}
		if oe, ok := err.(interface{ operationError() *OperationError }); ok {
			op := oe.operationError()
			obj.ErrorMessage = op.Message
			obj.StackTrace = op.StackTrace
			obj.Cause = newBatchErrorObjectAt(op.Err, depth+1)
		} else {
			obj.ErrorMessage = err.Error()
			obj.Cause = newBatchErrorObjectAt(errors.Unwrap(err), depth+1)
		}
		if len(obj.StackTrace) == 0 {
			obj.StackTrace = wrapperTrace
		}
		return obj
	}

	return &batchErrorObject{
		ErrorType:    userErrorTypeName(err),
		ErrorMessage: err.Error(),
		ErrorData:    data,
		StackTrace:   wrapperTrace,
		Cause:        newBatchErrorObjectAt(errors.Unwrap(err), depth+1),
	}
}

// headTraceOf returns the trace a FLAT-mode trace wrapper carries when the
// wrapper is among the transparent wrappers at the head of err's chain.
func headTraceOf(err error) []string {
	for {
		switch w := err.(type) {
		case *errorWithData:
			err = w.err
		case *flatItemTraceError:
			return w.trace
		default:
			return nil
		}
	}
}

// rebuild returns the error a stored error object describes. itemName is
// the batch item's name. It names the outermost level when that level is a
// [ChildContextError], the error a NORMAL-mode item fails with.
//
// A level whose ErrorType names an SDK error type is rebuilt as that type,
// so [errors.As] finds it. Its ErrorType field is the ErrorType of its
// Cause, its Message is the stored message, and its Err is the rebuilt
// Cause. Fields the stored form does not carry, such as
// [StepError.Attempts], [CallbackError.CallbackID], and the names of inner
// operations, are zero. Any other level is rebuilt as a stand-in that
// reports the stored type and message and unwraps to the rebuilt Cause; a
// stored ErrorData or StackTrace is attached through the transparent
// wrappers the SDK uses for them.
func (o *batchErrorObject) rebuild(itemName string) error {
	if o == nil {
		return nil
	}
	return o.rebuildAt(itemName, 0)
}

func (o *batchErrorObject) rebuildAt(itemName string, depth int) error {
	var cause error
	if o.Cause != nil && depth+1 < maxBatchErrorDepth {
		cause = o.Cause.rebuildAt("", depth+1)
	}
	if _, ok := sdkErrorsByWireType[o.ErrorType]; !ok {
		if sentinel := sdkSentinelFor(o); sentinel != nil {
			return sentinel
		}
		errType := o.ErrorType
		if errType == "" {
			errType = "Error"
		}
		var leaf error = &replayedError{errType: errType, message: o.ErrorMessage, sentinel: cause}
		if o.ErrorData != "" {
			leaf = &errorWithData{err: leaf, data: o.ErrorData}
		}
		if len(o.StackTrace) > 0 {
			leaf = &flatItemTraceError{err: leaf, trace: o.StackTrace}
		}
		return leaf
	}
	var name string
	if depth == 0 && o.ErrorType == "ChildContextError" {
		name = itemName
	}
	innerType := o.ErrorType
	if o.Cause != nil {
		innerType = o.Cause.ErrorType
	}
	err := reconstructSDKError(o.ErrorType, OperationError{
		Name: name, ErrorType: innerType, Message: o.ErrorMessage,
		ErrorData: o.ErrorData, StackTrace: o.StackTrace,
	}, nil)
	if cause != nil {
		setErrorCause(err, cause)
	}
	return err
}

// sdkSentinels lists the SDK sentinel errors a stored error chain may end
// in. A stored leaf that matches one is rebuilt as the sentinel itself, so
// [errors.Is] matches it after a rebuild.
var sdkSentinels = []error{ErrCallbackTimedOut, ErrInvokeTimedOut, ErrExecutionStopped, ErrExecutionCancelled}

// sdkSentinelFor returns the SDK sentinel a stored leaf describes, or nil.
func sdkSentinelFor(o *batchErrorObject) error {
	if o.ErrorType != "Error" || o.Cause != nil || o.ErrorData != "" || len(o.StackTrace) > 0 {
		return nil
	}
	for _, sentinel := range sdkSentinels {
		if sentinel.Error() == o.ErrorMessage {
			return sentinel
		}
	}
	return nil
}

// setErrorCause replaces the cause of an SDK error rebuilt by
// [reconstructSDKError] with cause. A sentinel the rebuilt cause carried,
// such as [ErrCallbackTimedOut], moves to cause when cause is a stand-in
// without one, so [errors.Is] still matches it. An SDK type that records no
// cause is left unchanged.
func setErrorCause(err, cause error) {
	replace := func(slot *error) {
		if old, ok := (*slot).(*replayedError); ok && old.sentinel != nil {
			if re, ok := cause.(*replayedError); ok && re.sentinel == nil {
				re.sentinel = old.sentinel
			}
		}
		*slot = cause
	}
	switch e := err.(type) {
	case *OperationError:
		replace(&e.Err)
	case *StepError:
		replace(&e.Err)
	case *InvokeError:
		replace(&e.Err)
	case *CallbackError:
		replace(&e.Err)
	case *CallbackExternalError:
		replace(&e.Err)
	case *CallbackTimeoutError:
		replace(&e.Err)
	case *CallbackSubmitterError:
		replace(&e.Err)
	case *ChildContextError:
		replace(&e.Err)
	case *WaitForConditionError:
		replace(&e.Err)
	case *RetryError:
		replace(&e.Err)
	case *SerdesError:
		replace(&e.Err)
	}
}

// storedBatchItemError returns the error a batch reports for a failed
// item: the error rebuilt from the stored form of err. The first run and
// every replay report this form, so they report equal errors.
func storedBatchItemError(itemName string, err error) error {
	return newBatchErrorObject(err).rebuild(itemName)
}

// normalizeBatchItemErrors replaces each failed item's error with its
// stored form, rebuilt. See [storedBatchItemError].
func normalizeBatchItemErrors[O any](result BatchResult[O]) BatchResult[O] {
	for i := range result.Items {
		if result.Items[i].Status == BatchItemFailed && result.Items[i].Err != nil {
			result.Items[i].Err = storedBatchItemError(result.Items[i].Name, result.Items[i].Err)
		}
	}
	return result
}

// errBatchResultNotJSON reports that an item serdes produced bytes that
// are not a JSON value, so the full payload cannot hold them.
var errBatchResultNotJSON = errors.New("item result is not a JSON value")

// fromBatchResult converts a live [BatchResult] into the full payload.
// Each succeeded item stores the bytes the item serdes produced when the
// item completed; an item without them is marshaled here. itemSctx returns
// the serdes context for the item at an input index.
//
// It returns [errBatchResultNotJSON] when an item's bytes are not a JSON
// value. The caller then stores the summary record instead, and replay
// rebuilds each item from its own recorded operations.
func fromBatchResult[O any](ctx context.Context, result BatchResult[O], itemSerdes Serdes, itemSctx func(index int) SerdesContext) (batchCheckpointPayload, error) {
	items := make([]batchCheckpointItem, len(result.Items))
	for i, item := range result.Items {
		items[i] = batchCheckpointItem{Index: item.Index, Status: item.Status}
		switch item.Status {
		case BatchItemSucceeded:
			raw := item.serialized
			if !item.hasSerialized {
				var err error
				raw, err = itemSerdes.Marshal(ctx, itemSctx(item.Index), item.Result)
				if err != nil {
					return batchCheckpointPayload{}, newSerdesError(batchItemOpName(item.Name, item.Index), serdesDirectionMarshal, err)
				}
			}
			if len(raw) > 0 && !json.Valid(raw) {
				return batchCheckpointPayload{}, errBatchResultNotJSON
			}
			items[i].Result = json.RawMessage(raw)
		case BatchItemFailed:
			if item.Err != nil {
				items[i].Error = newBatchErrorObject(item.Err)
			} else {
				items[i].Error = &batchErrorObject{ErrorType: "Error"}
			}
		}
	}
	return batchCheckpointPayload{All: items, CompletionReason: result.Reason}, nil
}

// toBatchResult converts a stored full payload back into a typed
// [BatchResult]. Item names come from options, by index. Each failed
// item's error is rebuilt from its stored error object.
func toBatchResult[O any](ctx context.Context, payload batchCheckpointPayload, options batchOptions, itemSctx func(index int) SerdesContext) (BatchResult[O], error) {
	items := make([]BatchItem[O], len(payload.All))
	for i, cp := range payload.All {
		name := itemNameForIndex(options, cp.Index)
		items[i] = BatchItem[O]{Index: cp.Index, Name: name, Status: cp.Status}
		switch cp.Status {
		case BatchItemSucceeded:
			// An absent result decodes from empty bytes, as the first
			// run decoded the empty output of Marshal.
			var out O
			if err := options.itemSerdes.Unmarshal(ctx, itemSctx(cp.Index), []byte(cp.Result), &out); err != nil {
				return BatchResult[O]{}, newSerdesError(batchItemOpName(name, cp.Index), serdesDirectionUnmarshal, err)
			}
			items[i].Result = out
			items[i].serialized = []byte(cp.Result)
			items[i].hasSerialized = true
		case BatchItemFailed:
			obj := cp.Error
			if obj == nil {
				obj = &batchErrorObject{ErrorType: "Error"}
			}
			items[i].Err = obj.rebuild(name)
		}
	}
	return BatchResult[O]{Items: items, Reason: payload.CompletionReason}, nil
}

// parseBatchCheckpointPayload parses a stored full payload. The second
// result is false when payload is not one.
func parseBatchCheckpointPayload(payload string) (batchCheckpointPayload, bool) {
	var p batchCheckpointPayload
	if err := json.Unmarshal([]byte(payload), &p); err != nil || p.All == nil {
		return batchCheckpointPayload{}, false
	}
	return p, true
}

// Summary record types.
const (
	batchSummaryTypeMap      = "MapResult"
	batchSummaryTypeParallel = "ParallelResult"
)

// Item markers of [batchSummaryRecord.ItemStatuses].
const (
	itemMarkerSucceeded = 'S'
	itemMarkerFailed    = 'F'
	itemMarkerNeither   = '-'
)

// batchSummaryRecord is the summary record a batch stores when its full
// payload is larger than the checkpoint size limit.
//
// ItemStatuses has one character per admitted item, in index order: 'S'
// for a succeeded item, 'F' for a failed item, and '-' for an item
// abandoned when the batch completed early. The batch admits items in
// strict index order, so the admitted items are the prefix
// [0, TotalCount). Replay re-drives each 'S' and 'F' item from its own
// recorded operations and reports each '-' item as started.
//
// StartedCount is stored for Parallel only. Summary is the caller's
// [WithBatchSummary] output; replay never reads it.
type batchSummaryRecord struct {
	Type             string           `json:"type"`
	TotalCount       int              `json:"totalCount"`
	SuccessCount     int              `json:"successCount"`
	FailureCount     int              `json:"failureCount"`
	StartedCount     *int             `json:"startedCount,omitempty"`
	CompletionReason CompletionReason `json:"completionReason"`
	Status           BatchItemStatus  `json:"status"`
	ItemStatuses     string           `json:"itemStatuses"`
	Summary          string           `json:"summary,omitempty"`
}

// newBatchSummaryRecord builds the summary record of a live batch result.
// summaryType is [batchSummaryTypeMap] or [batchSummaryTypeParallel].
func newBatchSummaryRecord[O any](summaryType string, result BatchResult[O]) batchSummaryRecord {
	markers := make([]byte, 0, len(result.Items))
	started := 0
	for i := range result.Items {
		switch result.Items[i].Status {
		case BatchItemSucceeded:
			markers = append(markers, itemMarkerSucceeded)
		case BatchItemFailed:
			markers = append(markers, itemMarkerFailed)
		default:
			markers = append(markers, itemMarkerNeither)
			started++
		}
	}
	record := batchSummaryRecord{
		Type:             summaryType,
		TotalCount:       result.TotalCount(),
		SuccessCount:     result.SuccessCount(),
		FailureCount:     result.FailureCount(),
		CompletionReason: result.Reason,
		Status:           result.Status(),
		ItemStatuses:     string(markers),
	}
	if summaryType == batchSummaryTypeParallel {
		record.StartedCount = &started
	}
	return record
}

// marshalBatchSummaryRecord serializes record so that it fits the
// checkpoint size limit. The record without its summary is bounded by the
// item count and fits. The summary is caller-supplied and may not: it is
// shortened on a UTF-8 boundary, in proportion to the excess of its JSON
// encoding over the space the rest of the record leaves, until the
// encoding fits. A summary with no fitting prefix is omitted.
func marshalBatchSummaryRecord(record batchSummaryRecord) ([]byte, error) {
	summary := record.Summary
	record.Summary = ""
	base, err := marshalNoHTMLEscape(record)
	if err != nil {
		return nil, err
	}
	if summary == "" {
		return base, nil
	}
	// The encoded summary is added as `,"summary":<encoded>` before the
	// closing brace, so this is the space it may occupy.
	budget := checkpointSizeLimitBytes - len(base) - len(`,"summary":`)
	for summary != "" {
		encoded, err := marshalNoHTMLEscape(summary)
		if err != nil {
			return nil, err
		}
		if len(encoded) <= budget {
			record.Summary = summary
			return marshalNoHTMLEscape(record)
		}
		// Shrink in proportion to the overrun. Each pass strictly
		// shortens the summary, so the loop ends.
		keep := len(summary) * budget / len(encoded)
		if keep >= len(summary) {
			keep = len(summary) - 1
		}
		summary = truncateUTF8(summary, max(keep, 0))
	}
	return base, nil
}

// parseBatchSummaryRecord parses a stored summary record. The second
// result is false when payload is not a well-formed summary record, so the
// caller can fall back to re-running the items in order.
func parseBatchSummaryRecord(payload string) (batchSummaryRecord, bool) {
	if payload == "" {
		return batchSummaryRecord{}, false
	}
	var record batchSummaryRecord
	if err := json.Unmarshal([]byte(payload), &record); err != nil {
		return batchSummaryRecord{}, false
	}
	if record.Type != batchSummaryTypeMap && record.Type != batchSummaryTypeParallel {
		return batchSummaryRecord{}, false
	}
	if strings.Trim(record.ItemStatuses, "SF-") != "" {
		return batchSummaryRecord{}, false
	}
	return record, true
}

// admitted returns the number of items the batch admitted: the length of
// the marker string.
func (r batchSummaryRecord) admitted() int { return len(r.ItemStatuses) }

// abandoned reports whether the item at index i was abandoned when the
// batch completed early.
func (r batchSummaryRecord) abandoned(i int) bool { return r.ItemStatuses[i] == itemMarkerNeither }

// batchSummaryType returns the summary record type of a batch by its
// operation subtype.
func batchSummaryType(subType string) string {
	if subType == OperationSubTypeParallel {
		return batchSummaryTypeParallel
	}
	return batchSummaryTypeMap
}

// MarshalJSON encodes the status as its [BatchItemStatus.String] form.
func (s BatchItemStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// UnmarshalJSON decodes a status from its [BatchItemStatus.String] form.
// An unrecognized string decodes to [BatchItemNotStarted].
func (s *BatchItemStatus) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return fmt.Errorf("durable: batch item status must be a JSON string: %w", err)
	}
	*s = BatchItemNotStarted
	for _, v := range []BatchItemStatus{BatchItemSucceeded, BatchItemFailed, BatchItemStarted} {
		if v.String() == text {
			*s = v
		}
	}
	return nil
}

// MarshalJSON encodes the reason as its [CompletionReason.String] form.
func (r CompletionReason) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.String())
}

// UnmarshalJSON decodes a reason from its [CompletionReason.String] form.
// An unrecognized string decodes to the zero value.
func (r *CompletionReason) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return fmt.Errorf("durable: completion reason must be a JSON string: %w", err)
	}
	*r = 0
	for _, v := range completionReasons {
		if v.String() == text {
			*r = v
		}
	}
	return nil
}
