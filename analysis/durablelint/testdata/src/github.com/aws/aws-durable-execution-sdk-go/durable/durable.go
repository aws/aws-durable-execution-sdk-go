// Package durable is a signature-only stand-in for the SDK package. The
// analyzers identify durable operations by package path and function name,
// so the tests only need the declarations, not the behaviour.
package durable

import (
	"context"
	"time"
)

type Context interface {
	context.Context
	ExecutionArn() string
	RequestID() string
	IsReplaying() bool
}

type StepContext interface {
	context.Context
	Attempt() int
}

type Void struct{}

type Future[O any] struct{ _ O }

func (f *Future[O]) Result() (O, error) { var z O; return z, nil }

type Awaitable interface{ await() error }

type Settled[O any] struct {
	Value O
	Err   error
}

type BatchResult[O any] struct{ Items []O }

func (r BatchResult[O]) ThrowIfError() error { return nil }

type Branch[O any] struct {
	Name string
	Func func(ctx Context) (O, error)
}

type Callback[O any] struct{ _ O }

func (c *Callback[O]) ID() string { return "" }

func (c *Callback[O]) Result() (O, error) { var z O; return z, nil }

type ConditionConfig[S any] struct {
	InitialState S
	WaitStrategy func(state S, attempt int) WaitDecision
}

type WaitDecision struct {
	Continue bool
	Delay    time.Duration
}

type (
	StepOption            interface{}
	WaitOption            interface{}
	InvokeOption          interface{}
	ChildOption           interface{}
	BatchOption           interface{}
	CallbackOption        interface{}
	WaitForCallbackOption interface{}
	RetryOption           interface{}
	RetryStrategy         interface{}
	HandlerOption         interface{}
)

type Handler[I, O any] func(ctx Context, event I) (O, error)

func Start[I, O any](handler Handler[I, O], opts ...HandlerOption) {}

func Wrap[I, O any](handler Handler[I, O], opts ...HandlerOption) func(context.Context, []byte) ([]byte, error) {
	return nil
}

func ExecutionStartTime(ctx Context) time.Time { return time.Time{} }

func Step[O any](ctx Context, name string, fn func(StepContext) (O, error), opts ...StepOption) (O, error) {
	var z O
	return z, nil
}

func StepAsync[O any](ctx Context, name string, fn func(StepContext) (O, error), opts ...StepOption) *Future[O] {
	return nil
}

func Wait(ctx Context, name string, d time.Duration, opts ...WaitOption) error { return nil }

func WaitAsync(ctx Context, name string, d time.Duration, opts ...WaitOption) *Future[Void] {
	return nil
}

func Invoke[O, I any](ctx Context, name, functionID string, input I, opts ...InvokeOption) (O, error) {
	var z O
	return z, nil
}

func InvokeAsync[O, I any](ctx Context, name, functionID string, input I, opts ...InvokeOption) *Future[O] {
	return nil
}

func RunInChildContext[O any](ctx Context, name string, fn func(Context) (O, error), opts ...ChildOption) (O, error) {
	var z O
	return z, nil
}

func RunInChildContextAsync[O any](ctx Context, name string, fn func(Context) (O, error), opts ...ChildOption) *Future[O] {
	return nil
}

func Go[O any](ctx Context, name string, fn func(Context) (O, error), opts ...ChildOption) *Future[O] {
	return nil
}

func WaitForCondition[S any](ctx Context, name string, check func(StepContext, S) (S, error), cfg ConditionConfig[S]) (S, error) {
	var z S
	return z, nil
}

func CreateCallback[O any](ctx Context, name string, opts ...CallbackOption) (*Callback[O], error) {
	return nil, nil
}

func WaitForCallback[O any](ctx Context, name string, submitter func(ctx StepContext, callbackID string) error, opts ...WaitForCallbackOption) (O, error) {
	var z O
	return z, nil
}

func Map[I, O any](ctx Context, name string, items []I, fn func(ctx Context, item I, index int) (O, error), opts ...BatchOption) (BatchResult[O], error) {
	return BatchResult[O]{}, nil
}

func Parallel[O any](ctx Context, name string, branches []Branch[O], opts ...BatchOption) (BatchResult[O], error) {
	return BatchResult[O]{}, nil
}

func Select[O any](ctx Context, name string, branches []Branch[O], opts ...ChildOption) (winner string, value O, err error) {
	return "", value, nil
}

func Retry[O any](ctx Context, name string, fn func(ctx Context, attempt int) (O, error), strategy RetryStrategy, opts ...RetryOption) (O, error) {
	var z O
	return z, nil
}

func All[O any](ctx Context, name string, fs []*Future[O], opts ...ChildOption) ([]O, error) {
	return nil, nil
}

func AllSettled[O any](ctx Context, name string, fs []*Future[O], opts ...ChildOption) ([]Settled[O], error) {
	return nil, nil
}

func Any[O any](ctx Context, name string, fs []*Future[O], opts ...ChildOption) (O, error) {
	var z O
	return z, nil
}

func Race[O any](ctx Context, name string, fs []*Future[O], opts ...ChildOption) (O, error) {
	var z O
	return z, nil
}

func Join(ctx Context, name string, fs []Awaitable, opts ...ChildOption) error { return nil }
