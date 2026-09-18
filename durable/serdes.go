package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

// JSONSerdes is the default [Serdes]. It encodes with [json.Marshal] and
// decodes with [json.Unmarshal]. The SDK uses it for every operation result
// when no serializer option is supplied: no handler-wide [WithSerdes], no
// per-operation option such as [WithStepSerdes], and no [ConfigureSerdes]
// call in the handler.
//
// JSONSerdes holds no state, so it is safe for concurrent use from any
// number of goroutines and executions.
//
// Use it to write a custom serdes that handles a few types itself and
// defers everything else to the default encoding. The serdes below stores a
// [time.Time] as Unix nanoseconds and leaves every other type to
// JSONSerdes:
//
//	type unixTimeSerdes struct{}
//
//	func (unixTimeSerdes) Marshal(ctx context.Context, meta durable.SerdesContext, v any) ([]byte, error) {
//		if t, ok := v.(time.Time); ok {
//			return []byte(strconv.FormatInt(t.UnixNano(), 10)), nil
//		}
//		return durable.JSONSerdes.Marshal(ctx, meta, v)
//	}
//
//	func (unixTimeSerdes) Unmarshal(ctx context.Context, meta durable.SerdesContext, data []byte, v any) error {
//		if t, ok := v.(*time.Time); ok {
//			ns, err := strconv.ParseInt(string(data), 10, 64)
//			if err != nil {
//				return err
//			}
//			*t = time.Unix(0, ns)
//			return nil
//		}
//		return durable.JSONSerdes.Unmarshal(ctx, meta, data, v)
//	}
//
//	durable.Start(handler, durable.WithSerdes(unixTimeSerdes{}))
var JSONSerdes = jsonSerdes{}

// jsonSerdes is the type of [JSONSerdes]. It has no fields, so every value
// of the type is the same serdes: JSONSerdes cannot be set to nil or
// replaced with a different implementation, and the SDK uses JSONSerdes
// itself as its default.
type jsonSerdes struct{}

var _ Serdes = jsonSerdes{}

func (jsonSerdes) Marshal(_ context.Context, _ SerdesContext, v any) ([]byte, error) {
	return json.Marshal(v)
}

func (jsonSerdes) Unmarshal(_ context.Context, _ SerdesContext, data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// SerdesOf adapts typed marshal and unmarshal functions to the [Serdes]
// interface. The returned Serdes rejects values of any other type with an
// error that names both the expected and the actual type.
//
// Use SerdesOf for a serdes written for one result type. It performs the
// type assertion once so that marshal and unmarshal receive and return T
// directly:
//
//	masked := durable.SerdesOf(
//		func(_ context.Context, _ durable.SerdesContext, r Receipt) ([]byte, error) {
//			r.Card = "****" + r.Card[len(r.Card)-4:]
//			return json.Marshal(r)
//		},
//		func(_ context.Context, _ durable.SerdesContext, b []byte) (Receipt, error) {
//			var r Receipt
//			return r, json.Unmarshal(b, &r)
//		},
//	)
//	receipt, err := durable.Step(ctx, "charge", chargeCard, durable.WithStepSerdes(masked))
//
// T is inferred from the function arguments. The returned Serdes works with
// every With*Serdes option. When attached handler-wide with [WithSerdes] it
// serves every operation result in the handler, so an operation whose result
// is not T fails at runtime with a [SerdesError]. That is the intended
// behaviour: a handler-wide serdes must accept every result type it is asked
// to serialize, and SerdesOf accepts exactly one.
func SerdesOf[T any](
	marshal func(ctx context.Context, meta SerdesContext, v T) ([]byte, error),
	unmarshal func(ctx context.Context, meta SerdesContext, data []byte) (T, error),
) Serdes {
	return typedSerdes[T]{marshal: marshal, unmarshal: unmarshal}
}

// typedSerdes is the Serdes returned by SerdesOf.
type typedSerdes[T any] struct {
	marshal   func(ctx context.Context, meta SerdesContext, v T) ([]byte, error)
	unmarshal func(ctx context.Context, meta SerdesContext, data []byte) (T, error)
}

func (s typedSerdes[T]) Marshal(ctx context.Context, meta SerdesContext, v any) ([]byte, error) {
	want := reflect.TypeFor[T]()
	if s.marshal == nil {
		return nil, fmt.Errorf("durable: SerdesOf[%v]: marshal function is nil", want)
	}
	typed, ok := v.(T)
	if !ok {
		return nil, fmt.Errorf("durable: SerdesOf[%v]: Marshal got %T, want %v", want, v, want)
	}
	return s.marshal(ctx, meta, typed)
}

func (s typedSerdes[T]) Unmarshal(ctx context.Context, meta SerdesContext, data []byte, v any) error {
	want := reflect.TypeFor[T]()
	if s.unmarshal == nil {
		return fmt.Errorf("durable: SerdesOf[%v]: unmarshal function is nil", want)
	}
	target, ok := v.(*T)
	if !ok {
		return fmt.Errorf("durable: SerdesOf[%v]: Unmarshal got %T, want %v", want, v, reflect.PointerTo(want))
	}
	if target == nil {
		return fmt.Errorf("durable: SerdesOf[%v]: Unmarshal got a nil %v", want, reflect.PointerTo(want))
	}
	out, err := s.unmarshal(ctx, meta, data)
	if err != nil {
		return err
	}
	*target = out
	return nil
}
