// Command serde-basic demonstrates per-operation serdes using
// [durable.WithStepSerdes]. The serdes is built with [durable.SerdesOf],
// which adapts marshal and unmarshal functions written for one type (User)
// to the untyped [durable.Serdes] interface. The functions prefix serialized
// data with a marker, proving that the SDK uses the configured serializer
// for checkpoint storage and the matching deserializer on replay.
//
// Go's type system preserves struct methods inherently — JSON
// deserialization into a typed pointer restores full method access. This
// example therefore demonstrates the custom-serdes hook itself rather than
// class method preservation.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// User is the domain type whose serialization we customize.
type User struct {
	FirstName string `json:"firstName"`
	LastName  string `json:"lastName"`
	Email     string `json:"email"`
}

// FullName demonstrates that methods survive deserialization natively in Go.
func (u User) FullName() string {
	return fmt.Sprintf("%s %s", u.FirstName, u.LastName)
}

// Greet demonstrates method availability after replay.
func (u User) Greet() string {
	return fmt.Sprintf("Hello, I'm %s. My email is %s", u.FullName(), u.Email)
}

// newUserSerdes returns a Serdes for User that wraps encoding/json with a
// prefix marker to prove custom serialization is exercised during
// checkpoint writes and reads. durable.SerdesOf performs the type assertion,
// so the functions receive and return User directly instead of any.
func newUserSerdes(prefix []byte) durable.Serdes {
	return durable.SerdesOf(
		func(_ context.Context, _ durable.SerdesContext, u User) ([]byte, error) {
			b, err := json.Marshal(u)
			if err != nil {
				return nil, err
			}
			return slices.Concat(prefix, b), nil
		},
		func(_ context.Context, _ durable.SerdesContext, data []byte) (User, error) {
			var u User
			if len(data) < len(prefix) {
				return u, fmt.Errorf("serde-basic: missing prefix in checkpoint data")
			}
			return u, json.Unmarshal(data[len(prefix):], &u)
		},
	)
}

type event struct {
	FirstName string `json:"firstName"`
	LastName  string `json:"lastName"`
	Email     string `json:"email"`
}

type output struct {
	User     User   `json:"user"`
	Greeting string `json:"greeting"`
}

func handler(ctx durable.Context, ev event) (output, error) {
	userSerdes := newUserSerdes([]byte("USR:"))

	// Step 1: create user with custom serdes.
	user, err := durable.Step(ctx, "create-user", func(_ durable.StepContext) (User, error) {
		return User(ev), nil
	}, durable.WithStepSerdes(userSerdes))
	if err != nil {
		return output{}, err
	}

	// Wait forces a replay — on the second invocation the step above
	// deserializes using our custom serdes.
	if err := durable.Wait(ctx, "pause", 1*time.Second); err != nil {
		return output{}, err
	}

	// Step 2: use the deserialized user — methods work because Go types
	// are structural, not prototype-chain-based.
	greeting, err := durable.Step(ctx, "greet-user", func(_ durable.StepContext) (string, error) {
		return user.Greet(), nil
	})
	if err != nil {
		return output{}, err
	}

	return output{User: user, Greeting: greeting}, nil
}

func main() { durable.Start(handler) }
