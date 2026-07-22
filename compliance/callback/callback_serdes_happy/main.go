// Command callback_serdes_happy implements conformance requirement 4-15:
// custom deserializer converts JSON callback payload with Date field.
package main

import (
	"encoding/json"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type callbackPayload struct {
	ID        string `json:"id"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
}

type resultOutput struct {
	Received receivedData `json:"received"`
}

type receivedData struct {
	ID        string `json:"id"`
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`
}

// callbackSerdes deserializes the raw callback payload (JSON object with a
// timestamp string) into our typed struct, converting the ISO timestamp to
// a Unix epoch second.
type callbackSerdes struct{}

func (callbackSerdes) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (callbackSerdes) Unmarshal(data []byte, v any) error {
	// The callback result payload is a JSON object string.
	var raw callbackPayload
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	t, err := time.Parse(time.RFC3339Nano, raw.Timestamp)
	if err != nil {
		return err
	}
	result := &receivedData{
		ID:        raw.ID,
		Message:   raw.Message,
		Timestamp: t.Unix(),
	}
	// v is *callbackPayload per the SDK generic machinery, but we need to
	// return the custom type. Use json round-trip for simplicity.
	b, _ := json.Marshal(result)
	return json.Unmarshal(b, v)
}

func handler(ctx durable.Context, name string) (resultOutput, error) {
	cb, err := durable.CreateCallback[receivedData](ctx, name,
		durable.WithCallbackSerdes(callbackSerdes{}))
	if err != nil {
		return resultOutput{}, err
	}
	result, err := cb.Result()
	if err != nil {
		return resultOutput{}, err
	}
	return resultOutput{Received: result}, nil
}

func main() { durable.Start(handler) }
