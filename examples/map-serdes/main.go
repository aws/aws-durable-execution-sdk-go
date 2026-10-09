// Command map-serdes demonstrates the two serializers of a [durable.Map]
// operation. [durable.WithBatchSerdes] serializes each item's result in
// that item's own checkpoint. [durable.WithBatchResultSerdes] serializes the
// whole [durable.BatchResult] in the checkpoint of the Map operation
// itself. With a result serdes set, the item serdes still writes each
// item's checkpoint, but the result serdes alone writes the aggregate.
//
// The handler prices each line of an order. lineSerdes stores a priced
// line as the compact text "SKU|quantity|cents" instead of a JSON object.
// resultSerdes stores the aggregate gzip-compressed and base64-encoded. An
// aggregate larger than 256 KiB is not stored, and replay rebuilds the
// result from the items' checkpoints instead; compressing a large,
// repetitive aggregate can keep it within the limit. For the two lines of
// the sample order the compressed form is larger than the JSON.
//
// The SDK decodes an item's result with the item serdes as soon as the
// item finishes, so lineSerdes runs in both directions on the first
// invocation. It decodes the aggregate only when a later invocation
// replays the completed Map, so the handler waits after the Map and the
// second invocation reads the lines back through resultSerdes.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// LineItem is one line of the incoming order.
type LineItem struct {
	SKU      string  `json:"sku"`
	Quantity int     `json:"quantity"`
	Price    float64 `json:"price"`
}

// Order is the event: the shape of examples/event.json.
type Order struct {
	OrderID string     `json:"orderId"`
	Items   []LineItem `json:"items"`
}

// Line is a priced line, the result of one Map item.
type Line struct {
	SKU        string `json:"sku"`
	Quantity   int    `json:"quantity"`
	TotalCents int64  `json:"totalCents"`
}

type Output struct {
	OrderID    string   `json:"orderId"`
	Lines      []Line   `json:"lines"`
	Rejected   []string `json:"rejected"`
	TotalCents int64    `json:"totalCents"`
}

// lineSerdes stores a Line as "SKU|quantity|cents". durable.SerdesOf
// performs the type assertion, so the functions receive and return Line.
var lineSerdes = durable.SerdesOf(
	func(_ context.Context, _ durable.SerdesContext, l Line) ([]byte, error) {
		if strings.Contains(l.SKU, "|") {
			return nil, fmt.Errorf("map-serdes: SKU %q contains the separator", l.SKU)
		}
		return fmt.Appendf(nil, "%s|%d|%d", l.SKU, l.Quantity, l.TotalCents), nil
	},
	func(_ context.Context, _ durable.SerdesContext, data []byte) (Line, error) {
		parts := strings.Split(string(data), "|")
		if len(parts) != 3 {
			return Line{}, fmt.Errorf("map-serdes: malformed line %q", data)
		}
		quantity, err := strconv.Atoi(parts[1])
		if err != nil {
			return Line{}, fmt.Errorf("map-serdes: quantity: %w", err)
		}
		cents, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			return Line{}, fmt.Errorf("map-serdes: cents: %w", err)
		}
		return Line{SKU: parts[0], Quantity: quantity, TotalCents: cents}, nil
	},
)

// batchRecord is the form of the BatchResult that resultSerdes compresses.
// BatchItem.Err is an error interface, which encoding/json can encode but
// cannot decode, so a failed item is stored by its message. Replay returns
// that failure as a plain error carrying the message, not the typed error
// the first invocation saw; this handler reads only the message.
type batchRecord struct {
	Reason durable.CompletionReason `json:"reason"`
	Items  []itemRecord             `json:"items"`
}

type itemRecord struct {
	Index  int                     `json:"index"`
	Name   string                  `json:"name,omitempty"`
	Status durable.BatchItemStatus `json:"status"`
	Line   Line                    `json:"line"`
	Err    string                  `json:"err,omitempty"`
}

// resultSerdes stores the whole BatchResult as a gzip-compressed,
// base64-encoded batchRecord. Base64 keeps the checkpoint payload a valid
// string.
var resultSerdes = durable.SerdesOf(
	func(_ context.Context, _ durable.SerdesContext, r durable.BatchResult[Line]) ([]byte, error) {
		rec := batchRecord{Reason: r.Reason, Items: make([]itemRecord, len(r.Items))}
		for i, item := range r.Items {
			rec.Items[i] = itemRecord{Index: item.Index, Name: item.Name, Status: item.Status, Line: item.Result}
			if item.Err != nil {
				rec.Items[i].Err = item.Err.Error()
			}
		}
		raw, err := json.Marshal(rec)
		if err != nil {
			return nil, err
		}
		var zipped bytes.Buffer
		zw := gzip.NewWriter(&zipped)
		if _, err := zw.Write(raw); err != nil {
			return nil, err
		}
		if err := zw.Close(); err != nil {
			return nil, err
		}
		return []byte(base64.StdEncoding.EncodeToString(zipped.Bytes())), nil
	},
	func(_ context.Context, _ durable.SerdesContext, data []byte) (durable.BatchResult[Line], error) {
		zipped, err := base64.StdEncoding.DecodeString(string(data))
		if err != nil {
			return durable.BatchResult[Line]{}, fmt.Errorf("map-serdes: aggregate: %w", err)
		}
		zr, err := gzip.NewReader(bytes.NewReader(zipped))
		if err != nil {
			return durable.BatchResult[Line]{}, fmt.Errorf("map-serdes: aggregate: %w", err)
		}
		raw, err := io.ReadAll(zr)
		if err != nil {
			return durable.BatchResult[Line]{}, fmt.Errorf("map-serdes: aggregate: %w", err)
		}
		var rec batchRecord
		if err := json.Unmarshal(raw, &rec); err != nil {
			return durable.BatchResult[Line]{}, fmt.Errorf("map-serdes: aggregate: %w", err)
		}
		items := make([]durable.BatchItem[Line], len(rec.Items))
		for i, item := range rec.Items {
			items[i] = durable.BatchItem[Line]{Index: item.Index, Name: item.Name, Status: item.Status, Result: item.Line}
			if item.Err != "" {
				items[i].Err = errors.New(item.Err)
			}
		}
		return durable.BatchResult[Line]{Items: items, Reason: rec.Reason}, nil
	},
)

func handler(ctx durable.Context, order Order) (Output, error) {
	result, err := durable.Map(ctx, "price-lines", order.Items,
		func(_ durable.Context, item LineItem, index int) (Line, error) {
			if item.Quantity <= 0 {
				return Line{}, fmt.Errorf("line %d (%s): quantity must be positive, got %d", index, item.SKU, item.Quantity)
			}
			cents := int64(math.Round(item.Price * 100))
			return Line{SKU: item.SKU, Quantity: item.Quantity, TotalCents: cents * int64(item.Quantity)}, nil
		},
		durable.WithBatchSerdes(lineSerdes),
		durable.WithBatchResultSerdes(resultSerdes),
		// Price every line: a rejected line is reported, not fatal.
		durable.WithCompletion(durable.CompletionConfig{ToleratedFailurePercentage: aws.Int(100)}),
	)
	// A rejected line is reported as a *durable.BatchError alongside the
	// populated result; this handler reports the result. Any other error
	// is an SDK failure and propagates.
	var berr *durable.BatchError
	if err != nil && !errors.As(err, &berr) {
		return Output{}, err
	}

	// The wait ends the first invocation. The second replays the completed
	// Map, and result is then the aggregate decoded by resultSerdes.
	if err := durable.Wait(ctx, "settle", 1*time.Second); err != nil {
		return Output{}, err
	}

	out := Output{OrderID: order.OrderID, Lines: result.Results(), Rejected: []string{}}
	for _, item := range result.Failed() {
		out.Rejected = append(out.Rejected, item.Err.Error())
	}
	for _, l := range out.Lines {
		out.TotalCents += l.TotalCents
	}
	return out, nil
}

func main() { durable.Start(handler) }
