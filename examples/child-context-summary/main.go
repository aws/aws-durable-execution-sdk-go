// Command child-context-summary demonstrates [durable.WithChildSummary].
// An import child context reads rows in chunks, one step per chunk, and
// returns them all. With the default four chunks the rows encode to more
// than the 256 KiB checkpoint limit, so the SDK does not store the result:
// it records that the child's operations are kept, and replay re-runs the
// child body, whose steps return their checkpointed chunks, to rebuild
// the rows. Without a summary that checkpoint carries no payload, and the
// execution history shows nothing about what the child produced. With
// WithChildSummary the SDK stores the summary string as the payload
// instead.
//
// The summary function runs only when the result exceeds the limit: an
// input of one chunk fits, so the full result is checkpointed and no
// summary is produced. The summary is advisory and the SDK never reads it
// back. A wait after the child suspends the execution, so the handler's
// result is computed on the second invocation from the rows the replayed
// child body rebuilt.
package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// rowsPerChunk and rowDataSize make each chunk about 85 KB of JSON, well
// under the limit for one step's result, while four chunks, about 340 KB,
// exceed the 256 KiB limit for the child's result.
const (
	rowsPerChunk  = 1000
	rowDataSize   = 64
	defaultChunks = 4
)

// Input optionally sets the number of chunks. Zero selects four.
type Input struct {
	Chunks int `json:"chunks"`
}

// Row is one imported record.
type Row struct {
	ID   int    `json:"id"`
	Data string `json:"data"`
}

// Output describes the rows the handler received from the child.
type Output struct {
	RowCount int `json:"rowCount"`
	FirstID  int `json:"firstId"`
	LastID   int `json:"lastId"`
}

// summarizeRows is the summary stored when the rows exceed the
// checkpoint limit. It depends only on its argument, so it is
// deterministic.
func summarizeRows(rows []Row) string {
	return fmt.Sprintf("%d rows imported", len(rows))
}

func handler(ctx durable.Context, in Input) (Output, error) {
	chunks := in.Chunks
	if chunks <= 0 {
		chunks = defaultChunks
	}

	rows, err := durable.RunInChildContext(ctx, "import-rows",
		func(child durable.Context) ([]Row, error) {
			var rows []Row
			for c := 0; c < chunks; c++ {
				chunk, err := durable.Step(child, fmt.Sprintf("read-chunk-%d", c),
					func(durable.StepContext) ([]Row, error) {
						return readChunk(c), nil
					})
				if err != nil {
					return nil, err
				}
				rows = append(rows, chunk...)
			}
			return rows, nil
		},
		durable.WithChildSummary(summarizeRows))
	if err != nil {
		return Output{}, err
	}

	// The wait ends the first invocation. The second replays import-rows:
	// a result over the limit was not checkpointed, so the child body runs
	// again and its steps return their checkpointed chunks.
	if err := durable.Wait(ctx, "before-report", 1*time.Second); err != nil {
		return Output{}, err
	}

	return Output{RowCount: len(rows), FirstID: rows[0].ID, LastID: rows[len(rows)-1].ID}, nil
}

// readChunk stands in for reading one page from a source system.
func readChunk(c int) []Row {
	rows := make([]Row, rowsPerChunk)
	for i := range rows {
		id := c*rowsPerChunk + i
		rows[i] = Row{ID: id, Data: strings.Repeat("x", rowDataSize)}
	}
	return rows
}

func main() { durable.Start(handler) }
