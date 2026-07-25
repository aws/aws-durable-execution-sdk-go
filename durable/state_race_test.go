package durable

import (
	"fmt"
	"sync"
	"testing"
)

// TestExecutionStateConcurrentGetMerge exercises concurrent get and merge
// calls on executionState. Without the sync.RWMutex, this test triggers a
// DATA RACE under go test -race.
func TestExecutionStateConcurrentGetMerge(t *testing.T) {
	const numOps = 100
	const numReaders = 10
	const numMerges = 50

	// Seed initial operations.
	initial := make([]*operation, numOps)
	for i := range initial {
		initial[i] = &operation{
			id:     hashID(fmt.Sprintf("op-%d", i)),
			status: statusStarted,
		}
	}
	state := newExecutionState(initial)

	var wg sync.WaitGroup

	// Concurrent readers calling get.
	for r := range numReaders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range numOps {
				_ = state.get(fmt.Sprintf("op-%d", (i+r)%numOps))
			}
		}()
	}

	// Concurrent writer calling merge.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for m := range numMerges {
			ops := []*operation{
				{id: hashID(fmt.Sprintf("op-%d", m%numOps)), status: statusSucceeded},
			}
			state.merge(ops)
		}
	}()

	wg.Wait()

	// Sanity: the map still has all entries.
	if n := state.numOperations(); n != numOps {
		t.Fatalf("numOperations() = %d, want %d", n, numOps)
	}
}
