package durable

import (
	"errors"
	"testing"
)

func TestGoidParsesCurrentGoroutine(t *testing.T) {
	id, ok := goid()
	if !ok {
		t.Fatal("goid() ok = false, want true")
	}
	if id == 0 {
		t.Error("goid() = 0, want a positive goroutine ID")
	}
}

func TestGoidDiffersAcrossGoroutines(t *testing.T) {
	mainID, ok := goid()
	if !ok {
		t.Fatal("goid() ok = false on test goroutine")
	}

	type result struct {
		id uint64
		ok bool
	}
	ch := make(chan result, 1)
	go func() {
		id, ok := goid()
		ch <- result{id: id, ok: ok}
	}()
	got := <-ch
	if !got.ok {
		t.Fatal("goid() ok = false on spawned goroutine")
	}
	if got.id == mainID {
		t.Errorf("spawned goroutine ID %d equals owner ID, want distinct IDs", got.id)
	}
}

func TestGoroutineOwnerCheck(t *testing.T) {
	owner := currentGoroutineOwner()

	if err := owner.check(); err != nil {
		t.Errorf("check() on owning goroutine = %v, want nil", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- owner.check()
	}()
	err := <-errCh
	if !errors.Is(err, ErrWrongGoroutine) {
		t.Errorf("check() from foreign goroutine = %v, want ErrWrongGoroutine", err)
	}
}

func TestGoroutineOwnerDisabledFailsOpen(t *testing.T) {
	owner := goroutineOwner{ok: false}
	errCh := make(chan error, 1)
	go func() {
		errCh <- owner.check()
	}()
	if err := <-errCh; err != nil {
		t.Errorf("check() with detection disabled = %v, want nil", err)
	}
}
