package migration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store"
)

func TestRetryRunsAgainOnlyWhileBusy(t *testing.T) {
	busy := fmt.Errorf("sqlite: begin: %w", store.ErrBusy)
	calls := 0
	if err := Retry(context.Background(), func() error {
		calls++
		if calls < 3 {
			return busy
		}
		return nil
	}); err != nil || calls != 3 {
		t.Fatalf("busy twice then success: err=%v calls=%d; want nil after 3", err, calls)
	}
	other := errors.New("no such table")
	calls = 0
	if err := Retry(context.Background(), func() error { calls++; return other }); !errors.Is(err, other) || calls != 1 {
		t.Fatalf("a failure that is not busy: err=%v calls=%d; want it returned at once", err, calls)
	}
}

func TestRetryStopsWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	begin := time.Now()
	err := Retry(ctx, func() error { return store.ErrBusy })
	if !errors.Is(err, store.ErrBusy) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v; want the busy failure joined with the deadline", err)
	}
	if took := time.Since(begin); took > 5*time.Second {
		t.Fatalf("Retry outlived its context by %v", took)
	}
}
