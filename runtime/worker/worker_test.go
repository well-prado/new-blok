package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestEffectTransportFailurePreservesKnownCauseBeforeContextTimer(t *testing.T) {
	for _, cause := range []error{context.DeadlineExceeded, context.Canceled} {
		ctx := context.Background() // Its timer has not observed the deadline.
		failure := transportFailure(ctx, fmt.Errorf("synthetic-sensitive-detail: %w", cause))
		if ctx.Err() != nil || !errors.Is(failure, cause) || !failure.Uncertain || failure.Class != "uncertain" {
			t.Fatalf("lost known cause: %v", failure)
		}
		if strings.Contains(failure.Error(), "synthetic-sensitive-detail") {
			t.Fatal("transport detail leaked")
		}
	}
	failure := transportFailure(context.Background(), errors.New("synthetic-sensitive-detail"))
	if failure.Err != nil || errors.Is(failure, context.DeadlineExceeded) || strings.Contains(failure.Error(), "synthetic-sensitive-detail") {
		t.Fatal("unrelated transport failure was reclassified or leaked")
	}
}
