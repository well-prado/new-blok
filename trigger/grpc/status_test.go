package grpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestStatusForSidesOfTheDeadline pins how a call that ended is reported,
// independent of timing (#230): ending at or after its deadline is
// DeadlineExceeded, whatever ended it; ending before is Canceled, which
// includes a client's reset at its own earlier deadline.
func TestStatusForSidesOfTheDeadline(t *testing.T) {
	deadline := time.Now().Add(time.Hour)
	for _, tc := range []struct {
		name  string
		ended time.Time
		want  codes.Code
	}{
		{"canceled before the deadline", deadline.Add(-time.Millisecond), codes.Canceled},
		{"canceled at the deadline", deadline, codes.DeadlineExceeded},
		{"canceled after the deadline", deadline.Add(time.Millisecond), codes.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			cancel()
			if got := status.Code(statusFor(ctx, ctx.Err(), tc.ended)); got != tc.want {
				t.Fatalf("status=%v; want %v", got, tc.want)
			}
		})
	}
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if got := status.Code(statusFor(expired, context.DeadlineExceeded, time.Now())); got != codes.DeadlineExceeded {
		t.Fatalf("an expired call reported %v; want DeadlineExceeded", got)
	}
	if got := status.Code(statusFor(context.Background(), errors.New("workflow failed"), time.Now())); got != codes.Internal {
		t.Fatalf("an unclassified failure reported %v; want Internal", got)
	}
}
