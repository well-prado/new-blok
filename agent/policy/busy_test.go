package policy

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/well-prado/new-blok/contract/approval"
	"github.com/well-prado/new-blok/contract/capacity"
	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/store"
)

// busyCommitJournal's CommitEffect finds the store busy.
type busyCommitJournal struct{ Journal }

func (busyCommitJournal) CommitEffect(context.Context, journal.EffectCommit) error {
	return fmt.Errorf("journal: commit effect: %w", store.ErrBusy)
}

// busyReader finds the store busy on its busyAt-th read.
type busyReader struct {
	approval.Reader
	reads, busyAt int
}

func (r *busyReader) Get(ctx context.Context, id string) (approval.Decision, bool, error) {
	r.reads++
	if r.reads == r.busyAt {
		return approval.Decision{}, false, fmt.Errorf("approval: read: %w", store.ErrBusy)
	}
	return r.Reader.Get(ctx, id)
}

// TestBusyStorePastDispatchIsUncertainNotSaturated: once the effect has
// been dispatched, a busy store (the approval recheck before or after the
// effect, or the journal commit after it) leaves the effect uncertain. The
// error must not read as saturation, which would tell the caller to retry
// an action whose outcome is unknown (#190).
func TestBusyStorePastDispatchIsUncertainNotSaturated(t *testing.T) {
	for _, test := range []struct {
		name    string
		busy    func(*rig)
		effects int32
	}{
		{"approval recheck before the effect", func(r *rig) { r.p.cfg.Approvals = &busyReader{Reader: r.a, busyAt: 2} }, 0},
		{"approval recheck after the effect", func(r *rig) { r.p.cfg.Approvals = &busyReader{Reader: r.a, busyAt: 3} }, 1},
		{"journal commit after the effect", func(r *rig) { r.p.cfg.Journal = busyCommitJournal{Journal: r.j} }, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := setup(t, filepath.Join(t.TempDir(), "journal.db"))
			r.approve(t, true)
			test.busy(r)
			out, err := r.p.Invoke(context.Background(), r.target, r.call)
			if out != nil || !errors.Is(err, ErrExecution) || errors.Is(err, capacity.ErrSaturated) || errors.Is(err, store.ErrBusy) {
				t.Fatalf("busy store past dispatch returned %s %v; want ErrExecution, not saturation", out, err)
			}
			if attempts, committed := r.trace(t); attempts != 1 || committed != 0 || r.effects.Load() != test.effects {
				t.Fatalf("attempts=%d committed=%d effects=%d; want 1, 0, %d", attempts, committed, r.effects.Load(), test.effects)
			}
		})
	}
}
