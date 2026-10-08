package journal

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/well-prado/new-blok/internal/engine"
)

// TestRunJournalSlots pins the slot journal on SQLite: a slot is written
// completed in one transaction, the same write again is a no-op, another
// result is final and permanent, and Slots reads exactly the loop's slots
// under the prefix (not a sibling loop's, not a nested loop's).
func TestRunJournalSlots(t *testing.T) {
	ctx, j, run, token := loopRig(t, "slots")
	rj := j.ForRun(run, token)
	digest, _ := engine.InputDigest(engineValue{Value: 4})
	if err := rj.VerifyRun(ctx, run, controlArtifact, digest); err != nil {
		t.Fatal(err)
	}
	scope := func(path, parent, kind string) engine.ScopeIdentity {
		return engine.ScopeIdentity{RunID: run, ArtifactDigest: controlArtifact, Path: path, ParentPath: parent, Kind: kind}
	}
	loop, err := rj.EnterScope(ctx, scope("loop@root", "", "each"), json.RawMessage(`{"items":2}`))
	if err != nil {
		t.Fatal(err)
	}
	other, err := rj.EnterScope(ctx, scope("loop2@root", "", "each"), json.RawMessage(`{"items":1}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, write := range []struct {
		entry engine.ScopeEntry
		slot  engine.ScopeIdentity
	}{
		{loop, scope("loop/body@loop[0]", "loop@root", "item")},
		{loop, scope("loop/body@loop[1]", "loop@root", "item")},
		{other, scope("loop2/body@loop2[0]", "loop2@root", "item")},
	} {
		if err := rj.RecordSlot(ctx, write.entry, write.slot, json.RawMessage(`{"output":1}`)); err != nil {
			t.Fatal(err)
		}
	}
	// A loop nested in item 0: its slot sorts just below the outer loop's
	// prefix ('/' < '@') and is not the outer loop's.
	inner, err := rj.EnterScope(ctx, scope("loop/body/inner@loop[0]", "loop/body@loop[0]", "each"), json.RawMessage(`{"items":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := rj.RecordSlot(ctx, inner, scope("loop/body/inner/body@loop[0]/inner[0]", "loop/body/inner@loop[0]", "item"), json.RawMessage(`{"output":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := rj.RecordSlot(ctx, loop, scope("loop/body@loop[0]", "loop@root", "item"), json.RawMessage(`{"output":1}`)); err != nil {
		t.Fatalf("the same slot again: %v", err)
	}
	if err := rj.RecordSlot(ctx, loop, scope("loop/body@loop[0]", "loop@root", "item"), json.RawMessage(`{"output":2}`)); !errors.Is(err, ErrRecordFinal) || !Permanent(err) {
		t.Fatalf("another result in a recorded slot: err=%v; want a permanent ErrRecordFinal", err)
	}
	if err := rj.RecordSlot(ctx, loop, scope("loop/body@loop[1]", "loop2@root", "item"), json.RawMessage(`{"output":1}`)); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("a slot under another loop than the one entered: err=%v", err)
	}
	slots, err := rj.Slots(ctx, loop, "loop/body@loop[")
	if err != nil || len(slots) != 2 || string(slots["loop/body@loop[1]"]) != `{"output":1}` {
		t.Fatalf("slots %v err=%v", slots, err)
	}
	if got := oneRow(t, j.database, `SELECT state || '|' || CAST(output_json AS TEXT) FROM journal_scopes WHERE run_id = '`+run+`' AND path = 'loop/body@loop[0]'`); got != `completed|{"output":1}` {
		t.Fatalf("slot row %s", got)
	}
}
