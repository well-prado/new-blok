//go:build ignore

// generate writes legacy-main-aaf633c.db: a journal written by origin/main
// at aaf633c, before #332, for the wait identity migration test. Its
// journal_waits table is keyed UNIQUE (run_id, name) and has no step or
// iteration identity; its journal is stamped schema version 3. It must be
// run from a checkout of that commit, never of a later one:
//
//	git worktree add --detach /tmp/main-aaf633c aaf633c
//	mkdir -p /tmp/main-aaf633c/testdata/restore/wait-identity-332
//	cp testdata/restore/wait-identity-332/generate.go /tmp/main-aaf633c/testdata/restore/wait-identity-332/
//	(cd /tmp/main-aaf633c && go run ./testdata/restore/wait-identity-332/generate.go /tmp/legacy.db)
//	gzip -9 -n -c /tmp/legacy.db > testdata/restore/wait-identity-332/legacy-main-aaf633c.db.gz
//
// The committed database decompresses to 155,648 bytes with sha256
// 7e8d68aec4e3d8b193f86f4d0e9f442d181a4966494aa1249f48eb991518fdaa. The
// hash pins the committed bytes only: running generate.go again writes a
// database with the same rows but a different hash, because aaf633c's
// Admit draws each run ID from crypto/rand and nothing outside that commit
// can fix it. The test finds each run by its request key, never by ID.
//
// Each run is admitted under its request key and holds the waits and
// signals named below, every one in a state origin/main can reach:
//
//	open      waits "approval" (open-approval) and "review" (open-review),
//	          both waiting: two names in one run
//	signaled  waits "approval" (signaled-approval), resumed by signal-1
//	early     signal early-1 for "approval" arrived first and is pending
//	canceled  waits "approval" (canceled-approval), canceled; signal late-1
//	          then arrived and was recorded late
//	due       waits "timer" (due-timer), due at the base time, unclaimed
//	claimed   waits "timer" (claimed-timer), due and claimed (resumed)
//
// The clock starts at 2026-10-05T12:00:00Z and advances one second per
// read. Every marker is synthetic. The fixture is a frozen historical
// snapshot, not a generated artifact checked for drift.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/well-prado/new-blok/contract/signal"
	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/store/sqlite"
)

func main() {
	if err := run(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(path string) error {
	ctx := context.Background()
	base := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	tick := base
	now := func() time.Time { tick = tick.Add(time.Second); return tick }
	db, err := (sqlite.Backend{}).Open(ctx, path)
	if err != nil {
		return err
	}
	defer db.Close()
	j, err := journal.New(ctx, db, journal.Config{Clock: now})
	if err != nil {
		return err
	}
	runs := map[string]string{}
	for _, key := range []string{"open", "signaled", "early", "canceled", "due", "claimed"} {
		admitted, err := j.Admit(ctx, journal.AdmissionRequest{RequestKey: key, Principal: "alice", Workflow: "orders", ArtifactDigest: "sha256:artifact", Input: []byte(`{"marker":"SYNTHETIC-332"}`)})
		if err != nil {
			return err
		}
		runs[key] = admitted.RunID
		fmt.Printf("%s run=%s\n", key, admitted.RunID)
	}
	later := base.Add(24 * time.Hour)
	for _, w := range []journal.WaitRequest{
		{RunID: runs["open"], WaitID: "open-approval", Name: "approval", DueAt: later},
		{RunID: runs["open"], WaitID: "open-review", Name: "review", DueAt: later},
		{RunID: runs["signaled"], WaitID: "signaled-approval", Name: "approval", DueAt: later},
		{RunID: runs["canceled"], WaitID: "canceled-approval", Name: "approval", DueAt: later},
		{RunID: runs["due"], WaitID: "due-timer", Name: "timer", DueAt: base},
	} {
		if _, err := j.ScheduleWait(ctx, w); err != nil {
			return err
		}
	}
	send := func(run, id string) (journal.SignalResult, error) {
		return j.Signal(ctx, signal.Envelope{RunID: runs[run], SignalID: id, Name: "approval", Principal: "operator", Payload: []byte(`{"marker":"SYNTHETIC-332-` + id + `"}`)}, true)
	}
	if result, err := send("signaled", "signal-1"); err != nil || !result.Resumed {
		return fmt.Errorf("signal-1: %+v %v", result, err)
	}
	if result, err := send("early", "early-1"); err != nil || !result.Accepted || result.Resumed {
		return fmt.Errorf("early-1: %+v %v", result, err)
	}
	if err := j.CancelWait(ctx, "canceled-approval"); err != nil {
		return err
	}
	if result, err := send("canceled", "late-1"); err != nil || !result.Late {
		return fmt.Errorf("late-1: %+v %v", result, err)
	}
	// claimed-timer is scheduled after due-timer was due, and claimed alone.
	if _, err := j.ScheduleWait(ctx, journal.WaitRequest{RunID: runs["claimed"], WaitID: "claimed-timer", Name: "timer", DueAt: base.Add(-time.Hour)}); err != nil {
		return err
	}
	claimed, err := j.ClaimDueWaits(ctx, base.Add(-time.Minute), 10)
	if err != nil || len(claimed) != 1 || claimed[0].WaitID != "claimed-timer" {
		return fmt.Errorf("claim: %+v %v", claimed, err)
	}
	return nil
}
