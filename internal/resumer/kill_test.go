package resumer

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestResumerKilledMidResumptionIsFinishedByAnother kills (SIGKILL) a
// process running the resumer while it executes a resumed run: the run
// was suspended at its wait, signalled, taken by a sweep, and is inside
// the node after the wait, its wakeup read but not consumed. A new
// resumer on the same journal file leaves the run while the dead
// process's lease lasts, then takes it as woken, runs the node again (it
// is pure) and completes it with the signal the dead process had read.
func TestResumerKilledMidResumptionIsFinishedByAnother(t *testing.T) {
	if os.Getenv("NEWBLOK_385_RESUMER_CHILD") == "1" {
		runResumerChild(t)
		return
	}
	directory := t.TempDir()
	path, marker := filepath.Join(directory, "journal.db"), filepath.Join(directory, "marker")
	child := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$")
	child.Env = append(os.Environ(), "NEWBLOK_385_RESUMER_CHILD=1", "NEWBLOK_385_JOURNAL="+path, "NEWBLOK_385_MARKER="+marker)
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- child.Wait() }()
	deadline := time.After(30 * time.Second)
	for ready := false; !ready; {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		select {
		case err := <-exited:
			if _, statErr := os.Stat(marker); statErr == nil {
				ready = true
				continue
			}
			t.Fatalf("the child exited before it was inside the resumed run: %v", err)
		case <-deadline:
			_ = child.Process.Kill()
			t.Fatal("the child never reached the resumed run's node")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-exited

	r := newRig(t, program(0, "test/hold"), "b", path)
	close(r.hold) // the node returns at once here
	run := r.row(`SELECT run_id FROM journal_runs WHERE request_key = 'crash'`)
	if got := r.runRow(run); got != "accepted|a|fired" {
		t.Fatalf("what the kill left: %s; want leased by the dead process, wakeup read and not consumed", got)
	}
	r.clock.Advance(10 * time.Second)
	if err := r.resumer.Sweep(r.ctx); err != nil {
		t.Fatal(err)
	}
	if got := r.runRow(run); got != "accepted|a|fired" || r.holding.Load() != 0 {
		t.Fatalf("while the dead process's lease lasts: %s, %d executions; want left alone", got, r.holding.Load())
	}
	r.clock.Advance(21 * time.Second) // past its lease
	if err := r.resumer.Sweep(r.ctx); err != nil {
		t.Fatal(err)
	}
	r.await(1)
	if got := r.runRow(run); got != "completed|-|acknowledged" {
		t.Fatalf("after the new resumer: %s; want completed, wakeup consumed, lease released", got)
	}
	if got := r.row(`SELECT signal_id FROM journal_waits WHERE run_id = ?`, run); got != "s1" {
		t.Fatalf("signal %s; want the one the dead process had read", got)
	}
	if got := r.row(`SELECT instr(CAST(output_json AS TEXT), '"s1"') > 0 FROM journal_runs WHERE run_id = ?`, run); got != "1" {
		t.Fatalf("output %s does not carry the signal", r.row(`SELECT CAST(output_json AS TEXT) FROM journal_runs WHERE run_id = ?`, run))
	}
	if n := r.holding.Load(); n != 1 {
		t.Fatalf("the new resumer ran the node %d times; want once", n)
	}
}

// runResumerChild suspends a run at its wait, signals it, resumes it with
// a sweep, and writes the marker once the node after the wait is running;
// that node never returns, so the parent's SIGKILL finds it there.
func runResumerChild(t *testing.T) {
	r := newRig(t, program(0, "test/hold"), "a", os.Getenv("NEWBLOK_385_JOURNAL"))
	run := r.admit("crash")
	if err := r.resumer.Start(r.ctx, run); err != nil {
		t.Fatal(err)
	}
	r.await(1)
	r.signal(run, "s1")
	if err := r.resumer.Sweep(r.ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return r.holding.Load() == 1 })
	if err := os.WriteFile(os.Getenv("NEWBLOK_385_MARKER"), []byte("inside"), 0o600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Minute)
	t.Fatal("not killed")
}
