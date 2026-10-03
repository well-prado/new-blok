package cron_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
	"github.com/well-prado/new-blok/trigger/cron"
	"github.com/well-prado/new-blok/trigger/worker"
)

// pausing stops a child process on one side of the submission commit.
type pausing struct {
	inner  trigger.Submitter
	marker string
	before bool
}

func (p pausing) Submit(ctx context.Context, s trigger.Submission) (bool, error) {
	if p.before {
		_ = os.WriteFile(p.marker, []byte("before"), 0o600)
		time.Sleep(30 * time.Second)
	}
	accepted, err := p.inner.Submit(ctx, s)
	if !p.before && err == nil {
		_ = os.WriteFile(p.marker, []byte("after"), 0o600)
		time.Sleep(30 * time.Second)
	}
	return accepted, err
}

// TestCrashAroundOccurrenceCommit kills a scheduler process before an
// occurrence is submitted, after it is submitted but before its cursor is
// written, and after the cursor is written. Each restart over the same store
// leaves exactly one job for the occurrence.
func TestCrashAroundOccurrenceCommit(t *testing.T) {
	if mode := os.Getenv("NEWBLOK_CRON_CHILD"); mode != "" {
		runCronChild(t, mode)
		return
	}
	for _, tc := range []struct {
		mode       string
		submitted  int
		duplicates int
	}{
		{"before-submit", 1, 0},
		{"after-submit", 0, 1},
		{"after-cursor", 0, 0},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			directory := t.TempDir()
			path, marker := filepath.Join(directory, "cron.db"), filepath.Join(directory, "marker")
			command := exec.Command(os.Args[0], "-test.run=^TestCrashAroundOccurrenceCommit$")
			command.Env = append(os.Environ(), "NEWBLOK_CRON_CHILD="+tc.mode, "NEWBLOK_CRON_DB="+path, "NEWBLOK_CRON_MARKER="+marker)
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = command.Process.Kill(); _ = command.Wait() }()
			deadline := time.Now().Add(20 * time.Second)
			for {
				if _, err := os.Stat(marker); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("child never reached the crash point")
				}
				time.Sleep(10 * time.Millisecond)
			}
			_ = command.Process.Kill()
			_ = command.Wait()

			database, err := (sqlite.Backend{}).Open(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			queue, err := worker.New(context.Background(), database, nil)
			if err != nil {
				t.Fatal(err)
			}
			clock := newClock(at(11, 0))
			s, err := cron.New(context.Background(), database, queue, queue, clock)
			if err != nil {
				t.Fatal(err)
			}
			if added, err := s.Add(context.Background(), schedule("hourly", "0 * * * *")); err != nil || added {
				t.Fatalf("resume: added=%v err=%v", added, err)
			}
			result := tick(t, s)
			if len(result.Submitted) != tc.submitted || len(result.Duplicates) != tc.duplicates || jobs(t, database) != 1 {
				t.Fatalf("after restart: %s jobs=%d, want submitted=%d duplicates=%d and one job", fmt.Sprintf("%+v", result), jobs(t, database), tc.submitted, tc.duplicates)
			}
			if again := tick(t, s); len(again.Submitted)+len(again.Duplicates) != 0 {
				t.Fatalf("a further tick handled the occurrence again: %+v", again)
			}
		})
	}
}

func runCronChild(t *testing.T, mode string) {
	database, err := (sqlite.Backend{}).Open(context.Background(), os.Getenv("NEWBLOK_CRON_DB"))
	if err != nil {
		t.Fatal(err)
	}
	queue, err := worker.New(context.Background(), database, nil)
	if err != nil {
		t.Fatal(err)
	}
	marker := os.Getenv("NEWBLOK_CRON_MARKER")
	var submit trigger.Submitter = queue
	if mode != "after-cursor" {
		submit = pausing{inner: queue, marker: marker, before: mode == "before-submit"}
	}
	clock := newClock(at(10, 30))
	s, err := cron.New(context.Background(), database, submit, queue, clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Add(context.Background(), schedule("hourly", "0 * * * *")); err != nil {
		t.Fatal(err)
	}
	clock.Set(at(11, 0))
	if _, err := s.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(marker, []byte("after-cursor"), 0o600)
	time.Sleep(30 * time.Second)
}
