package resumer

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/signal"
	"github.com/well-prado/new-blok/internal/journal"
)

type waitFixture struct {
	Name    string `json:"name"`
	Program struct {
		TimeoutMillis int64  `json:"timeoutMillis"`
		Node          string `json:"node"`
		Artifact      string `json:"artifact"`
	} `json:"program"`
	Steps []struct {
		Do           string `json:"do"`
		ID           string `json:"id"`
		Run          string `json:"run"`
		Seconds      int    `json:"seconds"`
		Unauthorized bool   `json:"unauthorized"`
		Expect       string `json:"expect"`
	} `json:"steps"`
	Expect struct {
		Run       string    `json:"run"`
		Wait      string    `json:"wait"`
		Signal    string    `json:"signal"`
		Outcomes  []Outcome `json:"outcomes"`
		Calls     int32     `json:"calls"`
		Output    string    `json:"output"`
		ErrorCode string    `json:"errorCode"`
	} `json:"expect"`
}

// TestWaitFixtures runs every case of testdata/waits/fixtures.json, with
// its predeclared outcome, through the real journal, engine and resumer.
func TestWaitFixtures(t *testing.T) {
	raw, err := os.ReadFile("../../testdata/waits/fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		SchemaVersion int           `json:"schemaVersion"`
		Cases         []waitFixture `json:"cases"`
	}
	if err := json.Unmarshal(raw, &file); err != nil || file.SchemaVersion != 1 || len(file.Cases) < 9 {
		t.Fatalf("fixtures: version=%d cases=%d err=%v", file.SchemaVersion, len(file.Cases), err)
	}
	negative := 0
	for _, c := range file.Cases {
		if len(c.Name) > 9 && c.Name[:9] == "negative-" {
			negative++
		}
		t.Run(c.Name, func(t *testing.T) { runFixture(t, c) })
	}
	if negative < 5 {
		t.Fatalf("%d negative cases; want at least 5", negative)
	}
}

func runFixture(t *testing.T, c waitFixture) {
	r := newRig(t, program(c.Program.TimeoutMillis, c.Program.Node), "a", "")
	artifactDigest := artifact
	if c.Program.Artifact != "" {
		artifactDigest = c.Program.Artifact
	}
	admitted, err := r.journal.Admit(r.ctx, journal.AdmissionRequest{RequestKey: c.Name, Workflow: "approval", ArtifactDigest: artifactDigest, Input: []byte(`{"value":4,"kind":"order"}`)})
	if err != nil {
		t.Fatal(err)
	}
	run := admitted.RunID
	for i, step := range c.Steps {
		switch step.Do {
		case "start":
			if err := r.resumer.Start(r.ctx, run); err != nil {
				t.Fatalf("step %d start: %v", i, err)
			}
		case "sweep":
			r.resumer.Sweep(r.ctx)
		case "advance":
			r.clock.Advance(time.Duration(step.Seconds) * time.Second)
		case "signal":
			target := run
			if step.Run != "" {
				target = step.Run
			}
			result, err := r.journal.Signal(r.ctx, signal.Envelope{RunID: target, SignalID: step.ID, Name: "approval", Principal: "operator", Payload: []byte(`{"approved":true}`)}, !step.Unauthorized)
			if got := signalOutcome(result, err); got != step.Expect {
				t.Fatalf("step %d signal %s: %s (%+v, %v); want %s", i, step.ID, got, result, err, step.Expect)
			}
		default:
			t.Fatalf("step %d: unknown action %q", i, step.Do)
		}
		r.resumer.executing.Wait()
	}
	got := r.row(`SELECT r.state || '|' || COALESCE((SELECT state || '|' || signal_id FROM journal_waits WHERE run_id = r.run_id), '|') || '|' || r.error_code FROM journal_runs r WHERE r.run_id = ?`, run)
	if want := fmt.Sprintf("%s|%s|%s|%s", c.Expect.Run, c.Expect.Wait, c.Expect.Signal, c.Expect.ErrorCode); got != want {
		t.Fatalf("run|wait|signal|error:\n got %s\nwant %s", got, want)
	}
	if c.Expect.Output != "" {
		if output := r.row(`SELECT COALESCE(output_json, '') FROM journal_runs WHERE run_id = ?`, run); output != c.Expect.Output {
			t.Fatalf("output %s; want %s", output, c.Expect.Output)
		}
	}
	if outcomes := r.outcomesOf(run); !reflect.DeepEqual(outcomes, c.Expect.Outcomes) {
		t.Fatalf("outcomes %v; want %v", outcomes, c.Expect.Outcomes)
	}
	if calls := r.notices.Load(); calls != c.Expect.Calls {
		t.Fatalf("node calls %d; want %d", calls, c.Expect.Calls)
	}
}

func signalOutcome(result journal.SignalResult, err error) string {
	switch {
	case errors.Is(err, journal.ErrUnauthorized):
		return "unauthorized"
	case errors.Is(err, journal.ErrNotFound):
		return "not_found"
	case err != nil:
		return "error: " + err.Error()
	case result.Duplicate:
		return "duplicate"
	case result.Late:
		return "late"
	case result.Resumed:
		return "delivered"
	case result.Accepted:
		return "pending"
	}
	return "unknown"
}
