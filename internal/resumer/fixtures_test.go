package resumer

import (
	"bytes"
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
		Lease     string    `json:"lease"`
		Wait      string    `json:"wait"`
		Signal    string    `json:"signal"`
		Outcomes  []Outcome `json:"outcomes"`
		Calls     int32     `json:"calls"`
		Output    *string   `json:"output"` // null: the run has no output
		ErrorCode string    `json:"errorCode"`
	} `json:"expect"`
}

// fixtureFields are the fields every fixture must spell out: a case that
// leaves one out would otherwise assert its zero value without saying so.
var fixtureFields = map[string][]string{
	"file":    {"schemaVersion", "purpose", "cases"},
	"case":    {"name", "program", "steps", "expect"},
	"program": {"timeoutMillis", "node"},
	"expect":  {"run", "lease", "wait", "signal", "outcomes", "calls", "output", "errorCode"},
	"step":    {"do"},
	"signal":  {"do", "id", "expect"},
	"advance": {"do", "seconds"},
}

// loadWaitFixtures decodes the fixture file strictly: unknown fields,
// missing required fields and duplicate case names are errors.
func loadWaitFixtures(raw []byte) ([]waitFixture, error) {
	var file struct {
		SchemaVersion int           `json:"schemaVersion"`
		Purpose       string        `json:"purpose"`
		Cases         []waitFixture `json:"cases"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&file); err != nil {
		return nil, err
	}
	if file.SchemaVersion != 1 {
		return nil, fmt.Errorf("schemaVersion %d; want 1", file.SchemaVersion)
	}
	var shape struct {
		Cases []struct {
			Name    string            `json:"name"`
			Program json.RawMessage   `json:"program"`
			Steps   []json.RawMessage `json:"steps"`
			Expect  json.RawMessage   `json:"expect"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &shape); err != nil {
		return nil, err
	}
	if err := requireFields("file", raw); err != nil {
		return nil, err
	}
	var cases []json.RawMessage
	if err := json.Unmarshal(mustField(raw, "cases"), &cases); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for i, c := range cases {
		name := shape.Cases[i].Name
		if seen[name] {
			return nil, fmt.Errorf("case %d: duplicate name %q", i, name)
		}
		seen[name] = true
		checks := []struct {
			kind string
			raw  json.RawMessage
		}{{"case", c}, {"program", shape.Cases[i].Program}, {"expect", shape.Cases[i].Expect}}
		for _, step := range shape.Cases[i].Steps {
			kind := "step"
			var do struct {
				Do string `json:"do"`
			}
			if err := json.Unmarshal(step, &do); err != nil {
				return nil, err
			}
			if do.Do == "signal" || do.Do == "advance" {
				kind = do.Do
			}
			checks = append(checks, struct {
				kind string
				raw  json.RawMessage
			}{kind, step})
		}
		for _, check := range checks {
			if err := requireFields(check.kind, check.raw); err != nil {
				return nil, fmt.Errorf("case %q: %w", name, err)
			}
		}
	}
	return file.Cases, nil
}

func requireFields(kind string, raw json.RawMessage) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return fmt.Errorf("%s: %w", kind, err)
	}
	for _, name := range fixtureFields[kind] {
		if _, ok := fields[name]; !ok {
			return fmt.Errorf("%s: missing required field %q", kind, name)
		}
	}
	return nil
}

func mustField(raw json.RawMessage, name string) json.RawMessage {
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	return fields[name]
}

// TestWaitFixtures runs every case of testdata/waits/fixtures.json, with
// its predeclared outcome, through the real journal, engine and resumer.
func TestWaitFixtures(t *testing.T) {
	raw, err := os.ReadFile("../../testdata/waits/fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	cases, err := loadWaitFixtures(raw)
	if err != nil || len(cases) < 9 {
		t.Fatalf("fixtures: cases=%d err=%v", len(cases), err)
	}
	negative := 0
	for _, c := range cases {
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
	got := r.row(`SELECT r.state || '|' || COALESCE(r.lease_owner, '-') || '|' || COALESCE((SELECT state || '|' || signal_id FROM journal_waits WHERE run_id = r.run_id), '|') || '|' || r.error_code FROM journal_runs r WHERE r.run_id = ?`, run)
	if want := fmt.Sprintf("%s|%s|%s|%s|%s", c.Expect.Run, c.Expect.Lease, c.Expect.Wait, c.Expect.Signal, c.Expect.ErrorCode); got != want {
		t.Fatalf("run|lease|wait|signal|error:\n got %s\nwant %s", got, want)
	}
	want := "<null>"
	if c.Expect.Output != nil {
		want = *c.Expect.Output
	}
	if output := r.row(`SELECT COALESCE(CAST(output_json AS TEXT), '<null>') FROM journal_runs WHERE run_id = ?`, run); output != want {
		t.Fatalf("output %s; want %s", output, want)
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
