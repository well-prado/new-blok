package inspect_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/inspect"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/observe/redact"
	"github.com/well-prado/new-blok/store/sqlite"
)

type secretInput struct {
	SKU      string `json:"sku"`
	Password string `json:"password"`
	Note     string `json:"note"`
	Payload  string `json:"payload"`
	Blob     string `json:"blob"`
	Query    string `json:"query"`
	Fail     bool   `json:"fail"`
}

type secretOutput struct {
	Receipt string `json:"receipt"`
	Echo    string `json:"echo"`
}

var (
	secretInputSchema  = []byte(`{"type":"object","properties":{"sku":{"type":"string"},"password":{"type":"string"},"note":{"type":"string"},"payload":{"type":"string"},"blob":{"type":"string"},"query":{"type":"string"},"fail":{"type":"boolean"}},"required":["sku"]}`)
	secretOutputSchema = []byte(`{"type":"object","properties":{"receipt":{"type":"string"},"echo":{"type":"string"}},"required":["receipt","echo"]}`)
)

type redactionCase struct {
	Name     string      `json:"name"`
	Input    secretInput `json:"input"`
	Expected struct {
		Status               inspection.Status `json:"status"`
		ErrorCode            string            `json:"errorCode"`
		Effects              int64             `json:"effects"`
		Steps                int               `json:"steps"`
		Logs                 int               `json:"logs"`
		RedactedInputValues  int               `json:"redactedInputValues"`
		RedactedOutputValues int               `json:"redactedOutputValues"`
		LogMessageRedacted   bool              `json:"logMessageRedacted"`
		RedactedLogAttrs     int               `json:"redactedLogAttrs"`
	} `json:"expected"`
}

type redactionFixtures struct {
	SecretMarker    string          `json:"secretMarker"`
	ErrorCodeSecret string          `json:"errorCodeSecret"`
	Cases           []redactionCase `json:"cases"`
}

func loadRedactionFixtures(t *testing.T) redactionFixtures {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "testdata", "inspection", "redaction", "cases.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixtures redactionFixtures
	if err := json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	if fixtures.SecretMarker == "" || len(fixtures.Cases) < 2 {
		t.Fatal("redaction fixture inventory is incomplete")
	}
	return fixtures
}

// secretNode is a real Go node that sees secret-shaped content in every
// channel inspection can project: it logs a base64 credential in its message
// and secret attributes, returns encoded credentials, and fails with a
// label-shaped encoded credential as its code and one in its error text.
func secretNode(t *testing.T, codeSecret string, effects *atomic.Int64) node.Definition[secretInput, secretOutput] {
	t.Helper()
	return node.MustDefine("test/secret-handler", "1.0.0", func(ctx context.Context, in secretInput) (secretOutput, error) {
		effects.Add(1)
		node.Logger(ctx).Info("forwarding header "+in.Blob, "api_token", "SYNTHETIC-log-attr-0001", "note", in.Note, "sku", in.SKU)
		if in.Fail {
			return secretOutput{}, &node.DomainError{Code: codeSecret, Class: "business", Err: errors.New("provider said password=SYNTHETIC-error-0001")}
		}
		receipt := base64.StdEncoding.EncodeToString([]byte(`{"api_key":"SYNTHETIC-output-0001"}`))
		return secretOutput{Receipt: receipt, Echo: in.Payload}, nil
	}, node.Description("synthetic secret-shaped content in every channel"), node.Schemas(secretInputSchema, secretOutputSchema), node.Effects("synthetic:effect"))
}

func secretProgram() contract.InternalProgram {
	return contract.InternalProgram{WorkflowID: "secrets", Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "handle", Kind: "call", Node: "test/secret-handler"},
		{Index: 1, ID: "respond", Kind: "output", References: []contract.Reference{{Step: "handle", Path: []string{"receipt"}}}},
	}}
}

func everyField() inspection.Policy {
	return inspection.Policy{Fields: map[inspection.Field]bool{inspection.FieldInput: true, inspection.FieldOutput: true, inspection.FieldError: true, inspection.FieldLogs: true}, MaxPageSize: 10, MaxPayloadBytes: 4096}
}

func markers(raw json.RawMessage) int { return strings.Count(string(raw), `"`+redact.Marker+`"`) }

// TestSecretFixturesAreRedactedInEveryProjectedChannel runs each fixture
// through the real engine and checks the authorized owner's full-field
// projection: inputs, outputs, error labels and logs carry no secret-shaped
// value, plain or encoded, and the predeclared counts hold.
func TestSecretFixturesAreRedactedInEveryProjectedChannel(t *testing.T) {
	fixtures := loadRedactionFixtures(t)
	for _, item := range fixtures.Cases {
		t.Run(item.Name, func(t *testing.T) {
			recorder := inspect.NewRecorder()
			var effects atomic.Int64
			definition := secretNode(t, fixtures.ErrorCodeSecret, &effects)
			_, runErr := engine.New(map[string]node.Any{"test/secret-handler": definition.Any()}).WithObserver(recorder).RunObserved(context.Background(), secretProgram(), item.Input, inspection.Invocation{RunID: item.Name, Principal: "tenant-a/alice", Tenant: "tenant-a"})
			if (runErr != nil) != (item.Expected.Status == inspection.StatusFailed) {
				t.Fatalf("run error=%v for status %s", runErr, item.Expected.Status)
			}
			page, err := recorder.Inspect("tenant-a/alice", everyField(), inspection.Query{Version: inspection.Version, RunID: item.Name})
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(page)
			for _, leaked := range []string{fixtures.SecretMarker, item.Input.Blob, item.Input.Query, fixtures.ErrorCodeSecret} {
				if strings.Contains(string(raw), leaked) {
					t.Fatalf("projection leaked %q: %s", leaked, raw)
				}
			}
			handle := page.Steps[0]
			gotLogs := len(handle.Logs)
			logRedacted, logAttrs := false, 0
			if gotLogs > 0 {
				logRedacted = handle.Logs[0].Message == redact.MessageMarker
				logAttrs = markers(handle.Logs[0].Attrs)
			}
			got := []any{page.Run.Status, page.Run.ErrorCode, effects.Load(), len(page.Steps), gotLogs, markers(handle.Input), markers(handle.Output), logRedacted, logAttrs, markers(page.Run.Input)}
			want := []any{item.Expected.Status, item.Expected.ErrorCode, item.Expected.Effects, item.Expected.Steps, item.Expected.Logs, item.Expected.RedactedInputValues, item.Expected.RedactedOutputValues, item.Expected.LogMessageRedacted, item.Expected.RedactedLogAttrs, item.Expected.RedactedInputValues}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("field %d: got %v want %v\n(status, errorCode, effects, steps, logs, input markers, output markers, log message redacted, log attr markers, run input markers)\npage=%s", i, got[i], want[i], raw)
				}
			}
			if item.Expected.ErrorCode != "" && handle.ErrorCode != item.Expected.ErrorCode {
				t.Fatalf("step error code=%q", handle.ErrorCode)
			}
		})
	}
}

// TestFieldPolicyIsAppliedToAnAuthorizedReader: the owner is authorized, but
// a policy granting only outputs projects only outputs, through both the
// recorder and the durable journal source.
func TestFieldPolicyIsAppliedToAnAuthorizedReader(t *testing.T) {
	fixtures := loadRedactionFixtures(t)
	recorder := inspect.NewRecorder()
	var effects atomic.Int64
	failing := fixtures.Cases[1].Input
	_, _ = engine.New(map[string]node.Any{"test/secret-handler": secretNode(t, fixtures.ErrorCodeSecret, &effects).Any()}).WithObserver(recorder).RunObserved(context.Background(), secretProgram(), failing, inspection.Invocation{RunID: "policy", Principal: "alice"})
	outputsOnly := inspection.Policy{Fields: map[inspection.Field]bool{inspection.FieldOutput: true}, MaxPageSize: 10}
	page, err := recorder.Inspect("alice", outputsOnly, inspection.Query{Version: inspection.Version, RunID: "policy"})
	if err != nil {
		t.Fatal(err)
	}
	checkOutputsOnly(t, "recorder", page)

	j, runID := journalWithSecretRun(t, fixtures, "alice")
	page, err = inspect.InspectSource(context.Background(), j, "alice", outputsOnly, inspection.Query{Version: inspection.Version, RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	checkOutputsOnly(t, "journal", page)
}

func checkOutputsOnly(t *testing.T, source string, page inspection.Page) {
	t.Helper()
	if page.Run.Input != nil || page.Run.ErrorCode != "" || page.Run.ErrorClass != "" {
		t.Fatalf("%s: run projected unauthorized fields: %+v", source, page.Run)
	}
	for _, step := range page.Steps {
		if step.Input != nil || step.ErrorCode != "" || step.ErrorClass != "" || step.Logs != nil {
			t.Fatalf("%s: step projected unauthorized fields: %+v", source, step)
		}
		for _, attempt := range step.Attempts {
			if attempt.Input != nil || attempt.ErrorCode != "" {
				t.Fatalf("%s: attempt projected unauthorized fields: %+v", source, attempt)
			}
		}
	}
}

// journalWithSecretRun admits a run carrying the fixture's secret-shaped
// input into a real SQLite journal and fails it with the encoded error code.
func journalWithSecretRun(t *testing.T, fixtures redactionFixtures, principal string) (*journal.Journal, string) {
	t.Helper()
	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	j, err := journal.New(ctx, database, journal.Config{})
	if err != nil {
		t.Fatal(err)
	}
	input, _ := json.Marshal(fixtures.Cases[0].Input)
	run, err := j.Admit(ctx, journal.AdmissionRequest{RequestKey: "secret", Principal: principal, Workflow: "secrets", ArtifactDigest: "sha256:secrets", Input: input})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.FailRun(ctx, run.RunID, fixtures.ErrorCodeSecret, "business"); err != nil {
		t.Fatal(err)
	}
	return j, run.RunID
}

// TestJournalSourceRedactsStoredSecrets: the durable source holds the raw
// input and an encoded-credential error code; its projection holds neither.
func TestJournalSourceRedactsStoredSecrets(t *testing.T) {
	fixtures := loadRedactionFixtures(t)
	j, runID := journalWithSecretRun(t, fixtures, "tenant-a/alice")
	page, err := inspect.InspectSource(context.Background(), j, "tenant-a/alice", everyField(), inspection.Query{Version: inspection.Version, RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(page)
	if strings.Contains(string(raw), fixtures.SecretMarker) || strings.Contains(string(raw), fixtures.ErrorCodeSecret) {
		t.Fatalf("journal projection leaked: %s", raw)
	}
	if page.Run.ErrorCode != "redacted_label" || page.Run.ErrorClass != "business" || markers(page.Run.Input) != fixtures.Cases[0].Expected.RedactedInputValues {
		t.Fatalf("journal run projection=%+v", page.Run)
	}
}

// TestTwoPrincipalAndTwoTenantInspectionDenial: runs owned by principals of
// two tenants (with the same user name) in the recorder and in the journal.
// Each owner reads its own run; every other reader, including the other
// tenant's same-named user and a bare user name, gets exactly the
// not-found result an unknown run gets, with the widest field policy.
func TestTwoPrincipalAndTwoTenantInspectionDenial(t *testing.T) {
	fixtures := loadRedactionFixtures(t)
	recorder := inspect.NewRecorder()
	var effects atomic.Int64
	run := func(id, principal, tenant string) {
		_, err := engine.New(map[string]node.Any{"test/secret-handler": secretNode(t, fixtures.ErrorCodeSecret, &effects).Any()}).WithObserver(recorder).RunObserved(context.Background(), secretProgram(), fixtures.Cases[0].Input, inspection.Invocation{RunID: id, Principal: principal, Tenant: tenant})
		if err != nil {
			t.Fatal(err)
		}
	}
	run("run-a", "tenant-a/alice", "tenant-a")
	run("run-b", "tenant-b/alice", "tenant-b")
	_, unknownErr := recorder.Inspect("tenant-a/alice", everyField(), inspection.Query{Version: inspection.Version, RunID: "run-unknown"})
	if !errors.Is(unknownErr, inspect.ErrNotFound) {
		t.Fatalf("unknown run err=%v", unknownErr)
	}
	for runID, owner := range map[string]string{"run-a": "tenant-a/alice", "run-b": "tenant-b/alice"} {
		if _, err := recorder.Inspect(owner, everyField(), inspection.Query{Version: inspection.Version, RunID: runID}); err != nil {
			t.Fatalf("owner %s denied its run: %v", owner, err)
		}
		for _, reader := range []string{"tenant-a/alice", "tenant-b/alice", "alice", "tenant-a/bob"} {
			if reader == owner {
				continue
			}
			page, err := recorder.Inspect(reader, everyField(), inspection.Query{Version: inspection.Version, RunID: runID})
			if !errors.Is(err, inspect.ErrNotFound) || err.Error() != unknownErr.Error() || page.Run.ID != "" {
				t.Fatalf("recorder: %s read %s: page=%+v err=%v", reader, runID, page, err)
			}
		}
	}
	j, runID := journalWithSecretRun(t, fixtures, "tenant-a/alice")
	for _, reader := range []string{"tenant-b/alice", "alice"} {
		page, err := inspect.InspectSource(context.Background(), j, reader, everyField(), inspection.Query{Version: inspection.Version, RunID: runID})
		if !errors.Is(err, inspect.ErrNotFound) || page.Run.ID != "" {
			t.Fatalf("journal: %s read tenant-a's run: page=%+v err=%v", reader, page, err)
		}
	}
}
