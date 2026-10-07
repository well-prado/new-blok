package journal

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

// journalFixtures is testdata/store/journal-fixtures.json (#44, #339).
// Each case's expect object is decoded into the type its runner reports,
// and must equal what the runner observes on a real SQLite journal.
type journalFixtures struct {
	SchemaVersion string                `json:"schemaVersion"`
	Cases         []journalFixtureCase  `json:"cases"`
	CrashBarriers []journalFixtureCrash `json:"crashBarriers"`
	Limits        []string              `json:"limits"`
}

type journalFixtureCase struct {
	Name   string          `json:"name"`
	Expect json.RawMessage `json:"expect"`
}

type journalFixtureCrash struct {
	Name   string `json:"name"`
	Before string `json:"before"`
	After  string `json:"after"`
}

const journalFixtureSchema = "journal/v2"

// journalFixtureRunners runs each case the file must declare. A runner
// returns the case's observation; its type is the type the case's expect
// object decodes into.
var journalFixtureRunners = map[string]func(*testing.T) any{
	"concurrent duplicate admission": runDuplicateAdmissionFixture,
	"committed result replay":        runCommittedReplayFixture,
	"uncertain external outcome":     runUncertainOutcomeFixture,
	"new replay lineage":             runReplayLineageFixture,
	"stale attempt result":           runStaleAttemptFixture,
	"integrity failure":              runIntegrityFailureFixture,
	"write failure":                  runWriteFailureFixture,
}

// The cases and crash barriers the file must declare, in order: a case
// dropped from the file, or one added without a runner, fails instead of
// going unrun.
var (
	journalFixtureCases = []string{"concurrent duplicate admission", "committed result replay", "uncertain external outcome", "new replay lineage", "stale attempt result", "integrity failure", "write failure"}
	// journalFixtureBarriers maps each crash barrier to the transition the
	// barrier children kill (runAdmissionChild, runTransitionChild).
	journalFixtureBarriers = map[string]string{
		"admission":                 "admission",
		"effect intent":             "intent",
		"attempt dispatch":          "dispatch",
		"result commit":             "result",
		"completion acknowledgment": "ack",
	}
	journalFixtureBarrierOrder = []string{"admission", "effect intent", "attempt dispatch", "result commit", "completion acknowledgment"}
)

func loadJournalFixtures(t *testing.T) journalFixtures {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "store", "journal-fixtures.json"))
	if err != nil {
		t.Fatal(err)
	}
	fixtures, err := decodeJournalFixtures(raw)
	if err != nil {
		t.Fatalf("journal fixtures: %v", err)
	}
	return fixtures
}

// decodeJournalFixtures reads the fixture strictly: no unknown field, no
// field left out (see strictJSON), no content after it, the known schema,
// exactly the declared cases and barriers, and every case's expectation
// decodes strictly into its runner's observation type.
func decodeJournalFixtures(raw []byte) (journalFixtures, error) {
	var fixtures journalFixtures
	if err := strictJSON(raw, &fixtures); err != nil {
		return journalFixtures{}, err
	}
	if fixtures.SchemaVersion != journalFixtureSchema {
		return journalFixtures{}, fmt.Errorf("schema %q, want %s", fixtures.SchemaVersion, journalFixtureSchema)
	}
	var cases, barriers []string
	for _, fixture := range fixtures.Cases {
		cases = append(cases, fixture.Name)
	}
	for _, barrier := range fixtures.CrashBarriers {
		barriers = append(barriers, barrier.Name)
	}
	if !slices.Equal(cases, journalFixtureCases) || !slices.Equal(barriers, journalFixtureBarrierOrder) {
		return journalFixtures{}, fmt.Errorf("cases %q and crash barriers %q, want %q and %q", cases, barriers, journalFixtureCases, journalFixtureBarrierOrder)
	}
	for _, fixture := range fixtures.Cases {
		if _, err := decodeJournalExpectation(fixture); err != nil {
			return journalFixtures{}, fmt.Errorf("case %q: %w", fixture.Name, err)
		}
	}
	return fixtures, nil
}

// decodeJournalExpectation decodes a case's expect object into a fresh
// value of the type its runner observes.
func decodeJournalExpectation(fixture journalFixtureCase) (any, error) {
	run, ok := journalFixtureRunners[fixture.Name]
	if !ok {
		return nil, errors.New("no runner")
	}
	kind, ok := journalFixtureTypes[fixture.Name]
	if !ok || run == nil {
		return nil, errors.New("no observation type")
	}
	expected := reflect.New(kind)
	if err := strictJSON(fixture.Expect, expected.Interface()); err != nil {
		return nil, err
	}
	return expected.Elem().Interface(), nil
}

// journalFixtureTypes is the observation type of each runner.
var journalFixtureTypes = map[string]reflect.Type{
	"concurrent duplicate admission": reflect.TypeFor[duplicateAdmissionObservation](),
	"committed result replay":        reflect.TypeFor[committedReplayObservation](),
	"uncertain external outcome":     reflect.TypeFor[uncertainOutcomeObservation](),
	"new replay lineage":             reflect.TypeFor[replayLineageObservation](),
	"stale attempt result":           reflect.TypeFor[staleAttemptObservation](),
	"integrity failure":              reflect.TypeFor[integrityFailureObservation](),
	"write failure":                  reflect.TypeFor[writeFailureObservation](),
}

// strictJSON decodes raw into value, refusing unknown fields and content
// after the value, then requires value to encode back to raw's document,
// which fails for a field raw leaves out: an expectation dropped from the
// file is not read as its zero value.
func strictJSON(raw []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("content after the fixture object (%v)", err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	var want, got any
	if err := json.Unmarshal(raw, &want); err != nil {
		return err
	}
	if err := json.Unmarshal(encoded, &got); err != nil {
		return err
	}
	if !reflect.DeepEqual(want, got) {
		return fmt.Errorf("the file and the fields the tests read differ (a field is missing or null):\nfile: %s\nread: %s", raw, encoded)
	}
	return nil
}

// TestJournalFixtureGuards proves the loader's guards can fail: every
// declared case has a runner and an observation type and vice versa, and
// each mutation of today's fixture is refused.
func TestJournalFixtureGuards(t *testing.T) {
	declared := slices.Clone(journalFixtureCases)
	sort.Strings(declared)
	for name, set := range map[string][]string{"runners": keysOf(journalFixtureRunners), "observation types": keysOf(journalFixtureTypes)} {
		if !slices.Equal(set, declared) {
			t.Fatalf("%s %q, want exactly the declared cases %q", name, set, declared)
		}
	}
	barriers := keysOf(journalFixtureBarriers)
	order := slices.Clone(journalFixtureBarrierOrder)
	sort.Strings(order)
	if !slices.Equal(barriers, order) {
		t.Fatalf("barrier transitions %q, want the declared barriers %q", barriers, order)
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "store", "journal-fixtures.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeJournalFixtures(raw); err != nil {
		t.Fatalf("today's fixture: %v", err)
	}
	for _, mutation := range []struct{ name, old, new, want string }{
		{"trailing content", "\n}\n", "\n}\n[]\n", "content after"},
		{"unknown top-level field", `"schemaVersion": "journal/v2",`, `"schemaVersion": "journal/v2", "extra": true,`, "unknown field"},
		{"unknown expectation", `"currentAttemptWins": true`, `"currentAttemptWins": true, "winner": "second"`, "unknown field"},
		{"missing expectation", `"storedRuns": 0,`, ``, "a field is missing"},
		{"dropped case", `"name": "stale attempt result"`, `"name": "integrity failure"`, "cases"},
		{"case without a runner", `"name": "write failure"`, `"name": "disk full"`, "cases"},
		{"dropped barrier", `{"name": "result commit", "before": "dispatched", "after": "committed"},`, ``, "crash barriers"},
		{"schema", `"journal/v2"`, `"journal/v1"`, "schema"},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			mutated := strings.Replace(string(raw), mutation.old, mutation.new, 1)
			if mutated == string(raw) {
				t.Fatalf("mutation %q did not apply to the fixture", mutation.old)
			}
			if _, err := decodeJournalFixtures([]byte(mutated)); err == nil || !strings.Contains(err.Error(), mutation.want) {
				t.Fatalf("mutated fixture: err=%v, want one containing %q", err, mutation.want)
			}
		})
	}
}

func keysOf[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// TestJournalFixtureCases runs every case against a real SQLite journal and
// compares the observation with the case's expectation.
func TestJournalFixtureCases(t *testing.T) {
	for _, fixture := range loadJournalFixtures(t).Cases {
		t.Run(fixture.Name, func(t *testing.T) {
			expected, err := decodeJournalExpectation(fixture)
			if err != nil {
				t.Fatal(err)
			}
			observed := journalFixtureRunners[fixture.Name](t)
			if !reflect.DeepEqual(observed, expected) {
				got, _ := json.Marshal(observed)
				t.Fatalf("observed %s, fixture expects %s", got, fixture.Expect)
			}
		})
	}
}

// journalErrorName names the journal's typed refusals as the fixture does.
func journalErrorName(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ErrRequestConflict):
		return "request_conflict"
	case errors.Is(err, ErrNotDispatchable):
		return "not_dispatchable"
	case errors.Is(err, ErrUncertain):
		return "uncertain"
	case errors.Is(err, ErrStaleAttempt):
		return "stale_attempt"
	default:
		return "unexpected: " + err.Error()
	}
}

func countJournalRows(t *testing.T, database store.Database, query string, args ...any) int {
	t.Helper()
	count := -1
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), query, args...).Scan(&count)
	}); err != nil {
		t.Fatal(err)
	}
	return count
}

var fixtureRequest = AdmissionRequest{RequestKey: "fixture-order", Workflow: "orders/quote", ArtifactDigest: "sha256:artifact", Input: []byte(`{"sku":"coffee","quantity":2}`)}

type duplicateAdmissionObservation struct {
	Callers           int    `json:"callers"`
	Accepted          int    `json:"accepted"`
	RunIDs            int    `json:"runIds"`
	StoredRuns        int    `json:"storedRuns"`
	ConflictingReuse  string `json:"conflictingReuse"`
	ObservableEffects int    `json:"observableEffects"`
}

// runDuplicateAdmissionFixture admits one request from 32 concurrent
// callers, then reuses its key with a different input.
func runDuplicateAdmissionFixture(t *testing.T) any {
	database, journal := newJournal(t, "journal.db", Config{})
	defer database.Close()
	const callers = 32
	admissions := make([]Admission, callers)
	failures := make([]error, callers)
	start := make(chan struct{})
	var group sync.WaitGroup
	for index := range callers {
		group.Go(func() {
			<-start
			admissions[index], failures[index] = journal.Admit(context.Background(), fixtureRequest)
		})
	}
	close(start)
	group.Wait()
	observed := duplicateAdmissionObservation{Callers: callers}
	runIDs := map[string]bool{}
	for index, admission := range admissions {
		if failures[index] != nil {
			t.Fatalf("caller %d: %v", index, failures[index])
		}
		if admission.Accepted {
			observed.Accepted++
		}
		runIDs[admission.RunID] = true
	}
	observed.RunIDs = len(runIDs)
	conflict := fixtureRequest
	conflict.Input = []byte(`{"sku":"tea","quantity":2}`)
	_, err := journal.Admit(context.Background(), conflict)
	observed.ConflictingReuse = journalErrorName(err)
	observed.StoredRuns = countJournalRows(t, database, `SELECT COUNT(*) FROM journal_runs WHERE request_key = ?`, fixtureRequest.RequestKey)
	observed.ObservableEffects = countJournalRows(t, database, `SELECT COUNT(*) FROM journal_operations`) + countJournalRows(t, database, `SELECT COUNT(*) FROM journal_attempts`)
	return observed
}

type committedReplayObservation struct {
	Attempts              int    `json:"attempts"`
	ProviderOperationKeys int    `json:"providerOperationKeys"`
	CommittedAttempts     int    `json:"committedAttempts"`
	ReplayState           string `json:"replayState"`
	ReplayResult          string `json:"replayResult"`
	RedispatchAfterReopen string `json:"redispatchAfterReopen"`
}

// runCommittedReplayFixture fails one attempt retryably, commits the next,
// completes the run, reopens the journal and replays the effect.
func runCommittedReplayFixture(t *testing.T) any {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "journal.db")
	database, journal := newJournalAtPath(t, path, Config{})
	admission, err := journal.Admit(ctx, fixtureRequest)
	if err != nil {
		t.Fatal(err)
	}
	identity := OperationIdentity{RunID: admission.RunID, ArtifactDigest: fixtureRequest.ArtifactDigest, InvocationPath: "charge", IterationPath: "root"}
	operation, err := journal.BeginEffect(ctx, EffectIntent{Identity: identity})
	if err != nil {
		t.Fatal(err)
	}
	first, err := journal.StartAttempt(ctx, operation.Key)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.FailAttempt(ctx, operation.Key, first.ID, true, "temporary provider timeout"); err != nil {
		t.Fatal(err)
	}
	second, err := journal.StartAttempt(ctx, operation.Key)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.CommitEffect(ctx, EffectCommit{OperationKey: operation.Key, AttemptID: second.ID, Result: []byte(`{"charged":true}`)}); err != nil {
		t.Fatal(err)
	}
	if err := journal.CompleteRun(ctx, admission.RunID, []byte(`{"totalCents":3000}`)); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, journal = newJournalAtPath(t, path, Config{})
	defer database.Close()
	var observed committedReplayObservation
	observed.Attempts = countJournalRows(t, database, `SELECT COUNT(*) FROM journal_attempts WHERE operation_key = ?`, operation.Key)
	observed.ProviderOperationKeys = countJournalRows(t, database, `SELECT COUNT(DISTINCT provider_operation_key) FROM journal_attempts WHERE operation_key = ?`, operation.Key)
	observed.CommittedAttempts = countJournalRows(t, database, `SELECT COUNT(*) FROM journal_attempts WHERE operation_key = ? AND state = ?`, operation.Key, attemptCommitted)
	replayed, err := journal.BeginEffect(ctx, EffectIntent{Identity: identity})
	if err != nil {
		t.Fatal(err)
	}
	observed.ReplayState, observed.ReplayResult = replayed.State, string(replayed.Result)
	_, err = journal.StartAttempt(ctx, operation.Key)
	observed.RedispatchAfterReopen = journalErrorName(err)
	return observed
}

type uncertainOutcomeObservation struct {
	State                     string `json:"state"`
	Redispatch                string `json:"redispatch"`
	CompleteRun               string `json:"completeRun"`
	ReconciledState           string `json:"reconciledState"`
	RedispatchAfterReconcile  string `json:"redispatchAfterReconcile"`
	CompleteRunAfterReconcile string `json:"completeRunAfterReconcile"`
}

// runUncertainOutcomeFixture marks a dispatched attempt uncertain, tries to
// redispatch it and to complete the run, reconciles it, and tries again.
func runUncertainOutcomeFixture(t *testing.T) any {
	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	journal, err := New(ctx, database, Config{Audit: newTestAudit(t, database)})
	if err != nil {
		t.Fatal(err)
	}
	admission, err := journal.Admit(ctx, fixtureRequest)
	if err != nil {
		t.Fatal(err)
	}
	identity := OperationIdentity{RunID: admission.RunID, ArtifactDigest: fixtureRequest.ArtifactDigest, InvocationPath: "charge", IterationPath: "root"}
	operation, err := journal.BeginEffect(ctx, EffectIntent{Identity: identity})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := journal.StartAttempt(ctx, operation.Key)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.MarkUncertain(ctx, operation.Key, attempt.ID, "provider succeeded; local commit outcome unknown"); err != nil {
		t.Fatal(err)
	}
	var observed uncertainOutcomeObservation
	uncertain, err := journal.BeginEffect(ctx, EffectIntent{Identity: identity})
	if err != nil {
		t.Fatal(err)
	}
	observed.State = uncertain.State
	_, err = journal.StartAttempt(ctx, operation.Key)
	observed.Redispatch = journalErrorName(err)
	observed.CompleteRun = journalErrorName(journal.CompleteRun(ctx, admission.RunID, []byte(`{}`)))
	reconciled, err := journal.Reconcile(ctx, operation.Key, "operator", "provider ledger shows the charge", []byte(`{"charged":true}`), true)
	if err != nil {
		t.Fatal(err)
	}
	observed.ReconciledState = reconciled.State
	_, err = journal.StartAttempt(ctx, operation.Key)
	observed.RedispatchAfterReconcile = journalErrorName(err)
	observed.CompleteRunAfterReconcile = journalErrorName(journal.CompleteRun(ctx, admission.RunID, []byte(`{}`)))
	return observed
}

type replayLineageObservation struct {
	RunIdentity       string `json:"runIdentity"`
	Lineage           string `json:"lineage"`
	OperationIdentity string `json:"operationIdentity"`
	ReusedReplayKey   string `json:"reusedReplayKey"`
}

// runReplayLineageFixture replays a completed run, reads the replay back
// from the store, and replays again under the same request key.
func runReplayLineageFixture(t *testing.T) any {
	ctx := context.Background()
	database, journal := newJournal(t, "journal.db", Config{})
	defer database.Close()
	source, err := journal.Admit(ctx, fixtureRequest)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.CompleteRun(ctx, source.RunID, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	replay, err := journal.Replay(ctx, source.RunID, "fixture-order-replay")
	if err != nil {
		t.Fatal(err)
	}
	observed := replayLineageObservation{RunIdentity: "reused", Lineage: "none", OperationIdentity: "reused"}
	if replay.RunID != "" && replay.RunID != source.RunID {
		observed.RunIdentity = "fresh"
	}
	stored, err := journal.Run(ctx, replay.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ReplayOf == source.RunID {
		observed.Lineage = "source_run_id"
	} else if stored.ReplayOf != "" {
		observed.Lineage = "other: " + stored.ReplayOf
	}
	sourceIdentity := OperationIdentity{RunID: source.RunID, ArtifactDigest: stored.ArtifactDigest, InvocationPath: "charge", IterationPath: "root"}
	replayIdentity := sourceIdentity
	replayIdentity.RunID = stored.RunID
	if replayIdentity.Key() != sourceIdentity.Key() {
		observed.OperationIdentity = "fresh"
	}
	_, err = journal.Replay(ctx, source.RunID, "fixture-order-replay")
	observed.ReusedReplayKey = journalErrorName(err)
	return observed
}

type staleAttemptObservation struct {
	StalePublish       string `json:"stalePublish"`
	CurrentAttemptWins bool   `json:"currentAttemptWins"`
}

// runStaleAttemptFixture fails an attempt retryably, starts the next, and
// lets the first try to publish before the current one does.
func runStaleAttemptFixture(t *testing.T) any {
	ctx := context.Background()
	database, journal := newJournal(t, "journal.db", Config{})
	defer database.Close()
	admission, err := journal.Admit(ctx, fixtureRequest)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := journal.BeginEffect(ctx, EffectIntent{Identity: OperationIdentity{RunID: admission.RunID, ArtifactDigest: fixtureRequest.ArtifactDigest, InvocationPath: "charge", IterationPath: "root"}})
	if err != nil {
		t.Fatal(err)
	}
	stale, err := journal.StartAttempt(ctx, operation.Key)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.FailAttempt(ctx, operation.Key, stale.ID, true, "lease lost"); err != nil {
		t.Fatal(err)
	}
	current, err := journal.StartAttempt(ctx, operation.Key)
	if err != nil {
		t.Fatal(err)
	}
	var observed staleAttemptObservation
	observed.StalePublish = journalErrorName(journal.CommitEffect(ctx, EffectCommit{OperationKey: operation.Key, AttemptID: stale.ID, Result: []byte(`{"from":"stale"}`)}))
	commitErr := journal.CommitEffect(ctx, EffectCommit{OperationKey: operation.Key, AttemptID: current.ID, Result: []byte(`{"from":"current"}`)})
	stored, err := journal.Operation(ctx, operation.Key)
	if err != nil {
		t.Fatal(err)
	}
	observed.CurrentAttemptWins = commitErr == nil && stored.State == operationCommitted && stored.CurrentAttemptID == current.ID && string(stored.Result) == `{"from":"current"}`
	return observed
}

type integrityFailureObservation struct {
	TruncatedStore string `json:"truncatedStore"`
}

// runIntegrityFailureFixture truncates a closed journal to half its size
// and requires opening the store or the journal over it to fail.
func runIntegrityFailureFixture(t *testing.T) any {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "journal.db")
	database, journal := newJournalAtPath(t, path, Config{})
	if _, err := journal.Admit(ctx, fixtureRequest); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, contents[:len(contents)/2], 0o600); err != nil {
		t.Fatal(err)
	}
	corrupted, err := (sqlite.Backend{}).Open(ctx, path)
	if err != nil {
		t.Logf("open refused the truncated journal: %v", err)
		return integrityFailureObservation{TruncatedStore: "fails_closed"}
	}
	defer corrupted.Close()
	if _, err := New(ctx, corrupted, Config{}); err != nil {
		t.Logf("journal refused the truncated store: %v", err)
		return integrityFailureObservation{TruncatedStore: "fails_closed"}
	}
	return integrityFailureObservation{TruncatedStore: "opened"}
}

type writeFailureObservation struct {
	SimulatedDiskFull     string `json:"simulatedDiskFull"`
	Acknowledged          bool   `json:"acknowledged"`
	StoredRuns            int    `json:"storedRuns"`
	AcceptedAfterRecovery bool   `json:"acceptedAfterRecovery"`
}

// runWriteFailureFixture admits through a store that refuses every write
// (failingDatabase), then through the same store once it accepts writes.
func runWriteFailureFixture(t *testing.T) any {
	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	failing := &failingDatabase{Database: database}
	journal, err := New(ctx, failing, Config{})
	if err != nil {
		t.Fatal(err)
	}
	failing.fail = true
	var observed writeFailureObservation
	admission, err := journal.Admit(ctx, fixtureRequest)
	observed.SimulatedDiskFull = "acknowledged"
	if err != nil {
		observed.SimulatedDiskFull = "fails_closed"
	}
	observed.Acknowledged = admission.Accepted
	failing.fail = false
	observed.StoredRuns = countJournalRows(t, database, `SELECT COUNT(*) FROM journal_runs WHERE request_key = ?`, fixtureRequest.RequestKey)
	recovered, err := journal.Admit(ctx, fixtureRequest)
	observed.AcceptedAfterRecovery = err == nil && recovered.Accepted
	return observed
}

// TestJournalFixtureCrashBarriers kills a real child process before and
// after each declared barrier's commit (runAdmissionChild and
// runTransitionChild) and reads the state the reopened journal holds.
func TestJournalFixtureCrashBarriers(t *testing.T) {
	for _, barrier := range loadJournalFixtures(t).CrashBarriers {
		transition := journalFixtureBarriers[barrier.Name]
		for _, phase := range []struct{ name, want string }{{"before", barrier.Before}, {"after", barrier.After}} {
			t.Run(barrier.Name+"/"+phase.name, func(t *testing.T) {
				directory := t.TempDir()
				path := filepath.Join(directory, "journal.db")
				marker := filepath.Join(directory, "marker")
				var command *exec.Cmd
				if transition == "admission" {
					command = exec.Command(os.Args[0], "-test.run=^TestAdmissionCommitBarrierSurvivesOrRollsBackProcessKill$")
					command.Env = append(os.Environ(), "NEWBLOK_JOURNAL_CHILD=1")
				} else {
					command = exec.Command(os.Args[0], "-test.run=^TestIntentDispatchResultAndCompletionBarriers$")
					command.Env = append(os.Environ(), "NEWBLOK_JOURNAL_TRANSITION_CHILD=1", "NEWBLOK_JOURNAL_TRANSITION="+transition)
				}
				command.Env = append(command.Env, "NEWBLOK_JOURNAL_PHASE="+phase.name, "NEWBLOK_JOURNAL_PATH="+path, "NEWBLOK_JOURNAL_MARKER="+marker)
				if err := command.Start(); err != nil {
					t.Fatal(err)
				}
				exited := make(chan struct{})
				go func() {
					_ = command.Wait()
					close(exited)
				}()
				waitForJournalMarkerWithin(t, marker, 10*time.Second, exited)
				if err := command.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				<-exited
				got := barrierState(t, path, transition)
				t.Logf("%s killed %s commit: %s", barrier.Name, phase.name, got)
				if got != phase.want {
					t.Fatalf("%s killed %s commit: state %q, fixture expects %q", barrier.Name, phase.name, got, phase.want)
				}
			})
		}
	}
}

// barrierState reads, without writing, the state a killed child left for
// its transition: the run's for admission and completion, the operation's
// otherwise.
func barrierState(t *testing.T, path, transition string) string {
	t.Helper()
	database, _ := newJournalAtPath(t, path, Config{})
	defer database.Close()
	requestKey := "barrier-order"
	if transition != "admission" {
		requestKey = "barrier-" + transition
	}
	var runID, runState string
	err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `SELECT run_id, state FROM journal_runs WHERE request_key = ?`, requestKey).Scan(&runID, &runState)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return "not accepted"
	}
	if err != nil {
		t.Fatal(err)
	}
	if transition == "admission" || transition == "ack" {
		return runState
	}
	key := OperationIdentity{RunID: runID, ArtifactDigest: "sha256:artifact", InvocationPath: "charge", IterationPath: "root"}.Key()
	var operationState string
	err = database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `SELECT state FROM journal_operations WHERE operation_key = ?`, key).Scan(&operationState)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return "absent"
	}
	if err != nil {
		t.Fatal(err)
	}
	return operationState
}
