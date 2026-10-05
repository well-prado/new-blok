package cluster

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/store/distributed"
)

// TestTerminalRecordsFallBackToARecordThatFits pins the order finish tries
// terminal records in (#265) and proves the last one fits even for a run
// whose input and output are each at the bound and whose bounded identity
// fields are all at their worst encoding.
func TestTerminalRecordsFallBackToARecordThatFits(t *testing.T) {
	worst := strings.Repeat("<", maxOwnerIDBytes)
	huge := json.RawMessage(`"` + strings.Repeat("a", distributed.MaxPayloadBytes) + `"`)
	run := RunRecord{RunID: "run-" + strings.Repeat("f", 32), Tenant: worst, RequestKey: worst, Workflow: "orders", ArtifactDigest: "sha256:" + strings.Repeat("a", 64), InputDigest: "sha256:" + strings.Repeat("b", 64), Input: huge, State: "completed", Output: huge, OwnerID: worst, Fence: math.MaxInt64, GlobalSlot: worst, TenantSlot: worst}

	records := terminalRecords(run)
	if len(records) != 3 {
		t.Fatalf("completed run has %d terminal records, want intended, failed, minimal", len(records))
	}
	if records[0].State != "completed" || string(records[0].Output) != string(huge) || string(records[0].Input) != string(huge) {
		t.Fatalf("first record=%s/%q, want the intended completed record", records[0].State, records[0].ErrorCode)
	}
	if records[1].State != "failed" || records[1].ErrorCode != recordTooLargeCode || records[1].Output != nil || string(records[1].Input) != string(huge) {
		t.Fatalf("second record=%s/%q output=%d input=%d, want failed/%s without output, input kept", records[1].State, records[1].ErrorCode, len(records[1].Output), len(records[1].Input), recordTooLargeCode)
	}
	minimal := records[2]
	if minimal.State != "failed" || minimal.ErrorCode != recordTooLargeCode || minimal.Input != nil || minimal.Output != nil || minimal.InputDigest != run.InputDigest {
		t.Fatalf("minimal record=%+v, want the failure without input or output and with the input digest", minimal)
	}
	if err := checkEncoded(minimal); err != nil {
		t.Fatalf("minimal terminal record does not fit: %v", err)
	}
	if err := checkEncoded(records[1]); err == nil {
		t.Fatal("fixture input fits; the minimal record is not exercised")
	}

	// A failure or uncertainty keeps its own state and code; only its input
	// is dropped when the record cannot hold it.
	for _, state := range []string{"failed", "uncertain"} {
		ended := run
		ended.State, ended.ErrorCode, ended.Output = state, "node_error", nil
		records := terminalRecords(ended)
		if len(records) != 2 || records[0].State != state || records[1].State != state || records[1].ErrorCode != "node_error" || records[1].Input != nil || string(records[0].Input) != string(huge) {
			t.Fatalf("%s run terminal records=%d first=%s second=%s/%q input=%d, want intended then the same without input", state, len(records), records[0].State, records[1].State, records[1].ErrorCode, len(records[1].Input))
		}
	}
}

// TestStepTransitionRecordsNeverOutgrowTheirDispatch shows why the uncertain
// marking in Load and the retryable/uncertain transitions cannot overflow:
// each rewrites a committed dispatch record with a one-byte-shorter state and
// no output. Only the dispatch record (its declared effects) and the
// committed record (its output) can exceed the bound, and both are
// classified ErrRecordOverflow.
func TestStepTransitionRecordsNeverOutgrowTheirDispatch(t *testing.T) {
	dispatched := stepRecord{
		Identity:       engine.StepIdentity{RunID: "run-" + strings.Repeat("f", 32), ArtifactDigest: "sha256:" + strings.Repeat("a", 64), StepID: strings.Repeat("s", 64), InputDigest: "sha256:" + strings.Repeat("b", 64), OperationKey: "op:" + strings.Repeat("c", 64)},
		State:          "dispatched",
		AttemptNumber:  math.MaxInt,
		CurrentAttempt: strings.Repeat("d", 32),
		Effects:        []string{strings.Repeat("<", distributed.MaxPayloadBytes/8)},
	}
	encoded, _ := json.Marshal(dispatched)
	for _, state := range []string{"uncertain", "retryable"} {
		next := dispatched
		next.State, next.Output = state, nil
		rewritten, _ := json.Marshal(next)
		if len(rewritten) > len(encoded) {
			t.Fatalf("%s record is %d bytes, larger than its %d-byte dispatch record", state, len(rewritten), len(encoded))
		}
	}
}

func TestOverflowOrErrClassifiesOnlySizeRejections(t *testing.T) {
	tooLarge := &distributed.RecordTooLargeError{Record: "state", StateID: "step-x", Size: distributed.MaxPayloadBytes + 1, Limit: distributed.MaxPayloadBytes}
	classified := overflowOrErr(tooLarge)
	if !errors.Is(classified, ErrRecordOverflow) || !errors.Is(classified, distributed.ErrRecordTooLarge) || errors.Is(classified, ErrInvalid) || errors.Is(classified, ErrUnavailable) {
		t.Fatalf("size rejection classified as %v, want ErrRecordOverflow only", classified)
	}
	outage := errors.New("distributed store: fenced state commit (outcome must be reconciled by transition ID): context deadline exceeded")
	if got := overflowOrErr(outage); got != outage || errors.Is(got, ErrRecordOverflow) {
		t.Fatalf("storage outage classified as %v, want it unchanged (retryable)", got)
	}
}
