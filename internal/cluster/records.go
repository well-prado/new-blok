package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/well-prado/new-blok/store/distributed"
)

// ErrRecordOverflow reports that a record the runtime itself grows (run
// metadata, a step's dispatch or result record, or a terminal run record)
// exceeds the store bound. It is definite, because retrying the same
// transition can never fit, and it is not the caller's fault: it must surface
// as an internal failure, never as ErrInvalid (400) or ErrUnavailable (503).
// Inside a run it is a terminal failure classified recordTooLargeCode (#265).
var ErrRecordOverflow = errors.New("cluster: a runtime-grown record exceeds the store bound")

// recordTooLargeCode is the terminal error code of a run that ended because
// one of its runtime-grown records cannot be written (#265).
const recordTooLargeCode = "record_too_large"

// maxOwnerIDBytes is the longest owner ID the store accepts, and
// maxJSONExpansion the most bytes encoding/json writes for one input byte
// ('<' becomes <, an invalid UTF-8 byte becomes �).
const (
	maxOwnerIDBytes  = 180
	maxJSONExpansion = 6
)

// maxNonTerminalRun returns run with every field the runtime may set before a
// terminal transition at its largest encoding: the longest non-terminal state
// ("accepted", against "running" and "waiting") and a claiming or signalling
// owner's ID and fence. Claim, suspension and signal or timer re-admission
// only ever set those fields, so a run whose maxNonTerminalRun fits can never
// outgrow the store bound before it finishes. Terminal output and error codes
// are not reserved: terminalRecords falls back to smaller terminal records
// instead (#265).
func maxNonTerminalRun(run RunRecord) RunRecord {
	run.State = "accepted"
	run.OwnerID = strings.Repeat("<", maxOwnerIDBytes)
	run.Fence = math.MaxInt64
	return run
}

// checkEncoded encodes v exactly as it will be persisted and reports a
// record over distributed.MaxPayloadBytes as a *distributed.RecordTooLargeError.
func checkEncoded(v any) error {
	encoded, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return distributed.CheckRecord(encoded)
}

// classifyTooLarge maps a store size rejection to the runtime's outcome. When
// callerOwned reports that the over-bound part is the caller's own request
// (its input, its signal), the rejection is ErrInvalid; otherwise the runtime
// grew the record and it is ErrRecordOverflow. It returns nil for any other
// error, which callers classify as before.
func classifyTooLarge(err error, callerOwned func(*distributed.RecordTooLargeError) bool) error {
	var tooLarge *distributed.RecordTooLargeError
	if !errors.As(err, &tooLarge) {
		return nil
	}
	if callerOwned(tooLarge) {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	return fmt.Errorf("%w: %w", ErrRecordOverflow, err)
}

func callerOwnsAll(*distributed.RecordTooLargeError) bool { return true }

func runtimeOwnsAll(*distributed.RecordTooLargeError) bool { return false }

// overflowOrErr classifies a store size rejection of a runtime-grown record
// as ErrRecordOverflow and returns any other error unchanged.
func overflowOrErr(err error) error {
	if overflow := classifyTooLarge(err, runtimeOwnsAll); overflow != nil {
		return overflow
	}
	return err
}

// terminalRecords returns the terminal record of run (already carrying its
// terminal state, code, output and owner) followed by the smaller records
// finish falls back to, in order, when a larger one exceeds the bound (#265):
//
//  1. the intended record;
//  2. for a completed run, a failure: state "failed", code
//     recordTooLargeCode, no output, because the output cannot be stored;
//  3. the last of those without its input. The input digest stays, which is
//     what duplicate admission compares; a terminal run is never replayed,
//     so nothing reads the input again. Every remaining field is a
//     fixed-size digest, a name the store bounds at 180 bytes (tenant, owner,
//     slots), the request key (bounded the same way at admission) or the
//     application's registered workflow name, so this record fits unless
//     that name alone approaches the bound; finish then reports
//     ErrRecordOverflow.
//
// The state of an uncertain or failed run is never rewritten: only a
// completed run's output is turned into a failure.
func terminalRecords(run RunRecord) []RunRecord {
	records := []RunRecord{run}
	if run.State == "completed" {
		failed := run
		failed.State, failed.ErrorCode, failed.Output = "failed", recordTooLargeCode, nil
		records = append(records, failed)
	}
	minimal := records[len(records)-1]
	minimal.Input = nil
	return append(records, minimal)
}

func isTerminal(state string) bool {
	return state == "completed" || state == "failed" || state == "uncertain"
}
