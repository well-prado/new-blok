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
// metadata today; step output and terminal state are #265) exceeds the store
// bound. It is definite, because retrying the same transition can never fit,
// and it is not the caller's fault: it must surface as an internal failure,
// never as ErrInvalid (400) or ErrUnavailable (503).
var ErrRecordOverflow = errors.New("cluster: a runtime-grown record exceeds the store bound")

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
// are not covered (#265).
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
