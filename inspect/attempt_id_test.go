package inspect

import (
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/inspection"
)

// An attempt id longer than the recorder keeps is stored bounded; its
// completion is matched on the same bounded form, so the attempt completes
// instead of staying running (Review R round 1 on #333 slice 1b).
func TestLongAttemptIDsComplete(t *testing.T) {
	recorder := NewRecorder()
	at := time.Now().UTC()
	id := "attempt:x/" + strings.Repeat("s", 200) + "/1"
	recorder.Observe(inspection.Event{Kind: inspection.RunStarted, RunID: "run-1", Principal: "p", Workflow: "w", At: at})
	recorder.Observe(inspection.Event{Kind: inspection.StepProcessing, RunID: "run-1", Principal: "p", Workflow: "w", StepID: "step", Attempt: 1, AttemptID: id, At: at})
	recorder.Observe(inspection.Event{Kind: inspection.StepCompleted, RunID: "run-1", Principal: "p", Workflow: "w", StepID: "step", Attempt: 1, AttemptID: id, At: at})
	page, err := recorder.Inspect("p", inspection.Policy{}, inspection.Query{Version: inspection.Version, RunID: "run-1"})
	if err != nil || len(page.Steps) != 1 || len(page.Steps[0].Attempts) != 1 {
		t.Fatalf("page=%+v err=%v", page, err)
	}
	if attempt := page.Steps[0].Attempts[0]; attempt.Status != inspection.StatusCompleted {
		t.Fatalf("attempt %+v did not complete", attempt)
	}
}
