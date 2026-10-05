package clusterapp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/internal/cluster"
)

// htmlPayload carries every character encoding/json HTML-escapes. The store
// persists it as escapedPayload: compact, with '<', '>' and '&' written as
// \u003c, \u003e and \u0026 (ADR 0019 keeps that encoding, #258).
const (
	htmlPayload    = `{"note":"<a> & <b>","n":1}`
	escapedPayload = `{"note":"\u003ca\u003e \u0026 \u003cb\u003e","n":1}`
)

// TestDistributedSignalRetryWithHTMLCharactersIsDuplicate covers issue #259
// over HTTP on real etcd. Retrying a signal with the same ID and the same
// payload must be acknowledged as a duplicate, never a 409, even when the
// payload contains characters the store HTML-escapes.
//
// "Same payload" means the same JSON text once encoded the way the store
// persists it: compact and HTML-escaped. So a retry that differs only in
// insignificant whitespace, or that already spells '<' as \u003c, is the same
// signal. A retry that reorders object keys or changes any value is a
// different signal: the store keeps the payload as an opaque JSON text, not
// as a decoded value, and a late signal has always been reconciled by
// comparing those encoded bytes (ADR 0019).
func TestDistributedSignalRetryWithHTMLCharactersIsDuplicate(t *testing.T) {
	f := newEncodedSizeFixture(t, 1)
	f.startWorker(t, "w-259")
	response := f.admit("signal-duplicate-259", `{"text":"x"}`)
	var admission cluster.Admission
	if response.Code != http.StatusAccepted || json.Unmarshal(response.Body.Bytes(), &admission) != nil || !admission.Accepted {
		t.Fatalf("fixture admission status=%d body=%q", response.Code, strings.TrimSpace(response.Body.String()))
	}
	if state := f.awaitRun(admission.RunID, "waiting", 15*time.Second); state != "waiting" {
		t.Fatalf("fixture run state=%q, want suspended at its wait", state)
	}
	waitID := cluster.WaitIDFor(admission.RunID, "approval")

	// Waiting path: the first delivery closes the open wait; every retry of
	// it is answered from the signalled wait record.
	expectSignal(t, "first delivery to the open wait", f.signal(waitID, "approve-259", []byte(htmlPayload)), cluster.SignalResult{Accepted: true})
	wait, err := f.runtime.GetWait(f.ctx, f.tenant, waitID)
	if err != nil || wait.State != "signaled" || string(wait.Payload) != escapedPayload {
		t.Fatalf("signalled wait=%+v payload=%s err=%v, want stored bytes %s (encoding unchanged)", wait, wait.Payload, err, escapedPayload)
	}
	duplicate := cluster.SignalResult{Accepted: true, Duplicate: true}
	for _, retry := range []struct{ name, payload string }{
		{"identical bytes", htmlPayload},
		{"insignificant whitespace", "{ \"note\" : \"<a> & <b>\",\n  \"n\" : 1 }"},
		{"already escaped", escapedPayload},
	} {
		expectSignal(t, "waiting-path retry with "+retry.name, f.signal(waitID, "approve-259", []byte(retry.payload)), duplicate)
	}
	for _, conflict := range []struct{ name, payload string }{
		{"reordered keys", `{"n":1,"note":"<a> & <b>"}`},
		{"a different value", `{"note":"<a> & <c>","n":1}`},
	} {
		expectConflict(t, "waiting-path retry with "+conflict.name, f.signal(waitID, "approve-259", []byte(conflict.payload)))
	}

	// Late path: the wait is already closed, so a new signal ID is recorded
	// as a late signal; its retries reconcile against that late record.
	late := cluster.SignalResult{Late: true}
	expectSignal(t, "first late delivery", f.signal(waitID, "late-259", []byte(htmlPayload)), late)
	for _, retry := range []struct{ name, payload string }{
		{"identical bytes", htmlPayload},
		{"insignificant whitespace", "{ \"note\" : \"<a> & <b>\",\n  \"n\" : 1 }"},
		{"already escaped", escapedPayload},
	} {
		expectSignal(t, "late-path retry with "+retry.name, f.signal(waitID, "late-259", []byte(retry.payload)), late)
	}
	for _, conflict := range []struct{ name, payload string }{
		{"reordered keys", `{"n":1,"note":"<a> & <b>"}`},
		{"a different value", `{"note":"<a> & <c>","n":1}`},
	} {
		expectConflict(t, "late-path retry with "+conflict.name, f.signal(waitID, "late-259", []byte(conflict.payload)))
	}

	// The run resumes exactly once from the first signal and keeps its
	// payload; no retry above re-admitted or altered it.
	if state := f.awaitRun(admission.RunID, "completed", 15*time.Second); state != "completed" {
		t.Fatalf("signalled run state=%q, want completed", state)
	}
	if wait, err = f.runtime.GetWait(f.ctx, f.tenant, waitID); err != nil || wait.SignalID != "approve-259" || string(wait.Payload) != escapedPayload {
		t.Fatalf("wait after retries=%+v payload=%s err=%v, want the first signal unchanged", wait, wait.Payload, err)
	}
	// The wait is closed and its run finished; a retry of the original
	// signal is still the same delivery.
	expectSignal(t, "retry after the run completed", f.signal(waitID, "approve-259", []byte(htmlPayload)), duplicate)
}

// TestDistributedAdmissionRetryWithHTMLCharactersIsDuplicate audits the same
// comparison for admission idempotency (#259). Admission decodes the input
// with the workflow's typed decoder and digests its encoding, so a retry is
// compared by value: whitespace, escaping and key order cannot matter, and a
// different value is a 409. The digest and the stored input bytes are pinned
// to their pre-#259 values so no rolling upgrade sees a different identity.
func TestDistributedAdmissionRetryWithHTMLCharactersIsDuplicate(t *testing.T) {
	f := newEncodedSizeFixture(t, 1)
	first := f.admit("admission-duplicate-259", `{"text":"a<b>&c"}`)
	var admission cluster.Admission
	if first.Code != http.StatusAccepted || json.Unmarshal(first.Body.Bytes(), &admission) != nil || !admission.Accepted {
		t.Fatalf("first admission status=%d body=%q", first.Code, strings.TrimSpace(first.Body.String()))
	}
	run, err := f.runtime.GetRun(f.ctx, f.tenant, admission.RunID)
	const pinnedDigest = "sha256:5b33e06d90b069ce6a8f647aad6056a69dc8fdb61008b37c6b2143fe3df443d4"
	if err != nil || run.InputDigest != pinnedDigest || string(run.Input) != `{"text":"a\u003cb\u003e\u0026c"}` {
		t.Fatalf("stored run digest=%q input=%s err=%v, want digest %s over the HTML-escaped input (unchanged by #259)", run.InputDigest, run.Input, err, pinnedDigest)
	}
	for _, retry := range []struct{ name, body string }{
		{"identical bytes", `{"text":"a<b>&c"}`},
		{"insignificant whitespace", "{ \"text\" :\n \"a<b>&c\" }"},
		{"already escaped", `{"text":"a\u003cb\u003e\u0026c"}`},
	} {
		response := f.admit("admission-duplicate-259", retry.body)
		var duplicate cluster.Admission
		if response.Code != http.StatusAccepted || json.Unmarshal(response.Body.Bytes(), &duplicate) != nil || duplicate.Accepted || duplicate.RunID != admission.RunID {
			t.Errorf("admission retry with %s status=%d body=%q, want 202 duplicate of %s", retry.name, response.Code, strings.TrimSpace(response.Body.String()), admission.RunID)
		}
	}
	if response := f.admit("admission-duplicate-259", `{"text":"a<b>&d"}`); response.Code != http.StatusConflict {
		t.Fatalf("admission retry with a different value status=%d body=%q, want 409", response.Code, strings.TrimSpace(response.Body.String()))
	}
}

func expectSignal(t *testing.T, label string, response *httptest.ResponseRecorder, want cluster.SignalResult) {
	t.Helper()
	var got cluster.SignalResult
	if response.Code != http.StatusAccepted || json.Unmarshal(response.Body.Bytes(), &got) != nil || got != want {
		t.Errorf("%s: status=%d body=%q, want 202 %+v", label, response.Code, strings.TrimSpace(response.Body.String()), want)
	}
}

func expectConflict(t *testing.T, label string, response *httptest.ResponseRecorder) {
	t.Helper()
	if response.Code != http.StatusConflict {
		t.Errorf("%s: status=%d body=%q, want 409 conflict", label, response.Code, strings.TrimSpace(response.Body.String()))
	}
}
