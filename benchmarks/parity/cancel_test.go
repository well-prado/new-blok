package parity

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

// TestPersistentAppCancellationStopsInFlightWorkflow runs the same two-step
// reserve -> commit workflow in the long-lived published Blok 2.5.0 process
// and in the native engine process. A positive control proves both complete
// the workflow when nobody cancels; then the caller disconnects while the
// reserve provider call is held open. Each engine must abort that in-flight
// provider call, never run commit, commit no effect, and keep serving.
func TestPersistentAppCancellationStopsInFlightWorkflow(t *testing.T) {
	requireOldEngine(t)
	success := findWorkload(t, "order-reserve-commit-success")
	cancelled := findWorkload(t, "order-cancel-in-flight")
	quote := findWorkload(t, "quote-success")
	want := cancelled.Expected.Cancellation
	if want == nil || len(success.Expected.Output) == 0 || len(quote.Expected.Output) == 0 {
		t.Fatal("cancellation workloads must predeclare their expected outputs and counts")
	}
	for _, side := range []struct {
		name  string
		start func(*testing.T, string) (*persistentAppProcess, time.Duration)
	}{
		{name: "published-blok-2.5.0", start: startOldPersistentApp},
		{name: "native-engine", start: startNewPersistentApp},
	} {
		t.Run(side.name, func(t *testing.T) {
			provider := newProvider(t)
			app, _ := side.start(t, provider.URL)
			client := persistentHTTPClient()

			control, err := postPersistent(context.Background(), client, app.url+"/reserve-commit", success.Request)
			if err != nil || !equalJSON(control, success.Expected.Output) {
				t.Fatalf("uncancelled reserve-commit control = %s, %v; want %s", control, err, success.Expected.Output)
			}
			controlCalls, controlEffects := provider.ledger.snapshot()
			if controlCalls != success.Expected.ProviderCalls || controlEffects != success.Expected.CommittedEffects {
				t.Fatalf("control provider calls=%d effects=%d, want %d/%d", controlCalls, controlEffects, success.Expected.ProviderCalls, success.Expected.CommittedEffects)
			}

			key, _ := cancelled.Request["requestKey"].(string)
			provider.ledger.mu.Lock()
			provider.ledger.holdKeys[key] = true
			provider.ledger.mu.Unlock()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := postPersistent(ctx, client, app.url+"/reserve-commit", cancelled.Request)
				done <- err
			}()
			select {
			case <-provider.ledger.holdEntered:
			case err := <-done:
				t.Fatalf("reserve-commit returned before the provider held the reservation: %v", err)
			case <-time.After(15 * time.Second):
				t.Fatal("reserve provider call never started")
			}
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("caller error = %v, want context.Canceled", err)
			}
			outcomeLine, err := waitPersistentLine(app.lines, "CANCELLED ", reserveHoldFallback/2)
			if err != nil {
				t.Fatalf("%s did not report the cancelled run before the provider fallback: %v; stderr=%s", side.name, err, app.stderr.String())
			}
			var outcome map[string]any
			if err := json.Unmarshal([]byte(outcomeLine), &outcome); err != nil {
				t.Fatalf("decode cancellation outcome %q: %v", outcomeLine, err)
			}
			if outcome["success"] != false || (outcome["signalAborted"] != true && outcome["contextCanceled"] != true) {
				t.Fatalf("%s cancellation outcome = %s; want an unsuccessful run classified as caller cancellation", side.name, outcomeLine)
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				provider.ledger.mu.Lock()
				aborted := provider.ledger.aborted["reserve"]
				provider.ledger.mu.Unlock()
				if aborted >= want.ReserveAbortedByCaller || time.Now().After(deadline) {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			calls, effects := provider.ledger.snapshot()
			provider.ledger.mu.Lock()
			aborted := provider.ledger.aborted["reserve"]
			provider.ledger.mu.Unlock()
			reserveCalls := provider.attemptsFor("reserve", key)
			commitCalls := provider.attemptsFor("commit", key+"-commit")
			if reserveCalls != want.ReserveCalls || aborted != want.ReserveAbortedByCaller || commitCalls != want.CommitCalls || effects-controlEffects != want.CommittedEffects || calls-controlCalls != want.ReserveCalls+want.CommitCalls {
				t.Fatalf("%s cancellation reserve=%d aborted=%d commit=%d new_effects=%d new_calls=%d; want reserve=%d aborted=%d commit=%d effects=%d", side.name, reserveCalls, aborted, commitCalls, effects-controlEffects, calls-controlCalls, want.ReserveCalls, want.ReserveAbortedByCaller, want.CommitCalls, want.CommittedEffects)
			}

			after, err := postPersistent(context.Background(), client, app.url+"/quote", copyRequest(quote.Request, "after-cancel-quote"))
			if serves := err == nil && equalJSON(after, quote.Expected.Output); serves != want.ProcessServesAfterCancel {
				t.Fatalf("%s served after cancellation=%t (%s, %v); want %t", side.name, serves, after, err, want.ProcessServesAfterCancel)
			}
			t.Logf("E20-T01 cancellation engine=%s outcome=%s reserve_calls=%d reserve_aborted=%d commit_calls=%d cancel_effects=%d control_output=%s", side.name, outcomeLine, reserveCalls, aborted, commitCalls, effects-controlEffects, control)
		})
	}
}

// postPersistent posts to a persistent parity application and returns the
// business response from its {ok, response} envelope.
func postPersistent(ctx context.Context, client *http.Client, endpoint string, request map[string]any) (json.RawMessage, error) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(encoded))
	if err != nil {
		return nil, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	response, err := client.Do(httpRequest)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	var envelope struct {
		OK       bool            `json:"ok"`
		Response json.RawMessage `json:"response"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || response.StatusCode != http.StatusOK || !envelope.OK {
		return nil, fmt.Errorf("status=%d body=%s decode=%v", response.StatusCode, body, err)
	}
	return envelope.Response, nil
}
