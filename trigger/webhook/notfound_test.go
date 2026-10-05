package webhook_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/trigger"
	"github.com/well-prado/new-blok/trigger/webhook"
	"github.com/well-prado/new-blok/trigger/worker"
)

// TestNotFoundWorkflowIsTerminalAndNotRetried pins how a webhook expresses
// the not-found class (#306), and that the class is terminal. The webhook
// answers the provider when the event is committed, before the workflow
// runs, so a workflow that later fails not-found cannot change that 202. The
// job then ends dead after its one attempt, with the code as its error, even
// though it had attempts left: only a retryable HandlerError is retried, and
// the class does not make one. A Submitter that itself returns a not-found
// error is not a sentinel the webhook maps, and is answered 500 like any
// other unexpected admission failure.
func TestNotFoundWorkflowIsTerminalAndNotRetried(t *testing.T) {
	f := loadFixture(t)
	key := secret(t, "k1")
	e := newEnv(t, f, []webhook.Key{{ID: "k1", Secret: key}}, nil)
	if status, result := e.send(t, sign(key, "evt_hidden", t0, []byte(f.Body))); status != http.StatusAccepted || result != "accepted" {
		t.Fatalf("status=%d result=%q; want 202 accepted", status, result)
	}
	attempts := 0
	handler := func(context.Context, worker.Tx, worker.Job) error {
		attempts++
		return fmt.Errorf("run: %w", &node.DomainError{Code: "not_found", Class: trigger.ClassNotFound, Err: errors.New("synthetic: belongs to another principal")})
	}
	for range 3 {
		e.clock.Set(e.clock.Now().Add(10 * time.Minute))
		if _, err := e.queue.ProcessOnce(context.Background(), handler); err != nil {
			t.Fatal(err)
		}
	}
	job, err := e.queue.Get(context.Background(), webhook.SubmissionKey("shop", "evt_hidden"))
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || job.State != worker.StateDead || job.Attempt != 1 || job.MaxAttempts < 2 || job.Error != "not_found" {
		t.Fatalf("attempts=%d job=%+v; want one attempt, dead, error not_found, with attempts left", attempts, job)
	}

	refused := newEnv(t, f, []webhook.Key{{ID: "k1", Secret: key}}, func(*worker.Queue) trigger.Submitter {
		return stubSubmitter{&node.DomainError{Code: "not_found", Class: trigger.ClassNotFound}}
	})
	if status, result := refused.send(t, sign(key, "evt_refused", t0, []byte(f.Body))); status != http.StatusInternalServerError || result != "internal" {
		t.Fatalf("not-found from the submitter: status=%d result=%q; want 500 internal", status, result)
	}
}
