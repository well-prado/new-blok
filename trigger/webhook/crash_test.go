package webhook

import (
	"bytes"
	"context"
	"database/sql"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
	"github.com/well-prado/new-blok/trigger/worker"
)

const crashSchema = `{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer","minimum":1,"maximum":100}},"required":["sku","quantity"]}`

func crashSecret() Secret {
	s, err := NewSecret([]byte("synthetic-crash-test-signing-key"))
	if err != nil {
		panic(err)
	}
	return s
}

// pausingSubmitter stops a child process just before the durable submission.
type pausingSubmitter struct {
	inner  trigger.Submitter
	marker string
}

func (p pausingSubmitter) Submit(ctx context.Context, s trigger.Submission) (bool, error) {
	if err := os.WriteFile(p.marker, []byte("before"), 0o600); err != nil {
		return false, err
	}
	time.Sleep(30 * time.Second)
	return p.inner.Submit(ctx, s)
}

func serve(t testing.TB, databasePath string, wrap func(trigger.Submitter) trigger.Submitter, after func()) (*Server, *worker.Queue, func()) {
	database, err := (sqlite.Backend{}).Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := worker.New(context.Background(), database, nil)
	if err != nil {
		t.Fatal(err)
	}
	application, err := app.New(app.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var submitter trigger.Submitter = queue
	if wrap != nil {
		submitter = wrap(queue)
	}
	server, err := New(application, nil, []Endpoint{{Path: "/hooks", Provider: "shop", Kind: "order.event", Principal: trigger.Principal{ID: "provider:shop"}, Verifier: StandardWebhooks{Keys: []Key{{ID: "k1", Secret: crashSecret()}}}, Submit: submitter, InputSchema: []byte(crashSchema)}})
	if err != nil {
		t.Fatal(err)
	}
	server.afterSubmit = after
	return server, queue, func() { _ = application.Shutdown(context.Background()); _ = database.Close() }
}

func signedRequest(t *testing.T, url, id string) *http.Request {
	t.Helper()
	body := []byte(`{"sku":"coffee","quantity":2}`)
	now := time.Now()
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("webhook-id", id)
	request.Header.Set("webhook-timestamp", strconv.FormatInt(now.Unix(), 10))
	request.Header.Set("webhook-signature", SignStandard(crashSecret(), id, now, body))
	return request
}

// TestCrashAroundDurableSubmission kills the process either after the event
// is committed but before the provider is acknowledged, or before it is
// committed. In both cases the provider sees no acknowledgment and retries,
// and the retry yields exactly one run.
func TestCrashAroundDurableSubmission(t *testing.T) {
	if mode := os.Getenv("NEWBLOK_WEBHOOK_CHILD"); mode != "" {
		runCrashChild(t, mode)
		return
	}
	for _, tc := range []struct {
		mode                string
		jobsAfterCrash      int
		retryStatus         int
		wantStateAfterCrash string
	}{
		{mode: "after-commit", jobsAfterCrash: 1, retryStatus: http.StatusOK, wantStateAfterCrash: worker.StatePending},
		{mode: "before-commit", jobsAfterCrash: 0, retryStatus: http.StatusAccepted},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			directory := t.TempDir()
			databasePath := filepath.Join(directory, "jobs.db")
			marker := filepath.Join(directory, "marker")
			address := filepath.Join(directory, "address")
			command := exec.Command(os.Args[0], "-test.run=^TestCrashAroundDurableSubmission$")
			command.Env = append(os.Environ(), "NEWBLOK_WEBHOOK_CHILD="+tc.mode, "NEWBLOK_WEBHOOK_DB="+databasePath, "NEWBLOK_WEBHOOK_MARKER="+marker, "NEWBLOK_WEBHOOK_ADDRESS="+address)
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = command.Process.Kill(); _ = command.Wait() }()
			listening := waitForFile(t, address)

			type reply struct {
				status int
				err    error
			}
			replies := make(chan reply, 1)
			go func() {
				response, err := http.DefaultClient.Do(signedRequest(t, "http://"+string(listening)+"/hooks", "evt_crash"))
				if err != nil {
					replies <- reply{err: err}
					return
				}
				response.Body.Close()
				replies <- reply{status: response.StatusCode}
			}()
			waitForFile(t, marker)
			if err := command.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = command.Wait()
			select {
			case r := <-replies:
				if r.err == nil {
					t.Fatalf("provider received status %d although the process died before acknowledging", r.status)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("provider request did not end after the crash")
			}

			server, queue, closeAll := serve(t, databasePath, nil, nil)
			defer closeAll()
			count := func() int {
				var n int
				database, err := (sqlite.Backend{}).Open(context.Background(), databasePath)
				if err != nil {
					t.Fatal(err)
				}
				defer database.Close()
				if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
					return tx.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM worker_jobs`).Scan(&n)
				}); err != nil {
					t.Fatal(err)
				}
				return n
			}
			if got := count(); got != tc.jobsAfterCrash {
				t.Fatalf("jobs after crash=%d, want %d", got, tc.jobsAfterCrash)
			}
			if tc.wantStateAfterCrash != "" {
				job, err := queue.Get(context.Background(), SubmissionKey("shop", "evt_crash"))
				if err != nil || job.State != tc.wantStateAfterCrash || job.Principal.ID != "provider:shop" {
					t.Fatalf("committed job=%+v err=%v", job, err)
				}
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			httpServer := &http.Server{Handler: server}
			go func() { _ = httpServer.Serve(listener) }()
			defer httpServer.Close()
			// The provider retries the unacknowledged event, re-signed.
			response, err := http.DefaultClient.Do(signedRequest(t, "http://"+listener.Addr().String()+"/hooks", "evt_crash"))
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != tc.retryStatus || count() != 1 {
				t.Fatalf("retry status=%d jobs=%d, want %d and exactly one job", response.StatusCode, count(), tc.retryStatus)
			}
			runs := 0
			for {
				processed, err := queue.ProcessOnce(context.Background(), func(context.Context, worker.Tx, worker.Job) error { runs++; return nil })
				if err != nil {
					t.Fatal(err)
				}
				if !processed {
					break
				}
			}
			if runs != 1 {
				t.Fatalf("runs=%d, want exactly one", runs)
			}
		})
	}
}

func runCrashChild(t *testing.T, mode string) {
	marker := os.Getenv("NEWBLOK_WEBHOOK_MARKER")
	var wrap func(trigger.Submitter) trigger.Submitter
	var after func()
	switch mode {
	case "before-commit":
		wrap = func(inner trigger.Submitter) trigger.Submitter { return pausingSubmitter{inner: inner, marker: marker} }
	case "after-commit":
		after = func() {
			_ = os.WriteFile(marker, []byte("after"), 0o600)
			time.Sleep(30 * time.Second)
		}
	}
	server, _, closeAll := serve(t, os.Getenv("NEWBLOK_WEBHOOK_DB"), wrap, after)
	defer closeAll()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("NEWBLOK_WEBHOOK_ADDRESS"), []byte(listener.Addr().String()), 0o600); err != nil {
		t.Fatal(err)
	}
	_ = http.Serve(listener, server)
}

func waitForFile(t *testing.T, path string) []byte {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(path); err == nil && len(data) > 0 {
			return data
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("child did not write %s", path)
	return nil
}
