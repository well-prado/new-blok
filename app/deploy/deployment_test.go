package deploy

import (
	"context"
	"errors"
	"github.com/well-prado/new-blok/app"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/deployment"
)

func TestDeploymentSignalHelper(t *testing.T) {
	if os.Getenv("BLOK_TEST_CHILD") != "1" {
		return
	}
	a, _ := app.New(app.Config{Dependencies: []app.Dependency{{Name: "resource", Start: func(context.Context) error { return nil }, Close: func(context.Context) error { _, _ = os.Stdout.WriteString("closed\n"); return nil }}}})
	c := deployment.Config{ListenerAddress: os.Getenv("BLOK_TEST_ADDR"), MaxAdmission: 1, DrainTimeout: time.Second}
	if os.Getenv("BLOK_TEST_TIMEOUT") == "1" {
		c.DrainTimeout = 100 * time.Millisecond
	}
	d, err := NewDeployment(a, c, DeploymentChecks{Artifact: func(context.Context) error { return nil }}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = os.Stdout.WriteString("entered\n")
		if os.Getenv("BLOK_TEST_TIMEOUT") == "1" {
			<-r.Context().Done()
			return
		}
		time.Sleep(150 * time.Millisecond)
		_, _ = w.Write([]byte("completed"))
	}))
	if err != nil {
		os.Exit(3)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	err = d.Run(context.Background(), signals)
	if os.Getenv("BLOK_TEST_TIMEOUT") == "1" {
		if !errors.Is(err, app.ErrDrainTimeout) {
			os.Exit(4)
		}
	} else if err != nil {
		os.Exit(5)
	}
	os.Exit(0)
}

// Real child processes receive SIGTERM while an HTTP request is active.
func TestDeploymentSIGTERMDrain(t *testing.T) {
	for _, timeout := range []string{"0", "1"} {
		t.Run(timeout, func(t *testing.T) {
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			addr := l.Addr().String()
			l.Close()
			exe, _ := os.Executable()
			cmd := exec.Command(exe, "-test.run=^TestDeploymentSignalHelper$")
			cmd.Env = append(os.Environ(), "BLOK_TEST_CHILD=1", "BLOK_TEST_ADDR="+addr, "BLOK_TEST_TIMEOUT="+timeout)
			stdout, _ := cmd.StdoutPipe()
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = cmd.Process.Kill() }()
			client := &http.Client{Timeout: 2 * time.Second}
			ready := false
			for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
				resp, err := client.Get("http://" + addr + "/readyz")
				if err == nil {
					resp.Body.Close()
					if resp.StatusCode == 200 {
						ready = true
						break
					}
				}
				time.Sleep(10 * time.Millisecond)
			}
			if !ready {
				t.Fatal("child not ready")
			}
			completed := make(chan string, 1)
			go func() {
				resp, err := client.Get("http://" + addr + "/slow")
				if err != nil {
					completed <- ""
					return
				}
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				completed <- string(body)
			}()
			marker := make([]byte, len("entered\n"))
			if _, err := io.ReadFull(stdout, marker); err != nil || string(marker) != "entered\n" {
				t.Fatalf("entry: %s %v", marker, err)
			}
			start := time.Now()
			if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			closed, _ := io.ReadAll(stdout)
			if err := cmd.Wait(); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(closed), "closed") || time.Since(start) > 2*time.Second {
				t.Fatalf("drain: %s %s", closed, time.Since(start))
			}
			if body := <-completed; timeout == "0" && body != "completed" {
				t.Fatalf("lost active response %q", body)
			}
		})
	}
}

func TestDeploymentLiveAdmissionAndProbes(t *testing.T) {
	a, _ := app.New(app.Config{})
	if err := a.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer a.Shutdown(context.Background())
	var workerReady atomic.Bool
	workerReady.Store(true)
	entered, finish := make(chan struct{}, 1), make(chan struct{})
	d, err := NewDeployment(a, deployment.Config{ListenerAddress: "127.0.0.1:0", MaxAdmission: 1, DrainTimeout: time.Second, WorkerRequired: true}, DeploymentChecks{
		Artifact: func(context.Context) error { return nil },
		Worker: func(context.Context) error {
			if !workerReady.Load() {
				return errors.New("private-token")
			}
			return nil
		},
	}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { entered <- struct{}{}; <-finish; w.WriteHeader(201) }))
	if err != nil {
		t.Fatal(err)
	}
	s := httptest.NewServer(d)
	defer s.Close()
	get := func(path string, want int, contains string) {
		t.Helper()
		resp, err := s.Client().Get(s.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != want || !strings.Contains(string(body), contains) || strings.Contains(string(body), "private-token") {
			t.Fatalf("%s: %d %s", path, resp.StatusCode, body)
		}
	}
	get("/readyz", 200, `"ready":true`)
	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, err := s.Client().Get(s.URL + "/work")
		if err == nil {
			resp.Body.Close()
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("not admitted")
	}
	get("/work", 503, "unavailable")
	get("/healthz", 200, `"health":true`)
	get("/metrics", 200, "blok_active 1")
	workerReady.Store(false)
	get("/readyz", 503, "worker")
	d.limiter.BeginDrain()
	get("/work", 503, "unavailable")
	get("/metrics", 200, "blok_draining 1")
	close(finish)
	<-done
}

func TestDeploymentSelectedChecksFailClosed(t *testing.T) {
	for _, missing := range []string{"artifact", "store", "worker", "secret"} {
		t.Run(missing, func(t *testing.T) {
			a, _ := app.New(app.Config{})
			a.Start(context.Background())
			defer a.Shutdown(context.Background())
			check := func(name string) func(context.Context) error {
				return func(context.Context) error {
					if missing == name {
						return errors.New("sensitive-value")
					}
					return nil
				}
			}
			d, err := NewDeployment(a, deployment.Config{ListenerAddress: "127.0.0.1:0", MaxAdmission: 1, DrainTimeout: time.Second, StoreRequired: true, WorkerRequired: true, RequiredSecrets: []string{"TOKEN"}}, DeploymentChecks{Artifact: check("artifact"), Store: check("store"), Worker: check("worker"), Secret: func(string) bool { return missing != "secret" }}, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("unexpected effect") }))
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRecorder()
			d.ServeHTTP(r, httptest.NewRequest("GET", "/readyz", nil))
			if r.Code != 503 || strings.Contains(r.Body.String(), "sensitive-value") {
				t.Fatalf("%d %s", r.Code, r.Body.String())
			}
			r = httptest.NewRecorder()
			d.ServeHTTP(r, httptest.NewRequest("POST", "/work", nil))
			if r.Code != 503 {
				t.Fatal(r.Code)
			}
		})
	}
}

func TestDeploymentUnselectedChecksNeverRun(t *testing.T) {
	a, _ := app.New(app.Config{})
	a.Start(context.Background())
	defer a.Shutdown(context.Background())
	unselected := func(context.Context) error { t.Error("unselected dependency probed"); return errors.New("missing") }
	d, err := NewDeployment(a, deployment.Config{ListenerAddress: "127.0.0.1:0", MaxAdmission: 1, DrainTimeout: time.Second}, DeploymentChecks{Artifact: func(context.Context) error { return nil }, Store: unselected, Worker: unselected}, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	if !d.status(context.Background()).Ready {
		t.Fatal("native deployment not ready")
	}
}
