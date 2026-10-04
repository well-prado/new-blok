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
	"sync"
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

func TestDeploymentLeaseKeepsSharedStoreOpenUntilHandlerReturns(t *testing.T) {
	var active atomic.Int32
	var closedUnderWork atomic.Bool
	application, err := app.New(app.Config{DrainTimeout: time.Second, Dependencies: []app.Dependency{{
		Name:  "store",
		Start: func(context.Context) error { return nil },
		Close: func(context.Context) error {
			closedUnderWork.Store(active.Load() != 0)
			return nil
		},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	entered, finish := make(chan struct{}), make(chan struct{})
	var finishOnce sync.Once
	finishHandler := func() { finishOnce.Do(func() { close(finish) }) }
	d, err := NewDeployment(application, deployment.Config{ListenerAddress: "127.0.0.1:0", MaxAdmission: 1, DrainTimeout: time.Second}, DeploymentChecks{
		Artifact: func(context.Context) error { return nil },
	}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Model a caller-facing trigger inside Deployment: both layers own a
		// lease, while the deployment limiter remains the admission bound.
		nestedLease, err := application.Begin()
		if err != nil {
			t.Errorf("nested trigger lease: %v", err)
			return
		}
		nestedCtx, unbind := nestedLease.Bind(r.Context())
		defer unbind()
		defer nestedLease.Release()
		active.Add(1)
		defer active.Add(-1)
		close(entered)
		select {
		case <-finish:
		case <-nestedCtx.Done():
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(d)
	defer func() {
		finishHandler()
		server.CloseClientConnections()
		server.Close()
	}()
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		resp, err := server.Client().Get(server.URL + "/work")
		if err == nil {
			resp.Body.Close()
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- application.Shutdown(context.Background()) }()
	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown finished with the admitted handler held: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	if closedUnderWork.Load() {
		t.Fatal("shared store closed while deployment handler was running")
	}

	finishHandler()
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish after handler returned")
	}
	<-requestDone
	if closedUnderWork.Load() || active.Load() != 0 {
		t.Fatalf("store closed under work=%v active=%d", closedUnderWork.Load(), active.Load())
	}
}

func TestDeploymentLeaseCancelsHandlerBeforeSharedStoreClose(t *testing.T) {
	var active atomic.Int32
	var canceled atomic.Bool
	var closedUnderWork atomic.Bool
	var storeClosed atomic.Bool
	application, err := app.New(app.Config{DrainTimeout: 30 * time.Millisecond, AbortGrace: time.Second, Dependencies: []app.Dependency{{
		Name:  "store",
		Start: func(context.Context) error { return nil },
		Close: func(context.Context) error {
			closedUnderWork.Store(active.Load() != 0 || !canceled.Load())
			storeClosed.Store(true)
			return nil
		},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	d, err := NewDeployment(application, deployment.Config{ListenerAddress: "127.0.0.1:0", MaxAdmission: 1, DrainTimeout: time.Second}, DeploymentChecks{
		Artifact: func(context.Context) error { return nil },
	}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		active.Add(1)
		defer active.Add(-1)
		close(entered)
		<-r.Context().Done()
		canceled.Store(app.Aborted(r.Context()))
	}))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(d)
	defer func() {
		server.CloseClientConnections()
		server.Close()
	}()
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		resp, err := server.Client().Get(server.URL + "/work")
		if err == nil {
			resp.Body.Close()
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}

	shutdownErr := application.Shutdown(context.Background())
	if !errors.Is(shutdownErr, app.ErrDrainTimeout) {
		t.Fatalf("shutdown error = %v, want ErrDrainTimeout", shutdownErr)
	}
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("timed-out handler did not return after cancellation")
	}
	if !canceled.Load() || closedUnderWork.Load() || !storeClosed.Load() || active.Load() != 0 {
		t.Fatalf("canceled=%v closedUnderWork=%v storeClosed=%v active=%d", canceled.Load(), closedUnderWork.Load(), storeClosed.Load(), active.Load())
	}
}

func TestDeploymentRejectedAdmissionsReleaseTheirCapacity(t *testing.T) {
	application, err := app.New(app.Config{DrainTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	entered, finish := make(chan struct{}), make(chan struct{})
	var finishOnce sync.Once
	finishHandler := func() { finishOnce.Do(func() { close(finish) }) }
	d, err := NewDeployment(application, deployment.Config{ListenerAddress: "127.0.0.1:0", MaxAdmission: 1, DrainTimeout: time.Second}, DeploymentChecks{
		Artifact: func(context.Context) error { return nil },
	}, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(entered)
		<-finish
	}))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(d)
	defer func() {
		finishHandler()
		server.CloseClientConnections()
		server.Close()
	}()
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		resp, err := server.Client().Get(server.URL + "/work")
		if err == nil {
			resp.Body.Close()
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	resp, err := server.Client().Get(server.URL + "/rejected")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("saturated request status=%d, want 503", resp.StatusCode)
	}
	d.limiter.BeginDrain()
	finishHandler()
	<-requestDone

	active, draining := d.limiter.Snapshot()
	if active != 0 || !draining {
		t.Fatalf("limiter active=%d draining=%v after requests finished", active, draining)
	}
	if err := application.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown after rejected request: %v", err)
	}
}

func TestDeploymentReleasesLimiterWhenApplicationRejectsLease(t *testing.T) {
	application, err := app.New(app.Config{DrainTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	d, err := NewDeployment(application, deployment.Config{ListenerAddress: "127.0.0.1:0", MaxAdmission: 1, DrainTimeout: time.Second}, DeploymentChecks{
		Artifact: func(context.Context) error { return nil },
	}, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRecorder()
	d.ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/work", nil))
	if r.Code != http.StatusServiceUnavailable {
		t.Fatalf("draining application status=%d, want 503", r.Code)
	}
	active, draining := d.limiter.Snapshot()
	if active != 0 || draining {
		t.Fatalf("limiter active=%d draining=%v after app rejection", active, draining)
	}
}

func TestDeploymentRunWaitsForCanceledHandlerBeforeClosingDependencies(t *testing.T) {
	runDeploymentRunCleanup(t, true)
}

func TestDeploymentRunBoundsNonCooperativeHandler(t *testing.T) {
	runDeploymentRunCleanup(t, false)
}

func runDeploymentRunCleanup(t *testing.T, releaseWithinGrace bool) {
	t.Helper()
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := probe.Addr().String()
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}

	var handlerReturned atomic.Bool
	var handlerAborted atomic.Bool
	var dependencyClosedUnderHandler atomic.Bool
	var dependencyClosedWithoutAbort atomic.Bool
	dependencyClosed := make(chan struct{})
	application, err := app.New(app.Config{
		DrainTimeout: time.Second,
		AbortGrace:   300 * time.Millisecond,
		Dependencies: []app.Dependency{{
			Name:  "store",
			Start: func(context.Context) error { return nil },
			Close: func(context.Context) error {
				dependencyClosedUnderHandler.Store(!handlerReturned.Load())
				dependencyClosedWithoutAbort.Store(!handlerAborted.Load())
				close(dependencyClosed)
				return nil
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	canceled := make(chan struct{})
	cleanup := make(chan struct{})
	var cleanupOnce sync.Once
	releaseCleanup := func() { cleanupOnce.Do(func() { close(cleanup) }) }
	d, err := NewDeployment(application, deployment.Config{
		ListenerAddress: address,
		MaxAdmission:    1,
		DrainTimeout:    80 * time.Millisecond,
	}, DeploymentChecks{Artifact: func(context.Context) error { return nil }}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		handlerAborted.Store(app.Aborted(r.Context()))
		close(canceled)
		<-cleanup
		handlerReturned.Store(true)
	}))
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	runDone := make(chan error, 1)
	var runComplete atomic.Bool
	go func() {
		err := d.Run(runCtx, nil)
		runComplete.Store(true)
		runDone <- err
	}()
	defer func() {
		releaseCleanup()
		if !runComplete.Load() {
			cancelRun()
			select {
			case <-runDone:
			case <-time.After(2 * time.Second):
				t.Error("deployment Run did not stop during test cleanup")
			}
		}
	}()

	client := &http.Client{Timeout: 2 * time.Second}
	ready := false
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		resp, err := client.Get("http://" + address + "/readyz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				ready = true
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ready {
		t.Fatal("deployment did not become ready")
	}
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		resp, err := client.Get("http://" + address + "/work")
		if err == nil {
			resp.Body.Close()
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}

	started := time.Now()
	cancelRun()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("handler did not observe Run's drain cancellation")
	}
	if releaseWithinGrace {
		select {
		case <-dependencyClosed:
			t.Fatal("dependency closed while the canceled handler was still unwinding")
		case <-time.After(25 * time.Millisecond):
		}
		releaseCleanup()
	} else {
		select {
		case <-dependencyClosed:
		case <-time.After(application.AbortGrace() + time.Second):
			t.Fatal("Run did not force dependency close at the abort-grace bound")
		}
	}
	if err := <-runDone; !errors.Is(err, app.ErrDrainTimeout) {
		t.Fatalf("Run error=%v, want ErrDrainTimeout", err)
	}
	if dependencyClosedUnderHandler.Load() != !releaseWithinGrace || handlerReturned.Load() != releaseWithinGrace {
		t.Fatalf("releaseWithinGrace=%v dependencyClosedUnderHandler=%v handlerReturned=%v", releaseWithinGrace, dependencyClosedUnderHandler.Load(), handlerReturned.Load())
	}
	if !handlerAborted.Load() || dependencyClosedWithoutAbort.Load() {
		t.Fatalf("handlerAborted=%v dependencyClosedWithoutAbort=%v", handlerAborted.Load(), dependencyClosedWithoutAbort.Load())
	}
	if elapsed, max := time.Since(started), d.config.DrainTimeout+application.AbortGrace()+250*time.Millisecond; elapsed > max {
		t.Fatalf("Run took %v, beyond drain plus abort grace bound %v", elapsed, max)
	} else if !releaseWithinGrace && elapsed < application.AbortGrace()*3/4 {
		t.Fatalf("Run returned after %v before the non-cooperative handler's abort grace elapsed", elapsed)
	}
	releaseCleanup()
	<-requestDone
}
