//go:build !windows

package devtool

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/deployment"
)

// fixtureProject is a real scaffolded application whose main package is
// replaced by testdata/tooling/dev/<fixture>/main.go, with extra files
// copied in, then tidied for the framework packages the new main imports.
func fixtureProject(t *testing.T, layout, fixture string, extra map[string]string) string {
	t.Helper()
	dir := scaffolded(t, layout)
	source := filepath.Join(repoRoot(t), "testdata", "tooling", "dev", fixture)
	files := map[string]string{"cmd/shop/main.go": "main.go"}
	for to, from := range extra {
		files[to] = from
	}
	for to, from := range files {
		data, err := os.ReadFile(filepath.Join(source, from))
		if err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(dir, filepath.FromSlash(to))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	tidy := exec.Command("go", "mod", "tidy")
	tidy.Dir = dir
	if output, err := tidy.CombinedOutput(); err != nil {
		t.Fatalf("go mod tidy: %v\n%s", err, output)
	}
	return dir
}

// groupMembers lists the live processes in a process group.
func groupMembers(t *testing.T, group int) []int {
	t.Helper()
	output, err := exec.Command("ps", "-A", "-o", "pid=,pgid=").Output()
	if err != nil {
		t.Fatal(err)
	}
	var members []int
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		pid, _ := strconv.Atoi(fields[0])
		pgid, _ := strconv.Atoi(fields[1])
		if pgid == group && alive(pid) {
			members = append(members, pid)
		}
	}
	return members
}

// executableOf is the path a process was started from (its argv[0]: blok
// dev starts the application by absolute path).
func executableOf(t *testing.T, pid int) string {
	t.Helper()
	output, err := exec.Command("ps", "-o", "command=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(output))
	if len(fields) == 0 {
		t.Fatalf("no command line for %d", pid)
	}
	return fields[0]
}

func fileDigest(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

type ledgerLine struct{ kind, request, pid string }

func readLedger(t *testing.T, name string) []ledgerLine {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	var lines []ledgerLine
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if fields := strings.Fields(line); len(fields) == 3 {
			lines = append(lines, ledgerLine{fields[0], fields[1], fields[2]})
		}
	}
	return lines
}

type quoteReply struct {
	status     int
	TotalCents int64  `json:"totalCents"`
	Marker     string `json:"marker"`
	WorkerPID  string `json:"workerPid"`
	Generation int    `json:"generation"`
	PID        int    `json:"pid"`
	err        error
}

func slowQuote(address, request string, delay int) quoteReply {
	client := http.Client{Timeout: time.Minute}
	body := fmt.Sprintf(`{"requestId":%q,"sku":"coffee","quantity":2,"delayMs":%d}`, request, delay)
	var response *http.Response
	var err error
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		response, err = client.Post("http://"+address+"/quotes", "application/json", strings.NewReader(body))
		if err == nil || !errors.Is(err, syscall.ECONNREFUSED) || time.Now().After(deadline) {
			break
		}
	}
	if err != nil {
		return quoteReply{err: err}
	}
	defer response.Body.Close()
	reply := quoteReply{status: response.StatusCode}
	reply.err = json.NewDecoder(response.Body).Decode(&reply)
	return reply
}

func waitFor(t *testing.T, what string, done func() bool) {
	t.Helper()
	for deadline := time.Now().Add(time.Minute); !done(); time.Sleep(25 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// TestDevNodeWorkerReload drives a Go application that owns a persistent
// Node.js worker (ADR 0004) through blok dev, in both layouts, with the
// node's source inside the project:
//
//   - an edit to the Node node's source rebuilds and restarts; the old
//     application drains: the four requests already running on the old
//     worker each finish exactly once, with the old code, and nothing is
//     repeated on the new worker, which serves the new code under a new
//     generation; the old application, worker and group are gone;
//   - after a crash the group sweep kills the worker the application can no
//     longer stop; a worker left listening on the worker address with the
//     crashed start's generation is refused at the handshake by the next
//     start, never used, and the start after it serves.
//
// It needs the built Node worker: set BLOK_NODE_INTEGRATION_ROOT.
func TestDevNodeWorkerReload(t *testing.T) {
	framework := os.Getenv("BLOK_NODE_INTEGRATION_ROOT")
	if framework == "" {
		t.Skip("set BLOK_NODE_INTEGRATION_ROOT to a framework checkout with the Node worker built")
	}
	nodePath, err := exec.LookPath("node")
	if err != nil {
		t.Fatal("BLOK_NODE_INTEGRATION_ROOT is set but node is not installed")
	}
	for _, layout := range []string{"classic", "unified"} {
		t.Run(layout, func(t *testing.T) { runNodeReload(t, layout, framework, nodePath) })
	}
}

func runNodeReload(t *testing.T, layout, framework, nodePath string) {
	nodeDir := "runtimes/node/nodes/slow-quote"
	if layout == "unified" {
		nodeDir = "nodes/node/slow-quote"
	}
	dir := fixtureProject(t, layout, "node", map[string]string{nodeDir + "/node.json": "node.json", nodeDir + "/index.mjs": "index.mjs"})
	ledger := filepath.Join(t.TempDir(), "ledger")
	address, workerAddress := freeAddress(t), freeAddress(t)
	const token = "synthetic-dev-fixture-token-000000000001"
	sdk := "file://" + filepath.Join(framework, "runtime/nodejs/dist/sdk/nodejs/index.js")
	env := append(os.Environ(), "ADDR="+address, "BLOK_FIXTURE_FRAMEWORK="+framework, "BLOK_FIXTURE_NODE_DIR="+nodeDir,
		"BLOK_FIXTURE_WORKER_ADDRESS="+workerAddress, "BLOK_FIXTURE_TOKEN="+token, "BLOK_FIXTURE_SDK="+sdk, "BLOK_FIXTURE_EFFECTS="+ledger)
	session := startDev(t, DevOptions{Options: Options{Root: dir}, AppEnv: env, Backoff: 2 * time.Second, StableAfter: time.Hour})
	first := session.await(func(e DevEvent) bool { return e.Event == EventAppStarted && e.Build == 1 }, "build 1 started")

	warm := slowQuote(address, "r0", 0)
	if warm.err != nil || warm.status != 200 || warm.Marker != "v1" || warm.Generation != 1 || warm.TotalCents != 3000 {
		t.Fatalf("first reply %+v\n%s", warm, session.dump())
	}
	oldWorker := warm.WorkerPID
	if members := groupMembers(t, first.PID); len(members) != 2 {
		t.Fatalf("application group %d holds %v; want the application and its worker", first.PID, members)
	}

	// Four requests in flight on the old worker, then an edit to the node.
	replies := make([]quoteReply, 4)
	var inflight sync.WaitGroup
	for index := range replies {
		inflight.Go(func() { replies[index] = slowQuote(address, "r"+strconv.Itoa(index+1), 2500) })
	}
	waitFor(t, "four started executions", func() bool { return len(readLedger(t, ledger)) == 6 })
	applyEdits(t, dir, []fixtureEdit{{File: nodeDir + "/index.mjs", Replace: [2]string{`const marker = "v1";`, `const marker = "v2";`}}}, placeholders(layout))
	stopped := session.await(func(e DevEvent) bool { return e.Event == EventAppStopped && e.Build == 1 }, "build 1 stopped")
	if stopped.Status != "exit status 0" || len(stopped.Diagnostics) != 0 {
		t.Fatalf("the old application did not drain cleanly: %+v\n%s", stopped.DevEvent, session.dump())
	}
	second := session.await(func(e DevEvent) bool { return e.Event == EventAppStarted && e.Build == 2 }, "build 2 started")
	inflight.Wait()
	for index, reply := range replies {
		if reply.err != nil || reply.status != 200 || reply.Marker != "v1" || reply.TotalCents != 3000 || reply.WorkerPID != oldWorker {
			t.Fatalf("in-flight request r%d: %+v (old worker %s)\n%s", index+1, reply, oldWorker, session.dump())
		}
	}
	fresh := slowQuote(address, "r5", 0)
	if fresh.err != nil || fresh.status != 200 || fresh.Marker != "v2" || fresh.Generation != second.Generation || fresh.WorkerPID == oldWorker || second.Generation <= first.Generation {
		t.Fatalf("reply after reload %+v (old worker %s, generations %d→%d)\n%s", fresh, oldWorker, first.Generation, second.Generation, session.dump())
	}
	oldPID, _ := strconv.Atoi(oldWorker)
	if alive(first.PID) || alive(oldPID) || len(groupMembers(t, first.PID)) != 0 {
		t.Fatalf("old application %d or worker %d survived the reload", first.PID, oldPID)
	}
	executions := map[string][]ledgerLine{}
	for _, line := range readLedger(t, ledger) {
		executions[line.request] = append(executions[line.request], line)
	}
	for request, lines := range executions {
		if len(lines) != 2 || lines[0].kind != "start" || lines[1].kind != "finish" {
			t.Fatalf("request %s ran %v: dropped or repeated", request, lines)
		}
	}
	if len(executions) != 6 {
		t.Fatalf("executions %v; want r0..r5 once each", executions)
	}

	// A crash: the group sweep kills the worker the application can no
	// longer drain.
	client := http.Client{Timeout: 10 * time.Second}
	if response, err := client.Post("http://"+address+"/crash", "text/plain", nil); err == nil {
		response.Body.Close()
	}
	crashed := session.await(func(e DevEvent) bool { return e.Event == EventAppExited && e.Generation == second.Generation }, "crash")
	if crashed.Status != "exit status 3" {
		t.Fatalf("crash status %q", crashed.Status)
	}
	secondWorker, _ := strconv.Atoi(fresh.WorkerPID)
	waitFor(t, "the crashed application's worker to be swept", func() bool { return !alive(secondWorker) && len(groupMembers(t, second.PID)) == 0 })

	// A worker left on the address with the crashed start's generation.
	digest := sha256.Sum256(append(readFile(t, filepath.Join(dir, nodeDir, "node.json")), readFile(t, filepath.Join(dir, nodeDir, "index.mjs"))...))
	stale := exec.Command(nodePath, filepath.Join(framework, "runtime/nodejs/dist/runtime/nodejs/main.js"), filepath.Join(dir, nodeDir, "index.mjs"))
	stale.Env = append(os.Environ(), "BLOK_WORKER_ADDRESS="+workerAddress, "BLOK_WORKER_TOKEN="+token, "BLOK_WORKER_PRINCIPAL=dev-fixture",
		"BLOK_WORKER_ARTIFACT=sha256:"+hex.EncodeToString(digest[:]), "BLOK_WORKER_GENERATION="+strconv.Itoa(second.Generation),
		"BLOK_WORKER_CAPABILITIES=[]", "BLOK_FIXTURE_SDK="+sdk, "BLOK_FIXTURE_EFFECTS="+ledger)
	if err := stale.Start(); err != nil {
		t.Fatal(err)
	}
	staleDone := make(chan struct{})
	go func() { _ = stale.Wait(); close(staleDone) }()
	t.Cleanup(func() { _ = stale.Process.Kill(); <-staleDone })
	waitFor(t, "the stale worker to listen", func() bool {
		connection, err := net.DialTimeout("tcp", workerAddress, 100*time.Millisecond)
		if err == nil {
			connection.Close()
		}
		return err == nil
	})
	refused := session.await(func(e DevEvent) bool { return e.Event == EventAppExited && e.Generation > second.Generation }, "a start refused by the stale worker")
	if refused.Status != "exit status 1" || len(refused.Diagnostics) != 1 || refused.Diagnostics[0].Code != "dev_app_exited" {
		t.Fatalf("the start next to a stale worker was not refused: %+v\n%s", refused.DevEvent, session.dump())
	}
	_ = stale.Process.Kill()
	<-staleDone
	recovered := session.await(func(e DevEvent) bool { return e.Event == EventAppStarted && e.Generation > refused.Generation }, "a start after the stale worker is gone")
	after := slowQuote(address, "r6", 0)
	if after.err != nil || after.status != 200 || after.Generation != recovered.Generation || after.Marker != "v2" {
		t.Fatalf("reply after recovery %+v\n%s", after, session.dump())
	}
	stalePID := strconv.Itoa(stale.Process.Pid)
	for _, line := range readLedger(t, ledger) {
		if line.pid == stalePID {
			t.Fatalf("the stale worker executed %v", line)
		}
	}
	assertNoProcesses(t, session.stop(), oldPID, secondWorker)
}

func readFile(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// TestDevDurableRunIsNotRelabelled drives examples/deploy's journaled order
// deployment through blok dev, in both layouts. An order admitted under
// build 1 waits in the journal while the code changes: build 2 is a new
// executable at a new path with a new digest, and the application's own
// retained-artifact check refuses to adopt the run instead of serving it
// under the new code. The refusal is structured (exit status
// deployment.ExitRetainedIncompatible): blok dev reports
// dev_durable_incompatible, does not restart build 2, keeps build 1's
// executable unchanged and prints the command that runs it, which does
// adopt the run. Reverting the edit builds byte-identical code, which
// adopts the run too; the order is processed exactly once.
func TestDevDurableRunIsNotRelabelled(t *testing.T) {
	for _, layout := range []string{"classic", "unified"} {
		t.Run(layout, func(t *testing.T) {
			dir := fixtureProject(t, layout, "durable", nil)
			address := freeAddress(t)
			const token = "synthetic-dev-token"
			env := append(os.Environ(), "BLOK_LISTEN="+address, "BLOK_VOLUME="+filepath.Join(t.TempDir(), "orders.db"), "BLOK_DEPLOY_TOKEN="+token)
			session := startDev(t, DevOptions{Options: Options{Root: dir}, AppEnv: env, Backoff: 300 * time.Millisecond, MaxBackoff: 600 * time.Millisecond})
			call := func(method, path, body string) (int, string) {
				request, _ := http.NewRequest(method, "http://"+address+path, strings.NewReader(body))
				request.Header.Set("Authorization", "Bearer "+token)
				response, err := http.DefaultClient.Do(request)
				if err != nil {
					return 0, err.Error()
				}
				defer response.Body.Close()
				data, _ := io.ReadAll(response.Body)
				return response.StatusCode, strings.TrimSpace(string(data))
			}
			ready := func() {
				waitFor(t, "readiness", func() bool { status, _ := call("GET", "/readyz", ""); return status == 200 })
			}
			first := session.await(func(e DevEvent) bool { return e.Event == EventAppStarted && e.Build == 1 }, "build 1 started")
			firstExecutable := executableOf(t, first.PID)
			firstDigest := fileDigest(t, firstExecutable)
			ready()
			if status, body := call("POST", "/orders", `{"requestKey":"dev-1","sku":"coffee","quantity":2}`); status != 202 {
				t.Fatalf("admission %d %s", status, body)
			}

			applyEdits(t, dir, []fixtureEdit{{File: "cmd/shop/main.go", Replace: [2]string{`"durable fixture v1"`, `"durable fixture v2"`}}}, placeholders(layout))
			second := session.await(func(e DevEvent) bool { return e.Event == EventAppStarted && e.Build == 2 }, "build 2 started")
			secondExecutable := executableOf(t, second.PID)
			refused := session.await(func(e DevEvent) bool { return e.Event == EventAppExited && e.Build == 2 }, "build 2 refusing the retained run")
			if !strings.Contains(strings.Join(refused.Output, "\n"), "retained journal incompatible") {
				t.Fatalf("build 2 adopted or mishandled the retained run: %+v\n%s", refused.DevEvent, session.dump())
			}
			// The refusal is structured (deployment.ExitRetainedIncompatible),
			// reported as such, and not crash-looped: blok dev waits for the
			// next change, and keeps build 1, which admitted the run.
			if refused.Status != "exit status "+strconv.Itoa(deployment.ExitRetainedIncompatible) || len(refused.Diagnostics) != 1 || refused.Diagnostics[0].Code != "dev_durable_incompatible" {
				t.Fatalf("the refusal was not reported as dev_durable_incompatible: %+v\n%s", refused.DevEvent, session.dump())
			}
			if secondExecutable == firstExecutable {
				t.Fatalf("build 2 reused build 1's executable path %s", firstExecutable)
			}
			if refused.Resume == "" || !strings.Contains(refused.Resume, firstExecutable) || !strings.Contains(refused.Diagnostics[0].Remediation, refused.Resume) {
				t.Fatalf("no command resumes the run with build 1's executable %s: %+v\n%s", firstExecutable, refused.DevEvent, session.dump())
			}
			if digest := fileDigest(t, firstExecutable); digest != firstDigest {
				t.Fatalf("build 1's executable changed: %s, was %s", digest, firstDigest)
			}
			time.Sleep(2 * time.Second)
			for _, event := range session.snapshot() {
				if event.Build == 2 && (event.Event == EventRestartScheduled || event.Event == EventAppStarted && event.PID != second.PID) {
					t.Fatalf("build 2 was restarted after a deterministic refusal: %+v\n%s", event.DevEvent, session.dump())
				}
			}
			// The printed command, run by a shell as a developer would in
			// another terminal while blok dev waits, resumes the run with the
			// kept executable: it adopts the journal and is ready. SIGTERM
			// (Ctrl+C's sibling) to the shell's process group stops it.
			resumed := exec.Command("/bin/sh", "-c", refused.Resume)
			resumed.Env = env
			resumed.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			if err := resumed.Start(); err != nil {
				t.Fatal(err)
			}
			group := resumed.Process.Pid
			// The resumed application is under the same pipe guard blok dev
			// gives its own: the test binary holds the pipe, so however it
			// dies (a -timeout panic, SIGQUIT, SIGKILL) the guard kills the
			// group and nothing outlives the test.
			guard, err := startGuard(resumed.Process, "")
			if err != nil {
				_ = syscall.Kill(-group, syscall.SIGKILL)
				t.Fatal(err)
			}
			t.Cleanup(guard.release)
			t.Cleanup(func() { _ = syscall.Kill(-group, syscall.SIGKILL) })
			ready()
			if err := syscall.Kill(-group, syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			_ = resumed.Wait()
			waitFor(t, "the resumed executable to stop", func() bool { return syscall.Kill(-group, 0) != nil })

			applyEdits(t, dir, []fixtureEdit{{File: "cmd/shop/main.go", Replace: [2]string{`"durable fixture v2"`, `"durable fixture v1"`}}}, placeholders(layout))
			third := session.await(func(e DevEvent) bool { return e.Event == EventAppStarted && e.Build == 3 }, "build 3 started")
			if digest := fileDigest(t, executableOf(t, third.PID)); digest != firstDigest {
				t.Fatalf("reverted code built a different executable: %s, build 1 %s", digest, firstDigest)
			}
			ready()
			if status, body := call("GET", "/orders/dev-1", ""); status != 404 {
				t.Fatalf("the order was processed before its run was adopted: %d %s", status, body)
			}
			// Processed once: the second pass finds nothing left to do.
			for index, want := range []string{`{"processed":true}`, `{"processed":false}`} {
				status, body := call("POST", "/process", "")
				if status != 200 || body != want {
					t.Fatalf("process %d: %d %s", index, status, body)
				}
			}
			if status, body := call("GET", "/orders/dev-1", ""); status != 200 {
				t.Fatalf("the retained order was lost: %d %s", status, body)
			}
			assertNoProcesses(t, session.stop())
		})
	}
}
