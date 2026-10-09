package execution_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/well-prado/new-blok/execution"
)

// Durable workflows in a real application process (#333 slice 5): the
// application in testdata/durableapp is built, run, SIGKILLed and run
// again on the same data directory; every accepted run finishes after the
// restart without being asked, and every effect happens exactly once.

// durableApp is one data directory and the application processes run on it.
type durableApp struct {
	t        *testing.T
	binary   string
	dir      string
	process  *exec.Cmd
	base     string
	holdAt   string
	register string
}

func newDurableApp(t *testing.T, holdAt string) *durableApp {
	t.Helper()
	dir := t.TempDir()
	binary := filepath.Join(dir, "durableapp")
	build := exec.Command("go", "build", "-o", binary, "./testdata/durableapp")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		t.Fatalf("build the application: %v", err)
	}
	a := &durableApp{t: t, binary: binary, dir: dir, holdAt: holdAt}
	t.Cleanup(a.kill)
	return a
}

func (a *durableApp) path(name string) string { return filepath.Join(a.dir, name) }

// start runs the application on the data directory, leaving out the
// workflows unregister names.
func (a *durableApp) start(unregister string) {
	a.t.Helper()
	_ = os.Remove(a.path("addr"))
	_ = os.Remove(a.path("held"))
	a.process = exec.Command(a.binary)
	a.process.Env = append(os.Environ(), "JOURNAL="+a.path("data/journal.db"), "EFFECTS="+a.path("effects"), "GATE="+a.path("gate"), "HELD="+a.path("held"),
		"HOLD_AT="+a.holdAt, "LEASE=1s", "ADDR_FILE="+a.path("addr"), "UNREGISTER="+unregister)
	a.process.Stderr = os.Stderr
	if err := a.process.Start(); err != nil {
		a.t.Fatal(err)
	}
	a.waitFor(func() bool {
		addr, err := os.ReadFile(a.path("addr"))
		if err == nil && len(addr) > 0 {
			a.base = "http://" + string(addr)
			return true
		}
		return false
	}, "the application to listen")
}

// kill SIGKILLs the running application, if any.
func (a *durableApp) kill() {
	if a.process == nil || a.process.Process == nil {
		return
	}
	_ = a.process.Process.Signal(syscall.SIGKILL)
	_ = a.process.Wait()
	a.process = nil
}

func (a *durableApp) waitFor(ready func() bool, what string) {
	a.t.Helper()
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(25 * time.Millisecond) {
		if ready() {
			return
		}
	}
	a.t.Fatalf("timed out waiting for %s", what)
}

func (a *durableApp) request(method, path string, body string, header ...string) execution.DurableResult {
	a.t.Helper()
	request, err := http.NewRequest(method, a.base+path, strings.NewReader(body))
	if err != nil {
		a.t.Fatal(err)
	}
	for index := 0; index+1 < len(header); index += 2 {
		request.Header.Set(header[index], header[index+1])
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		a.t.Fatal(err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK {
		a.t.Fatalf("%s %s: %d %s", method, path, response.StatusCode, data)
	}
	var result execution.DurableResult
	if err := json.Unmarshal(data, &result); err != nil {
		a.t.Fatalf("%s %s: %s: %v", method, path, data, err)
	}
	return result
}

func (a *durableApp) run(workflow, input string) execution.DurableResult {
	a.t.Helper()
	return a.request(http.MethodPost, "/runs/"+workflow, input)
}

func (a *durableApp) status(runID string) execution.DurableResult {
	a.t.Helper()
	return a.request(http.MethodGet, "/runs/"+runID, "")
}

// settled polls runID, without changing anything, until it ends.
func (a *durableApp) settled(runID string) execution.DurableResult {
	a.t.Helper()
	var result execution.DurableResult
	a.waitFor(func() bool {
		result = a.status(runID)
		return result.State != "accepted" && result.State != "suspended"
	}, "run "+runID+" to end")
	return result
}

// held waits until the hold node is blocked on its item.
func (a *durableApp) held() {
	a.t.Helper()
	a.waitFor(func() bool { _, err := os.Stat(a.path("held")); return err == nil }, "the run to reach the hold")
}

func (a *durableApp) openGate() {
	a.t.Helper()
	if err := os.WriteFile(a.path("gate"), []byte("open"), 0o600); err != nil {
		a.t.Fatal(err)
	}
}

// effects lists every effect the processes ran on this data directory.
func (a *durableApp) effects() []string {
	a.t.Helper()
	data, err := os.ReadFile(a.path("effects"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		a.t.Fatal(err)
	}
	effects := strings.Fields(string(data))
	sort.Strings(effects)
	return effects
}

func (a *durableApp) expect(result execution.DurableResult, output string, effects ...string) {
	a.t.Helper()
	if result.State != "completed" || compact(a.t, result.Output) != output {
		a.t.Fatalf("run %+v (output %s); want completed with %s", result, result.Output, output)
	}
	sort.Strings(effects)
	if got := a.effects(); !reflect.DeepEqual(got, effects) {
		a.t.Fatalf("effects across every process %q; want each of %q exactly once", got, effects)
	}
}

func compact(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var buffer bytes.Buffer
	if err := json.Compact(&buffer, raw); err != nil {
		return string(raw)
	}
	return buffer.String()
}

// TestDurableAppSurvivesAKillMidRun: the application is killed while a
// run blocks between its charge and its shipment. After a restart the run
// resumes on its own, the charge is not repeated, and it completes.
func TestDurableAppSurvivesAKillMidRun(t *testing.T) {
	a := newDurableApp(t, "5")
	a.start("")
	run := a.run("pay", `{"value":4}`)
	if run.State != "accepted" {
		t.Fatalf("a run blocked mid-way: %+v; want accepted", run)
	}
	a.held()
	a.kill()
	a.openGate()
	a.start("")
	a.expect(a.settled(run.RunID), `{"value":5}`, "charge-4", "ship-5")
}

// TestDurableAppKeepsAWaitAcrossAKill: a run suspended at a wait stays
// suspended across a kill and restart, running nothing, until its signal
// arrives; then it completes.
func TestDurableAppKeepsAWaitAcrossAKill(t *testing.T) {
	a := newDurableApp(t, "")
	a.start("")
	run := a.run("approve", `{"value":1}`)
	if run.State != "suspended" {
		t.Fatalf("a run at a wait: %+v; want suspended", run)
	}
	a.kill()
	a.start("")
	time.Sleep(3 * time.Second) // three leases: long enough to be resumed, were it taken
	if got := a.status(run.RunID); got.State != "suspended" || !reflect.DeepEqual(a.effects(), []string{"charge-1"}) {
		t.Fatalf("after a restart with no signal: %+v, effects %q; want suspended, charge-1 only", got, a.effects())
	}
	a.request(http.MethodPost, "/runs/"+run.RunID+"/signals/approval", `{"approved":true}`, "Signal-Id", "s1")
	a.expect(a.settled(run.RunID), `{"value":2}`, "charge-1", "ship-2")
}

// TestDurableAppSurvivesAKillMidLoop: killed while the third of five loop
// items blocks; after a restart the loop resumes at that item, no item is
// charged twice, and it completes with every item's result in order.
func TestDurableAppSurvivesAKillMidLoop(t *testing.T) {
	a := newDurableApp(t, "4")
	a.start("")
	run := a.run("batch", `{"items":[{"value":1},{"value":2},{"value":3},{"value":4},{"value":5}]}`)
	a.held()
	a.kill()
	if got, want := a.effects(), []string{"charge-1", "charge-2", "charge-3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("effects at the kill %q; want %q", got, want)
	}
	a.openGate()
	a.start("")
	a.expect(a.settled(run.RunID), `[{"value":2},{"value":3},{"value":4},{"value":5},{"value":6}]`, "charge-1", "charge-2", "charge-3", "charge-4", "charge-5")
}

// TestDurableAppSurvivesAKillWhileAChildRuns: killed while the parent's
// child run blocks; after a restart the child resumes, completes, wakes
// its parent, and the parent completes with the child's result.
func TestDurableAppSurvivesAKillWhileAChildRuns(t *testing.T) {
	a := newDurableApp(t, "5")
	a.start("")
	run := a.run("parent", `{"value":4}`)
	a.held()
	a.kill()
	a.openGate()
	a.start("")
	a.expect(a.settled(run.RunID), `{"value":50}`, "charge-4", "vip-5")
}

// TestDurableAppLeavesUnregisteredWorkflowsAlone: a build that no longer
// registers a workflow leaves its accepted runs untouched (they run
// nothing and stay accepted) while it serves the others; a build that
// registers it again completes them.
func TestDurableAppLeavesUnregisteredWorkflowsAlone(t *testing.T) {
	a := newDurableApp(t, "5")
	a.start("")
	run := a.run("pay", `{"value":4}`)
	a.held()
	a.kill()
	a.openGate()
	a.start("pay")
	served := a.run("kid", `{"value":2}`)
	if served.State != "completed" || compact(t, served.Output) != `{"value":20}` {
		t.Fatalf("a registered workflow's run, answered synchronously: %+v (%s)", served, served.Output)
	}
	time.Sleep(3 * time.Second)
	if got := a.status(run.RunID); got.State != "accepted" || !reflect.DeepEqual(a.effects(), []string{"charge-4", "vip-2"}) {
		t.Fatalf("the unregistered workflow's run: %+v, effects %q; want accepted and untouched", got, a.effects())
	}
	a.kill()
	a.start("")
	a.expect(a.settled(run.RunID), `{"value":5}`, "charge-4", "ship-5", "vip-2")
}
