//go:build windows

package runtime

import (
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

// processEnded reports whether pid has exited within the wait.
func processEnded(pid int, wait time.Duration) bool {
	h, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(pid))
	if err != nil {
		return true // no such process
	}
	defer windows.CloseHandle(h)
	event, _ := windows.WaitForSingleObject(h, uint32(wait/time.Millisecond))
	return event == windows.WAIT_OBJECT_0
}

func TestOwnedProcessStopEndsTheWholeTree(t *testing.T) {
	cmd, owner, grandchild := startOwnedChild(t, false)
	if processEnded(grandchild, 0) {
		t.Fatal("grandchild was not running before stop")
	}
	owner.stop()
	_ = cmd.Wait()
	// Checked before release: closing the job would end the grandchild too
	// and hide a stop that reached only the direct child.
	ended := processEnded(grandchild, 5*time.Second)
	owner.release()
	if !ended {
		t.Fatal("stop left the worker's grandchild running")
	}
}

func TestOwnedProcessReleaseReapsDescendantsOfAnExitedChild(t *testing.T) {
	cmd, owner, grandchild := startOwnedChild(t, true)
	if err := cmd.Wait(); err != nil {
		t.Fatalf("child: %v", err)
	}
	if processEnded(grandchild, 200*time.Millisecond) {
		t.Fatal("grandchild ended with its parent; the test proves nothing")
	}
	owner.release()
	if !processEnded(grandchild, 5*time.Second) {
		t.Fatal("release left an orphaned grandchild running")
	}
}

// A released owner's job handle may be reissued to another job; stopping or
// killing through it afterwards must not reach anyone.
func TestReleasedOwnerNeverTerminatesAnotherJob(t *testing.T) {
	first, firstOwner, _ := startOwnedChild(t, true)
	_ = first.Wait()
	firstOwner.release()
	_, _, other := startOwnedChild(t, false)
	firstOwner.stop()
	firstOwner.kill()
	if firstOwner.job != 0 {
		t.Fatal("released owner kept its job handle")
	}
	if processEnded(other, 300*time.Millisecond) {
		t.Fatal("a released owner terminated another worker's tree")
	}
}

// runConsoleHost plays a deployment on its own console process group: it
// starts a worker as Connect does, waits for Ctrl+Break, then reports whether
// the event also reached the worker.
func runConsoleHost() {
	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt)
	dir := os.Getenv("BLOK_OWNED_DIR")
	ready, marker := filepath.Join(dir, "probe.ready"), filepath.Join(dir, "probe.interrupted")
	owner, err := newProcessOwner()
	if err != nil {
		os.Exit(3)
	}
	cmd := workerCommand(os.Args[0], []string{"-test.run=^TestOwnedProcessHelper$"}, "", []string{"BLOK_OWNED_ROLE=interrupt-probe", "BLOK_OWNED_READY_FILE=" + ready, "BLOK_OWNED_MARKER=" + marker})
	if owner.start(cmd) != nil {
		os.Exit(4)
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			os.Exit(5)
		}
	}
	_ = os.WriteFile(filepath.Join(dir, "host.ready"), []byte("ready"), 0o600)
	select {
	case <-interrupts:
	case <-time.After(15 * time.Second):
		os.Exit(6)
	}
	time.Sleep(500 * time.Millisecond)
	result := "survived"
	if _, err := os.Stat(marker); err == nil {
		result = "interrupted"
	} else if processEnded(cmd.Process.Pid, 0) {
		result = "terminated"
	}
	_ = os.WriteFile(filepath.Join(dir, "result"), []byte(result), 0o600)
	owner.kill()
	_ = cmd.Wait()
	owner.release()
	os.Exit(0)
}

// Ctrl+C or Ctrl+Break on a deployment's console is the host's shutdown
// request. It must not also reach the worker, which would cut its calls
// while the host is still draining.
func TestConsoleBreakDoesNotReachWorker(t *testing.T) {
	dir := t.TempDir()
	host := exec.Command(os.Args[0], "-test.run=^TestOwnedProcessHelper$")
	host.Env = append(os.Environ(), "BLOK_OWNED_ROLE=console-host", "BLOK_OWNED_DIR="+dir)
	host.SysProcAttr = &syscall.SysProcAttr{CreationFlags: windows.CREATE_NEW_PROCESS_GROUP}
	if err := host.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = host.Process.Kill() }()
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		if _, err := os.Stat(filepath.Join(dir, "host.ready")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("console host never started its worker")
		}
	}
	if err := windows.GenerateConsoleCtrlEvent(windows.CTRL_BREAK_EVENT, uint32(host.Process.Pid)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- host.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("console host: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("console host did not finish")
	}
	result, err := os.ReadFile(filepath.Join(dir, "result"))
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "survived" {
		t.Fatalf("the host's Ctrl+Break reached the worker: %s", result)
	}
}

// ownedChildCommand builds a worker that, as its very first action, launches
// a long-lived grandchild and writes the grandchild's PID to the returned
// file.
func ownedChildCommand(t *testing.T, exitEarly bool) (*exec.Cmd, string) {
	t.Helper()
	pidFile := filepath.Join(t.TempDir(), "grandchild.pid")
	exit := "0"
	if exitEarly {
		exit = "1"
	}
	return workerCommand(os.Args[0], []string{"-test.run=^TestOwnedProcessHelper$"}, "", []string{"BLOK_OWNED_ROLE=child", "BLOK_OWNED_PID_FILE=" + pidFile, "BLOK_OWNED_EXIT=" + exit}), pidFile
}

// ownershipBarrier is how long the barrier holds a started worker before it
// joins the job. A worker that is running reaches its first action, writing
// its grandchild's PID, well within it.
const ownershipBarrier = 3 * time.Second

// A worker must not run before it is owned. The barrier holds the launch
// between process creation and job assignment for long enough that a running
// worker would launch its grandchild there, outside the job (#224). A worker
// started running and assigned afterwards fails both checks: its grandchild
// appears during the barrier, and it survives stop and release.
func TestWorkerRunsNothingBeforeItIsOwned(t *testing.T) {
	for _, exitEarly := range []bool{false, true} {
		name := map[bool]string{false: "stop", true: "release-after-exit"}[exitEarly]
		t.Run(name, func(t *testing.T) {
			cmd, pidFile := ownedChildCommand(t, exitEarly)
			owner, err := newProcessOwner()
			if err != nil {
				t.Fatalf("newProcessOwner: %v", err)
			}
			ranEarly := false
			owner.beforeAssign = func() {
				for deadline := time.Now().Add(ownershipBarrier); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
					if raw, err := os.ReadFile(pidFile); err == nil && len(raw) > 0 {
						ranEarly = true
						return
					}
				}
			}
			if err := owner.start(cmd); err != nil {
				owner.release()
				t.Fatalf("start: %v", err)
			}
			grandchild := waitForPID(t, pidFile)
			t.Cleanup(func() { // never leave a stray process behind
				if p, err := os.FindProcess(grandchild); err == nil {
					_ = p.Kill()
				}
				owner.kill()
				owner.release()
			})
			if ranEarly {
				t.Error("the worker ran and launched its grandchild before it joined the job")
			}
			if exitEarly {
				if err := cmd.Wait(); err != nil {
					t.Fatalf("child: %v", err)
				}
				if processEnded(grandchild, 200*time.Millisecond) {
					t.Fatal("grandchild ended with its parent; the test proves nothing")
				}
				owner.release()
				if !processEnded(grandchild, 5*time.Second) {
					t.Fatal("release left a grandchild launched before ownership running")
				}
				return
			}
			owner.stop()
			_ = cmd.Wait()
			// Checked before release, which would end the job's members too.
			ended := processEnded(grandchild, 5*time.Second)
			owner.release()
			if !ended {
				t.Fatal("stop left a grandchild launched before ownership running")
			}
		})
	}
}

// A worker that cannot be owned is ended before it runs: it never reaches its
// first action, so it leaves no descendant behind, and start reports the
// ownership failure Connect turns into its error.
func TestWorkerThatCannotBeOwnedNeverRuns(t *testing.T) {
	cmd, pidFile := ownedChildCommand(t, false)
	owner, err := newProcessOwner()
	if err != nil {
		t.Fatalf("newProcessOwner: %v", err)
	}
	defer owner.release()
	injected := errors.New("injected assignment failure")
	owner.assign = func(windows.Handle, windows.Handle) error { return injected }
	// Give a worker that was wrongly left running time to act before the
	// failure is reported.
	owner.beforeAssign = func() { time.Sleep(ownershipBarrier) }
	err = owner.start(cmd)
	if !errors.Is(err, errProcessOwnership) || !errors.Is(err, injected) {
		t.Fatalf("start error=%v; want the ownership failure", err)
	}
	if cmd.ProcessState == nil {
		t.Fatal("the unowned worker was not reaped")
	}
	time.Sleep(500 * time.Millisecond)
	if raw, err := os.ReadFile(pidFile); err == nil {
		if pid, err := strconv.Atoi(strings.TrimSpace(string(raw))); err == nil {
			if p, err := os.FindProcess(pid); err == nil { // never leave it behind
				_ = p.Kill()
			}
		}
		t.Fatalf("the unowned worker ran and launched grandchild %s", raw)
	}
}
