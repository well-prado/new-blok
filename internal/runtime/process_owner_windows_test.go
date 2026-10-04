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
	"unsafe"

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

var (
	kernel32           = windows.NewLazySystemDLL("kernel32.dll")
	procIsProcessInJob = kernel32.NewProc("IsProcessInJob")
	procSuspendThread  = kernel32.NewProc("SuspendThread")
)

// processSuspended reports whether every thread of pid is suspended. Each
// thread is suspended once more to read its previous count, then resumed.
func processSuspended(pid uint32) (bool, error) {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPTHREAD, 0)
	if err != nil {
		return false, err
	}
	defer windows.CloseHandle(snapshot)
	threads, suspended := 0, true
	entry := windows.ThreadEntry32{Size: uint32(unsafe.Sizeof(windows.ThreadEntry32{}))}
	for err = windows.Thread32First(snapshot, &entry); err == nil; err = windows.Thread32Next(snapshot, &entry) {
		if entry.OwnerProcessID != pid {
			continue
		}
		thread, err := windows.OpenThread(windows.THREAD_SUSPEND_RESUME, false, entry.ThreadID)
		if err != nil {
			return false, err
		}
		previous, _, callErr := procSuspendThread.Call(uintptr(thread))
		if uint32(previous) == 0xFFFFFFFF {
			windows.CloseHandle(thread)
			return false, callErr
		}
		_, _ = windows.ResumeThread(thread)
		windows.CloseHandle(thread)
		threads++
		suspended = suspended && previous > 0
	}
	if !errors.Is(err, windows.ERROR_NO_MORE_FILES) {
		return false, err
	}
	if threads == 0 {
		return false, errors.New("process has no threads")
	}
	return suspended, nil
}

// openGrandchild holds the grandchild open, so its PID cannot be reused by
// another process while the test inspects or terminates it.
func openGrandchild(t *testing.T, pid int) windows.Handle {
	t.Helper()
	h, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION|windows.SYNCHRONIZE|windows.PROCESS_TERMINATE, false, uint32(pid))
	if err != nil {
		t.Fatalf("open grandchild %d: %v", pid, err)
	}
	t.Cleanup(func() { // never leave it behind, and never kill a reused PID
		_ = windows.TerminateProcess(h, 1)
		_ = windows.CloseHandle(h)
	})
	return h
}

func inJob(t *testing.T, process, job windows.Handle) bool {
	t.Helper()
	var member int32
	if ok, _, err := procIsProcessInJob.Call(uintptr(process), uintptr(job), uintptr(unsafe.Pointer(&member))); ok == 0 {
		t.Fatalf("IsProcessInJob: %v", err)
	}
	return member != 0
}

func handleEnded(h windows.Handle, wait time.Duration) bool {
	event, _ := windows.WaitForSingleObject(h, uint32(wait/time.Millisecond))
	return event == windows.WAIT_OBJECT_0
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
// between process creation and job assignment (#224). There the worker must
// be suspended, which is checked directly, and must not reach its first
// action, launching a grandchild, even given ownershipBarrier to do so. Once
// started, that grandchild must be in the job, and stop or release must end
// it. A worker started running and assigned afterwards fails these checks.
func TestWorkerRunsNothingBeforeItIsOwned(t *testing.T) {
	for _, exitEarly := range []bool{false, true} {
		name := map[bool]string{false: "stop", true: "release-after-exit"}[exitEarly]
		t.Run(name, func(t *testing.T) {
			cmd, pidFile := ownedChildCommand(t, exitEarly)
			owner, err := newProcessOwner()
			if err != nil {
				t.Fatalf("newProcessOwner: %v", err)
			}
			var suspended, ranEarly bool
			var suspendedErr error
			owner.beforeAssign = func() {
				suspended, suspendedErr = processSuspended(uint32(cmd.Process.Pid))
				for deadline := time.Now().Add(ownershipBarrier); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
					if raw, err := os.ReadFile(pidFile); err == nil && len(raw) > 0 {
						ranEarly = true
						return
					}
				}
			}
			if err := owner.start(cmd); err != nil {
				owner.release()
				// A worker that ran early may have left a grandchild.
				if raw, readErr := os.ReadFile(pidFile); readErr == nil {
					if pid, convErr := strconv.Atoi(strings.TrimSpace(string(raw))); convErr == nil {
						openGrandchild(t, pid)
					}
				}
				t.Fatalf("start: %v", err)
			}
			t.Cleanup(func() {
				owner.kill()
				owner.release()
			})
			grandchild := openGrandchild(t, waitForPID(t, pidFile))
			if suspendedErr != nil || !suspended {
				t.Errorf("the worker was not suspended before it joined the job (err=%v)", suspendedErr)
			}
			if ranEarly {
				t.Error("the worker ran and launched its grandchild before it joined the job")
			}
			if !inJob(t, grandchild, owner.job) {
				t.Error("the worker's grandchild is not in the job")
			}
			if exitEarly {
				if err := cmd.Wait(); err != nil {
					t.Fatalf("child: %v", err)
				}
				if handleEnded(grandchild, 200*time.Millisecond) {
					t.Fatal("grandchild ended with its parent; the test proves nothing")
				}
				owner.release()
				if !handleEnded(grandchild, 5*time.Second) {
					t.Fatal("release left the worker's grandchild running")
				}
				return
			}
			owner.stop()
			_ = cmd.Wait()
			// Checked before release, which would end the job's members too.
			ended := handleEnded(grandchild, 5*time.Second)
			owner.release()
			if !ended {
				t.Fatal("stop left the worker's grandchild running")
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
			openGrandchild(t, pid) // terminated at cleanup
		}
		t.Fatalf("the unowned worker ran and launched grandchild %s", raw)
	}
}
