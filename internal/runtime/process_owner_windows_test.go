//go:build windows

package runtime

import (
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
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
	if cmd.Start() != nil || owner.adopt(cmd) != nil {
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
