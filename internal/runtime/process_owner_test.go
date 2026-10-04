package runtime

import (
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestOwnedProcessHelper is re-executed in the roles below.
func TestOwnedProcessHelper(t *testing.T) {
	switch os.Getenv("BLOK_OWNED_ROLE") {
	case "grandchild":
		time.Sleep(time.Minute)
		os.Exit(0)
	case "child":
		// The grandchild inherits the child's standard streams, the way a
		// .cmd wrapper's node.exe does.
		grandchild := exec.Command(os.Args[0], "-test.run=^TestOwnedProcessHelper$")
		grandchild.Env = append(os.Environ(), "BLOK_OWNED_ROLE=grandchild")
		grandchild.Stdout, grandchild.Stderr = os.Stdout, os.Stderr
		if err := grandchild.Start(); err != nil {
			os.Exit(3)
		}
		if err := os.WriteFile(os.Getenv("BLOK_OWNED_PID_FILE"), []byte(strconv.Itoa(grandchild.Process.Pid)), 0o600); err != nil {
			os.Exit(4)
		}
		if os.Getenv("BLOK_OWNED_EXIT") == "1" {
			os.Exit(0)
		}
		time.Sleep(time.Minute)
		os.Exit(0)
	case "interrupt-probe":
		interrupts := make(chan os.Signal, 1)
		signal.Notify(interrupts, os.Interrupt)
		if err := os.WriteFile(os.Getenv("BLOK_OWNED_READY_FILE"), []byte("ready"), 0o600); err != nil {
			os.Exit(4)
		}
		select {
		case <-interrupts:
			_ = os.WriteFile(os.Getenv("BLOK_OWNED_MARKER"), []byte("interrupted"), 0o600)
			os.Exit(0)
		case <-time.After(time.Minute):
			os.Exit(0)
		}
	case "console-host":
		runConsoleHost()
	}
}

// startOwnedChild starts a worker child through the same command builder
// Connect uses, adopts it, and returns the PID of the grandchild it launched.
func startOwnedChild(t *testing.T, exitEarly bool) (*exec.Cmd, *processOwner, int) {
	t.Helper()
	pidFile := filepath.Join(t.TempDir(), "grandchild.pid")
	exit := "0"
	if exitEarly {
		exit = "1"
	}
	owner, err := newProcessOwner()
	if err != nil {
		t.Fatalf("newProcessOwner: %v", err)
	}
	cmd := workerCommand(os.Args[0], []string{"-test.run=^TestOwnedProcessHelper$"}, "", []string{"BLOK_OWNED_ROLE=child", "BLOK_OWNED_PID_FILE=" + pidFile, "BLOK_OWNED_EXIT=" + exit})
	if err := cmd.Start(); err != nil {
		owner.release()
		t.Fatal(err)
	}
	if err := owner.adopt(cmd); err != nil {
		_ = cmd.Process.Kill()
		owner.release()
		t.Fatalf("adopt: %v", err)
	}
	pid := waitForPID(t, pidFile)
	t.Cleanup(func() { // never leave a stray process behind, even on failure
		if p, err := os.FindProcess(pid); err == nil {
			_ = p.Kill()
		}
		owner.kill()
		owner.release()
	})
	return cmd, owner, pid
}

func waitForPID(t *testing.T, file string) int {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		if raw, err := os.ReadFile(file); err == nil && len(raw) > 0 {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(raw))); err == nil {
				return pid
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no PID written to %s", file)
		}
	}
}

// A worker whose child outlives it must still be seen to exit. Pipes for the
// worker's standard streams would make Wait block until the grandchild
// holding them exited, and the supervisor would wait on a dead worker.
func TestWorkerWaitReturnsDespiteSurvivingGrandchild(t *testing.T) {
	cmd, _, _ := startOwnedChild(t, true)
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	select {
	case err := <-waited:
		if err != nil {
			t.Fatalf("worker: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait blocked on the worker's surviving grandchild")
	}
}
