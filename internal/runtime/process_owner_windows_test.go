//go:build windows

package runtime

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

// TestOwnedProcessHelper is re-executed as a worker that launches a
// grandchild, the way a .cmd wrapper launches node.exe.
func TestOwnedProcessHelper(t *testing.T) {
	switch os.Getenv("BLOK_OWNED_ROLE") {
	case "grandchild":
		time.Sleep(time.Minute)
		os.Exit(0)
	case "child":
		grandchild := exec.Command(os.Args[0], "-test.run=^TestOwnedProcessHelper$")
		grandchild.Env = append(os.Environ(), "BLOK_OWNED_ROLE=grandchild")
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
	}
}

func startOwnedChild(t *testing.T, exitEarly bool) (*exec.Cmd, ownedProcess, uint32) {
	t.Helper()
	pidFile := filepath.Join(t.TempDir(), "grandchild.pid")
	cmd := exec.Command(os.Args[0], "-test.run=^TestOwnedProcessHelper$")
	cmd.Env = append(os.Environ(), "BLOK_OWNED_ROLE=child", "BLOK_OWNED_PID_FILE="+pidFile, "BLOK_OWNED_EXIT="+map[bool]string{true: "1", false: "0"}[exitEarly])
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	owned, err := ownProcess(cmd)
	if err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("ownProcess: %v", err)
	}
	var pid uint64
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		if raw, err := os.ReadFile(pidFile); err == nil && len(raw) > 0 {
			if pid, err = strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 32); err == nil {
				break
			}
		}
		if time.Now().After(deadline) {
			owned.kill()
			owned.release()
			t.Fatal("child never reported its grandchild")
		}
	}
	t.Cleanup(func() { // never leave a stray process behind, even on failure
		if h, err := windows.OpenProcess(windows.PROCESS_TERMINATE, false, uint32(pid)); err == nil {
			_ = windows.TerminateProcess(h, 1)
			_ = windows.CloseHandle(h)
		}
	})
	return cmd, owned, uint32(pid)
}

// processEnded reports whether pid has exited within the wait.
func processEnded(pid uint32, wait time.Duration) bool {
	h, err := windows.OpenProcess(windows.SYNCHRONIZE, false, pid)
	if err != nil {
		return true // no such process
	}
	defer windows.CloseHandle(h)
	event, _ := windows.WaitForSingleObject(h, uint32(wait/time.Millisecond))
	return event == windows.WAIT_OBJECT_0
}

func TestOwnedProcessStopEndsTheWholeTree(t *testing.T) {
	cmd, owned, grandchild := startOwnedChild(t, false)
	if processEnded(grandchild, 0) {
		t.Fatal("grandchild was not running before stop")
	}
	owned.stop()
	_ = cmd.Wait()
	// Checked before release: closing the job would end the grandchild too
	// and hide a stop that reached only the direct child.
	ended := processEnded(grandchild, 5*time.Second)
	owned.release()
	if !ended {
		t.Fatal("stop left the worker's grandchild running")
	}
}

func TestOwnedProcessReleaseReapsDescendantsOfAnExitedChild(t *testing.T) {
	cmd, owned, grandchild := startOwnedChild(t, true)
	if err := cmd.Wait(); err != nil {
		t.Fatalf("child: %v", err)
	}
	if processEnded(grandchild, 200*time.Millisecond) {
		t.Fatal("grandchild ended with its parent; the test proves nothing")
	}
	owned.release()
	if !processEnded(grandchild, 5*time.Second) {
		t.Fatal("release left an orphaned grandchild running")
	}
}
