//go:build windows

package trigger_test

import (
	"os/exec"
	"syscall"

	"golang.org/x/sys/windows"
)

// prepareShutdownChild gives the child its own console process group, so a
// Ctrl+Break aimed at it does not also reach the test process.
func prepareShutdownChild(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: windows.CREATE_NEW_PROCESS_GROUP}
}

// requestShutdown asks a selected binary to drain the way a Windows console
// host does: Ctrl+Break, which Go delivers as os.Interrupt. Windows has no
// SIGTERM to send another process.
func requestShutdown(cmd *exec.Cmd) error {
	return windows.GenerateConsoleCtrlEvent(windows.CTRL_BREAK_EVENT, uint32(cmd.Process.Pid))
}
