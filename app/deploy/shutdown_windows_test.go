//go:build windows

package deploy

import (
	"os"
	"os/exec"
	"syscall"

	"golang.org/x/sys/windows"
)

// shutdownSignal is how a Windows console host asks a deployment to stop:
// Ctrl+C or Ctrl+Break, both delivered to Go as os.Interrupt. Windows has no
// SIGTERM to send another process.
var shutdownSignal os.Signal = os.Interrupt

// prepareShutdownChild gives the child its own console process group, so a
// Ctrl+Break aimed at it does not also reach the test process.
func prepareShutdownChild(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: windows.CREATE_NEW_PROCESS_GROUP}
}

func requestShutdown(cmd *exec.Cmd) error {
	return windows.GenerateConsoleCtrlEvent(windows.CTRL_BREAK_EVENT, uint32(cmd.Process.Pid))
}
