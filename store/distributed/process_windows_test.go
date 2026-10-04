//go:build windows

package distributed

import (
	"fmt"
	"os"
	"runtime"

	"golang.org/x/sys/windows"
)

var ntdll = windows.NewLazySystemDLL("ntdll.dll")

func suspendProcess(process *os.Process) error { return processControl(process, "NtSuspendProcess") }

func resumeProcess(process *os.Process) error { return processControl(process, "NtResumeProcess") }

func processControl(process *os.Process, name string) error {
	procedure := ntdll.NewProc(name)
	if err := procedure.Find(); err != nil {
		return err
	}
	status, _, callErr := procedure.Call(process.Handle())
	runtime.KeepAlive(process)
	if status != 0 {
		return fmt.Errorf("%s returned NTSTATUS %#x: %w", name, status, callErr)
	}
	return nil
}
