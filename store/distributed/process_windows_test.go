//go:build windows

package distributed

import (
	"fmt"
	"os"

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
	var processErr error
	if err := process.WithHandle(func(handle uintptr) {
		status, _, callErr := procedure.Call(handle)
		if status != 0 {
			processErr = fmt.Errorf("%s returned NTSTATUS %#x: %w", name, status, callErr)
		}
	}); err != nil {
		return fmt.Errorf("access process handle for %s: %w", name, err)
	}
	return processErr
}
