//go:build !windows

package devtool

import (
	"os"
	"os/exec"
	"strconv"
	"syscall"
)

// isolate starts the go command in its own process group, so an interrupt
// reaches it and every test binary it started, and nothing else.
func isolate(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func interruptGroup(process *os.Process) error {
	return syscall.Kill(-process.Pid, syscall.SIGINT)
}

func killGroup(process *os.Process) error {
	return syscall.Kill(-process.Pid, syscall.SIGKILL)
}

// guardScript waits for its standard input to close and then kills the
// process group named by $1. Only blok holds the pipe's write end, and the
// kernel closes it whenever blok exits, however it exits — SIGKILL
// included — so the go command and its test binaries cannot outlive blok.
// It writes "kill -KILL -PGID" without "--": dash (Debian's /bin/sh)
// rejects "--" there as an illegal number, bash (macOS's) accepts both.
const guardScript = `read line; kill -KILL "-$1" 2>/dev/null`

// guardShell runs the guard; a variable only so a test can make it absent.
var guardShell = "/bin/sh"

// processGuard is the /bin/sh process watching blok on the go command's
// behalf. It runs in its own process group, so neither a terminal's Ctrl+C
// nor the interrupt sent to the go command's group reaches it.
type processGuard struct {
	command *exec.Cmd
	pipe    *os.File
}

func startGuard(target *os.Process) (*processGuard, error) {
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	command := exec.Command(guardShell, "-c", guardScript, "blok-guard", strconv.Itoa(target.Pid))
	command.Stdin = reader
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	err = command.Start()
	_ = reader.Close()
	if err != nil {
		_ = writer.Close()
		return nil, err
	}
	return &processGuard{command: command, pipe: writer}, nil
}

// release retires the guard once the go command has exited. The guard is
// killed before the pipe closes, so it never signals a process group whose
// id the system may since have reused.
func (g *processGuard) release() {
	_ = g.command.Process.Kill()
	_ = g.pipe.Close()
	_ = g.command.Wait()
}
