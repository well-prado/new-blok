//go:build darwin || linux || freebsd || netbsd || openbsd || dragonfly

package clustertest

import (
	"errors"
	"os"
	"syscall"
)

// lockSupported reports whether the lock is enforced across processes.
const lockSupported = true

func tryLock(file *os.File, mode Mode) error {
	how := syscall.LOCK_SH
	if mode == Exclusive {
		how = syscall.LOCK_EX
	}
	for {
		err := syscall.Flock(int(file.Fd()), how|syscall.LOCK_NB)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, syscall.EINTR):
			continue
		case errors.Is(err, syscall.EWOULDBLOCK):
			return errWouldBlock
		default:
			return &os.PathError{Op: "flock", Path: file.Name(), Err: err}
		}
	}
}

func unlockFile(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
}

func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
