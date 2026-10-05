//go:build windows

package packagemanager

import (
	"errors"
	"math/rand/v2"
	"time"

	"golang.org/x/sys/windows"
)

// replaceRetryBudget bounds how long replaceFile retries a rename that
// Windows refuses only because another handle briefly holds the target.
const replaceRetryBudget = 2 * time.Second

// replaceFile atomically installs src at dst. Windows refuses to sync a
// directory handle, so the rename asks for MOVEFILE_WRITE_THROUGH instead.
// Microsoft documents the flush for moves done as copy-and-delete; a
// same-volume rename relies on NTFS metadata journaling. It is the strongest
// durability available to a standard user.
//
// Windows also refuses a rename onto a file another process or goroutine has
// open or is replacing at that moment (ERROR_ACCESS_DENIED or
// ERROR_SHARING_VIOLATION), where POSIX would just swap the name. Those
// errors are transient, so the rename is retried with jittered backoff for up
// to replaceRetryBudget, the approach the Go toolchain itself takes
// (cmd/go/internal/robustio).
func replaceFile(src, dst string) error {
	from, err := windows.UTF16PtrFromString(src)
	if err != nil {
		return err
	}
	to, err := windows.UTF16PtrFromString(dst)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(replaceRetryBudget)
	delay := time.Millisecond
	for {
		err := windows.MoveFileEx(from, to, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH)
		if err == nil || !transientReplaceError(err) || time.Now().After(deadline) {
			return err
		}
		time.Sleep(delay + rand.N(delay))
		delay = min(2*delay, 100*time.Millisecond)
	}
}

func transientReplaceError(err error) bool {
	return errors.Is(err, windows.ERROR_ACCESS_DENIED) || errors.Is(err, windows.ERROR_SHARING_VIOLATION)
}
