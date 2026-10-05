//go:build !(darwin || linux || freebsd || netbsd || openbsd || dragonfly)

package clustertest

import "os"

// lockSupported is false here: the cluster tests do not run on this platform
// (the voters are paused with docker from a Unix host), so the lock is a
// process-local no-op that only keeps the package compiling and vetting.
const lockSupported = false

func tryLock(*os.File, Mode) error { return nil }

func unlockFile(*os.File) error { return nil }

func processAlive(int) bool { return true }
