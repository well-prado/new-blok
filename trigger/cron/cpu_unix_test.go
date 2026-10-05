//go:build !windows

package cron

import (
	"syscall"
	"time"
)

// ProcessCPUTime reports the CPU time this test process has used, user and
// system. Bounds on CPU-bound work use it rather than wall time, which grows
// with host contention, not with the work (#214).
func ProcessCPUTime() time.Duration {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		panic(err)
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}
