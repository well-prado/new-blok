//go:build windows

package cron

import (
	"syscall"
	"time"
)

// ProcessCPUTime reports the CPU time this test process has used, user and
// kernel. Bounds on CPU-bound work use it rather than wall time, which grows
// with host contention, not with the work (#214).
func ProcessCPUTime() time.Duration {
	var creation, exit, kernel, user syscall.Filetime
	if err := syscall.GetProcessTimes(syscall.Handle(^uintptr(0)), &creation, &exit, &kernel, &user); err != nil {
		panic(err)
	}
	ticks := func(f syscall.Filetime) int64 { return int64(f.HighDateTime)<<32 | int64(f.LowDateTime) }
	return time.Duration((ticks(kernel) + ticks(user)) * 100)
}
