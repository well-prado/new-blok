package devtool

import "syscall"

func statChange(stat *syscall.Stat_t) int64 { return stat.Ctim.Nano() }
