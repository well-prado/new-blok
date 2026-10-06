//go:build linux || darwin

package devtool

import (
	"io/fs"
	"syscall"
)

// fileIdentity is the inode and the status-change time (ctime) of the file
// info describes. The kernel sets ctime on every write, chmod, rename and
// utimes and no system call sets it back, so an edit that restores a file's
// size and modification time still changes it; a replacement renamed over
// the file has a new inode.
func fileIdentity(info fs.FileInfo) (inode uint64, change int64) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0
	}
	return uint64(stat.Ino), statChange(stat)
}
