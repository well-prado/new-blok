//go:build unix

package layout

import (
	"os"

	"golang.org/x/sys/unix"
)

// openFlags opens without blocking, so a FIFO swapped in for a listed
// regular file returns at once instead of waiting for a writer.
const openFlags = os.O_RDONLY | unix.O_NONBLOCK

// linkCount reports the open file's hard-link count.
func linkCount(file *os.File) (uint64, bool) {
	var status unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &status); err != nil {
		return 0, false
	}
	return uint64(status.Nlink), true
}
