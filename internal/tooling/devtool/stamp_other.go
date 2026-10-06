//go:build !linux && !darwin

package devtool

import "io/fs"

// fileIdentity is unavailable here: Windows (unverified, the Windows track
// #297) and the Unix systems blok dev is not verified on keep the size,
// modification time and mode stamp, which misses an edit that keeps the
// size and restores the modification time.
func fileIdentity(fs.FileInfo) (inode uint64, change int64) { return 0, 0 }
