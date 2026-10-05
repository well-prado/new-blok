//go:build !windows

package packagemanager

import (
	"os"
	"path/filepath"
)

// replaceFile atomically installs src at dst and makes the rename durable by
// syncing the parent directory.
func replaceFile(src, dst string) error {
	if err := os.Rename(src, dst); err != nil {
		return err
	}
	parent, err := os.Open(filepath.Dir(dst))
	if err != nil {
		return nil
	}
	defer parent.Close()
	return parent.Sync()
}
