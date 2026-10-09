//go:build !windows

package packagemanager

import "os"

// mkdirPrivate creates dir and any missing parents as owner-only (0700).
// Existing directories are left as they are.
func mkdirPrivate(dir string) error { return os.MkdirAll(dir, 0o700) }
