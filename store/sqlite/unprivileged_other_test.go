//go:build !unix

package sqlite

import "testing"

const unprivilegedChildEnv = "NEWBLOK_SQLITE_UNPRIVILEGED_CHILD"

func rerunUnprivileged(t *testing.T, test string) {
	t.Helper()
	t.Skipf("%s needs POSIX credentials to run without root", test)
}
