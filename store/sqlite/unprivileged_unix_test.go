//go:build unix

package sqlite

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// unprivilegedChildEnv marks a test binary rerun by rerunUnprivileged.
const unprivilegedChildEnv = "NEWBLOK_SQLITE_UNPRIVILEGED_CHILD"

// unprivilegedID is the user and group a test run as root drops to: nobody
// and nogroup on Linux.
const unprivilegedID = 65534

// rerunUnprivileged runs the named top-level test again, as uid and gid
// 65534, in a copy of the test binary, and fails unless it passes there.
// Root ignores file permissions, so a test about an unwritable path proves
// nothing as root. The copy, its working directory and TMPDIR live in a
// fresh directory that user can read and write, since go test's build
// directory is root's alone. If the system refuses to drop to that user, the
// test is skipped with the reason.
func rerunUnprivileged(t *testing.T, test string) {
	t.Helper()
	directory, err := os.MkdirTemp("", "newblok-unprivileged-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	if err := os.Chmod(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(directory, "sqlite.test")
	if err := copyExecutable(os.Args[0], binary); err != nil {
		t.Fatal(err)
	}
	work := filepath.Join(directory, "work")
	if err := os.Mkdir(work, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(work, unprivilegedID, unprivilegedID); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "-test.run=^"+test+"$", "-test.count=1", "-test.v")
	command.Dir = work
	command.Env = append(os.Environ(), unprivilegedChildEnv+"=1", "TMPDIR="+work, "HOME="+work)
	command.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: unprivilegedID, Gid: unprivilegedID}}
	output, err := command.CombinedOutput()
	if err != nil && errors.Is(err, syscall.EPERM) && len(output) == 0 {
		t.Skipf("running as root, and the system refuses to drop to uid %d: %v", unprivilegedID, err)
	}
	t.Logf("as uid %d:\n%s", unprivilegedID, output)
	if err != nil {
		t.Fatalf("%s failed as uid %d: %v", test, unprivilegedID, err)
	}
	if !strings.Contains(string(output), "--- PASS: "+test+" ") || strings.Contains(string(output), "--- SKIP") {
		t.Fatalf("%s did not run to a pass as uid %d", test, unprivilegedID)
	}
}

func copyExecutable(from, to string) error {
	source, err := os.Open(from)
	if err != nil {
		return err
	}
	defer source.Close()
	target, err := os.OpenFile(to, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(target, source); err != nil {
		_ = target.Close()
		return err
	}
	return target.Close()
}
