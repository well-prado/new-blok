// Package repohygiene holds repository-wide checks that keep accidental build
// outputs out of git. It has no production code; the checks run under
// `go test ./...`.
package repohygiene

import (
	"bytes"
	"errors"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"testing"
)

// maxTrackedBytes is the largest size a tracked file may have. The biggest
// legitimate file today is well under 1 MiB; a compiled Go binary is ~16 MiB.
const maxTrackedBytes = 2 << 20

// allowedLarge lists tracked files (slash-separated, repo-relative) that may
// exceed maxTrackedBytes, with the reason. It is empty on purpose: add an
// entry only for a fixture that genuinely needs the room.
var allowedLarge = map[string]string{}

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("repository root not found at %s: %v", root, err)
	}
	return root
}

// git runs git in root. It skips the test when git or the repository metadata
// is unavailable (for example a source tarball); in CI both are present.
func git(t *testing.T, root string, args ...string) (string, error) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	if _, err := os.Stat(filepath.Join(root, ".git")); err != nil {
		t.Skip("not a git checkout")
	}
	cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
	err := cmd.Run()
	if err != nil && stderr.Len() > 0 {
		err = errors.Join(err, errors.New(strings.TrimSpace(stderr.String())))
	}
	return out.String(), err
}

func TestNoLargeTrackedFiles(t *testing.T) {
	root := repoRoot(t)
	out, err := git(t, root, "ls-files", "-z")
	if err != nil {
		t.Fatal(err)
	}
	for rel := range strings.SplitSeq(out, "\x00") {
		if rel == "" {
			continue
		}
		info, err := os.Lstat(filepath.Join(root, rel))
		if errors.Is(err, fs.ErrNotExist) {
			continue // tracked but deleted in the working tree
		}
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() <= maxTrackedBytes {
			continue
		}
		if _, ok := allowedLarge[rel]; ok {
			continue
		}
		t.Errorf("%s is %d bytes, over the %d byte limit for tracked files; "+
			"is it a build output? Ignore it, or add it to allowedLarge with a reason",
			rel, info.Size(), maxTrackedBytes)
	}
}

// TestIgnoreRulesDoNotHideTrackedFiles guards the other direction: an ignore
// entry that is too broad would swallow a source file.
func TestIgnoreRulesDoNotHideTrackedFiles(t *testing.T) {
	root := repoRoot(t)
	out, err := git(t, root, "ls-files", "-ci", "--exclude-standard")
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(out) != "" {
		t.Errorf("tracked files match an ignore rule:\n%s", out)
	}
}

// TestMainPackageBinariesAreIgnored finds every directory holding a buildable
// `package main` and checks that the binary a bare `go build` would leave
// there is git-ignored, so a new example cannot reintroduce the trap.
func TestMainPackageBinariesAreIgnored(t *testing.T) {
	root := repoRoot(t)
	if _, err := git(t, root, "rev-parse", "--git-dir"); err != nil {
		t.Fatal(err)
	}
	found := 0
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		if name := d.Name(); name == ".git" || name == "node_modules" {
			return filepath.SkipDir
		}
		if !hasMainPackage(t, p) {
			return nil
		}
		found++
		bin := binaryName(t, p)
		rel, err := filepath.Rel(root, filepath.Join(p, bin))
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if _, err := git(t, root, "check-ignore", "-q", "--", rel); err != nil {
			t.Errorf("`go build` in %s leaves %s, which git does not ignore; add /%s to .gitignore",
				path.Dir(rel), rel, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if found == 0 {
		t.Fatal("found no main packages; the walk is broken")
	}
}

// hasMainPackage reports whether dir has a non-test Go file in package main
// that a plain build would include (files marked `//go:build ignore` are
// generators and produce no binary).
func hasMainPackage(t *testing.T, dir string) bool {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, name), nil, parser.PackageClauseOnly|parser.ParseComments)
		if err != nil || f.Name.Name != "main" {
			continue
		}
		ignored := false
		for _, cg := range f.Comments {
			if cg.End() > f.Package {
				break
			}
			for _, c := range cg.List {
				if strings.TrimSpace(c.Text) == "//go:build ignore" {
					ignored = true
				}
			}
		}
		if !ignored {
			return true
		}
	}
	return false
}

// binaryName is what `go build` names the output: the last element of the
// module path when dir is a module root, otherwise the directory name.
func binaryName(t *testing.T, dir string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "go.mod"))
	if errors.Is(err, fs.ErrNotExist) {
		return filepath.Base(dir)
	}
	if err != nil {
		t.Fatal(err)
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		if mod, ok := strings.CutPrefix(strings.TrimSpace(line), "module "); ok {
			return path.Base(strings.Trim(strings.TrimSpace(mod), `"`))
		}
	}
	t.Fatalf("no module line in %s/go.mod", dir)
	return ""
}
