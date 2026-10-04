package packagemanager

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	contractpackage "github.com/well-prado/new-blok/contract/package"
)

func TestCaptureNativeGoAndNPMLocksWithTheirManagers(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("Go native manager is unavailable")
	}
	if _, err := exec.LookPath("npm"); err != nil {
		t.Skip("npm native manager is unavailable")
	}
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("test source path unavailable")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	goModBefore, err := digestFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	goSumBefore, _ := digestFile(filepath.Join(root, "go.sum"))
	goLocks, err := CaptureNativeLocks(context.Background(), root)
	if err != nil {
		t.Fatalf("Go native lock capture: %v", err)
	}
	var goModules int
	for _, lock := range goLocks {
		if lock.Manager != "go" {
			t.Fatalf("unexpected manager in Go project: %s", lock.Manager)
		}
		if lock.Path == "go.mod" {
			if lock.ToolVersion == "" || lock.ContextDigest == "" || len(lock.Packages) == 0 {
				t.Fatalf("Go module graph/context was not captured: %+v", lock)
			}
			goModules = len(lock.Packages)
		}
	}
	if goModules == 0 {
		t.Fatal("go.mod lock did not capture exact native module graph")
	}
	goModAfter, err := digestFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	goSumAfter, _ := digestFile(filepath.Join(root, "go.sum"))
	if goModBefore != goModAfter || goSumBefore != goSumAfter {
		t.Fatal("Go graph inspection modified native lock files")
	}
	npmRoot := filepath.Join(root, "runtime", "nodejs")
	npmManifestBefore, err := digestFile(filepath.Join(npmRoot, "package.json"))
	if err != nil {
		t.Fatal(err)
	}
	npmLockBefore, err := digestFile(filepath.Join(npmRoot, "package-lock.json"))
	if err != nil {
		t.Fatal(err)
	}
	npmLocks, err := CaptureNativeLocks(context.Background(), npmRoot)
	if err != nil {
		t.Fatalf("npm native lock capture: %v", err)
	}
	var npmPackages int
	for _, lock := range npmLocks {
		if lock.Manager != "npm" {
			t.Fatalf("unexpected manager in npm project: %s", lock.Manager)
		}
		if lock.Path == "package-lock.json" {
			if lock.ToolVersion == "" || lock.RuntimeVersion == "" || lock.ContextDigest == "" || len(lock.Packages) == 0 {
				t.Fatalf("npm lock graph/context was not captured: %+v", lock)
			}
			npmPackages = len(lock.Packages)
		}
	}
	if npmPackages == 0 {
		t.Fatal("package-lock.json did not capture exact npm package graph")
	}
	npmManifestAfter, err := digestFile(filepath.Join(npmRoot, "package.json"))
	if err != nil {
		t.Fatal(err)
	}
	npmLockAfter, err := digestFile(filepath.Join(npmRoot, "package-lock.json"))
	if err != nil {
		t.Fatal(err)
	}
	if npmManifestBefore != npmManifestAfter || npmLockBefore != npmLockAfter {
		t.Fatal("npm graph inspection modified native lock files")
	}
}

func TestGoLocalReplaceSourceIsPinnedAndExternalSourceRejected(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("Go native manager is unavailable")
	}
	root := filepath.Join(t.TempDir(), "project")
	dependency := filepath.Join(root, "dep")
	writeFixtureFile(t, filepath.Join(root, "go.mod"), "module example.test/app\n\ngo 1.27.1\n\nrequire example.test/dep v1.0.0\nreplace example.test/dep => ./dep\n")
	writeFixtureFile(t, filepath.Join(dependency, "go.mod"), "module example.test/dep\n\ngo 1.27.1\n")
	writeFixtureFile(t, filepath.Join(dependency, "dep.go"), "package dep\nconst Value = 1\n")

	captured, err := CaptureNativeLocks(context.Background(), root)
	if err != nil {
		t.Fatalf("capture local Go replace: %v", err)
	}
	local := findGoModule(t, captured, "example.test/dep")
	first := local.Artifact
	if local.Version != "local" || local.Source != "dep" || !isFileDigest(local.Artifact) {
		t.Fatalf("local replacement was not exact and project-relative: %+v", local)
	}
	writeFixtureFile(t, filepath.Join(dependency, "dep.go"), "package dep\nconst Value = 2\n")
	captured, err = CaptureNativeLocks(context.Background(), root)
	if err != nil {
		t.Fatalf("recapture changed local Go replace: %v", err)
	}
	if changed := findGoModule(t, captured, "example.test/dep").Artifact; changed == first {
		t.Fatal("local replacement source change did not change the native lock")
	}

	external := filepath.Join(t.TempDir(), "external")
	writeFixtureFile(t, filepath.Join(external, "go.mod"), "module example.test/dep\n\ngo 1.27.1\n")
	writeFixtureFile(t, filepath.Join(external, "dep.go"), "package dep\n")
	writeFixtureFile(t, filepath.Join(root, "go.mod"), "module example.test/app\n\ngo 1.27.1\n\nrequire example.test/dep v1.0.0\nreplace example.test/dep => "+filepath.ToSlash(external)+"\n")
	if _, err := CaptureNativeLocks(context.Background(), root); err == nil || !strings.Contains(err.Error(), "outside project root is unsupported") {
		t.Fatalf("external local replacement did not fail clearly: %v", err)
	}
}

func writeFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}

func findGoModule(t *testing.T, locks []contractpackage.NativeLock, name string) contractpackage.NativeLockedPackage {
	t.Helper()
	for _, lock := range locks {
		if lock.Manager == "go" && lock.Path == "go.mod" {
			for _, module := range lock.Packages {
				if module.Name == name {
					return module
				}
			}
		}
	}
	t.Fatalf("captured Go graph omitted module %s", name)
	return contractpackage.NativeLockedPackage{}
}
