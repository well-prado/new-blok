package packagemanager

import (
	"context"
	"crypto/sha512"
	"encoding/base64"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	contractpackage "github.com/well-prado/new-blok/contract/package"
)

func TestCaptureNativeNPMRepeatedCopiesAreDeterministic(t *testing.T) {
	if _, err := exec.LookPath("npm"); err != nil {
		t.Skip("npm native manager is unavailable")
	}
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("Node.js runtime is unavailable")
	}
	root := t.TempDir()
	manifest := `{"name":"synthetic-repeat","version":"1.0.0","dependencies":{"parent-a":"1.0.0","parent-b":"1.0.0"}}`
	lock := `{"name":"synthetic-repeat","version":"1.0.0","lockfileVersion":3,"packages":{"":{"name":"synthetic-repeat","version":"1.0.0","dependencies":{"parent-a":"1.0.0","parent-b":"1.0.0"}},"node_modules/parent-a":{"version":"1.0.0","integrity":"INTEGRITY_A","dependencies":{"shared":"1.0.0"}},"node_modules/parent-b":{"version":"1.0.0","integrity":"INTEGRITY_B","dependencies":{"shared":"1.0.0"}},"node_modules/parent-a/node_modules/shared":{"version":"1.0.0","integrity":"INTEGRITY_C"},"node_modules/parent-b/node_modules/shared":{"version":"1.0.0","integrity":"INTEGRITY_D"}}}`
	for marker, payload := range map[string]string{"INTEGRITY_A": "parent-a", "INTEGRITY_B": "parent-b", "INTEGRITY_C": "copy-a", "INTEGRITY_D": "copy-b"} {
		digest := sha512.Sum512([]byte(payload))
		lock = strings.ReplaceAll(lock, marker, "sha512-"+base64.StdEncoding.EncodeToString(digest[:]))
	}
	writeFixtureFile(t, filepath.Join(root, "package.json"), manifest)
	writeFixtureFile(t, filepath.Join(root, "package-lock.json"), lock)

	var first []byte
	for i := 0; i < 30; i++ {
		locks, err := CaptureNativeLocks(context.Background(), root)
		if err != nil {
			t.Fatalf("capture iteration %d: %v", i, err)
		}
		canonical, err := (contractpackage.Lock{FormatVersion: 1, Native: locks}).Canonical()
		if err != nil {
			t.Fatalf("canonicalize iteration %d: %v", i, err)
		}
		if i == 0 {
			first = canonical
		} else if string(canonical) != string(first) {
			t.Fatalf("unchanged npm lock produced a different canonical graph on iteration %d", i)
		}
		if i == 0 {
			var graph []contractpackage.NativeLockedPackage
			for _, native := range locks {
				if native.Manager == "npm" && native.Path == "package-lock.json" {
					graph = native.Packages
				}
			}
			copies := 0
			for _, pkg := range graph {
				if pkg.Name == "shared" {
					copies++
					if pkg.Source == "" {
						t.Fatal("npm lock graph discarded the package-lock entry path")
					}
				}
			}
			if copies != 2 {
				t.Fatalf("expected both nested shared copies in the graph, got %d", copies)
			}
		}
	}
}

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

func TestCaptureNativeLocksRejectsMissingOrUnsupportedNPMLocks(t *testing.T) {
	t.Run("declared dependencies without package lock", func(t *testing.T) {
		root := t.TempDir()
		marker := filepath.Join(root, "lifecycle-ran")
		writeFixtureFile(t, filepath.Join(root, "package.json"), `{"name":"fixture","version":"1.0.0","scripts":{"preinstall":"/usr/bin/touch `+marker+`"},"dependencies":{"example.test/pkg":"1.0.0"}}`)
		if locks, err := CaptureNativeLocks(context.Background(), root); err == nil || !strings.Contains(err.Error(), "package-lock.json is missing") {
			t.Fatalf("declared npm dependency without a lock did not fail closed: locks=%+v err=%v", locks, err)
		}
		if _, err := os.Stat(marker); !os.IsNotExist(err) {
			t.Fatalf("npm lifecycle marker exists after read-only capture: %v", err)
		}
	})
	t.Run("npm shrinkwrap is explicitly unsupported", func(t *testing.T) {
		root := t.TempDir()
		writeFixtureFile(t, filepath.Join(root, "package.json"), `{"name":"fixture","version":"1.0.0","dependencies":{"example.test/pkg":"1.0.0"}}`)
		writeFixtureFile(t, filepath.Join(root, "npm-shrinkwrap.json"), `{}`)
		if _, err := CaptureNativeLocks(context.Background(), root); err == nil || !strings.Contains(err.Error(), "npm-shrinkwrap.json is unsupported") {
			t.Fatalf("npm shrinkwrap input was silently ignored: %v", err)
		}
	})
	t.Run("other package manager is explicitly unsupported", func(t *testing.T) {
		root := t.TempDir()
		writeFixtureFile(t, filepath.Join(root, "package.json"), `{"name":"fixture","version":"1.0.0","packageManager":"pnpm@10.0.0","dependencies":{"example.test/pkg":"1.0.0"}}`)
		writeFixtureFile(t, filepath.Join(root, "pnpm-lock.yaml"), "lockfileVersion: '9.0'\n")
		if _, err := CaptureNativeLocks(context.Background(), root); err == nil || !strings.Contains(err.Error(), "pnpm-lock.yaml is unsupported") {
			t.Fatalf("pnpm lock input was silently ignored: %v", err)
		}
	})
	t.Run("npm workspace source is explicitly unsupported", func(t *testing.T) {
		root := t.TempDir()
		writeFixtureFile(t, filepath.Join(root, "package.json"), `{"name":"fixture","version":"1.0.0","workspaces":["packages/*"]}`)
		if _, err := CaptureNativeLocks(context.Background(), root); err == nil || !strings.Contains(err.Error(), "npm workspaces are unsupported") {
			t.Fatalf("npm workspace source was silently omitted: %v", err)
		}
	})
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
