// Package packagemanager coordinates verified Blok package artifacts with native
// language package managers. It never installs or resolves foreign packages.
package packagemanager

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	contractpackage "github.com/well-prado/new-blok/contract/package"
)

const maxNativeLockBytes = 16 << 20

// CaptureNativeLocks asks Go and npm to validate their own lock inputs and
// records their exact graphs and file digests. It does not install packages.
func CaptureNativeLocks(ctx context.Context, projectRoot string) ([]contractpackage.NativeLock, error) {
	root, err := filepath.Abs(projectRoot)
	if err != nil {
		return nil, fmt.Errorf("package: resolve project root: %w", err)
	}
	if resolved, err := filepath.EvalSymlinks(root); err == nil {
		root = resolved
	} else {
		return nil, fmt.Errorf("package: resolve project root symlinks: %w", err)
	}
	var locks []contractpackage.NativeLock
	if fileExists(filepath.Join(root, "go.mod")) {
		captured, err := captureGo(ctx, root)
		if err != nil {
			return nil, err
		}
		locks = append(locks, captured...)
	}
	captureNPMProject, err := inspectNPMProject(root)
	if err != nil {
		return nil, err
	}
	if captureNPMProject {
		captured, err := captureNPM(ctx, root)
		if err != nil {
			return nil, err
		}
		locks = append(locks, captured...)
	}
	sort.Slice(locks, func(i, j int) bool { return locks[i].Manager+"/"+locks[i].Path < locks[j].Manager+"/"+locks[j].Path })
	return locks, nil
}

func inspectNPMProject(root string) (bool, error) {
	manifestPath := filepath.Join(root, "package.json")
	manifestExists, err := regularNativeFile(manifestPath)
	if err != nil {
		return false, err
	}
	managerInputFiles := []string{"npm-shrinkwrap.json", "yarn.lock", "pnpm-lock.yaml", "pnpm-workspace.yaml", "bun.lock", "bun.lockb"}
	var foundManagerLock string
	for _, name := range managerInputFiles {
		exists, err := regularNativeFile(filepath.Join(root, name))
		if err != nil {
			return false, err
		}
		if exists {
			foundManagerLock = name
			break
		}
	}
	packageLockExists, err := regularNativeFile(filepath.Join(root, "package-lock.json"))
	if err != nil {
		return false, err
	}
	if !manifestExists {
		if foundManagerLock != "" {
			return false, fmt.Errorf("package: native lock %s exists without package.json", foundManagerLock)
		}
		if packageLockExists {
			return false, fmt.Errorf("package: package-lock.json exists without package.json")
		}
		return false, nil
	}
	if foundManagerLock == "npm-shrinkwrap.json" {
		return false, fmt.Errorf("package: npm-shrinkwrap.json is unsupported; native capture requires package-lock.json")
	}
	if foundManagerLock != "" {
		return false, fmt.Errorf("package: native manager input %s is unsupported; Go/npm capture supports package-lock.json only", foundManagerLock)
	}
	data, err := readNativeBounded(manifestPath, maxNativeLockBytes)
	if err != nil {
		return false, err
	}
	var manifest struct {
		PackageManager       string                     `json:"packageManager"`
		Dependencies         map[string]json.RawMessage `json:"dependencies"`
		DevDependencies      map[string]json.RawMessage `json:"devDependencies"`
		OptionalDependencies map[string]json.RawMessage `json:"optionalDependencies"`
		PeerDependencies     map[string]json.RawMessage `json:"peerDependencies"`
		BundledDependencies  []string                   `json:"bundledDependencies"`
		BundleDependencies   []string                   `json:"bundleDependencies"`
		Workspaces           json.RawMessage            `json:"workspaces"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return false, fmt.Errorf("package: package.json is malformed; npm lock capture cannot determine declared dependencies")
	}
	if manager := strings.TrimSpace(manifest.PackageManager); manager != "" && manager != "npm" && !strings.HasPrefix(manager, "npm@") {
		name, _, _ := strings.Cut(manager, "@")
		if name == "" {
			name = "unknown"
		}
		return false, fmt.Errorf("package: package.json declares unsupported package manager %q; native capture currently supports npm package-lock.json only", name)
	}
	if len(manifest.Workspaces) > 0 && string(manifest.Workspaces) != "null" {
		return false, fmt.Errorf("package: npm workspaces are unsupported until workspace source trees can be pinned")
	}
	declaresDependencies := len(manifest.Dependencies)+len(manifest.DevDependencies)+len(manifest.OptionalDependencies)+len(manifest.PeerDependencies)+len(manifest.BundledDependencies)+len(manifest.BundleDependencies) > 0
	if !packageLockExists {
		if declaresDependencies {
			return false, fmt.Errorf("package: package.json declares dependencies but package-lock.json is missing; generate and commit an npm lock with scripts disabled")
		}
		return false, nil
	}
	return true, nil
}

func regularNativeFile(path string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("package: inspect native lock input %s", filepath.Base(path))
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return false, fmt.Errorf("package: native lock input %s must be a regular file", filepath.Base(path))
	}
	return true, nil
}

// ResolveProject binds the Blok package graph to the language-native lock
// state from the same project root. Go/npm remain responsible for their own
// dependency graph and artifact verification.
func ResolveProject(ctx context.Context, projectRoot string, roots []contractpackage.Dependency, source contractpackage.Source, policy contractpackage.TrustPolicy, env contractpackage.Environment) (contractpackage.Lock, error) {
	native, err := CaptureNativeLocks(ctx, projectRoot)
	if err != nil {
		return contractpackage.Lock{}, err
	}
	return contractpackage.Resolve(ctx, roots, source, policy, env, native)
}

func captureGo(ctx context.Context, root string) ([]contractpackage.NativeLock, error) {
	goContext, err := captureGoContext(ctx, root)
	if err != nil {
		return nil, err
	}
	if err := runNative(ctx, root, "go", "mod", "verify"); err != nil {
		return nil, fmt.Errorf("package: Go native lock verification failed")
	}
	output, err := runNativeOutput(ctx, root, "go", "list", "-m", "-mod=readonly", "-json", "all")
	if err != nil {
		return nil, fmt.Errorf("package: Go native module graph resolution failed")
	}
	decoder := json.NewDecoder(bytes.NewReader(output))
	var modules []contractpackage.NativeLockedPackage
	for {
		var module struct {
			Path     string
			Version  string
			Sum      string
			GoModSum string
			Dir      string
			Main     bool
			Replace  *struct {
				Path     string
				Version  string
				Sum      string
				GoModSum string
				Dir      string
			}
		}
		err := decoder.Decode(&module)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("package: Go returned an invalid native module graph")
		}
		if module.Main {
			if filepath.Clean(module.Dir) == filepath.Clean(root) {
				continue
			}
			rel, relErr := filepath.Rel(root, module.Dir)
			if relErr != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
				return nil, fmt.Errorf("package: Go workspace module outside project root is unsupported")
			}
			sourceDigest, digestErr := snapshotDirectory(module.Dir)
			if digestErr != nil {
				return nil, fmt.Errorf("package: cannot snapshot Go workspace module source")
			}
			modules = append(modules, contractpackage.NativeLockedPackage{Name: module.Path, Version: "local", Artifact: sourceDigest, Integrity: "local-source", Source: filepath.ToSlash(rel)})
			continue
		}
		artifact, integrity := module.Sum, module.GoModSum
		version := module.Version
		if module.Replace != nil {
			if module.Replace.Version != "" {
				version = module.Replace.Version
			}
			if module.Replace.Sum != "" {
				artifact = module.Replace.Sum
			}
			if module.Replace.GoModSum != "" {
				integrity = module.Replace.GoModSum
			}
			if module.Replace.Version == "" {
				rel, relErr := filepath.Rel(root, module.Replace.Dir)
				if relErr != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
					return nil, fmt.Errorf("package: local Go replace outside project root is unsupported; keep the source inside the project")
				}
				sourceDigest, digestErr := snapshotDirectory(module.Replace.Dir)
				if digestErr != nil {
					return nil, fmt.Errorf("package: cannot snapshot local Go replace source")
				}
				modules = append(modules, contractpackage.NativeLockedPackage{Name: module.Path, Version: "local", Artifact: sourceDigest, Integrity: "local-source", Source: filepath.ToSlash(rel)})
				continue
			}
		}
		if module.Path == "" || version == "" {
			return nil, fmt.Errorf("package: Go native module graph omitted an exact module identity")
		}
		modules = append(modules, contractpackage.NativeLockedPackage{Name: module.Path, Version: version, Artifact: artifact, Integrity: integrity})
		if len(modules) > 16384 {
			return nil, fmt.Errorf("package: Go native graph exceeds 16384 modules")
		}
	}
	goModDigest, err := digestFile(filepath.Join(root, "go.mod"))
	if err != nil {
		return nil, err
	}
	goSumDigest, err := digestFile(filepath.Join(root, "go.sum"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	locks := []contractpackage.NativeLock{{Manager: "go", Path: "go.mod", Digest: goModDigest, ToolVersion: goContext.ToolVersion, ContextDigest: goContext.ContextDigest, Packages: modules}}
	if goSumDigest != "" {
		locks = append(locks, contractpackage.NativeLock{Manager: "go", Path: "go.sum", Digest: goSumDigest, ToolVersion: goContext.ToolVersion, ContextDigest: goContext.ContextDigest})
	}
	if goContext.WorkPath != "" {
		workDigest, err := digestFile(filepath.Join(root, filepath.FromSlash(goContext.WorkPath)))
		if err != nil {
			return nil, fmt.Errorf("package: workspace lock file is unreadable")
		}
		locks = append(locks, contractpackage.NativeLock{Manager: "go", Path: goContext.WorkPath, Digest: workDigest, ToolVersion: goContext.ToolVersion, ContextDigest: goContext.ContextDigest})
		workSum := strings.TrimSuffix(goContext.WorkPath, ".work") + ".work.sum"
		if sum, err := digestFile(filepath.Join(root, filepath.FromSlash(workSum))); err == nil {
			locks = append(locks, contractpackage.NativeLock{Manager: "go", Path: workSum, Digest: sum, ToolVersion: goContext.ToolVersion, ContextDigest: goContext.ContextDigest})
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("package: workspace checksum file is unreadable")
		}
	}
	return locks, nil
}

func captureNPM(ctx context.Context, root string) ([]contractpackage.NativeLock, error) {
	// npm remains responsible for interpreting its lock. package-lock-only
	// performs a read-only graph projection and neither installs nor runs hooks.
	if _, err := runNativeOutput(ctx, root, "npm", "ls", "--package-lock-only", "--all", "--json"); err != nil {
		return nil, fmt.Errorf("package: npm native lock graph validation failed")
	}
	npmVersion, err := runNativeOutput(ctx, root, "npm", "--version")
	if err != nil {
		return nil, fmt.Errorf("package: npm version could not be read")
	}
	npmToolVersion := strings.TrimSpace(string(npmVersion))
	if npmToolVersion == "" || len(npmToolVersion) > 64 {
		return nil, fmt.Errorf("package: npm returned an invalid version")
	}
	nodeOutput, err := runNativeOutput(ctx, root, "node", "--version")
	if err != nil {
		return nil, fmt.Errorf("package: Node.js runtime version could not be read")
	}
	nodeVersion := strings.TrimSpace(string(nodeOutput))
	if nodeVersion == "" || len(nodeVersion) > 64 {
		return nil, fmt.Errorf("package: Node.js returned an invalid version")
	}
	contextBytes, _ := json.Marshal(struct{ Node, NPM string }{nodeVersion, npmToolVersion})
	contextDigest := digestBytes(contextBytes)
	lockPath := filepath.Join(root, "package-lock.json")
	data, err := readNativeBounded(lockPath, maxNativeLockBytes)
	if err != nil {
		return nil, err
	}
	var lock struct {
		LockfileVersion int `json:"lockfileVersion"`
		Packages        map[string]struct {
			Version   string `json:"version"`
			Resolved  string `json:"resolved"`
			Integrity string `json:"integrity"`
			Link      bool   `json:"link"`
		} `json:"packages"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&lock); err != nil || lock.LockfileVersion < 2 || lock.LockfileVersion > 3 {
		return nil, fmt.Errorf("package: npm lock must be a valid lockfileVersion 2 or 3")
	}
	if len(lock.Packages) > 16384 {
		return nil, fmt.Errorf("package: npm native graph exceeds 16384 packages")
	}
	packages := make([]contractpackage.NativeLockedPackage, 0, len(lock.Packages))
	for path, item := range lock.Packages {
		if path == "" {
			continue
		}
		if item.Link || strings.HasPrefix(item.Resolved, "file:") || strings.HasPrefix(item.Resolved, "workspace:") {
			return nil, fmt.Errorf("package: npm workspace or local link dependencies are unsupported until their source trees can be locked")
		}
		name := strings.TrimPrefix(path, "node_modules/")
		if strings.Contains(name, "/node_modules/") {
			name = name[strings.LastIndex(name, "/node_modules/")+len("/node_modules/"):]
		}
		if name == "" || item.Version == "" {
			return nil, fmt.Errorf("package: npm lock contains an unresolved package entry")
		}
		if item.Integrity == "" {
			return nil, fmt.Errorf("package: npm dependency lacks an exact integrity digest")
		}
		// Keep only integrity metadata. Resolved URLs may contain private mirror
		// hostnames, so they are deliberately excluded from persisted evidence.
		packages = append(packages, contractpackage.NativeLockedPackage{Name: name, Version: item.Version, Integrity: item.Integrity, Source: path})
	}
	sort.Slice(packages, func(i, j int) bool {
		if packages[i].Name != packages[j].Name {
			return packages[i].Name < packages[j].Name
		}
		if packages[i].Version != packages[j].Version {
			return packages[i].Version < packages[j].Version
		}
		if packages[i].Source != packages[j].Source {
			return packages[i].Source < packages[j].Source
		}
		if packages[i].Artifact != packages[j].Artifact {
			return packages[i].Artifact < packages[j].Artifact
		}
		return packages[i].Integrity < packages[j].Integrity
	})
	if len(packages) > 16384 {
		return nil, fmt.Errorf("package: npm native graph exceeds 16384 packages")
	}
	lockDigest := digestBytes(data)
	manifestDigest, err := digestFile(filepath.Join(root, "package.json"))
	if err != nil {
		return nil, err
	}
	return []contractpackage.NativeLock{
		{Manager: "npm", Path: "package-lock.json", Digest: lockDigest, ToolVersion: npmToolVersion, RuntimeVersion: nodeVersion, ContextDigest: contextDigest, Packages: packages},
		{Manager: "npm", Path: "package.json", Digest: manifestDigest, ToolVersion: npmToolVersion, RuntimeVersion: nodeVersion, ContextDigest: contextDigest},
	}, nil
}

func runNative(ctx context.Context, dir, name string, args ...string) error {
	_, err := runNativeOutput(ctx, dir, name, args...)
	return err
}

func runNativeOutput(ctx context.Context, dir, name string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, args...)
	command.Dir = dir
	command.Env = append(os.Environ(), "GOTOOLCHAIN=local", "npm_config_ignore_scripts=true")
	stdout := &limitedBuffer{limit: maxNativeLockBytes}
	command.Stdout = stdout
	command.Stderr = io.Discard
	if err := command.Run(); err != nil {
		return nil, err
	}
	return stdout.Bytes(), nil
}

type limitedBuffer struct {
	bytes.Buffer
	limit int
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.Len() {
		return 0, fmt.Errorf("native tool output exceeds limit")
	}
	return b.Buffer.Write(p)
}

func readNativeBounded(path string, limit int) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("package: read native lock %s: %w", filepath.Base(path), err)
	}
	defer file.Close()
	reader := bufio.NewReader(io.LimitReader(file, int64(limit+1)))
	data, err := io.ReadAll(reader)
	if err != nil || len(data) > limit {
		return nil, fmt.Errorf("package: native lock exceeds size limit")
	}
	return data, nil
}

func digestFile(path string) (string, error) {
	data, err := readNativeBounded(path, maxNativeLockBytes)
	if err != nil {
		return "", err
	}
	return digestBytes(data), nil
}

func digestBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}
func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}

type goContext struct{ ToolVersion, ContextDigest, WorkPath string }

func captureGoContext(ctx context.Context, root string) (goContext, error) {
	contextKeys := []string{
		"GOVERSION", "GOTOOLCHAIN", "GOOS", "GOARCH", "GOAMD64", "GOARM", "GOARM64", "GO386", "GOMIPS", "GOMIPS64",
		"GOEXPERIMENT", "CGO_ENABLED", "GOFLAGS", "GOWORK", "CC", "CXX", "CGO_CFLAGS", "CGO_CPPFLAGS", "CGO_CXXFLAGS", "CGO_LDFLAGS",
	}
	args := append([]string{"env", "-json"}, contextKeys...)
	data, err := runNativeOutput(ctx, root, "go", args...)
	if err != nil {
		return goContext{}, fmt.Errorf("package: Go execution context could not be captured")
	}
	var values map[string]string
	if err := json.Unmarshal(data, &values); err != nil {
		return goContext{}, fmt.Errorf("package: Go execution context is invalid")
	}
	if values["GOVERSION"] == "" || values["GOOS"] == "" || values["GOARCH"] == "" {
		return goContext{}, fmt.Errorf("package: Go tool omitted execution context fields")
	}
	flags := strings.Fields(values["GOFLAGS"])
	for i, flag := range flags {
		if strings.HasPrefix(flag, "-modfile=") || strings.HasPrefix(flag, "-overlay=") || ((flag == "-modfile" || flag == "-overlay") && i+1 < len(flags)) {
			return goContext{}, fmt.Errorf("package: GOFLAGS modfile/overlay inputs are unsupported; use the project's go.mod and go.work")
		}
	}
	var workPath string
	if work := values["GOWORK"]; work != "" && work != "off" {
		rel, err := filepath.Rel(root, work)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
			return goContext{}, fmt.Errorf("package: external go.work files are unsupported; keep go.work within the project")
		}
		workPath = filepath.ToSlash(rel)
	}
	context := make(map[string]string, len(contextKeys))
	for _, key := range contextKeys {
		context[key] = values[key]
	}
	context["GOWORK"] = filepath.Base(values["GOWORK"])
	contextBytes, _ := json.Marshal(context)
	return goContext{ToolVersion: values["GOVERSION"], ContextDigest: digestBytes(contextBytes), WorkPath: workPath}, nil
}

func snapshotDirectory(root string) (string, error) {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	var total int64
	files := 0
	err = filepath.WalkDir(absolute, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() && entry.Name() == ".git" {
			return filepath.SkipDir
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("local replacement contains a symlink")
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("local replacement contains a special file")
		}
		files++
		if files > 8192 {
			return fmt.Errorf("local replacement exceeds 8192 files")
		}
		rel, err := filepath.Rel(absolute, path)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(h, "%d:%s:%d:%o:", len(filepath.ToSlash(rel)), filepath.ToSlash(rel), info.Size(), info.Mode().Perm()); err != nil {
			return err
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		remaining := int64(128<<20) - total
		limited := &io.LimitedReader{R: file, N: remaining + 1}
		written, copyErr := io.Copy(h, limited)
		closeErr := file.Close()
		total += written
		if total > 128<<20 {
			return fmt.Errorf("local replacement exceeds 128 MiB")
		}
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}
