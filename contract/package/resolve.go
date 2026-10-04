package packagecontract

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
)

const (
	MaxResolvedPackages     = 256
	MaxResolutionCandidates = 2048
)

var (
	ErrResolutionConflict = errors.New("package: dependency version conflict")
	ErrResolutionCycle    = errors.New("package: dependency cycle")
	ErrUnsupportedRange   = errors.New("package: unsupported version range")
	ErrOfflineMissing     = errors.New("package: required artifact is missing from offline cache")
)

// Source lists immutable versions by package name and returns the exact bundle
// for an identity. Implementations must not execute package contents.
type Source interface {
	Versions(context.Context, string) ([]Identity, error)
	Get(context.Context, Identity) (Bundle, error)
}

type LockedDependency struct {
	Name           string `json:"name"`
	Version        string `json:"version"`
	ManifestDigest string `json:"manifestDigest"`
}

type LockedPackage struct {
	Identity       Identity           `json:"identity"`
	ManifestDigest string             `json:"manifestDigest"`
	ArtifactDigest string             `json:"artifactDigest"`
	Trust          TrustLevel         `json:"trust"`
	Dependencies   []LockedDependency `json:"dependencies,omitempty"`
}

// NativeLock records exact graphs resolved by language-native package tools.
// Entries are input to the package lock; they do not make this package a Go,
// npm, or other language dependency manager.
type NativeLock struct {
	Manager        string                `json:"manager"`
	Path           string                `json:"path"`
	Digest         string                `json:"digest"`
	ToolVersion    string                `json:"toolVersion,omitempty"`
	RuntimeVersion string                `json:"runtimeVersion,omitempty"`
	ContextDigest  string                `json:"contextDigest,omitempty"`
	Packages       []NativeLockedPackage `json:"packages"`
}

type NativeLockedPackage struct {
	Name      string `json:"name"`
	Version   string `json:"version"`
	Artifact  string `json:"artifact,omitempty"`
	Integrity string `json:"integrity,omitempty"`
	Source    string `json:"source,omitempty"`
}

type Lock struct {
	FormatVersion int             `json:"formatVersion"`
	Roots         []Dependency    `json:"roots"`
	Packages      []LockedPackage `json:"packages"`
	Native        []NativeLock    `json:"native,omitempty"`
}

func (l Lock) Canonical() ([]byte, error) {
	if err := l.Validate(); err != nil {
		return nil, err
	}
	c := l
	c.Roots = append([]Dependency(nil), l.Roots...)
	sort.Slice(c.Roots, func(i, j int) bool {
		if c.Roots[i].Name != c.Roots[j].Name {
			return c.Roots[i].Name < c.Roots[j].Name
		}
		return c.Roots[i].Version < c.Roots[j].Version
	})
	c.Packages = append([]LockedPackage(nil), l.Packages...)
	sort.Slice(c.Packages, func(i, j int) bool { return packageKey(c.Packages[i].Identity) < packageKey(c.Packages[j].Identity) })
	for i := range c.Packages {
		c.Packages[i].Dependencies = append([]LockedDependency(nil), c.Packages[i].Dependencies...)
		sort.Slice(c.Packages[i].Dependencies, func(a, b int) bool { return c.Packages[i].Dependencies[a].Name < c.Packages[i].Dependencies[b].Name })
	}
	c.Native = cloneNativeLocks(l.Native)
	sort.Slice(c.Native, func(i, j int) bool {
		return c.Native[i].Manager+"/"+c.Native[i].Path < c.Native[j].Manager+"/"+c.Native[j].Path
	})
	for i := range c.Native {
		sort.Slice(c.Native[i].Packages, func(a, b int) bool {
			if c.Native[i].Packages[a].Name != c.Native[i].Packages[b].Name {
				return c.Native[i].Packages[a].Name < c.Native[i].Packages[b].Name
			}
			return c.Native[i].Packages[a].Version < c.Native[i].Packages[b].Version
		})
	}
	return json.Marshal(c)
}

func (l Lock) Digest() (string, error) {
	data, err := l.Canonical()
	if err != nil {
		return "", err
	}
	return digestBytes(data), nil
}

func (l Lock) Validate() error {
	if l.FormatVersion != 1 {
		return &Error{Code: "invalid_lock", Path: "formatVersion", Message: "unsupported package lock format"}
	}
	if len(l.Roots) > MaxResolvedPackages || len(l.Packages) > MaxResolvedPackages || len(l.Native) > 16 {
		return &Error{Code: "invalid_lock", Path: "lock", Message: "lock collection count exceeds its documented limit"}
	}
	for i, root := range l.Roots {
		if err := validateRange(root.Version); err != nil {
			return unsupportedRange(fmt.Sprintf("roots[%d].version", i), root.Version)
		}
		if err := (Identity{Name: root.Name, Version: "1.0.0"}).Validate(); err != nil || (root.ManifestDigest != "" && !digestRE.MatchString(root.ManifestDigest)) {
			return &Error{Code: "invalid_lock", Path: fmt.Sprintf("roots[%d]", i), Message: "root dependency identity or digest pin is invalid"}
		}
	}
	rootNames := make(map[string]struct{}, len(l.Roots))
	for _, root := range l.Roots {
		if _, exists := rootNames[root.Name]; exists {
			return &Error{Code: "invalid_lock", Path: "roots", Message: "root package names must be unique"}
		}
		rootNames[root.Name] = struct{}{}
	}
	seen := make(map[string]LockedPackage, len(l.Packages))
	byName := make(map[string]LockedPackage, len(l.Packages))
	for i, p := range l.Packages {
		if err := p.Identity.Validate(); err != nil || !digestRE.MatchString(p.ManifestDigest) || !digestRE.MatchString(p.ArtifactDigest) || (p.Trust != TrustTrusted && p.Trust != TrustUnsignedLocal) {
			return &Error{Code: "invalid_lock", Path: fmt.Sprintf("packages[%d]", i), Message: "package identity and SHA-256 digests are required"}
		}
		key := packageKey(p.Identity)
		if _, ok := seen[key]; ok {
			return &Error{Code: "invalid_lock", Path: fmt.Sprintf("packages[%d]", i), Message: "package identity appears more than once"}
		}
		seen[key] = p
		if previous, ok := byName[p.Identity.Name]; ok && previous.Identity.Version != p.Identity.Version {
			return &Error{Code: "invalid_lock", Path: "packages", Message: "one package name cannot resolve to multiple versions"}
		}
		byName[p.Identity.Name] = p
		if len(p.Dependencies) > MaxDependencies {
			return &Error{Code: "invalid_lock", Path: "packages.dependencies", Message: "dependency count exceeds limit"}
		}
		for _, dep := range p.Dependencies {
			if !digestRE.MatchString(dep.ManifestDigest) {
				return &Error{Code: "invalid_lock", Path: "packages.dependencies", Message: "dependency manifest digest is invalid"}
			}
		}
	}
	for _, p := range l.Packages {
		for _, dep := range p.Dependencies {
			resolved, ok := seen[dep.Name+"@"+dep.Version]
			if !ok || resolved.ManifestDigest != dep.ManifestDigest {
				return &Error{Code: "invalid_lock", Path: "packages.dependencies", Message: "dependency does not resolve to its exact locked manifest"}
			}
		}
	}
	graph := make(map[string][]string, len(l.Packages))
	for _, p := range l.Packages {
		for _, dep := range p.Dependencies {
			graph[p.Identity.Name] = append(graph[p.Identity.Name], dep.Name)
		}
	}
	for name := range graph {
		sort.Strings(graph[name])
	}
	if cycle := findLockedCycle(graph); cycle != "" {
		return &Error{Code: "invalid_lock", Path: "packages", Message: "dependency graph contains a cycle: " + cycle}
	}
	for _, root := range l.Roots {
		found := false
		for _, p := range l.Packages {
			if p.Identity.Name == root.Name && satisfies(p.Identity.Version, root.Version) && (root.ManifestDigest == "" || root.ManifestDigest == p.ManifestDigest) {
				found = true
				break
			}
		}
		if !found {
			return &Error{Code: "invalid_lock", Path: "roots." + root.Name, Message: "root dependency is not satisfied by the locked graph"}
		}
	}
	nativePaths := make(map[string]struct{}, len(l.Native))
	for _, native := range l.Native {
		if (native.Manager != "go" && native.Manager != "npm") || native.Path == "" || !digestRE.MatchString(native.Digest) || !digestRE.MatchString(native.ContextDigest) || native.ToolVersion == "" || len(native.ToolVersion) > 64 || len(native.RuntimeVersion) > 64 || (native.Manager == "npm" && native.RuntimeVersion == "") {
			return &Error{Code: "invalid_lock", Path: "native", Message: "native lock requires a manager, relative path, and SHA-256 digest"}
		}
		if path.Clean(native.Path) != native.Path || path.IsAbs(native.Path) || strings.Contains(native.Path, "\\") || native.Path == "." || strings.HasPrefix(native.Path, "../") {
			return &Error{Code: "invalid_lock", Path: "native.path", Message: "native lock path must be a clean relative path"}
		}
		key := native.Manager + "/" + native.Path
		if _, exists := nativePaths[key]; exists {
			return &Error{Code: "invalid_lock", Path: "native.path", Message: "native lock path appears more than once"}
		}
		nativePaths[key] = struct{}{}
		if len(native.Packages) > 16384 {
			return &Error{Code: "invalid_lock", Path: "native.packages", Message: "native package graph exceeds 16384 entries"}
		}
		for _, pkg := range native.Packages {
			if pkg.Name == "" || pkg.Version == "" {
				return &Error{Code: "invalid_lock", Path: "native.packages", Message: "native package identity is incomplete"}
			}
			if native.Manager == "go" && native.Path == "go.mod" && pkg.Artifact == "" {
				return &Error{Code: "invalid_lock", Path: "native.packages.artifact", Message: "Go module checksum or local source digest is required"}
			}
			if native.Manager == "npm" && native.Path == "package-lock.json" && pkg.Integrity == "" {
				return &Error{Code: "invalid_lock", Path: "native.packages.integrity", Message: "npm package integrity is required"}
			}
			if pkg.Source != "" && (path.Clean(pkg.Source) != pkg.Source || path.IsAbs(pkg.Source) || strings.Contains(pkg.Source, "\\") || pkg.Source == ".." || strings.HasPrefix(pkg.Source, "../")) {
				return &Error{Code: "invalid_lock", Path: "native.packages.source", Message: "native source path escapes the project"}
			}
		}
	}
	return nil
}

type requirement struct {
	rangeExpr string
	digest    string
	parent    string
}

type resolutionState struct {
	requirements map[string][]requirement
	selected     map[string]candidate
}

type candidate struct {
	bundle   Bundle
	verified Verified
}

func Resolve(ctx context.Context, roots []Dependency, source Source, policy TrustPolicy, env Environment, native []NativeLock) (Lock, error) {
	if source == nil {
		return Lock{}, &Error{Code: "invalid_resolution", Message: "package source is required"}
	}
	if len(roots) == 0 {
		return Lock{}, &Error{Code: "invalid_resolution", Path: "roots", Message: "at least one package root is required"}
	}
	if len(roots) > MaxResolvedPackages {
		return Lock{}, &Error{Code: "invalid_resolution", Path: "roots", Message: "root count exceeds the 256 package limit"}
	}
	state := resolutionState{requirements: make(map[string][]requirement), selected: make(map[string]candidate)}
	rootCopy := append([]Dependency(nil), roots...)
	sort.Slice(rootCopy, func(i, j int) bool { return rootCopy[i].Name < rootCopy[j].Name })
	rootNames := make(map[string]struct{}, len(rootCopy))
	for _, root := range rootCopy {
		if _, exists := rootNames[root.Name]; exists {
			return Lock{}, &Error{Code: "invalid_resolution", Path: "roots." + root.Name, Message: "a package may be declared as a root only once"}
		}
		if err := (Identity{Name: root.Name, Version: "1.0.0"}).Validate(); err != nil {
			return Lock{}, &Error{Code: "invalid_resolution", Path: "roots", Message: "root package name is invalid"}
		}
		rootNames[root.Name] = struct{}{}
		if err := validateRange(root.Version); err != nil {
			return Lock{}, unsupportedRange("roots."+root.Name, root.Version)
		}
		if root.ManifestDigest != "" && !digestRE.MatchString(root.ManifestDigest) {
			return Lock{}, &Error{Code: "invalid_resolution", Path: "roots." + root.Name, Message: "manifest digest pin is invalid"}
		}
		state.requirements[root.Name] = append(state.requirements[root.Name], requirement{rangeExpr: root.Version, digest: root.ManifestDigest, parent: "<root>"})
	}
	versions := make(map[string][]Identity)
	var candidatesRead int
	var search func(resolutionState) (resolutionState, error)
	search = func(current resolutionState) (resolutionState, error) {
		if len(current.selected) > MaxResolvedPackages {
			return resolutionState{}, &Error{Code: "resolution_limit", Path: "graph", Message: "resolved graph exceeds the 256 package limit"}
		}
		name := ""
		for n := range current.requirements {
			if _, done := current.selected[n]; !done && (name == "" || n < name) {
				name = n
			}
		}
		if name == "" {
			if err := validateAcyclic(current.selected); err != nil {
				return resolutionState{}, err
			}
			return current, nil
		}
		ids, loaded := versions[name]
		if !loaded {
			listed, err := source.Versions(ctx, name)
			if err != nil {
				return resolutionState{}, err
			}
			ids = append([]Identity(nil), listed...)
			if len(ids) > MaxResolutionCandidates {
				return resolutionState{}, &Error{Code: "resolution_limit", Path: "candidates", Message: "source catalog exceeds the 2048 candidate limit"}
			}
			for _, id := range ids {
				if id.Name != name || id.Validate() != nil {
					return resolutionState{}, &Error{Code: "invalid_source_catalog", Path: name, Message: "source returned an invalid or differently named package identity"}
				}
			}
			sort.Slice(ids, func(i, j int) bool { return compareVersionString(ids[i].Version, ids[j].Version) > 0 })
			for i := 1; i < len(ids); i++ {
				if ids[i-1] == ids[i] {
					return resolutionState{}, &Error{Code: "invalid_source_catalog", Path: name, Message: "source returned a duplicate package identity"}
				}
			}
			versions[name] = ids
		}
		var lastErr error
		for _, id := range ids {
			if !satisfiesAll(id.Version, current.requirements[name]) {
				continue
			}
			candidatesRead++
			if candidatesRead > MaxResolutionCandidates {
				return resolutionState{}, &Error{Code: "resolution_limit", Path: "candidates", Message: "candidate inspection exceeds the 2048 package limit"}
			}
			bundle, err := source.Get(ctx, id)
			if err != nil {
				lastErr = err
				continue
			}
			verified, err := bundle.Verify(policy, env)
			if err != nil {
				lastErr = err
				continue
			}
			if verified.Identity != id {
				lastErr = &Error{Code: "invalid_source_package", Path: packageKey(id), Message: "source returned a bundle for a different identity"}
				continue
			}
			if !requirementsMatch(verified, current.requirements[name]) {
				lastErr = conflict(name, current.requirements[name])
				continue
			}
			trial := cloneResolutionState(current)
			trial.selected[name] = candidate{bundle: cloneBundle(bundle), verified: verified}
			valid := true
			for _, dep := range bundle.Manifest.Dependencies {
				if validateRange(dep.Version) != nil {
					return resolutionState{}, unsupportedRange("dependencies."+dep.Name, dep.Version)
				}
				trial.requirements[dep.Name] = append(trial.requirements[dep.Name], requirement{rangeExpr: dep.Version, digest: dep.ManifestDigest, parent: packageKey(id)})
				if selected, exists := trial.selected[dep.Name]; exists && !requirementsMatch(selected.verified, trial.requirements[dep.Name]) {
					valid = false
					lastErr = conflict(dep.Name, trial.requirements[dep.Name])
					break
				}
			}
			if !valid {
				continue
			}
			resolved, err := search(trial)
			if err == nil {
				return resolved, nil
			}
			lastErr = err
			if errors.Is(err, ErrResolutionCycle) {
				continue
			}
			if errors.Is(err, ErrUnsupportedRange) || strings.HasPrefix(errorCode(err), "invalid_") {
				return resolutionState{}, err
			}
		}
		if lastErr != nil && len(ids) == 0 {
			return resolutionState{}, lastErr
		}
		if len(ids) > 0 && lastErr != nil && !errors.Is(lastErr, ErrNotFound) {
			if errors.Is(lastErr, ErrResolutionCycle) {
				return resolutionState{}, lastErr
			}
			return resolutionState{}, lastErr
		}
		if len(current.requirements[name]) > 1 || len(ids) > 0 {
			return resolutionState{}, conflict(name, current.requirements[name])
		}
		return resolutionState{}, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	resolved, err := search(state)
	if err != nil {
		return Lock{}, err
	}
	lock := Lock{FormatVersion: 1, Roots: rootCopy, Native: cloneNativeLocks(native)}
	for _, name := range sortedKeys(resolved.selected) {
		item := resolved.selected[name]
		locked := LockedPackage{Identity: item.verified.Identity, ManifestDigest: item.verified.ManifestDigest, ArtifactDigest: item.verified.ArtifactDigest, Trust: item.verified.Trust}
		for _, dep := range item.bundle.Manifest.Dependencies {
			selected, ok := resolved.selected[dep.Name]
			if !ok || !satisfies(selected.verified.Identity.Version, dep.Version) || (dep.ManifestDigest != "" && dep.ManifestDigest != selected.verified.ManifestDigest) {
				return Lock{}, conflict(dep.Name, resolved.requirements[dep.Name])
			}
			locked.Dependencies = append(locked.Dependencies, LockedDependency{Name: dep.Name, Version: selected.verified.Identity.Version, ManifestDigest: selected.verified.ManifestDigest})
		}
		sort.Slice(locked.Dependencies, func(i, j int) bool { return locked.Dependencies[i].Name < locked.Dependencies[j].Name })
		lock.Packages = append(lock.Packages, locked)
	}
	sort.Slice(lock.Native, func(i, j int) bool {
		return lock.Native[i].Manager+"/"+lock.Native[i].Path < lock.Native[j].Manager+"/"+lock.Native[j].Path
	})
	if err := lock.Validate(); err != nil {
		return Lock{}, err
	}
	return lock, nil
}

// VerifyLock fetches exactly the identities in an existing lock and proves
// that their current verified artifacts and dependency edges still match it.
func VerifyLock(ctx context.Context, lock Lock, source Source, policy TrustPolicy, env Environment) ([]Bundle, error) {
	if err := lock.Validate(); err != nil {
		return nil, err
	}
	if source == nil {
		return nil, &Error{Code: "invalid_resolution", Message: "package source is required"}
	}
	bundles := make([]Bundle, 0, len(lock.Packages))
	for _, expected := range lock.Packages {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		bundle, err := source.Get(ctx, expected.Identity)
		if err != nil {
			return nil, err
		}
		verified, err := bundle.Verify(policy, env)
		if err != nil {
			return nil, err
		}
		if verified.Identity != expected.Identity || verified.ManifestDigest != expected.ManifestDigest || verified.ArtifactDigest != expected.ArtifactDigest || verified.Trust != expected.Trust {
			return nil, &Error{Code: "locked_artifact_mismatch", Path: packageKey(expected.Identity), Message: "verified artifact no longer matches the exact lock"}
		}
		if err := verifyLockedDependencies(bundle.Manifest, expected); err != nil {
			return nil, err
		}
		bundles = append(bundles, cloneBundle(bundle))
	}
	return bundles, nil
}

func verifyLockedDependencies(manifest Manifest, locked LockedPackage) error {
	if len(manifest.Dependencies) != len(locked.Dependencies) {
		return &Error{Code: "locked_artifact_mismatch", Path: packageKey(locked.Identity), Message: "manifest dependency set differs from the lock"}
	}
	actual := make(map[string]Dependency, len(manifest.Dependencies))
	for _, dep := range manifest.Dependencies {
		actual[dep.Name] = dep
	}
	for _, edge := range locked.Dependencies {
		dep, ok := actual[edge.Name]
		if !ok || !satisfies(edge.Version, dep.Version) || (dep.ManifestDigest != "" && dep.ManifestDigest != edge.ManifestDigest) {
			return &Error{Code: "locked_artifact_mismatch", Path: packageKey(locked.Identity), Message: "locked dependency edge does not satisfy its package manifest"}
		}
	}
	return nil
}

func findLockedCycle(graph map[string][]string) string {
	state := make(map[string]uint8, len(graph))
	path := make([]string, 0, len(graph))
	var visit func(string) string
	visit = func(name string) string {
		if state[name] == 2 {
			return ""
		}
		if state[name] == 1 {
			start := 0
			for i, item := range path {
				if item == name {
					start = i
					break
				}
			}
			cycle := append(append([]string(nil), path[start:]...), name)
			return strings.Join(cycle, " -> ")
		}
		state[name] = 1
		path = append(path, name)
		for _, dep := range graph[name] {
			if cycle := visit(dep); cycle != "" {
				return cycle
			}
		}
		path = path[:len(path)-1]
		state[name] = 2
		return ""
	}
	for _, name := range sortedKeys(graph) {
		if cycle := visit(name); cycle != "" {
			return cycle
		}
	}
	return ""
}

func unsupportedRange(path, value string) error {
	return &Error{Code: "unsupported_version_range", Path: path, Message: fmt.Sprintf("version range %q is outside the supported exact/comparator grammar", value)}
}

func satisfiesAll(version string, requirements []requirement) bool {
	for _, req := range requirements {
		if !satisfies(version, req.rangeExpr) {
			return false
		}
	}
	return true
}

func requirementsMatch(verified Verified, requirements []requirement) bool {
	if !satisfiesAll(verified.Identity.Version, requirements) {
		return false
	}
	for _, req := range requirements {
		if req.digest != "" && req.digest != verified.ManifestDigest {
			return false
		}
	}
	return true
}

func conflict(name string, requirements []requirement) error {
	paths := make([]string, 0, len(requirements))
	for _, req := range requirements {
		paths = append(paths, req.parent+" requires "+req.rangeExpr)
	}
	sort.Strings(paths)
	return &Error{Code: "resolution_conflict", Path: name, Message: strings.Join(paths, "; "), cause: ErrResolutionConflict}
}

func validateAcyclic(selected map[string]candidate) error {
	const (
		unseen = iota
		visiting
		visited
	)
	marks := make(map[string]int, len(selected))
	stack := make([]string, 0, len(selected))
	var visit func(string) error
	visit = func(name string) error {
		if marks[name] == visited {
			return nil
		}
		if marks[name] == visiting {
			start := 0
			for i, value := range stack {
				if value == name {
					start = i
					break
				}
			}
			cycle := append(append([]string(nil), stack[start:]...), name)
			return &Error{Code: "resolution_cycle", Path: name, Message: strings.Join(cycle, " -> "), cause: ErrResolutionCycle}
		}
		marks[name] = visiting
		stack = append(stack, name)
		for _, dep := range selected[name].bundle.Manifest.Dependencies {
			if err := visit(dep.Name); err != nil {
				return err
			}
		}
		stack = stack[:len(stack)-1]
		marks[name] = visited
		return nil
	}
	for _, name := range sortedKeys(selected) {
		if err := visit(name); err != nil {
			return err
		}
	}
	return nil
}

func cloneResolutionState(source resolutionState) resolutionState {
	copy := resolutionState{requirements: make(map[string][]requirement, len(source.requirements)), selected: make(map[string]candidate, len(source.selected))}
	for key, value := range source.requirements {
		copy.requirements[key] = append([]requirement(nil), value...)
	}
	for key, value := range source.selected {
		copy.selected[key] = value
	}
	return copy
}

func compareVersionString(a, b string) int {
	av, aok := parseVersion(a)
	bv, bok := parseVersion(b)
	if !aok || !bok {
		return strings.Compare(a, b)
	}
	return compareVersion(av, bv)
}

func sortedKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func cloneNativeLocks(values []NativeLock) []NativeLock {
	copy := append([]NativeLock(nil), values...)
	for i := range copy {
		copy[i].Packages = append([]NativeLockedPackage(nil), copy[i].Packages...)
	}
	return copy
}

func errorCode(err error) string {
	var typed *Error
	if errors.As(err, &typed) {
		return typed.Code
	}
	return ""
}
