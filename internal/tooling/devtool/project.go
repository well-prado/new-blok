package devtool

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/well-prado/new-blok/internal/diagnostic"
	"github.com/well-prado/new-blok/internal/tooling/layout"
)

// MaxSourceFiles bounds the Go files goPackages lists for routes and test
// references; past it the listing fails with layout_limit_exceeded.
const MaxSourceFiles = 20000

// MaxSourceFileBytes is the per-file read bound, the same as discovery's.
const MaxSourceFileBytes = layout.MaxFileBytes

// Workspace is what check, test and inspect know about an application
// before they look inside its source.
type Workspace struct {
	// Root is the absolute project directory as given.
	Root   string
	Module string
	// Manifest is blok.json, read strictly by internal/tooling/layout; nil
	// when it is missing or invalid.
	Manifest *layout.Manifest
	// Discovered reports that layout discovery succeeded, so Nodes and
	// Workflows are the project's whole catalog.
	Discovered bool
	Nodes      []layout.Node
	Workflows  []layout.Workflow
	// Packages are the module's Go package directories, sorted by Dir,
	// for HTTP routes and test and example references.
	Packages []Package
}

// Package is one directory of Go files. Paths are slash-separated and
// relative to the workspace root; the root directory is ".".
type Package struct {
	Dir        string
	ImportPath string
	Files      []string
	TestFiles  []string
}

// ProjectSource finds a project without executing any of its source.
type ProjectSource interface {
	Load(ctx context.Context, root string) (Workspace, []diagnostic.Diagnostic, error)
}

// LayoutSource is the project discovery E12-T01 owns (ADR 0023):
// layout.Discover for the manifest, module, nodes and workflows, and its
// diagnostics unchanged. When discovery fails it still reads blok.json
// through the strict layout.LoadManifest and go.mod's module directive, so
// the checks that need only those can run; it never invents a catalog.
type LayoutSource struct{}

func (LayoutSource) Load(ctx context.Context, root string) (Workspace, []diagnostic.Diagnostic, error) {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return Workspace{}, nil, err
	}
	workspace := Workspace{Root: absolute}
	if info, err := os.Stat(absolute); err != nil {
		return workspace, nil, err
	} else if !info.IsDir() {
		return workspace, nil, &fs.PathError{Op: "open", Path: absolute, Err: errors.New("not a directory")}
	}
	var found []diagnostic.Diagnostic
	project, err := layout.Discover(absolute)
	var discovery *layout.Error
	switch {
	case err == nil:
		workspace.Discovered = true
		manifest := project.Manifest
		workspace.Manifest, workspace.Module = &manifest, project.Module
		workspace.Nodes, workspace.Workflows = project.Nodes, project.Workflows
	case errors.As(err, &discovery):
		found = append(found, discovery.Diagnostics...)
		if manifest, err := layout.LoadManifest(absolute); err == nil {
			workspace.Manifest = &manifest
		}
		workspace.Module = moduleOf(filepath.Join(absolute, "go.mod"))
	default:
		return workspace, nil, err
	}
	if workspace.Module == "" {
		return workspace, found, nil
	}
	packages, problem, err := goPackages(ctx, absolute, workspace.Module)
	if err != nil {
		return workspace, found, err
	}
	if problem != nil {
		found = append(found, *problem)
	}
	workspace.Packages = packages
	return workspace, found, nil
}

// goPackages lists the module's Go package directories the go command would
// see (vendor, testdata, nested modules and "."/"_" directories skipped).
// It never follows a symbolic link: a link is never a regular entry, and
// WalkDir does not descend into a linked directory, so nothing outside root
// is read. It is a file listing, not discovery: identities come from layout.
func goPackages(ctx context.Context, root, module string) ([]Package, *diagnostic.Diagnostic, error) {
	packages := map[string]*Package{}
	files := 0
	err := filepath.WalkDir(root, func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			if name == root {
				return nil
			}
			base := entry.Name()
			if base == "vendor" || base == "testdata" || strings.HasPrefix(base, ".") || strings.HasPrefix(base, "_") {
				return filepath.SkipDir
			}
			if _, err := os.Lstat(filepath.Join(name, "go.mod")); err == nil {
				return filepath.SkipDir
			}
			return nil
		}
		if !entry.Type().IsRegular() || filepath.Ext(name) != ".go" {
			return nil
		}
		if files++; files > MaxSourceFiles {
			return errTooLarge
		}
		relative, err := filepath.Rel(root, name)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		dir := path.Dir(relative)
		item := packages[dir]
		if item == nil {
			importPath := module
			if dir != "." {
				importPath += "/" + dir
			}
			item = &Package{Dir: dir, ImportPath: importPath}
			packages[dir] = item
		}
		if strings.HasSuffix(relative, "_test.go") {
			item.TestFiles = append(item.TestFiles, relative)
		} else {
			item.Files = append(item.Files, relative)
		}
		return nil
	})
	if errors.Is(err, errTooLarge) {
		return nil, &diagnostic.Diagnostic{Code: layout.CodeLimitExceeded, Expected: "at most " + strconv.Itoa(MaxSourceFiles) + " Go files", Actual: "more", Remediation: "run blok from the application's own module root, or split the project", Message: "the module has more Go files than blok reads"}, nil
	}
	if err != nil {
		return nil, nil, err
	}
	result := make([]Package, 0, len(packages))
	for _, item := range packages {
		sort.Strings(item.Files)
		sort.Strings(item.TestFiles)
		result = append(result, *item)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Dir < result[j].Dir })
	return result, nil, nil
}

var errTooLarge = errors.New("too many source files")

// moduleOf is go.mod's module path, or "" when there is none. It reports
// nothing: discovery already said why (layout_module_missing).
func moduleOf(name string) string {
	data, err := readBounded(name, MaxSourceFileBytes)
	if err != nil {
		return ""
	}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 || fields[0] != "module" {
			continue
		}
		if unquoted, err := strconv.Unquote(fields[1]); err == nil {
			return unquoted
		}
		return fields[1]
	}
	return ""
}

// readBounded reads a regular file of at most limit bytes without following
// a symbolic link out of the project.
func readBounded(name string, limit int64) ([]byte, error) {
	info, err := os.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, &fs.PathError{Op: "read", Path: filepath.Base(name), Err: fs.ErrInvalid}
	}
	if info.Size() > limit {
		return nil, &fs.PathError{Op: "read", Path: filepath.Base(name), Err: errFileTooLarge}
	}
	return os.ReadFile(name)
}

var errFileTooLarge = errors.New("file is too large")

// errorText is an error's text without the absolute path an *fs.PathError
// carries, so reports do not depend on where the project lives.
func errorText(err error) string {
	var pathErr *fs.PathError
	if errors.As(err, &pathErr) {
		return filepath.Base(pathErr.Path) + ": " + pathErr.Err.Error()
	}
	return err.Error()
}

func (w Workspace) project() *Project {
	project := &Project{Module: w.Module}
	if w.Manifest != nil {
		project.Name, project.Runtime, project.Layout = w.Manifest.Name, w.Manifest.Runtime, w.Manifest.Layout
		project.Triggers = append([]string(nil), w.Manifest.Triggers...)
	}
	return project
}
