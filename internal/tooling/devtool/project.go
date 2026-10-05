package devtool

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/well-prado/new-blok/internal/diagnostic"
	"github.com/well-prado/new-blok/internal/scaffold"
)

// Bounds on what project loading reads.
const (
	MaxSourceFiles     = 20000
	MaxSourceFileBytes = 8 << 20
	maxManifestBytes   = 1 << 20
)

// Workspace is what check and inspect know about an application before they
// look inside its source: its root, module, manifest and Go packages.
type Workspace struct {
	// Root is the absolute project directory.
	Root     string
	Module   string
	Manifest *scaffold.Manifest
	// Packages are sorted by Dir.
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

// ProjectSource finds a project's packages without executing any of its
// source. It is the seam E12-T01 (#67) fills: its manifest-based discovery
// of both layouts replaces DirectorySource, and check and inspect consume
// whatever it returns unchanged.
type ProjectSource interface {
	Load(ctx context.Context, root string) (Workspace, []diagnostic.Diagnostic, error)
}

// DirectorySource is the interim ProjectSource: blok.json and go.mod at the
// root, and every Go package directory the go command itself would see under
// it (vendor, testdata, nested modules and directories starting with "." or
// "_" are skipped). Symbolic links are never followed, so nothing outside
// the root is read.
type DirectorySource struct{}

func (DirectorySource) Load(ctx context.Context, root string) (Workspace, []diagnostic.Diagnostic, error) {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return Workspace{}, nil, err
	}
	workspace := Workspace{Root: absolute}
	var found []diagnostic.Diagnostic
	module, problem := readModule(filepath.Join(absolute, "go.mod"))
	if problem != nil {
		found = append(found, *problem)
	}
	workspace.Module = module
	manifest, problems := readManifest(absolute, module)
	found = append(found, problems...)
	workspace.Manifest = manifest
	if module == "" {
		return workspace, found, nil
	}
	packages := map[string]*Package{}
	files := 0
	err = filepath.WalkDir(absolute, func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			if name == absolute {
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
		// A symbolic link is never a regular entry, and WalkDir does not
		// descend into a linked directory, so nothing outside root is read.
		if !entry.Type().IsRegular() || filepath.Ext(name) != ".go" {
			return nil
		}
		files++
		if files > MaxSourceFiles {
			return errTooLarge
		}
		relative, err := filepath.Rel(absolute, name)
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
		found = append(found, diagnostic.Diagnostic{Code: "project_too_large", Expected: "at most " + strconv.Itoa(MaxSourceFiles) + " Go files", Actual: "more", Remediation: "run blok from the application's own module root", Message: "the project has more Go files than blok reads"})
		return workspace, found, nil
	}
	if err != nil {
		return workspace, found, err
	}
	for _, item := range packages {
		sort.Strings(item.Files)
		sort.Strings(item.TestFiles)
		workspace.Packages = append(workspace.Packages, *item)
	}
	sort.Slice(workspace.Packages, func(i, j int) bool { return workspace.Packages[i].Dir < workspace.Packages[j].Dir })
	return workspace, found, nil
}

var errTooLarge = errors.New("too many source files")

// readModule returns go.mod's module path, or the diagnostic explaining why
// there is none.
func readModule(name string) (string, *diagnostic.Diagnostic) {
	data, err := readBounded(name, maxManifestBytes)
	if err != nil {
		return "", &diagnostic.Diagnostic{Code: "project_go_mod_missing", Source: "go.mod", Expected: "a go.mod at the project root", Actual: errorText(err), Remediation: "run blok from the application's module root, or create the application with blok new", Message: "the project has no readable go.mod"}
	}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for line := 1; scanner.Scan(); line++ {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 || fields[0] != "module" {
			continue
		}
		module := fields[1]
		if unquoted, err := strconv.Unquote(module); err == nil {
			module = unquoted
		}
		return module, nil
	}
	return "", &diagnostic.Diagnostic{Code: "project_go_mod_invalid", Source: "go.mod", Expected: "a module directive", Remediation: "add a module directive naming the application's module path", Message: "go.mod declares no module"}
}

// readManifest reads blok.json. It accepts fields it does not know, so a
// newer manifest still loads; it rejects what check cannot honour.
func readManifest(root, module string) (*scaffold.Manifest, []diagnostic.Diagnostic) {
	data, err := readBounded(filepath.Join(root, "blok.json"), maxManifestBytes)
	if err != nil {
		return nil, []diagnostic.Diagnostic{{Code: "project_manifest_missing", Source: "blok.json", Expected: "a blok.json at the project root", Actual: errorText(err), Remediation: "run blok from the application's root, or create the application with blok new", Message: "the project has no readable blok.json"}}
	}
	var manifest scaffold.Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, []diagnostic.Diagnostic{{Code: "project_manifest_invalid", Source: "blok.json", Expected: "a JSON object", Actual: err.Error(), Remediation: "fix blok.json so it is a valid JSON object", Message: "blok.json is not valid JSON"}}
	}
	var found []diagnostic.Diagnostic
	if module != "" && manifest.Module != module {
		found = append(found, diagnostic.Diagnostic{Code: "project_module_mismatch", Source: "blok.json", Field: "module", Expected: module, Actual: manifest.Module, Remediation: "set blok.json's module to the module path go.mod declares", Message: "blok.json and go.mod name different modules"})
	}
	if manifest.Layout != "classic" && manifest.Layout != "unified" {
		found = append(found, diagnostic.Diagnostic{Code: "project_layout_unsupported", Source: "blok.json", Field: "layout", Expected: "classic|unified", Actual: manifest.Layout, Remediation: "set blok.json's layout to classic or unified", Message: "blok.json names an unsupported layout"})
	}
	if manifest.Types != "" && !confined(manifest.Types) {
		found = append(found, diagnostic.Diagnostic{Code: "project_manifest_invalid", Source: "blok.json", Field: "types", Expected: "a relative path inside the project", Actual: manifest.Types, Remediation: "point blok.json's types at a Go file inside the project", Message: "blok.json's types path leaves the project"})
		manifest.Types = ""
	}
	return &manifest, found
}

// confined reports whether a manifest path stays inside the project root.
func confined(name string) bool {
	if name == "" || strings.Contains(name, "\\") || path.IsAbs(name) || filepath.IsAbs(name) || filepath.VolumeName(name) != "" {
		return false
	}
	clean := path.Clean(name)
	return clean != ".." && !strings.HasPrefix(clean, "../")
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
