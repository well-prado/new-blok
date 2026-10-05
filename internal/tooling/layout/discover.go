package layout

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/well-prado/new-blok/internal/diagnostic"
)

// Project is a discovered application. Paths are project-relative and use
// forward slashes. Dir and Source fields depend on the layout; the Catalog
// derived from a Project does not.
type Project struct {
	Root      string // absolute, symlink-resolved project root
	Module    string
	Manifest  Manifest
	Nodes     []Node
	Workflows []Workflow
}

// Node is one node directory and the identity its descriptor declares.
type Node struct {
	Name    string
	Version string
	Runtime string
	// Dir is the node root, such as runtimes/go/nodes/quote.
	Dir string
	// Files are every regular file the node owns, relative to Dir, sorted.
	Files []string
	// Descriptor is the project-relative file:line (Go) or node.json path
	// that declares the identity.
	Descriptor string
}

// Workflow is one static flow.Define call under a workflow path.
type Workflow struct {
	Name    string
	Version string
	// Path is the declared workflow path that owns Source.
	Path string
	// Source is the project-relative file:line of the definition.
	Source string
}

// Discover reads the project at root. It returns a Project, or an *Error
// carrying every diagnostic found, or a plain error when root itself cannot
// be opened.
func Discover(root string) (*Project, error) { return discoverWith(root, MaxFiles) }

// discoverWith is Discover with an entry bound, so tests can reach the
// bound without creating MaxFiles directories.
func discoverWith(root string, entryLimit int) (*Project, error) {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("layout: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil, fmt.Errorf("layout: open project root: %w", err)
	}
	opened, err := os.OpenRoot(resolved)
	if err != nil {
		return nil, fmt.Errorf("layout: open project root: %w", err)
	}
	defer opened.Close()
	d := &discoverer{root: opened, abs: resolved, c: &collector{}, nodeBytes: -1, entryLimit: entryLimit}
	project := d.discover()
	if err := d.c.err(); err != nil {
		return nil, err
	}
	project.Root = resolved
	return project, nil
}

// LoadManifest reads and validates root/blok.json alone.
func LoadManifest(root string) (Manifest, error) {
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return Manifest{}, fmt.Errorf("layout: open project root: %w", err)
	}
	if resolved, err = filepath.Abs(resolved); err != nil {
		return Manifest{}, fmt.Errorf("layout: %w", err)
	}
	opened, err := os.OpenRoot(resolved)
	if err != nil {
		return Manifest{}, fmt.Errorf("layout: open project root: %w", err)
	}
	defer opened.Close()
	d := &discoverer{root: opened, abs: resolved, c: &collector{}, nodeBytes: -1}
	manifest, _ := d.manifest()
	if err := d.c.err(); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

// discoverer never follows a symbolic link: entries are read with Lstat
// semantics, every link is classified and reported, and file contents are
// read through an os.Root, which refuses to leave the project root even if
// the tree changes during discovery.
type discoverer struct {
	root       *os.Root
	abs        string
	c          *collector
	files      int // directory entries visited
	entryLimit int
	bytes      int64
	// nodeBytes is the source read for the node being walked, or -1
	// outside a node.
	nodeBytes int64
}

func (d *discoverer) discover() *Project {
	manifest, ok := d.manifest()
	if !ok {
		return &Project{}
	}
	project := &Project{Manifest: manifest, Module: d.module(manifest)}
	roots := d.nodeRoots(manifest.Layout)
	if len(roots) > MaxNodes {
		d.c.add(CodeLimitExceeded, "", strconv.Itoa(MaxNodes), strconv.Itoa(len(roots)), "too many node directories")
		return project
	}
	goImports := map[string]map[string][]string{} // node dir → file → imports
	for _, root := range roots {
		node, imports, ok := d.node(root)
		if ok {
			project.Nodes = append(project.Nodes, node)
			goImports[node.Dir] = imports
		}
	}
	project.Workflows = d.workflows(manifest, roots)
	d.checkImports(project, goImports)
	d.checkIdentities(project)
	d.checkCollisions(project)
	sort.Slice(project.Nodes, func(i, j int) bool { return project.Nodes[i].Dir < project.Nodes[j].Dir })
	sort.Slice(project.Workflows, func(i, j int) bool { return project.Workflows[i].Source < project.Workflows[j].Source })
	return project
}

func (d *discoverer) manifest() (Manifest, bool) {
	info, err := d.root.Lstat(ManifestFile)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		d.c.add(CodeManifestMissing, ManifestFile, "", "", "the project has no blok.json; discovery needs the declared layout")
		return Manifest{}, false
	case err != nil:
		d.c.add(CodeManifestInvalid, ManifestFile, "", "", "blok.json cannot be inspected")
		return Manifest{}, false
	case info.Mode()&fs.ModeSymlink != 0:
		d.symlink(ManifestFile)
		return Manifest{}, false
	}
	data, ok := d.read(ManifestFile, info)
	if !ok {
		return Manifest{}, false
	}
	manifest, diagnostics := ParseManifest(data)
	d.c.addAll(diagnostics)
	return manifest, len(diagnostics) == 0
}

// module reads go.mod's module directive without invoking the go tool.
func (d *discoverer) module(manifest Manifest) string {
	info, err := d.root.Lstat("go.mod")
	if err != nil || info.Mode()&fs.ModeSymlink != 0 {
		if err == nil {
			d.symlink("go.mod")
		} else {
			d.c.add(CodeModuleMissing, "go.mod", "", "", "the project has no go.mod; an application owns its Go module")
		}
		return ""
	}
	data, ok := d.read("go.mod", info)
	if !ok {
		return ""
	}
	module := ""
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if rest, found := strings.CutPrefix(line, "module"); found && (strings.HasPrefix(rest, " ") || strings.HasPrefix(rest, "\t")) {
			rest = strings.TrimSpace(rest)
			if comment := strings.Index(rest, "//"); comment >= 0 {
				rest = strings.TrimSpace(rest[:comment])
			}
			if unquoted, err := strconv.Unquote(rest); err == nil {
				rest = unquoted
			}
			module = rest
			break
		}
	}
	if module == "" {
		d.c.add(CodeModuleMissing, "go.mod", "module directive", "", "go.mod has no module directive")
		return ""
	}
	if manifest.Module != "" && manifest.Module != module {
		d.c.addAll([]diagnostic.Diagnostic{manifestDiag("module", module, manifest.Module, "blok.json module differs from go.mod's module")})
	}
	return module
}

type nodeRoot struct {
	dir     string
	runtime string
}

// nodeRoots lists the node directories of the declared layout and reports
// any node directory of the other layout.
func (d *discoverer) nodeRoots(layout string) []nodeRoot {
	var roots []nodeRoot
	if layout == Unified {
		for _, runtime := range d.runtimeDirs("nodes") {
			roots = append(roots, d.nodeDirs(path.Join("nodes", runtime), runtime)...)
		}
		for _, runtime := range d.subdirs("runtimes", false, false) {
			if _, err := d.root.Lstat(path.Join("runtimes", runtime, "nodes")); err == nil {
				d.c.add(CodeMixedLayout, path.Join("runtimes", runtime, "nodes"), Unified, Classic, "a classic node directory exists in a unified project")
			}
		}
		return roots
	}
	for _, runtime := range d.runtimeDirs("runtimes") {
		base := path.Join("runtimes", runtime, "nodes")
		info, err := d.root.Lstat(base)
		switch {
		case err != nil:
			continue
		case info.Mode()&fs.ModeSymlink != 0:
			d.symlink(base)
			continue
		case !info.IsDir():
			d.c.add(CodeFileUnowned, base, "directory", "file", "runtimes/<runtime>/nodes must be a directory")
			continue
		}
		roots = append(roots, d.nodeDirs(base, runtime)...)
	}
	if _, err := d.root.Lstat("nodes"); err == nil {
		d.c.add(CodeMixedLayout, "nodes", Classic, Unified, "a unified node directory exists in a classic project")
	}
	return roots
}

// runtimeDirs returns the runtime directories under base with valid names.
// Files directly under base are not reported: base holds runtimes, and a
// README beside them owns nothing.
func (d *discoverer) runtimeDirs(base string) []string {
	if !d.realDir(base) {
		return nil
	}
	var runtimes []string
	for _, name := range d.subdirs(base, true, false) {
		if !runtimeGrammar.MatchString(name) {
			d.c.add(CodeInvalidRuntime, path.Join(base, name), runtimeGrammar.String(), name, "runtime directory name is not a runtime")
			continue
		}
		runtimes = append(runtimes, name)
	}
	return runtimes
}

// subdirs lists the real directories directly under dir, reporting links
// when reportLinks and files as unowned when reportFiles.
func (d *discoverer) subdirs(dir string, reportLinks, reportFiles bool) []string {
	entries, ok := d.readDir(dir)
	if !ok {
		return nil
	}
	var names []string
	for _, entry := range entries {
		name, rel := entry.Name(), path.Join(dir, entry.Name())
		switch {
		case skipped(name):
		case entry.Type()&fs.ModeSymlink != 0:
			if reportLinks {
				d.symlink(rel)
			}
		case entry.IsDir():
			names = append(names, name)
		case reportFiles:
			d.c.add(CodeFileUnowned, rel, "node directory", "file", "a file outside any node directory sits where nodes live")
		}
	}
	return names
}

func (d *discoverer) nodeDirs(base, runtime string) []nodeRoot {
	var roots []nodeRoot
	for _, name := range d.subdirs(base, true, true) {
		roots = append(roots, nodeRoot{dir: path.Join(base, name), runtime: runtime})
	}
	return roots
}

// node reads one node directory: every owned file, and the one identity its
// descriptor declares.
func (d *discoverer) node(root nodeRoot) (Node, map[string][]string, bool) {
	node := Node{Runtime: root.runtime, Dir: root.dir}
	source := newGoSource()
	d.nodeBytes = 0
	defer func() { d.nodeBytes = -1 }()
	hasDescriptorFile := false
	before := len(d.c.items)
	d.walk(root.dir, 0, func(rel string, info fs.FileInfo) {
		node.Files = append(node.Files, strings.TrimPrefix(rel, root.dir+"/"))
		switch {
		case rel == path.Join(root.dir, DescriptorFile):
			hasDescriptorFile = true
		case strings.HasSuffix(rel, ".go") && !strings.HasSuffix(rel, "_test.go"):
			if data, ok := d.read(rel, info); ok {
				if err := source.parse(rel, data); err != nil {
					d.c.add(CodeParseFailed, rel, "", "", err.Error())
				}
			}
		}
	})
	sort.Strings(node.Files)
	goNodes, goFlows := source.definitions(d.c)
	for _, flow := range goFlows {
		d.c.add(CodeDescriptorMisplaced, flow.source, "workflow under a workflow path", "flow.Define in node "+root.dir, "a workflow is defined inside a node directory")
	}
	if root.runtime == GoRuntime {
		if hasDescriptorFile {
			d.c.add(CodeRuntimeMismatch, path.Join(root.dir, DescriptorFile), "Go node.Define", "node.json", "a Go node declares its identity with node.Define; node.json is for foreign runtimes")
		}
		distinct := uniqueDefinitions(goNodes)
		switch len(distinct) {
		case 0:
			if len(d.c.items) == before {
				d.c.add(CodeDescriptorMissing, root.dir, "one node.Define call", "none", "the node directory declares no node")
			}
			return Node{}, nil, false
		case 1:
			node.Name, node.Version, node.Descriptor = distinct[0].name, distinct[0].version, distinct[0].source
		default:
			for _, extra := range distinct[1:] {
				d.c.add(CodeDescriptorMultiple, extra.source, distinct[0].name+"@"+distinct[0].version, extra.name+"@"+extra.version, "a node directory declares more than one node")
			}
			return Node{}, nil, false
		}
	} else {
		for _, definition := range goNodes {
			d.c.add(CodeRuntimeMismatch, definition.source, root.runtime, GoRuntime, "a Go node.Define sits under the "+root.runtime+" runtime directory")
		}
		if !hasDescriptorFile {
			d.c.add(CodeDescriptorMissing, root.dir, DescriptorFile, "none", "a foreign-runtime node needs a node.json descriptor")
			return Node{}, nil, false
		}
		descriptorPath := path.Join(root.dir, DescriptorFile)
		info, err := d.root.Lstat(filepath.FromSlash(descriptorPath))
		if err != nil {
			return Node{}, nil, false
		}
		data, ok := d.read(descriptorPath, info)
		if !ok {
			return Node{}, nil, false
		}
		descriptor, err := parseForeignDescriptor(data)
		if err != nil {
			d.c.add(CodeParseFailed, descriptorPath, "", "", "node.json is not valid JSON: "+err.Error())
			return Node{}, nil, false
		}
		if descriptor.Runtime != "" && descriptor.Runtime != root.runtime {
			d.c.add(CodeRuntimeMismatch, descriptorPath, root.runtime, descriptor.Runtime, "node.json declares a different runtime than its directory")
			return Node{}, nil, false
		}
		if len(goNodes) > 0 {
			return Node{}, nil, false
		}
		node.Name, node.Version, node.Descriptor = descriptor.Name, descriptor.Version, descriptorPath
	}
	if !validIdentity(node.Name, node.Version) {
		d.c.add(CodeDescriptorInvalid, node.Descriptor, "namespace/name and major.minor.patch", node.Name+"@"+node.Version, "the node identity is not a stable name and version")
		return Node{}, nil, false
	}
	return node, source.imports(), true
}

func validIdentity(name, version string) bool {
	return identityGrammar.MatchString(name) && !strings.Contains(name, "..") && !strings.HasSuffix(name, "/") && versionGrammar.MatchString(version)
}

func uniqueDefinitions(definitions []definition) []definition {
	seen := map[string]bool{}
	var out []definition
	for _, definition := range definitions {
		key := definition.name + "@" + definition.version
		if !seen[key] {
			seen[key] = true
			out = append(out, definition)
		}
	}
	return out
}

// workflows reads every declared workflow path.
func (d *discoverer) workflows(manifest Manifest, roots []nodeRoot) []Workflow {
	explicit := len(manifest.Workflows) > 0
	var workflows []Workflow
	for _, declared := range manifest.WorkflowPaths() {
		overlap := false
		for _, root := range roots {
			if root.dir == declared || strings.HasPrefix(root.dir, declared+"/") || strings.HasPrefix(declared, root.dir+"/") {
				d.c.add(CodeOwnershipOverlap, declared, "workflow path outside node directories", root.dir, "a workflow path and a node directory own the same files")
				overlap = true
			}
		}
		if _, isNode := NodeRoot(declared); isNode || overlap || !d.realParents(declared) {
			continue
		}
		info, err := d.root.Lstat(filepath.FromSlash(declared))
		switch {
		case errors.Is(err, fs.ErrNotExist):
			if explicit {
				d.c.add(CodeWorkflowPathMissing, declared, "existing directory or .go file", "missing", "a workflow path blok.json declares does not exist")
			}
			continue
		case err != nil:
			d.c.add(CodeFileUnsupported, declared, "", "", "workflow path cannot be inspected")
			continue
		case info.Mode()&fs.ModeSymlink != 0:
			d.symlink(declared)
			continue
		}
		source := newGoSource()
		parse := func(rel string, info fs.FileInfo) {
			if !strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, "_test.go") {
				return
			}
			if data, ok := d.read(rel, info); ok {
				if err := source.parse(rel, data); err != nil {
					d.c.add(CodeParseFailed, rel, "", "", err.Error())
				}
			}
		}
		switch {
		case info.IsDir():
			d.walk(declared, 0, parse)
		case info.Mode().IsRegular() && strings.HasSuffix(declared, ".go"):
			d.count(declared)
			parse(declared, info)
		default:
			d.c.add(CodeFileUnsupported, declared, "directory or .go file", "", "a workflow path must be a directory or a Go file")
			continue
		}
		goNodes, goFlows := source.definitions(d.c)
		for _, misplaced := range goNodes {
			d.c.add(CodeDescriptorMisplaced, misplaced.source, "node under a node directory", "node.Define in workflow path "+declared, "a node is defined inside a workflow path")
		}
		for _, flow := range goFlows {
			if !validIdentity(flow.name, flow.version) {
				d.c.add(CodeDescriptorInvalid, flow.source, "namespace/name and major.minor.patch", flow.name+"@"+flow.version, "the workflow identity is not a stable name and version")
				continue
			}
			workflows = append(workflows, Workflow{Name: flow.name, Version: flow.version, Path: declared, Source: flow.source})
		}
	}
	if len(workflows) > MaxWorkflows {
		d.c.add(CodeLimitExceeded, "", strconv.Itoa(MaxWorkflows), strconv.Itoa(len(workflows)), "too many workflows")
		return nil
	}
	return workflows
}

// checkImports allows a node's Go files to import the standard library,
// other modules, files of the same node however nested, and shared module
// packages. A node importing another node or a workflow fails.
func (d *discoverer) checkImports(project *Project, goImports map[string]map[string][]string) {
	if project.Module == "" {
		return
	}
	workflowPaths := project.Manifest.WorkflowPaths()
	for _, node := range project.Nodes {
		files := make([]string, 0, len(goImports[node.Dir]))
		for file := range goImports[node.Dir] {
			files = append(files, file)
		}
		sort.Strings(files)
		for _, file := range files {
			for _, imported := range goImports[node.Dir][file] {
				target, own := strings.CutPrefix(imported, project.Module+"/")
				if !own {
					continue
				}
				if root, isNode := NodeRoot(target); isNode && root != node.Dir {
					d.c.add(CodeNodeImportsNode, file, "same node, shared package or external module", imported, "a node imports another node")
					continue
				}
				for _, workflow := range workflowPaths {
					if target == workflow || strings.HasPrefix(target, workflow+"/") || target+".go" == workflow {
						d.c.add(CodeNodeImportsWorkflow, file, "same node, shared package or external module", imported, "a node imports a workflow package")
					}
				}
			}
		}
	}
}

// checkIdentities fails a repeated name@version, and a name repeated at a
// different version: an application's source holds one version per name.
func (d *discoverer) checkIdentities(project *Project) {
	type entry struct{ name, version, where string }
	check := func(kind string, entries []entry) {
		sort.Slice(entries, func(i, j int) bool {
			a, b := entries[i], entries[j]
			if a.name != b.name {
				return a.name < b.name
			}
			if a.version != b.version {
				return a.version < b.version
			}
			return a.where < b.where
		})
		for i := 1; i < len(entries); i++ {
			first, current := entries[i-1], entries[i]
			if first.name != current.name {
				continue
			}
			if first.version == current.version {
				d.c.add(CodeDuplicateIdentity, current.where, "unique "+kind+" identity", current.name+"@"+current.version, fmt.Sprintf("%s %s@%s is also declared by %s", kind, current.name, current.version, first.where))
			} else {
				d.c.add(CodeDuplicateVersion, current.where, first.name+"@"+first.version, current.name+"@"+current.version, fmt.Sprintf("%s %s is declared at two versions; %s declares %s", kind, current.name, first.where, first.version))
			}
		}
	}
	nodes := make([]entry, 0, len(project.Nodes))
	for _, node := range project.Nodes {
		nodes = append(nodes, entry{node.Name, node.Version, node.Dir})
	}
	check("node", nodes)
	workflows := make([]entry, 0, len(project.Workflows))
	for _, workflow := range project.Workflows {
		workflows = append(workflows, entry{workflow.Name, workflow.Version, workflow.Source})
	}
	check("workflow", workflows)
}

// checkCollisions fails two owned paths that differ only by letter case:
// they are one file on case-insensitive file systems, so ownership would
// depend on the machine.
func (d *discoverer) checkCollisions(project *Project) {
	var paths []string
	for _, node := range project.Nodes {
		paths = append(paths, node.Dir)
		for _, file := range node.Files {
			paths = append(paths, path.Join(node.Dir, file))
		}
	}
	for _, workflow := range project.Workflows {
		paths = append(paths, sourceFile(workflow.Source))
	}
	for _, collision := range Collisions(paths) {
		d.c.add(CodePathCollision, collision[1], "a path unique ignoring case", collision[0], fmt.Sprintf("%s and %s differ only by letter case", collision[0], collision[1]))
	}
}

// Collisions returns pairs of distinct paths equal ignoring letter case,
// each as {first, later} in sorted order.
func Collisions(paths []string) [][2]string {
	sorted := append([]string(nil), paths...)
	sort.Strings(sorted)
	sorted = slices.Compact(sorted)
	seen := map[string]string{}
	var out [][2]string
	for _, p := range sorted {
		folded := strings.ToLower(p)
		if first, ok := seen[folded]; ok {
			out = append(out, [2]string{first, p})
			continue
		}
		seen[folded] = p
	}
	return out
}

func sourceFile(source string) string {
	if index := strings.LastIndex(source, ":"); index > 0 {
		return source[:index]
	}
	return source
}

// walk visits the regular files under dir in sorted order without following
// links. Hidden and underscore entries, testdata, vendor and node_modules
// are skipped, as the go tool and package managers own them.
func (d *discoverer) walk(dir string, depth int, visit func(rel string, info fs.FileInfo)) {
	if depth > MaxDepth {
		d.c.add(CodeLimitExceeded, dir, strconv.Itoa(MaxDepth), "", "directory nesting exceeds the discovery depth bound")
		return
	}
	entries, ok := d.readDir(dir)
	if !ok {
		return
	}
	for _, entry := range entries {
		name, rel := entry.Name(), path.Join(dir, entry.Name())
		switch {
		case skipped(name):
		case entry.Type()&fs.ModeSymlink != 0:
			d.symlink(rel)
		case entry.IsDir():
			if name == "testdata" || name == "vendor" || name == "node_modules" {
				continue
			}
			if !d.count(rel) {
				return
			}
			d.walk(rel, depth+1, visit)
		case entry.Type().IsRegular():
			if !d.count(rel) {
				return
			}
			info, err := entry.Info()
			if err != nil {
				d.c.add(CodeFileUnsupported, rel, "", "", "file cannot be inspected")
				continue
			}
			visit(rel, info)
		default:
			d.c.add(CodeFileUnsupported, rel, "regular file", entry.Type().String(), "discovery reads only regular files and directories")
		}
	}
}

// realDir reports whether rel exists as a real directory. A link there is
// reported; a missing directory is simply absent.
func (d *discoverer) realDir(rel string) bool {
	info, err := d.root.Lstat(filepath.FromSlash(rel))
	switch {
	case err != nil:
		return false
	case info.Mode()&fs.ModeSymlink != 0:
		d.symlink(rel)
		return false
	}
	return info.IsDir()
}

// realParents reports whether every directory above rel is a real
// directory, reporting the first link. os.Root would otherwise follow an
// in-root link in an intermediate element silently.
func (d *discoverer) realParents(rel string) bool {
	parts := strings.Split(rel, "/")
	for i := 1; i < len(parts); i++ {
		prefix := strings.Join(parts[:i], "/")
		info, err := d.root.Lstat(filepath.FromSlash(prefix))
		if err != nil {
			return true // missing: reported as a missing workflow path
		}
		if info.Mode()&fs.ModeSymlink != 0 {
			d.symlink(prefix)
			return false
		}
	}
	return true
}

func (d *discoverer) count(rel string) bool {
	d.files++
	if d.files == d.entryLimit+1 {
		d.c.add(CodeLimitExceeded, rel, strconv.Itoa(d.entryLimit), "", "the project has more files and directories than discovery visits")
	}
	return d.files <= d.entryLimit
}

func skipped(name string) bool { return strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") }

func (d *discoverer) readDir(dir string) ([]fs.DirEntry, bool) {
	file, err := d.root.Open(filepath.FromSlash(dir))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false
	}
	if err != nil {
		d.c.add(CodeFileUnsupported, dir, "", "", "directory cannot be read")
		return nil, false
	}
	defer file.Close()
	entries, err := file.ReadDir(MaxDirEntries + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		d.c.add(CodeFileUnsupported, dir, "", "", "directory cannot be read")
		return nil, false
	}
	if len(entries) > MaxDirEntries {
		d.c.add(CodeLimitExceeded, dir, strconv.Itoa(MaxDirEntries), "", "a directory lists more entries than discovery reads")
		return nil, false
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	return entries, true
}

// read returns a regular file's bytes through the os.Root, bounded per
// file, per node and per discovery. The file is opened non-blocking where
// the platform allows and its type is checked again on the open handle, so
// a FIFO or device swapped in after the walk cannot block discovery; a
// hard-linked file is refused because its other name may sit outside the
// project.
func (d *discoverer) read(rel string, listed fs.FileInfo) ([]byte, bool) {
	if !listed.Mode().IsRegular() {
		d.c.add(CodeFileUnsupported, rel, "regular file", listed.Mode().String(), "discovery reads only regular files")
		return nil, false
	}
	file, err := d.root.OpenFile(filepath.FromSlash(rel), openFlags, 0)
	if err != nil {
		d.c.add(CodeFileUnsupported, rel, "", "", "file cannot be opened")
		return nil, false
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		d.c.add(CodeFileUnsupported, rel, "regular file", "changed during discovery", "the file is no longer a regular file")
		return nil, false
	}
	if links, known := linkCount(file); known && links > 1 {
		d.c.add(CodeFileUnsupported, rel, "one link", strconv.FormatUint(links, 10)+" links", "a hard-linked file may be a second name for a file outside the project")
		return nil, false
	}
	size := info.Size()
	switch {
	case size > MaxFileBytes:
		d.c.add(CodeLimitExceeded, rel, strconv.Itoa(MaxFileBytes), strconv.FormatInt(size, 10), "file exceeds the discovery size bound")
		return nil, false
	case d.nodeBytes >= 0 && d.nodeBytes+size > MaxNodeBytes:
		if d.nodeBytes <= MaxNodeBytes {
			d.c.add(CodeLimitExceeded, rel, strconv.Itoa(MaxNodeBytes), "", "the node's source exceeds the per-node read budget")
		}
		d.nodeBytes = MaxNodeBytes + 1 // report once per node
		return nil, false
	case d.bytes+size > MaxTotalBytes:
		if d.bytes <= MaxTotalBytes {
			d.c.add(CodeLimitExceeded, rel, strconv.Itoa(MaxTotalBytes), "", "the project's source exceeds the discovery read budget")
		}
		d.bytes = MaxTotalBytes + 1
		return nil, false
	}
	data, err := io.ReadAll(io.LimitReader(file, size+1))
	if err != nil || int64(len(data)) > size {
		d.c.add(CodeFileUnsupported, rel, "", "", "file changed while it was read")
		return nil, false
	}
	d.bytes += int64(len(data))
	if d.nodeBytes >= 0 {
		d.nodeBytes += int64(len(data))
	}
	return data, true
}

// symlink reports a link without following it.
func (d *discoverer) symlink(rel string) {
	code := d.classifyLink(rel)
	messages := map[string]string{
		CodeSymlinkEscape:   "a link points outside the project root",
		CodeSymlinkAlias:    "a link is a second path to source inside the project",
		CodeSymlinkDangling: "a link points to nothing inside the project",
		CodeSymlinkLoop:     "a link never resolves: it is part of a cycle",
	}
	d.c.add(code, rel, "regular file or directory", "symlink", messages[code])
}

// maxLinkHops bounds link resolution, like the kernel's ELOOP bound; a
// cycle exhausts it.
const maxLinkHops = 40

// classifyLink resolves the link at rel one path element at a time, the way
// the kernel does, using only the os.Root's Lstat and Readlink on
// symlink-free prefixes. A ".." above the root, or an absolute target
// outside it, is an escape decided lexically: nothing outside the root is
// ever stat-ed or read, so classification cannot reveal whether an outside
// path exists.
func (d *discoverer) classifyLink(rel string) string {
	pending := strings.Split(rel, "/")
	var resolved []string // symlink-free elements below the root
	for hops := 0; len(pending) > 0; {
		element := pending[0]
		pending = pending[1:]
		switch element {
		case "", ".":
			continue
		case "..":
			if len(resolved) == 0 {
				return CodeSymlinkEscape
			}
			resolved = resolved[:len(resolved)-1]
			continue
		}
		candidate := path.Join(append(append([]string(nil), resolved...), element)...)
		info, err := d.root.Lstat(filepath.FromSlash(candidate))
		if err != nil {
			return CodeSymlinkDangling
		}
		if info.Mode()&fs.ModeSymlink == 0 {
			resolved = append(resolved, element)
			continue
		}
		if hops++; hops > maxLinkHops {
			return CodeSymlinkLoop
		}
		target, err := d.root.Readlink(filepath.FromSlash(candidate))
		if err != nil {
			return CodeSymlinkDangling
		}
		if filepath.IsAbs(target) || filepath.VolumeName(target) != "" {
			inside, err := filepath.Rel(d.abs, filepath.Clean(target))
			if err != nil || d.abs == "" {
				return CodeSymlinkEscape
			}
			resolved, target = nil, inside
		}
		pending = append(strings.Split(filepath.ToSlash(target), "/"), pending...)
	}
	return CodeSymlinkAlias
}
