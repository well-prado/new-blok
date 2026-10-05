package ownership

import (
	"bytes"
	"encoding/json"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/well-prado/new-blok/internal/tooling/layout"
)

// nodeAdapter resolves Node.js and TypeScript imports from source alone: a
// file is a unit, every import form the lexer recognizes is resolved with
// Node's and TypeScript's rules (relative paths with TypeScript's extension
// substitution, tsconfig paths and baseUrl, package.json exports, imports
// and main, local and workspace packages, file:/link: dependencies), and the
// union of every candidate any of those tools could pick is checked, so a
// declaration file and a runtime file cannot disagree unnoticed. Nothing is
// installed, built or run; node_modules is read only to recognise an
// installed third-party package.
type nodeAdapter struct {
	files    *projectFiles
	project  *layout.Project
	packages map[string]*packageJSON // directory → package.json, nil when absent
	configs  map[string]*tsOptions   // directory → effective tsconfig options
	local    map[string][]string     // package name → local package directories
}

const nodeRuntime = "nodejs"

func newNodeAdapter(files *projectFiles, project *layout.Project) *nodeAdapter {
	return &nodeAdapter{files: files, project: project, packages: map[string]*packageJSON{}, configs: map[string]*tsOptions{}}
}

func (a *nodeAdapter) Runtime() string { return nodeRuntime }

var (
	scriptExtensions      = []string{".ts", ".mts", ".cts", ".js", ".mjs", ".cjs"}
	unsupportedExtensions = []string{".jsx", ".tsx", ".node", ".wasm"}
	// probeExtensions are appended to an extension-less specifier, in
	// TypeScript's then Node's order; every existing candidate is a target.
	probeExtensions = []string{".ts", ".tsx", ".d.ts", ".mts", ".d.mts", ".cts", ".d.cts", ".js", ".jsx", ".mjs", ".cjs", ".json", ".node"}
)

func hasExtension(name string, extensions []string) bool {
	for _, extension := range extensions {
		if strings.HasSuffix(name, extension) {
			return true
		}
	}
	return false
}

// Roots are the node's script files, except test files (*.test.*,
// *.spec.*), which, like Go's _test.go files, are not part of the node.
// A test file reached by an import is analyzed like any other file.
func (a *nodeAdapter) Roots(node layout.Node) []string {
	var roots []string
	for _, file := range node.Files {
		name := path.Base(file)
		if !hasExtension(name, scriptExtensions) && !hasExtension(name, unsupportedExtensions[:2]) {
			continue
		}
		if strings.Contains(name, ".test.") || strings.Contains(name, ".spec.") {
			continue
		}
		roots = append(roots, path.Join(node.Dir, file))
	}
	sort.Strings(roots)
	return roots
}

func (a *nodeAdapter) Analyze(file string) Analysis {
	var analysis Analysis
	name := path.Base(file)
	switch {
	case hasExtension(name, unsupportedExtensions):
		analysis.Findings = append(analysis.Findings, finding(CodeSourceUnsupported, file, path.Ext(name), "JSX, native addons and WebAssembly have no checked import grammar here; this file's imports cannot be verified"))
		return analysis
	case !hasExtension(name, scriptExtensions):
		return analysis // JSON and assets import nothing
	}
	data, problem := a.files.read(file)
	if problem != nil {
		analysis.Findings = append(analysis.Findings, *problem)
		return analysis
	}
	scan, err := scanJS(data)
	if err != nil {
		line := 0
		if lexErr, ok := err.(*lexError); ok {
			line = lexErr.line
		}
		analysis.Findings = append(analysis.Findings, finding(CodeParseFailed, at(file, line), "", "JavaScript/TypeScript lexical error; the file's imports cannot be read"))
		return analysis
	}
	if len(scan.imports) > maxEdgesPerFile {
		analysis.Findings = append(analysis.Findings, finding(CodeLimitExceeded, file, strconv.Itoa(len(scan.imports)), "the file has more imports than the ownership check resolves"))
		return analysis
	}
	for _, form := range scan.unverified {
		message := "a computed specifier cannot be resolved statically"
		if form.code == CodeUnsupportedForm {
			message = "this form loads or evaluates code without a static import; it cannot be verified"
		}
		analysis.Findings = append(analysis.Findings, finding(form.code, at(file, form.line), form.form, message))
	}
	for _, imported := range scan.imports {
		source := at(file, imported.line)
		var r resolved
		switch {
		case imported.form == "reference" && !imported.types:
			if rel, inside := cleanJoin(path.Dir(file), imported.specifier); inside {
				a.resolvePath(rel, source, &r)
			} else {
				r.findings = append(r.findings, finding(CodeImportOutsideRoot, source, imported.specifier, "the reference leaves the project root"))
			}
		default:
			a.resolveSpecifier(file, imported.specifier, source, &r, 0)
		}
		analysis.Findings = append(analysis.Findings, r.findings...)
		if len(r.targets) > 0 {
			analysis.Edges = append(analysis.Edges, Edge{Source: source, Specifier: imported.specifier, Targets: r.targets})
		}
	}
	return analysis
}

func (a *nodeAdapter) unresolved(r *resolved, source, specifier, why string) {
	r.findings = append(r.findings, finding(CodeImportUnresolved, source, specifier, why))
}

// resolveSpecifier resolves one specifier from file into r.
func (a *nodeAdapter) resolveSpecifier(file, specifier, source string, r *resolved, depth int) {
	if depth > maxResolutionDepth {
		r.findings = append(r.findings, finding(CodeLimitExceeded, source, specifier, "import resolution nests deeper than the ownership bound"))
		return
	}
	dir := path.Dir(file)
	switch {
	case specifier == "":
		a.unresolved(r, source, specifier, "an empty specifier names nothing")
	case strings.HasPrefix(specifier, "data:"), strings.HasPrefix(specifier, "http:"), strings.HasPrefix(specifier, "https:"):
		r.findings = append(r.findings, finding(CodeUnsupportedForm, source, specifier, "a data: or network URL import is code outside the project's source"))
	case strings.HasPrefix(specifier, "/"), strings.HasPrefix(specifier, "file:"), strings.Contains(specifier, "\\"), len(specifier) > 1 && specifier[1] == ':':
		r.findings = append(r.findings, finding(CodeImportOutsideRoot, source, specifier, "an absolute path or URL import cannot be checked against project ownership"))
	case specifier == "." || specifier == ".." || strings.HasPrefix(specifier, "./") || strings.HasPrefix(specifier, "../"):
		rel, inside := cleanJoin(dir, specifier)
		if !inside {
			r.findings = append(r.findings, finding(CodeImportOutsideRoot, source, specifier, "the relative import leaves the project root"))
			return
		}
		if !a.resolvePath(rel, source, r) {
			a.unresolved(r, source, specifier, "no file matches the relative import")
		}
	case strings.HasPrefix(specifier, "#"):
		a.resolveImportsField(file, specifier, source, r, depth)
	default:
		if name, builtin := builtinName(specifier); builtin {
			base, _, _ := strings.Cut(name, "/")
			if loaderModules[base] {
				r.findings = append(r.findings, finding(CodeUnsupportedForm, source, specifier, "this built-in module loads or evaluates code by computed name; it cannot be verified"))
			}
			return
		}
		a.resolveBare(file, specifier, source, r, depth)
	}
}

// resolvePath resolves a project-relative path the way TypeScript and Node
// do together: the exact file, TypeScript's source for a .js/.mjs/.cjs
// specifier, every probe extension for any specifier, and a directory's
// package.json main and index files. It reports whether anything resolved.
func (a *nodeAdapter) resolvePath(rel, source string, r *resolved) bool {
	candidates := []string{rel}
	for from, to := range map[string][]string{".js": {".ts", ".tsx", ".d.ts"}, ".mjs": {".mts", ".d.mts"}, ".cjs": {".cts", ".d.cts"}, ".jsx": {".tsx"}} {
		if base, ok := strings.CutSuffix(rel, from); ok {
			for _, extension := range to {
				candidates = append(candidates, base+extension)
			}
		}
	}
	for _, extension := range probeExtensions {
		candidates = append(candidates, rel+extension)
	}
	sort.Strings(candidates[1:])
	found := false
	for _, candidate := range candidates {
		e := a.files.lookup(candidate)
		switch e.kind {
		case kindFile:
			if !r.accept(e, source) {
				continue
			}
			if !e.caseMismatch {
				r.target(e.path, false)
			}
			found = true
		case kindDir:
			if candidate != rel {
				continue
			}
			if e.caseMismatch {
				r.accept(e, source)
				found = true
				continue
			}
			if a.resolveDirectory(e.path, source, r) {
				found = true
			}
		case kindLink, kindLimit:
			r.accept(e, source)
			found = true
		case kindOther:
			r.findings = append(r.findings, finding(CodeSourceUnsupported, source, e.path, "the import names a special file"))
			found = true
		}
	}
	return found
}

// resolveDirectory resolves a directory import: package.json main fields,
// then index files (CommonJS directory resolution).
func (a *nodeAdapter) resolveDirectory(dir, source string, r *resolved) bool {
	found := false
	if pkg := a.packageJSON(dir, r); pkg != nil {
		for _, field := range pkg.mainFields() {
			if rel, inside := cleanJoin(dir, field); inside && rel != dir && a.resolvePath(rel, source, r) {
				found = true
			}
		}
	}
	if a.resolvePath(path.Join(dir, "index"), source, r) {
		found = true
	}
	return found
}

// splitPackage splits a bare specifier into its package name and subpath.
func splitPackage(specifier string) (name, subpath string, ok bool) {
	parts := strings.SplitN(specifier, "/", 3)
	if strings.HasPrefix(specifier, "@") {
		if len(parts) < 2 || parts[0] == "@" || parts[1] == "" {
			return "", "", false
		}
		name = parts[0] + "/" + parts[1]
		if len(parts) == 3 {
			subpath = parts[2]
		}
	} else {
		name = parts[0]
		subpath = strings.TrimPrefix(specifier, name)
		subpath = strings.TrimPrefix(subpath, "/")
	}
	if name == "" || strings.HasPrefix(name, ".") || strings.ContainsAny(name, "%\\") {
		return "", "", false
	}
	return name, subpath, true
}

// resolveBare resolves a bare specifier: tsconfig paths and baseUrl, then a
// local, workspace or self package, then a declared dependency, then an
// installed package.
func (a *nodeAdapter) resolveBare(file, specifier, source string, r *resolved, depth int) {
	dir := path.Dir(file)
	options := a.config(dir, r)
	if options != nil {
		if substitutions, matched := options.match(specifier); matched {
			resolvedAny := false
			for _, substitution := range substitutions {
				if rel, inside := cleanJoin(options.pathsBase(), substitution); inside && a.resolvePath(rel, source, r) {
					resolvedAny = true
				}
			}
			if resolvedAny {
				return
			}
		}
		if options.hasBaseURL {
			if rel, inside := cleanJoin(options.baseURL, specifier); inside && a.resolvePath(rel, source, r) {
				return
			}
		}
	}
	name, subpath, ok := splitPackage(specifier)
	if !ok {
		a.unresolved(r, source, specifier, "the specifier is not a valid package name")
		return
	}
	if dirs := a.localPackages(name, dir, r); len(dirs) > 0 {
		for _, packageDir := range dirs {
			a.resolvePackage(packageDir, subpath, specifier, source, r)
		}
		return
	}
	if version, declaredIn, declared := a.dependency(name, dir, r); declared {
		for _, prefix := range []string{"file:", "link:", "portal:"} {
			if target, local := strings.CutPrefix(version, prefix); local {
				packageDir, inside := cleanJoin(declaredIn, target)
				if !inside {
					r.findings = append(r.findings, finding(CodeImportOutsideRoot, source, specifier, "the dependency "+name+" is a local path outside the project root"))
					return
				}
				a.resolvePackage(packageDir, subpath, specifier, source, r)
				return
			}
		}
		if strings.HasPrefix(version, "workspace:") {
			a.unresolved(r, source, specifier, "the workspace dependency "+name+" matches no workspace package")
		}
		return // a registry, git or tarball dependency: external
	}
	for current := dir; ; current = path.Dir(current) {
		if current == "." {
			current = ""
		}
		e := a.files.lookup(path.Join(current, "node_modules", name))
		switch e.kind {
		case kindDir, kindFile:
			return // an installed third-party package: external
		case kindLink:
			switch e.linkCode {
			case layout.CodeSymlinkAlias:
				if a.owned(e.linkTarget) {
					r.findings = append(r.findings, linkFinding(e, source))
					r.target(e.linkTarget, true)
				}
				return
			case layout.CodeSymlinkEscape:
				return // a package manager store outside the project: external
			}
			r.findings = append(r.findings, linkFinding(e, source))
			return
		}
		if current == "" {
			break
		}
	}
	a.unresolved(r, source, specifier, "the package is not a local, workspace or declared package and is not installed")
}

// owned reports a path inside a node directory or a workflow path.
func (a *nodeAdapter) owned(rel string) bool {
	if _, ok := layout.NodeRoot(rel); ok {
		return true
	}
	for _, workflow := range a.project.Manifest.WorkflowPaths() {
		if rel == workflow || strings.HasPrefix(rel, workflow+"/") {
			return true
		}
	}
	return false
}

// resolvePackage resolves a subpath of a local package directory through
// its exports, or its main fields and files when it has none.
func (a *nodeAdapter) resolvePackage(dir, subpath, specifier, source string, r *resolved) {
	pkg := a.packageJSON(dir, r)
	if pkg != nil && pkg.Exports != nil {
		key := "."
		if subpath != "" {
			key = "./" + subpath
		}
		targets, matched := mapTargets(pkg.Exports, key, true)
		if !matched {
			a.unresolved(r, source, specifier, "package "+dir+" does not export "+key)
			return
		}
		resolvedAny := false
		for _, target := range targets {
			rel, inside := cleanJoin(dir, target)
			if !strings.HasPrefix(target, "./") || !inside {
				r.findings = append(r.findings, finding(CodeConfigInvalid, path.Join(dir, "package.json"), target, "an exports target must be a ./ path inside the package"))
				continue
			}
			if a.resolvePath(rel, source, r) {
				resolvedAny = true
			}
		}
		if !resolvedAny {
			a.unresolved(r, source, specifier, "no exported file of package "+dir+" exists")
		}
		return
	}
	if subpath == "" {
		if !a.resolveDirectory(dir, source, r) {
			a.unresolved(r, source, specifier, "package "+dir+" has no main file")
		}
		return
	}
	rel, inside := cleanJoin(dir, subpath)
	if !inside || !a.resolvePath(rel, source, r) {
		a.unresolved(r, source, specifier, "no file of package "+dir+" matches the subpath")
	}
}

// resolveImportsField resolves a #specifier through the nearest
// package.json's imports.
func (a *nodeAdapter) resolveImportsField(file, specifier, source string, r *resolved, depth int) {
	scope, pkg := a.scope(path.Dir(file), r)
	if pkg == nil || pkg.Imports == nil {
		a.unresolved(r, source, specifier, "no package.json imports field defines this specifier")
		return
	}
	targets, matched := mapTargets(pkg.Imports, specifier, false)
	if !matched {
		a.unresolved(r, source, specifier, "the package.json imports field does not define this specifier")
		return
	}
	for _, target := range targets {
		if strings.HasPrefix(target, "./") {
			rel, inside := cleanJoin(scope, target)
			if !inside || !a.resolvePath(rel, source, r) {
				a.unresolved(r, source, specifier, "the imports target "+target+" does not exist")
			}
			continue
		}
		// An imports target may name a package.
		a.resolveSpecifier(path.Join(scope, "package.json"), target, source, r, depth+1)
	}
}

// packageJSON is the subset of package.json resolution reads.
type packageJSON struct {
	Name                 string            `json:"name"`
	Main                 any               `json:"main"`
	Module               any               `json:"module"`
	Types                any               `json:"types"`
	Typings              any               `json:"typings"`
	Exports              any               `json:"exports"`
	Imports              any               `json:"imports"`
	Workspaces           any               `json:"workspaces"`
	Dependencies         map[string]string `json:"dependencies"`
	DevDependencies      map[string]string `json:"devDependencies"`
	PeerDependencies     map[string]string `json:"peerDependencies"`
	OptionalDependencies map[string]string `json:"optionalDependencies"`
}

func (p *packageJSON) mainFields() []string {
	var out []string
	for _, field := range []any{p.Main, p.Module, p.Types, p.Typings} {
		if value, ok := field.(string); ok && value != "" {
			out = append(out, value)
		}
	}
	return out
}

// packageJSON reads dir/package.json once; nil when absent or invalid
// (invalid is reported).
func (a *nodeAdapter) packageJSON(dir string, r *resolved) *packageJSON {
	if cached, ok := a.packages[dir]; ok {
		return cached
	}
	a.packages[dir] = nil
	rel := path.Join(dir, "package.json")
	e := a.files.lookup(rel)
	if e.kind != kindFile || e.caseMismatch {
		if e.kind == kindLink {
			r.findings = append(r.findings, linkFinding(e, rel))
		}
		return nil
	}
	data, problem := a.files.read(rel)
	if problem != nil {
		r.findings = append(r.findings, *problem)
		return nil
	}
	var pkg packageJSON
	if err := json.Unmarshal(data, &pkg); err != nil {
		r.findings = append(r.findings, finding(CodeConfigInvalid, rel, "", "package.json is not valid JSON"))
		return nil
	}
	a.packages[dir] = &pkg
	return &pkg
}

// scope returns the nearest package.json at or above dir.
func (a *nodeAdapter) scope(dir string, r *resolved) (string, *packageJSON) {
	for current := dir; ; current = path.Dir(current) {
		if current == "." {
			current = ""
		}
		if pkg := a.packageJSON(current, r); pkg != nil {
			return current, pkg
		}
		if current == "" {
			return "", nil
		}
	}
}

// localPackages returns the directories of project packages named name:
// a package.json inside any node directory, a root workspace, or the
// importing file's own package (self-reference).
func (a *nodeAdapter) localPackages(name, dir string, r *resolved) []string {
	if a.local == nil {
		a.local = map[string][]string{}
		for _, node := range a.project.Nodes {
			for _, file := range node.Files {
				if path.Base(file) == "package.json" {
					packageDir := path.Dir(path.Join(node.Dir, file))
					if pkg := a.packageJSON(packageDir, r); pkg != nil && pkg.Name != "" {
						a.local[pkg.Name] = appendUnique(a.local[pkg.Name], packageDir)
					}
				}
			}
		}
		if root := a.packageJSON("", r); root != nil {
			for _, workspace := range a.workspaceDirs(root) {
				if pkg := a.packageJSON(workspace, r); pkg != nil && pkg.Name != "" {
					a.local[pkg.Name] = appendUnique(a.local[pkg.Name], workspace)
				}
			}
		}
		for _, dirs := range a.local {
			sort.Strings(dirs)
		}
	}
	dirs := append([]string(nil), a.local[name]...)
	if scopeDir, pkg := a.scope(dir, r); pkg != nil && pkg.Name == name {
		dirs = appendUnique(dirs, scopeDir)
	}
	return dirs
}

// workspaceDirs expands the root package.json workspaces: a directory, or
// a directory followed by /* or /** (one level of package directories).
func (a *nodeAdapter) workspaceDirs(root *packageJSON) []string {
	var patterns []string
	switch value := root.Workspaces.(type) {
	case []any:
		for _, item := range value {
			if s, ok := item.(string); ok {
				patterns = append(patterns, s)
			}
		}
	case map[string]any:
		if list, ok := value["packages"].([]any); ok {
			for _, item := range list {
				if s, ok := item.(string); ok {
					patterns = append(patterns, s)
				}
			}
		}
	}
	var dirs []string
	for _, pattern := range patterns {
		base, wildcard := strings.CutSuffix(pattern, "/**")
		if !wildcard {
			base, wildcard = strings.CutSuffix(pattern, "/*")
		}
		clean, inside := cleanJoin("", base)
		if !inside || strings.Contains(clean, "*") {
			continue
		}
		if !wildcard {
			dirs = append(dirs, clean)
			continue
		}
		l := a.files.list(clean)
		for name, mode := range l.entries {
			if mode.IsDir() && name != "node_modules" && !strings.HasPrefix(name, ".") {
				dirs = append(dirs, path.Join(clean, name))
			}
		}
	}
	sort.Strings(dirs)
	return dirs
}

// dependency finds name in the dependencies of the package.json files at
// or above dir, nearest first.
func (a *nodeAdapter) dependency(name, dir string, r *resolved) (version, declaredIn string, ok bool) {
	for current := dir; ; current = path.Dir(current) {
		if current == "." {
			current = ""
		}
		if pkg := a.packageJSON(current, r); pkg != nil {
			for _, deps := range []map[string]string{pkg.Dependencies, pkg.DevDependencies, pkg.PeerDependencies, pkg.OptionalDependencies} {
				if version, found := deps[name]; found {
					return version, current, true
				}
			}
		}
		if current == "" {
			return "", "", false
		}
	}
}

// mapTargets applies Node's exports/imports subpath matching to key and
// returns every target any condition could select (conditions are a
// union, so "types", "import", "require" and "default" are all checked).
// matched is false when key is not defined.
func mapTargets(field any, key string, exports bool) ([]string, bool) {
	var table map[string]any
	switch value := field.(type) {
	case map[string]any:
		sugar := exports
		for k := range value {
			if strings.HasPrefix(k, ".") || strings.HasPrefix(k, "#") {
				sugar = false
			}
		}
		if sugar && len(value) > 0 {
			table = map[string]any{".": value}
		} else {
			table = value
		}
	default:
		if !exports {
			return nil, false
		}
		table = map[string]any{".": value}
	}
	if value, ok := table[key]; ok && !strings.Contains(key, "*") {
		return collectTargets(value, "", 0), true
	}
	best, bestStar := "", ""
	for pattern := range table {
		prefix, suffix, star := strings.Cut(pattern, "*")
		if !star || strings.Contains(suffix, "*") {
			continue
		}
		if strings.HasPrefix(key, prefix) && strings.HasSuffix(key, suffix) && len(key) >= len(prefix)+len(suffix) && key != prefix {
			if len(prefix) > len(strings.SplitN(best, "*", 2)[0]) || best == "" || (len(prefix) == len(strings.SplitN(best, "*", 2)[0]) && len(pattern) > len(best)) {
				best, bestStar = pattern, key[len(prefix):len(key)-len(suffix)]
			}
		}
	}
	if best != "" {
		return collectTargets(table[best], bestStar, 0), true
	}
	return nil, false
}

func collectTargets(value any, star string, depth int) []string {
	if depth > maxConfigDepth*4 {
		return nil
	}
	switch v := value.(type) {
	case string:
		return []string{strings.ReplaceAll(v, "*", star)}
	case []any:
		var out []string
		for _, item := range v {
			out = append(out, collectTargets(item, star, depth+1)...)
		}
		return out
	case map[string]any:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var out []string
		for _, k := range keys {
			out = append(out, collectTargets(v[k], star, depth+1)...)
		}
		return out
	}
	return nil
}

// tsOptions is the effective tsconfig resolution configuration.
type tsOptions struct {
	baseURL    string
	hasBaseURL bool
	paths      map[string][]string
	pathsDir   string // directory of the config that declared paths
}

func (o *tsOptions) pathsBase() string {
	if o.hasBaseURL {
		return o.baseURL
	}
	return o.pathsDir
}

// match applies TypeScript's paths matching: an exact key, else the
// pattern with the longest prefix before its "*".
func (o *tsOptions) match(specifier string) ([]string, bool) {
	if o.paths == nil {
		return nil, false
	}
	if substitutions, ok := o.paths[specifier]; ok {
		return substitutions, true
	}
	best, bestPrefix, star := "", -1, ""
	for pattern := range o.paths {
		prefix, suffix, hasStar := strings.Cut(pattern, "*")
		if !hasStar || !strings.HasPrefix(specifier, prefix) || !strings.HasSuffix(specifier, suffix) || len(specifier) < len(prefix)+len(suffix) {
			continue
		}
		if len(prefix) > bestPrefix || (len(prefix) == bestPrefix && pattern < best) {
			best, bestPrefix, star = pattern, len(prefix), specifier[len(prefix):len(specifier)-len(suffix)]
		}
	}
	if best == "" {
		return nil, false
	}
	var out []string
	for _, substitution := range o.paths[best] {
		out = append(out, strings.ReplaceAll(substitution, "*", star))
	}
	return out, true
}

// config returns the options of the nearest tsconfig.json (or
// jsconfig.json) at or above dir, or nil.
func (a *nodeAdapter) config(dir string, r *resolved) *tsOptions {
	if cached, ok := a.configs[dir]; ok {
		return cached
	}
	var options *tsOptions
	for _, name := range []string{"tsconfig.json", "jsconfig.json"} {
		rel := path.Join(dir, name)
		if e := a.files.lookup(rel); e.kind == kindFile && !e.caseMismatch {
			loaded := a.loadConfig(rel, 0, map[string]bool{}, r)
			options = &loaded
			break
		} else if e.kind == kindLink {
			r.findings = append(r.findings, linkFinding(e, rel))
		}
	}
	if options == nil && dir != "" && dir != "." {
		parent := path.Dir(dir)
		if parent == "." {
			parent = ""
		}
		options = a.config(parent, r)
	}
	a.configs[dir] = options
	return options
}

type tsconfigFile struct {
	Extends         any `json:"extends"`
	CompilerOptions struct {
		BaseURL *string             `json:"baseUrl"`
		Paths   map[string][]string `json:"paths"`
	} `json:"compilerOptions"`
}

// loadConfig reads a tsconfig and the configs it extends; the extending
// config's baseUrl and paths override its bases'.
func (a *nodeAdapter) loadConfig(rel string, depth int, visiting map[string]bool, r *resolved) tsOptions {
	var options tsOptions
	if depth > maxConfigDepth || visiting[rel] {
		r.findings = append(r.findings, finding(CodeConfigInvalid, rel, "", "tsconfig extends chain is cyclic or deeper than the ownership bound"))
		return options
	}
	visiting[rel] = true
	data, problem := a.files.read(rel)
	if problem != nil {
		r.findings = append(r.findings, *problem)
		return options
	}
	var config tsconfigFile
	if err := json.Unmarshal(jsonc(data), &config); err != nil {
		r.findings = append(r.findings, finding(CodeConfigInvalid, rel, "", "tsconfig is not valid JSON with comments"))
		return options
	}
	dir := path.Dir(rel)
	if dir == "." {
		dir = ""
	}
	var bases []string
	switch value := config.Extends.(type) {
	case string:
		bases = []string{value}
	case []any:
		for _, item := range value {
			if s, ok := item.(string); ok {
				bases = append(bases, s)
			}
		}
	}
	for _, base := range bases {
		baseRel, found := a.extendsPath(dir, base, rel, r)
		if !found {
			continue
		}
		inherited := a.loadConfig(baseRel, depth+1, visiting, r)
		if inherited.hasBaseURL {
			options.baseURL, options.hasBaseURL = inherited.baseURL, true
		}
		if inherited.paths != nil {
			options.paths, options.pathsDir = inherited.paths, inherited.pathsDir
		}
	}
	if config.CompilerOptions.BaseURL != nil {
		if baseURL, inside := cleanJoin(dir, *config.CompilerOptions.BaseURL); inside {
			options.baseURL, options.hasBaseURL = baseURL, true
		} else {
			r.findings = append(r.findings, finding(CodeConfigInvalid, rel, *config.CompilerOptions.BaseURL, "tsconfig baseUrl leaves the project root"))
		}
	}
	if config.CompilerOptions.Paths != nil {
		options.paths, options.pathsDir = config.CompilerOptions.Paths, dir
	}
	delete(visiting, rel)
	return options
}

// extendsPath locates an extended config: a relative path (".json" added
// when missing), or a package's config under node_modules. A package config
// that is not installed is skipped: it cannot name project paths.
func (a *nodeAdapter) extendsPath(dir, base, from string, r *resolved) (string, bool) {
	if strings.HasPrefix(base, "./") || strings.HasPrefix(base, "../") {
		rel, inside := cleanJoin(dir, base)
		if !inside {
			r.findings = append(r.findings, finding(CodeConfigInvalid, from, base, "tsconfig extends a file outside the project root"))
			return "", false
		}
		for _, candidate := range []string{rel, rel + ".json"} {
			if e := a.files.lookup(candidate); e.kind == kindFile && !e.caseMismatch {
				return candidate, true
			}
		}
		r.findings = append(r.findings, finding(CodeConfigInvalid, from, base, "tsconfig extends a file that does not exist"))
		return "", false
	}
	for current := dir; ; current = path.Dir(current) {
		if current == "." {
			current = ""
		}
		for _, candidate := range []string{base, base + ".json", path.Join(base, "tsconfig.json")} {
			rel := path.Join(current, "node_modules", candidate)
			if e := a.files.lookup(rel); e.kind == kindFile && !e.caseMismatch {
				return rel, true
			}
		}
		if current == "" {
			return "", false
		}
	}
}

// jsonc removes comments and then trailing commas from JSON with
// comments, as tsconfig allows, leaving string contents untouched.
func jsonc(data []byte) []byte {
	return withoutTrailingCommas(withoutComments(data))
}

func withoutComments(data []byte) []byte {
	var out bytes.Buffer
	inString := false
	for i := 0; i < len(data); i++ {
		c := data[i]
		switch {
		case inString:
			out.WriteByte(c)
			if c == '\\' && i+1 < len(data) {
				i++
				out.WriteByte(data[i])
			} else if c == '"' {
				inString = false
			}
		case c == '"':
			inString = true
			out.WriteByte(c)
		case c == '/' && i+1 < len(data) && data[i+1] == '/':
			for i < len(data) && data[i] != '\n' {
				i++
			}
			out.WriteByte('\n')
		case c == '/' && i+1 < len(data) && data[i+1] == '*':
			end := bytes.Index(data[i+2:], []byte("*/"))
			if end < 0 {
				return []byte("invalid")
			}
			i += end + 3
			out.WriteByte(' ')
		default:
			out.WriteByte(c)
		}
	}
	return out.Bytes()
}

func withoutTrailingCommas(data []byte) []byte {
	var out bytes.Buffer
	inString := false
	for i := 0; i < len(data); i++ {
		c := data[i]
		switch {
		case inString:
			out.WriteByte(c)
			if c == '\\' && i+1 < len(data) {
				i++
				out.WriteByte(data[i])
			} else if c == '"' {
				inString = false
			}
		case c == '"':
			inString = true
			out.WriteByte(c)
		case c == ',':
			j := i + 1
			for j < len(data) && (data[j] == ' ' || data[j] == '\t' || data[j] == '\n' || data[j] == '\r') {
				j++
			}
			if j < len(data) && (data[j] == '}' || data[j] == ']') {
				continue
			}
			out.WriteByte(c)
		default:
			out.WriteByte(c)
		}
	}
	return out.Bytes()
}
