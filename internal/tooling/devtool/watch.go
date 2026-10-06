package devtool

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/scanner"
	"go/token"
	"io"
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

// Watch bounds. A scan past either fails with layout_limit_exceeded rather
// than reading without end.
const (
	// MaxWatchedFiles bounds the files blok dev watches, the same bound
	// goPackages applies to Go files.
	MaxWatchedFiles = MaxSourceFiles
	// MaxWatchEntries bounds the directory entries one scan visits.
	MaxWatchEntries = 100000
)

// stamp is what a scan records about one watched path: enough to see an
// edit, a replacement (an editor's write-and-rename), a deletion or a
// retargeted link, without reading the file. On Unix it adds the inode and
// the status-change time, which an edit that keeps the size and restores
// the modification time (touch -r, cp -p, rsync -a, tar -x) still changes;
// elsewhere both are zero (stamp_other.go).
type stamp struct {
	size    int64
	modTime int64
	mode    fs.FileMode
	inode   uint64
	change  int64
}

func stampOf(info fs.FileInfo) stamp {
	inode, change := fileIdentity(info)
	return stamp{size: info.Size(), modTime: info.ModTime().UnixNano(), mode: info.Mode(), inode: inode, change: change}
}

// snapshot maps project-relative, slash-separated paths to their stamps.
type snapshot map[string]stamp

// watchedFile reports whether a project-relative file is one blok dev
// rebuilds for: the manifest and the module files, the module's non-test Go
// source, and every file a node directory owns (ADR 0023's node roots, so a
// foreign node's node.json and sources are watched too). Test files are not
// built into the application, so editing one does not restart it.
func watchedFile(rel string) bool {
	switch {
	case rel == layout.ManifestFile || rel == "go.mod" || rel == "go.sum":
		return true
	case strings.HasSuffix(rel, "_test.go"):
		return false
	}
	if _, owned := layout.NodeRoot(rel); owned {
		return true
	}
	return strings.HasSuffix(rel, ".go")
}

// skippedDir is a directory the walk does not enter, as ./... and layout
// discovery do not: hidden and underscore directories, vendor, testdata and
// node_modules. The go command still builds a package in one when it is
// imported; buildReads refuses that build.
func skippedDir(name string) bool {
	return strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") || name == "vendor" || name == "testdata" || name == "node_modules"
}

var errWatchLimit = errors.New("watch limit")

// watchBounds are the bounds scanProject applies; a variable only so a test
// can lower them.
var watchBounds = struct{ files, entries int }{MaxWatchedFiles, MaxWatchEntries}

// scanProject records every watched file under root. It reads directory
// entries with Lstat semantics and never follows a symbolic link (ADR
// 0023): a link is recorded as the link itself, so adding, removing or
// retargeting one triggers a rebuild; nothing outside root is ever stat-ed,
// so a file a link points to outside the project is never watched. Nested
// modules are not walked.
//
// Every link is recorded, whatever its name, except one whose name starts
// with a dot, which no import path can name: the go command imports
// through a directory named _x, testdata or node_modules, at any depth,
// although ./... skips them. Whether the build reads through a link is
// decided at build time by buildReads, from the imports.
func scanProject(ctx context.Context, root string) (snapshot, *diagnostic.Diagnostic, error) {
	result := snapshot{}
	visited := 0
	err := filepath.WalkDir(root, func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			// A directory removed while it is walked is a change the next
			// scan sees; it is not an error.
			if errors.Is(walkErr, fs.ErrNotExist) && name != root {
				return nil
			}
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if visited++; visited > watchBounds.entries {
			return errWatchLimit
		}
		if name == root {
			return nil
		}
		if entry.IsDir() {
			if skippedDir(entry.Name()) {
				return filepath.SkipDir
			}
			if _, err := os.Lstat(filepath.Join(name, "go.mod")); err == nil {
				return filepath.SkipDir
			}
			return nil
		}
		relative, err := filepath.Rel(root, name)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		link := entry.Type()&fs.ModeSymlink != 0
		switch {
		case link && strings.HasPrefix(entry.Name(), "."):
			return nil
		case link:
		case !watchedFile(relative) || !entry.Type().IsRegular():
			return nil
		}
		info, err := entry.Info()
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if len(result) >= watchBounds.files {
			return errWatchLimit
		}
		result[relative] = stampOf(info)
		return nil
	})
	if errors.Is(err, errWatchLimit) {
		return nil, &diagnostic.Diagnostic{Code: layout.CodeLimitExceeded, Expected: "at most " + strconv.Itoa(watchBounds.files) + " watched files and " + strconv.Itoa(watchBounds.entries) + " directory entries", Actual: "more", Remediation: "run blok dev from the application's own module root, or move generated or vendored trees into a skipped directory (., _, vendor, testdata, node_modules)", Message: "the project has more files than blok dev watches"}, nil
	}
	if err != nil {
		return nil, nil, err
	}
	return result, nil, nil
}

// changes lists, sorted, every path added, removed or changed from before
// to after.
func changes(before, after snapshot) []string {
	var changed []string
	for name, now := range after {
		if was, ok := before[name]; !ok || was != now {
			changed = append(changed, name)
		}
	}
	for name := range before {
		if _, ok := after[name]; !ok {
			changed = append(changed, name)
		}
	}
	sort.Strings(changed)
	return changed
}

// refresh re-records one path after blok dev itself wrote it, so its own
// regeneration of the bindings is not mistaken for an edit.
func (s snapshot) refresh(root, rel string) {
	info, err := os.Lstat(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		delete(s, rel)
		return
	}
	s[rel] = stampOf(info)
}

// buildReads reports every path the build of the main package (project-
// relative) would read from files blok dev does not watch, and so could
// not rebuild after an edit to them; such a build is refused.
//
//   - A watched file that is a symbolic link (a linked .go file, go.mod, …):
//     the go command follows it wherever it points (dev_symlink_unwatched).
//   - A link the build reads a package through: the main package and every
//     package of the module it imports, transitively (build constraints
//     ignored, test files excluded), are resolved one path element at a
//     time with layout.ClassifyLink, which reads only inside the root. A
//     link that leaves the project, or leads into a skipped directory or a
//     nested module, or whose chain goes through a link the walk does not
//     record (a dot name, or inside a skipped directory), is refused
//     (dev_symlink_unwatched).
//   - A package in a directory the walk skips (_x, testdata, node_modules,
//     at any depth): the go command builds it when it is imported, but the
//     watcher never sees it (dev_package_unwatched).
//   - A Go file whose imports cannot be read: unreadable
//     (project_unreadable), or with import declarations that do not end
//     within importHeaderBytes (layout_limit_exceeded). importsOf reads only
//     a file's header, so a file of any size is followed, never skipped.
//
// A link nothing builds through (a LICENSE or docs link) is never read by
// the build and is not refused. Files the build reads that are not Go
// source (//go:embed patterns, cgo and assembly files) are not watched at
// all (ADR 0026, Limits); nor is dependency code (the module cache,
// vendor, local replace targets). root is the root's symlink-resolved
// path, module go.mod's module path.
func (s snapshot) buildReads(ctx context.Context, root, module, main string) ([]diagnostic.Diagnostic, error) {
	var found []diagnostic.Diagnostic
	sources := map[string][]string{} // symlink-free directory -> its non-test Go files
	for name, item := range s {
		switch {
		case item.mode&fs.ModeSymlink != 0 && watchedFile(name):
			found = append(found, diagnostic.Diagnostic{Code: "dev_symlink_unwatched", Source: name, Expected: "a regular file inside the project", Actual: "a symbolic link", Remediation: "replace the link with the file itself; blok dev never follows a link, so it cannot watch or build what one points to", Message: "a watched source file is a symbolic link"})
		case item.mode.IsRegular() && goSource(name):
			sources[path.Dir(name)] = append(sources[path.Dir(name)], name)
		}
	}
	if module == "" {
		diagnostic.Sort(found)
		return found, nil
	}
	fsRoot, err := os.OpenRoot(root)
	if err != nil {
		return nil, err
	}
	defer fsRoot.Close()
	type finding struct{ code, source string }
	reported := map[finding]bool{}
	report := func(item diagnostic.Diagnostic) {
		if key := (finding{item.Code, item.Source}); !reported[key] {
			reported[key] = true
			found = append(found, item)
		}
	}
	queue, queued, parsed := []string{main}, map[string]bool{main: true}, map[string]bool{}
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		dir := queue[0]
		queue = queue[1:]
		resolved, problem := s.resolvePackage(fsRoot, root, dir)
		if problem != nil {
			report(*problem)
			continue
		}
		if resolved == "" || parsed[resolved] {
			continue
		}
		parsed[resolved] = true
		for _, file := range sources[resolved] {
			imports, problem := importsOf(fsRoot, file)
			if problem != nil {
				report(*problem)
				continue
			}
			for _, imported := range imports {
				rel, ok := inModule(module, imported)
				if ok && !queued[rel] {
					queued[rel] = true
					queue = append(queue, rel)
				}
			}
		}
	}
	diagnostic.Sort(found)
	return found, nil
}

// goSource is a Go file the go command compiles into the application: not
// a test, and not a name it ignores (a leading _ or .).
func goSource(rel string) bool {
	base := path.Base(rel)
	return strings.HasSuffix(base, ".go") && !strings.HasSuffix(base, "_test.go") && !strings.HasPrefix(base, "_") && !strings.HasPrefix(base, ".")
}

// inModule is the project-relative directory of an import path of module,
// "." for the module's root package.
func inModule(module, imported string) (string, bool) {
	if imported == module {
		return ".", true
	}
	rel, ok := strings.CutPrefix(imported, module+"/")
	if !ok || !validRelative(rel) {
		return "", false
	}
	return rel, true
}

// importHeaderBytes bounds how much of one Go file importsOf reads: its
// header (comments, package clause, imports) must end within it. A
// variable only so a test can lower it.
var importHeaderBytes = MaxSourceFileBytes

// importsOf is the import paths of the Go file rel. Like the go command, it
// reads only the file's header, so a file of any size is read for its
// imports, never skipped; at most importHeaderBytes are read. It never
// fails open: a file it cannot read, or whose import declarations do not
// end within the bound, is a problem that refuses the build, since the
// build would read imports blok dev did not see. A syntax error in a
// header read whole is not: the go command reads the same header and
// fails the build itself; the imports parsed before the error still count.
func importsOf(fsRoot *os.Root, rel string) ([]string, *diagnostic.Diagnostic) {
	unreadable := func(err error) *diagnostic.Diagnostic {
		return &diagnostic.Diagnostic{Code: "project_unreadable", Source: rel, Expected: "a readable Go source file", Actual: errorText(err), Remediation: "make the file readable; blok dev reads every built file's imports and keeps watching", Message: "the project could not be read"}
	}
	file, err := fsRoot.Open(filepath.FromSlash(rel))
	if err != nil {
		return nil, unreadable(err)
	}
	defer file.Close()
	if info, err := file.Stat(); err != nil || !info.Mode().IsRegular() {
		if err == nil {
			err = &fs.PathError{Op: "read", Path: path.Base(rel), Err: fs.ErrInvalid}
		}
		return nil, unreadable(err)
	}
	data, err := io.ReadAll(io.LimitReader(file, int64(importHeaderBytes)+1))
	if err != nil {
		return nil, unreadable(err)
	}
	truncated := len(data) > importHeaderBytes
	if truncated {
		data = data[:importHeaderBytes]
	}
	files := token.NewFileSet()
	parsed, parseErr := parser.ParseFile(files, rel, data, parser.ImportsOnly)
	if truncated && (parseErr != nil || parsed == nil || !headerEnds(files, parsed, data)) {
		return nil, &diagnostic.Diagnostic{Code: layout.CodeLimitExceeded, Source: rel, Expected: "import declarations ending within the file's first " + strconv.Itoa(importHeaderBytes) + " bytes", Actual: "they do not", Remediation: "shorten the comments before the package clause or the import list; blok dev reads only a file's header for its imports", Message: "blok dev cannot read the imports of a Go file the build compiles"}
	}
	if parsed == nil {
		return nil, nil
	}
	var imports []string
	for _, spec := range parsed.Imports {
		if value, err := strconv.Unquote(spec.Path.Value); err == nil {
			imports = append(imports, value)
		}
	}
	return imports, nil
}

// headerEnds reports whether data, a file's first bytes, holds its whole
// header: after the last import declaration (or the package clause) comes
// a token that is not import and ends before data does. A file cut inside
// its imports, or right after one (the next might be another), does not.
func headerEnds(files *token.FileSet, parsed *ast.File, data []byte) bool {
	end := parsed.Name.End()
	if len(parsed.Decls) > 0 {
		end = parsed.Decls[len(parsed.Decls)-1].End()
	}
	offset := files.Position(end).Offset
	var reader scanner.Scanner
	failed := false
	rest := data[offset:]
	reader.Init(token.NewFileSet().AddFile("", -1, len(rest)), rest, func(token.Position, string) { failed = true }, 0)
	for {
		at, tok, literal := reader.Scan()
		if failed {
			return false
		}
		switch tok {
		case token.SEMICOLON:
			continue
		case token.EOF, token.IMPORT, token.ILLEGAL:
			return false
		}
		if literal == "" {
			literal = tok.String()
		}
		return int(at)-1+len(literal) < len(rest)
	}
}

// resolvePackage resolves the package directory dir the way the go command
// reaches it, one element at a time, and returns its symlink-free path, or
// the reason the build would read it from files the watcher does not see.
// It returns "" and no problem for a directory that does not exist (or a
// dangling link or a cycle): the build fails on its own.
func (s snapshot) resolvePackage(fsRoot *os.Root, root, dir string) (string, *diagnostic.Diagnostic) {
	if dir == "." {
		return ".", nil
	}
	linkProblem := func(link, why string) *diagnostic.Diagnostic {
		return &diagnostic.Diagnostic{Code: "dev_symlink_unwatched", Source: link, Expected: "a package directory inside the project, without symbolic links", Actual: why + "; the build reads package " + dir + " through it", Remediation: "replace the link with the directory itself, or stop importing through it; blok dev never follows a link, so it cannot watch what the build would read through it", Message: "a symbolic link would build files blok dev does not watch"}
	}
	resolved := ""
	for _, element := range strings.Split(dir, "/") {
		here := path.Join(resolved, element)
		info, err := fsRoot.Lstat(filepath.FromSlash(here))
		if err != nil {
			return "", nil
		}
		if info.Mode()&fs.ModeSymlink == 0 {
			if skippedDir(element) {
				return "", &diagnostic.Diagnostic{Code: "dev_package_unwatched", Source: dir, Expected: "a package outside the directories blok dev skips (., _, vendor, testdata, node_modules)", Actual: "in " + here + ", which blok dev does not watch; the build imports it", Remediation: "move the package out of " + here + ", or stop importing it; blok dev would not rebuild after an edit to it", Message: "the build imports a package blok dev does not watch"}
			}
			resolved = here
			continue
		}
		code, target := layout.ClassifyLink(fsRoot, root, here)
		switch code {
		case layout.CodeSymlinkEscape:
			return "", linkProblem(here, "a symbolic link leading outside the project")
		case layout.CodeSymlinkAlias:
		default:
			return "", nil
		}
		// Every link the chain goes through is read too: each must be one
		// the walk records, so retargeting it rebuilds. A link whose name
		// starts with a dot, or inside a skipped directory, is not.
		for _, hop := range linkHops(fsRoot, root, here) {
			if item, ok := s[hop]; !ok || item.mode&fs.ModeSymlink == 0 {
				return "", linkProblem(here, "a symbolic link resolved through the link "+hop+", which blok dev does not watch")
			}
		}
		if target != "" {
			elements := strings.Split(target, "/")
			for index := range elements {
				prefix := strings.Join(elements[:index+1], "/")
				if skippedDir(elements[index]) {
					return "", linkProblem(here, "a symbolic link into "+prefix+", which blok dev does not watch")
				}
				if _, err := fsRoot.Lstat(filepath.FromSlash(prefix + "/go.mod")); err == nil {
					return "", linkProblem(here, "a symbolic link into the nested module "+prefix+", which blok dev does not watch")
				}
			}
		}
		resolved = target
	}
	if resolved == "" {
		return ".", nil
	}
	return resolved, nil
}

// maxHops bounds linkHops; layout.ClassifyLink has already refused a loop.
const maxHops = 255

// linkHops is every symbolic link that resolving rel goes through, as
// symlink-free project-relative paths, in order: rel itself first. It reads
// only inside the root, as layout.ClassifyLink does, and is called only for
// a link ClassifyLink resolved inside the root.
func linkHops(fsRoot *os.Root, absRoot, rel string) []string {
	pending := strings.Split(rel, "/")
	var resolved, hops []string
	for len(pending) > 0 && len(hops) <= maxHops {
		element := pending[0]
		pending = pending[1:]
		switch element {
		case "", ".":
			continue
		case "..":
			if len(resolved) > 0 {
				resolved = resolved[:len(resolved)-1]
			}
			continue
		}
		candidate := path.Join(append(append([]string(nil), resolved...), element)...)
		info, err := fsRoot.Lstat(filepath.FromSlash(candidate))
		if err != nil {
			return hops
		}
		if info.Mode()&fs.ModeSymlink == 0 {
			resolved = append(resolved, element)
			continue
		}
		hops = append(hops, candidate)
		target, err := fsRoot.Readlink(filepath.FromSlash(candidate))
		if err != nil {
			return hops
		}
		if filepath.IsAbs(target) || filepath.VolumeName(target) != "" {
			inside, err := filepath.Rel(absRoot, filepath.Clean(target))
			if err != nil {
				return hops
			}
			resolved, target = nil, inside
		}
		pending = append(strings.Split(filepath.ToSlash(target), "/"), pending...)
	}
	return hops
}
