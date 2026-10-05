package devtool

import (
	"context"
	"errors"
	"io/fs"
	"os"
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
// retargeted link, without reading the file.
type stamp struct {
	size    int64
	modTime int64
	mode    fs.FileMode
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

// skippedDir is a directory the go command and layout discovery never read
// as part of the module: hidden and underscore directories, vendor,
// testdata and node_modules.
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
// retargeting one triggers a rebuild, whose layout discovery then reports
// it; nothing outside root is ever stat-ed, so a file a link points to
// outside the project is never watched. Nested modules are not walked.
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
		if !watchedFile(relative) || !(entry.Type().IsRegular() || entry.Type()&fs.ModeSymlink != 0) {
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
		result[relative] = stamp{size: info.Size(), modTime: info.ModTime().UnixNano(), mode: info.Mode()}
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
	s[rel] = stamp{size: info.Size(), modTime: info.ModTime().UnixNano(), mode: info.Mode()}
}

// links reports every watched path that is a symbolic link. The go command
// would follow a linked source file wherever it points, and blok dev never
// follows a link (ADR 0023), so it could not watch what it would build:
// such a build is refused. Layout discovery classifies the links it meets
// in node and workflow directories itself; this covers the rest of the
// module (cmd, internal, …).
func (s snapshot) links() []diagnostic.Diagnostic {
	var found []diagnostic.Diagnostic
	for name, item := range s {
		if item.mode&fs.ModeSymlink != 0 {
			found = append(found, diagnostic.Diagnostic{Code: "dev_symlink_unwatched", Source: name, Expected: "a regular file inside the project", Actual: "a symbolic link", Remediation: "replace the link with the file itself; blok dev never follows a link, so it cannot watch or build what one points to", Message: "a watched source file is a symbolic link"})
		}
	}
	diagnostic.Sort(found)
	return found
}
