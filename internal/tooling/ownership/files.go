package ownership

import (
	"errors"
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

// projectFiles is a read-only, link-refusing, case-exact view of the
// project, shared by every adapter. Each path element is matched against
// its directory's listing by exact name, so resolution gives the same
// answer on case-sensitive and case-insensitive file systems; a link is
// classified with ADR 0023's rules and never followed; files are opened
// with layout.OpenRegular and read within budgets.
type projectFiles struct {
	root     *os.Root
	abs      string
	listings map[string]listing
	bytes    int64
	// limited is set once the read budget is exhausted.
	limited bool
}

type listing struct {
	entries map[string]fs.FileMode // exact name → type bits
	folded  map[string]string      // lower-case name → first on-disk name
	tooMany bool                   // more than layout.MaxDirEntries entries
}

func newProjectFiles(root *os.Root, abs string) *projectFiles {
	return &projectFiles{root: root, abs: abs, listings: map[string]listing{}}
}

type entryKind uint8

const (
	kindMissing entryKind = iota
	kindFile
	kindDir
	kindLink  // an element of the path is a symbolic link
	kindOther // a device, FIFO or socket
	kindLimit // a directory on the path lists more entries than the bound
)

// entry is the result of looking up a project-relative path.
type entry struct {
	kind entryKind
	// path is the on-disk spelling of the looked-up path (or of the link).
	path string
	// caseMismatch is set when some element matched only ignoring case.
	caseMismatch bool
	// link is the code and lexical target for kindLink.
	linkCode, linkTarget string
}

func (f *projectFiles) list(dir string) listing {
	if cached, ok := f.listings[dir]; ok {
		return cached
	}
	l := listing{entries: map[string]fs.FileMode{}, folded: map[string]string{}}
	name := dir
	if name == "" {
		name = "."
	}
	file, err := f.root.Open(filepath.FromSlash(name))
	if err == nil {
		entries, readErr := file.ReadDir(layout.MaxDirEntries + 1)
		file.Close()
		l.tooMany = len(entries) > layout.MaxDirEntries
		if (readErr == nil || errors.Is(readErr, io.EOF)) && !l.tooMany {
			sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
			for _, e := range entries {
				l.entries[e.Name()] = e.Type()
				lower := strings.ToLower(e.Name())
				if _, taken := l.folded[lower]; !taken {
					l.folded[lower] = e.Name()
				}
			}
		}
	}
	f.listings[dir] = l
	return l
}

// lookup resolves rel element by element without following links.
func (f *projectFiles) lookup(rel string) entry {
	if rel == "" || rel == "." {
		return entry{kind: kindDir}
	}
	parts := strings.Split(rel, "/")
	var spelled []string
	mismatch := false
	for i, part := range parts {
		dir := strings.Join(spelled, "/")
		l := f.list(dir)
		if l.tooMany {
			return entry{kind: kindLimit, path: dir}
		}
		mode, exact := l.entries[part]
		name := part
		if !exact {
			onDisk, folded := l.folded[strings.ToLower(part)]
			if !folded {
				return entry{kind: kindMissing}
			}
			name, mode, mismatch = onDisk, l.entries[onDisk], true
		}
		spelled = append(spelled, name)
		current := strings.Join(spelled, "/")
		switch {
		case mode&fs.ModeSymlink != 0:
			code, target := layout.ClassifyLink(f.root, f.abs, current)
			if code == layout.CodeSymlinkAlias && i+1 < len(parts) {
				target = path.Join(append([]string{target}, parts[i+1:]...)...)
			}
			return entry{kind: kindLink, path: current, caseMismatch: mismatch, linkCode: code, linkTarget: target}
		case mode.IsDir():
			if i == len(parts)-1 {
				return entry{kind: kindDir, path: current, caseMismatch: mismatch}
			}
		case mode.IsRegular():
			if i == len(parts)-1 {
				return entry{kind: kindFile, path: current, caseMismatch: mismatch}
			}
			return entry{kind: kindMissing}
		default:
			return entry{kind: kindOther, path: current, caseMismatch: mismatch}
		}
	}
	return entry{kind: kindMissing}
}

// read returns a regular file's bytes within the per-file and total
// budgets (ADR 0023's bounds).
func (f *projectFiles) read(rel string) ([]byte, *diagnostic.Diagnostic) {
	file, info, err := layout.OpenRegular(f.root, rel)
	if err != nil {
		d := finding(CodeSourceUnsupported, rel, "", "the file cannot be read as a regular file with one link")
		return nil, &d
	}
	defer file.Close()
	size := info.Size()
	switch {
	case size > layout.MaxFileBytes:
		d := finding(CodeLimitExceeded, rel, strconv.FormatInt(size, 10), "file exceeds the ownership read bound")
		return nil, &d
	case f.bytes+size > layout.MaxTotalBytes:
		d := finding(CodeLimitExceeded, rel, "", "the project's source exceeds the ownership read budget")
		return nil, &d
	}
	data, err := io.ReadAll(io.LimitReader(file, size+1))
	if err != nil || int64(len(data)) > size {
		d := finding(CodeSourceUnsupported, rel, "", "file changed while it was read")
		return nil, &d
	}
	f.bytes += int64(len(data))
	return data, nil
}

// linkFinding reports a link met during resolution, from the importing
// file's point of view.
func linkFinding(e entry, source string) diagnostic.Diagnostic {
	messages := map[string]string{
		layout.CodeSymlinkEscape:   "an import resolves through a link that points outside the project root; links are never followed",
		layout.CodeSymlinkAlias:    "an import resolves through a link; links are never followed, import the real path",
		layout.CodeSymlinkDangling: "an import resolves through a link that points to nothing",
		layout.CodeSymlinkLoop:     "an import resolves through a link cycle",
	}
	return diagnostic.Diagnostic{Code: e.linkCode, Source: e.path, Field: source, Expected: "regular file or directory", Actual: "symlink", Message: messages[e.linkCode], Remediation: remediations[e.linkCode]}
}

// resolved collects a specifier's resolution: targets and findings.
type resolved struct {
	targets  []Target
	findings []diagnostic.Diagnostic
}

func (r *resolved) target(unit string, noFollow bool) {
	for _, existing := range r.targets {
		if existing.Unit == unit && existing.NoFollow == noFollow {
			return
		}
	}
	r.targets = append(r.targets, Target{Unit: unit, NoFollow: noFollow})
}

// accept records a looked-up entry: a file or directory is a target; a
// link or a case mismatch is a finding whose lexical or on-disk target is
// still checked for ownership but never traversed.
func (r *resolved) accept(e entry, source string) bool {
	switch e.kind {
	case kindLimit:
		r.findings = append(r.findings, diagnostic.Diagnostic{Code: CodeLimitExceeded, Source: source, Actual: e.path, Message: "a directory on the import's path lists more entries than the ownership check reads", Remediation: remediations[CodeLimitExceeded]})
		return true
	case kindLink:
		r.findings = append(r.findings, linkFinding(e, source))
		if e.linkCode == layout.CodeSymlinkAlias && e.linkTarget != "" {
			r.target(e.linkTarget, true)
		}
		return true
	case kindFile, kindDir:
		if e.caseMismatch {
			r.findings = append(r.findings, diagnostic.Diagnostic{Code: CodeImportCaseMismatch, Source: source, Expected: e.path, Message: "the import names " + e.path + " with different letter case", Remediation: remediations[CodeImportCaseMismatch]})
			r.target(e.path, true)
			return true
		}
		return true
	}
	return false
}
