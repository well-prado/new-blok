//go:build !windows

package devtool

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// linkAll makes each link (project-relative) point at its target.
func linkAll(t *testing.T, root string, links map[string]string) {
	t.Helper()
	for link, target := range links {
		name := filepath.Join(root, filepath.FromSlash(link))
		if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, name); err != nil {
			t.Fatal(err)
		}
	}
}

// mainImporting is cmd/fake/main.go importing each project-relative package
// of example.com/fake (heldApp's module).
func mainImporting(packages ...string) string {
	var imports strings.Builder
	for _, item := range packages {
		fmt.Fprintf(&imports, "\t_ %q\n", "example.com/fake/"+item)
	}
	return "package main\n\nimport (\n" + imports.String() + ")\n\nfunc main() {}\n"
}

// firstBuild is build 1's outcome and its diagnostics as sorted
// "code source" pairs.
func firstBuild(t *testing.T, options DevOptions) (string, []string, *devSession) {
	t.Helper()
	session := startDev(t, options)
	first := session.await(func(e DevEvent) bool {
		return (e.Event == EventBuildFailed || e.Event == EventBuildSucceeded) && e.Build == 1
	}, "build 1 to finish")
	var found []string
	for _, item := range first.Diagnostics {
		found = append(found, item.Code+" "+item.Source)
	}
	sort.Strings(found)
	return first.Event, found, session
}

// TestDevRefusesImportedLinksWhateverTheirName (review R round 2, finding
// 3): the go command imports through a directory named _x, testdata or
// node_modules, at any depth, although ./... skips them. A link with such a
// name that leaves the project, and that the build reads, is refused like
// any other; only a name Go cannot import (a leading dot) is never read.
func TestDevRefusesImportedLinksWhateverTheirName(t *testing.T) {
	options, _, _ := heldApp(t, `exit 0`)
	outside := t.TempDir()
	writeFiles(t, outside, map[string]string{"pkg/p.go": "package p\n"})
	pkg := filepath.Join(outside, "pkg")
	linkAll(t, options.Root, map[string]string{
		"_ext": pkg, "testdata": pkg, "node_modules": pkg, "internal/_x": pkg, ".hidden": pkg,
	})
	writeFiles(t, options.Root, map[string]string{"cmd/fake/main.go": mainImporting("_ext", "testdata", "node_modules", "internal/_x")})
	event, found, session := firstBuild(t, options)
	want := "[dev_symlink_unwatched _ext dev_symlink_unwatched internal/_x dev_symlink_unwatched node_modules dev_symlink_unwatched testdata]"
	if event != EventBuildFailed || fmt.Sprint(found) != want {
		t.Fatalf("build 1 %s %v, want build-failed %s\n%s", event, found, want, session.dump())
	}
	// The links are watched as links: removing them is a change, and the
	// next build runs without them.
	for _, link := range []string{"_ext", "testdata", "node_modules", "internal/_x"} {
		if err := os.Remove(filepath.Join(options.Root, filepath.FromSlash(link))); err != nil {
			t.Fatal(err)
		}
	}
	changed := session.await(func(e DevEvent) bool { return e.Event == EventChanged }, "the removal to be seen")
	if fmt.Sprint(changed.Changed) != "[_ext internal/_x node_modules testdata]" {
		t.Fatalf("changed %v\n%s", changed.Changed, session.dump())
	}
	session.stop()
}

// TestDevBuildsPastLinksTheBuildDoesNotRead (review R round 2, should-fix
// 1): a link leaving the project that no package the build compiles goes
// through (a LICENSE or docs link in a monorepo) does not fail the build;
// importing through it does.
func TestDevBuildsPastLinksTheBuildDoesNotRead(t *testing.T) {
	options, _, _ := heldApp(t, `trap 'exit 0' TERM
sleep 300 &
wait`)
	outside := t.TempDir()
	writeFiles(t, outside, map[string]string{"LICENSE": "MIT\n", "docs/index.md": "", "lib/l.go": "package lib\n"})
	linkAll(t, options.Root, map[string]string{
		"LICENSE":           filepath.Join(outside, "LICENSE"),
		"docs":              filepath.Join(outside, "docs"),
		"internal/vendored": filepath.Join(outside, "lib"),
	})
	event, found, session := firstBuild(t, options)
	if event != EventBuildSucceeded {
		t.Fatalf("build 1 %s %v, want build-succeeded\n%s", event, found, session.dump())
	}
	writeFiles(t, options.Root, map[string]string{"cmd/fake/main.go": mainImporting("internal/vendored")})
	second := session.await(func(e DevEvent) bool {
		return (e.Event == EventBuildFailed || e.Event == EventBuildSucceeded) && e.Build == 2
	}, "build 2 to finish")
	if second.Event != EventBuildFailed || len(second.Diagnostics) != 1 || second.Diagnostics[0].Code != "dev_symlink_unwatched" || second.Diagnostics[0].Source != "internal/vendored" || second.Running != 1 {
		t.Fatalf("importing through the link: %+v\n%s", second.DevEvent, session.dump())
	}
	assertNoProcesses(t, session.stop())
}

// TestDevRefusesImportedPackagesInSkippedDirectories: the walk never enters
// _x, testdata or node_modules, yet the go command builds a package there
// that the application imports, so blok dev would serve stale code after
// an edit to it. Such a build is refused (dev_package_unwatched).
func TestDevRefusesImportedPackagesInSkippedDirectories(t *testing.T) {
	options, _, _ := heldApp(t, `exit 0`)
	writeFiles(t, options.Root, map[string]string{
		"_x/x.go": "package x\n", "internal/testdata/p/p.go": "package p\n", "web/node_modules/n/n.go": "package n\n",
		"internal/ok/ok.go": "package ok\n",
		"cmd/fake/main.go":  mainImporting("_x", "internal/testdata/p", "web/node_modules/n", "internal/ok"),
	})
	event, found, session := firstBuild(t, options)
	want := "[dev_package_unwatched _x dev_package_unwatched internal/testdata/p dev_package_unwatched web/node_modules/n]"
	if event != EventBuildFailed || fmt.Sprint(found) != want {
		t.Fatalf("build 1 %s %v, want build-failed %s\n%s", event, found, want, session.dump())
	}
	session.stop()
}

// TestDevFollowsImportsTransitivelyAndThroughAliases: a package reached
// only through another package, or through a link inside the project, is
// read by the build too: a link it leads to outside the project is
// refused (reported where the link is, internal/lib/inner, though the
// import names it lib/inner), and one leading back into a watched
// directory is not.
func TestDevFollowsImportsTransitivelyAndThroughAliases(t *testing.T) {
	options, _, _ := heldApp(t, `exit 0`)
	outside := t.TempDir()
	writeFiles(t, outside, map[string]string{"far/f.go": "package far\n"})
	writeFiles(t, options.Root, map[string]string{
		"internal/a/a.go":    "package a\n\nimport _ \"example.com/fake/lib/inner\"\n",
		"internal/lib/l.go":  "package lib\n",
		"internal/real/r.go": "package real\n",
		// A test file's imports are not built into the application.
		"internal/a/a_test.go": "package a\n\nimport _ \"example.com/fake/testonly\"\n",
		"cmd/fake/main.go":     mainImporting("internal/a", "alias"),
	})
	linkAll(t, options.Root, map[string]string{
		"lib":                filepath.Join("internal", "lib"),
		"internal/lib/inner": filepath.Join(outside, "far"),
		"alias":              filepath.Join("internal", "real"),
		"testonly":           filepath.Join(outside, "far"),
	})
	event, found, session := firstBuild(t, options)
	if want := "[dev_symlink_unwatched internal/lib/inner]"; event != EventBuildFailed || fmt.Sprint(found) != want {
		t.Fatalf("build 1 %s %v, want build-failed %s\n%s", event, found, want, session.dump())
	}
	session.stop()
}

// TestDevResolvesLinksAgainstTheResolvedRoot: blok dev started through a
// link to the project (or a directory under one, like macOS's /var)
// resolves the root once, so a link inside the project with an absolute
// target written against the resolved path is inside it, not an escape.
func TestDevResolvesLinksAgainstTheResolvedRoot(t *testing.T) {
	options, _, _ := heldApp(t, `exit 0`)
	resolved, err := filepath.EvalSymlinks(options.Root)
	if err != nil {
		t.Fatal(err)
	}
	writeFiles(t, options.Root, map[string]string{"internal/real/r.go": "package real\n", "cmd/fake/main.go": mainImporting("internal/alias")})
	linkAll(t, options.Root, map[string]string{"internal/alias": filepath.Join(resolved, "internal", "real")})
	via := filepath.Join(t.TempDir(), "via")
	if err := os.Symlink(options.Root, via); err != nil {
		t.Fatal(err)
	}
	options.Root = via
	event, found, session := firstBuild(t, options)
	// blok.json, go.mod, cmd/fake/main.go, internal/real/r.go and the link.
	watching := eventsOf(session.snapshot(), EventWatching, 0)
	if event != EventBuildSucceeded || len(watching) != 1 || watching[0].Files != 5 {
		t.Fatalf("build 1 %s %v, want build-succeeded watching 5 files\n%s", event, found, session.dump())
	}
	session.stop()
}

// TestDevResolvesLinksWrittenThroughTheStartingPath (#347): an absolute
// link target written through the path blok dev was started with (a link
// to the project, or macOS's /var for /private/var) is inside the project
// too, as it is for the go command. One that leaves it lexically from that
// path is still an escape, and a chain written through it is checked hop by
// hop: internal/chain reaches internal/real through .cur, a link the walk
// does not record, so it is refused.
func TestDevResolvesLinksWrittenThroughTheStartingPath(t *testing.T) {
	options, _, _ := heldApp(t, `exit 0`)
	via := filepath.Join(t.TempDir(), "via")
	if err := os.Symlink(options.Root, via); err != nil {
		t.Fatal(err)
	}
	writeFiles(t, options.Root, map[string]string{"internal/real/r.go": "package real\n", "cmd/fake/main.go": mainImporting("internal/alias", "internal/out", "internal/chain")})
	writeFiles(t, filepath.Dir(via), map[string]string{"elsewhere/e.go": "package elsewhere\n"})
	linkAll(t, options.Root, map[string]string{
		"internal/alias": filepath.Join(via, "internal", "real"),
		"internal/out":   via + "/../elsewhere",
		"internal/chain": filepath.Join(via, ".cur"),
		".cur":           "internal/real",
	})
	options.Root = via
	event, found, session := firstBuild(t, options)
	if want := "[dev_symlink_unwatched internal/chain dev_symlink_unwatched internal/out]"; event != EventBuildFailed || fmt.Sprint(found) != want {
		t.Fatalf("build 1 %s %v, want build-failed %s\n%s", event, found, want, session.dump())
	}
	session.stop()
}

// TestShellQuoteRoundTrips: /bin/sh reads every quoted word back as the
// word itself, whatever it holds.
func TestShellQuoteRoundTrips(t *testing.T) {
	words := []string{"", "plain", "a b", "it's", `$HOME`, "a;b", "a&&b", "a|b", "`id`", "$(id)", "*", "~", "a\nb", `back\slash`, `"dq"`, "!x", "#c", "{a,b}", "--flag=a,b"}
	quoted := make([]string, 0, len(words))
	for _, word := range words {
		quoted = append(quoted, shellQuote(word))
	}
	output, err := exec.Command("/bin/sh", "-c", `printf '%s\0' `+strings.Join(quoted, " ")).CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, output)
	}
	if got := strings.Split(strings.TrimSuffix(string(output), "\x00"), "\x00"); fmt.Sprintf("%q", got) != fmt.Sprintf("%q", words) {
		t.Fatalf("the shell read\n%q\nwant\n%q", got, words)
	}
}

// TestDevReadsTheImportsOfLargeFiles (review R round 3, should-fix 1): a
// Go file past MaxSourceFileBytes is still compiled by the go command, so
// its imports are followed like any other file's: a link it imports
// through that leaves the project is refused. A file whose imports do not
// end within the bound is refused rather than skipped
// (layout_limit_exceeded).
func TestDevReadsTheImportsOfLargeFiles(t *testing.T) {
	options, _, _ := heldApp(t, `exit 0`)
	outside := t.TempDir()
	writeFiles(t, outside, map[string]string{"pkg/p.go": "package p\n"})
	linkAll(t, options.Root, map[string]string{"_ext": filepath.Join(outside, "pkg")})
	code := strings.Repeat("var _ = 1\n", MaxSourceFileBytes/10+1)
	comments := strings.Repeat("// filler\n", MaxSourceFileBytes/10+1)
	writeFiles(t, options.Root, map[string]string{
		"internal/big/big.go":   "package big\n\nimport _ \"example.com/fake/_ext\"\n\n" + code,
		"internal/huge/huge.go": comments + "package huge\n\nimport _ \"fmt\"\n",
		"cmd/fake/main.go":      mainImporting("internal/big", "internal/huge"),
	})
	event, found, session := firstBuild(t, options)
	want := "[dev_symlink_unwatched _ext layout_limit_exceeded internal/huge/huge.go]"
	if event != EventBuildFailed || fmt.Sprint(found) != want {
		t.Fatalf("build 1 %s %v, want build-failed %s\n%s", event, found, want, session.dump())
	}
	session.stop()
}

// TestImportsOfReadsTheHeaderWithinItsBound: importsOf reads a file's
// imports from at most importHeaderBytes of it, and refuses (never
// guesses) when the header does not provably end within them: a cut
// inside the import keyword, or right after an import declaration that
// another may follow.
func TestImportsOfReadsTheHeaderWithinItsBound(t *testing.T) {
	root := t.TempDir()
	header := "package p\n\nimport \"a\"\n"
	for name, item := range map[string]struct {
		content string
		want    string
	}{
		"whole.go":          {header + "import \"b\"\n", "[a b] <nil>"},
		"long-body.go":      {header + "func f() {}\n" + strings.Repeat("// x\n", 64), "[a] <nil>"},
		"cut-in-keyword.go": {header + strings.Repeat("\n", 64-len(header)-3) + "import \"b\"\n", "[] layout_limit_exceeded"},
		"cut-after-decl.go": {header + strings.Repeat("\n", 64-len(header)) + "import \"b\"\n", "[] layout_limit_exceeded"},
		"cut-in-comment.go": {header + "/*" + strings.Repeat("x", 64) + "*/\nimport \"b\"\n", "[] layout_limit_exceeded"},
		"long-comment.go":   {strings.Repeat("// x\n", 64) + header, "[] layout_limit_exceeded"},
		"syntax-error.go":   {"package p\n\nimport \"a\"\nimport )\n", "[a] <nil>"},
	} {
		writeFiles(t, root, map[string]string{name: item.content})
		fsRoot, err := os.OpenRoot(root)
		if err != nil {
			t.Fatal(err)
		}
		saved := importHeaderBytes
		importHeaderBytes = 64
		imports, problem := importsOf(fsRoot, name)
		importHeaderBytes = saved
		fsRoot.Close()
		code := "<nil>"
		if problem != nil {
			code = problem.Code
		}
		if got := fmt.Sprintf("%v %s", append([]string{}, imports...), code); got != item.want {
			t.Errorf("%s: %s, want %s", name, got, item.want)
		}
	}
}

// TestDevRefusesChainsThroughUnwatchedLinks (review R round 3, nit a): the
// build reads a package through every link of a chain, so every hop must
// be a link the walk records. A hop whose name starts with a dot, or that
// sits inside a skipped directory, is not watched (retargeting it would
// not rebuild), so a build through it is refused; a chain of watched links
// builds, and retargeting its middle hop rebuilds.
func TestDevRefusesChainsThroughUnwatchedLinks(t *testing.T) {
	options, _, _ := heldApp(t, `trap 'exit 0' TERM
sleep 300 &
wait`)
	writeFiles(t, options.Root, map[string]string{
		"internal/a/a.go": "package a\n", "internal/b/b.go": "package b\n", "internal/c/c.go": "package c\n", "internal/d/d.go": "package d\n",
		"_x/keep.txt":      "",
		"cmd/fake/main.go": mainImporting("dotted", "skipped", "chained"),
	})
	linkAll(t, options.Root, map[string]string{
		"dotted": ".cur", ".cur": filepath.Join("internal", "a"),
		"skipped": filepath.Join("_x", "l"), "_x/l": filepath.Join("..", "internal", "b"),
		"chained": "hop", "hop": filepath.Join("internal", "c"),
	})
	event, found, session := firstBuild(t, options)
	if want := "[dev_symlink_unwatched dotted dev_symlink_unwatched skipped]"; event != EventBuildFailed || fmt.Sprint(found) != want {
		t.Fatalf("build 1 %s %v, want build-failed %s\n%s", event, found, want, session.dump())
	}
	writeFiles(t, options.Root, map[string]string{"cmd/fake/main.go": mainImporting("chained")})
	second := session.await(func(e DevEvent) bool {
		return (e.Event == EventBuildFailed || e.Event == EventBuildSucceeded) && e.Build == 2
	}, "build 2 to finish")
	if second.Event != EventBuildSucceeded {
		t.Fatalf("build 2 through watched links: %+v\n%s", second.DevEvent, session.dump())
	}
	hop := filepath.Join(options.Root, "hop")
	if err := os.Remove(hop); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join("internal", "d"), hop); err != nil {
		t.Fatal(err)
	}
	changed := session.await(func(e DevEvent) bool { return e.Event == EventChanged }, "the retargeted hop to be seen")
	if fmt.Sprint(changed.Changed) != "[hop]" {
		t.Fatalf("changed %v, want [hop]\n%s", changed.Changed, session.dump())
	}
	session.await(func(e DevEvent) bool { return e.Event == EventBuildSucceeded && e.Build == 3 }, "build 3 after the retarget")
	assertNoProcesses(t, session.stop())
}

// TestDevFollowsTheModuleRootPackage (review R round 3, nit b): an import
// of the module path itself builds the package at the module root, whose
// imports are followed like any other package's.
func TestDevFollowsTheModuleRootPackage(t *testing.T) {
	options, _, _ := heldApp(t, `exit 0`)
	outside := t.TempDir()
	writeFiles(t, outside, map[string]string{"pkg/p.go": "package p\n"})
	linkAll(t, options.Root, map[string]string{"_ext": filepath.Join(outside, "pkg")})
	writeFiles(t, options.Root, map[string]string{
		"root.go":          "package fake\n\nimport _ \"example.com/fake/_ext\"\n",
		"cmd/fake/main.go": "package main\n\nimport _ \"example.com/fake\"\n\nfunc main() {}\n",
	})
	event, found, session := firstBuild(t, options)
	if want := "[dev_symlink_unwatched _ext]"; event != EventBuildFailed || fmt.Sprint(found) != want {
		t.Fatalf("build 1 %s %v, want build-failed %s\n%s", event, found, want, session.dump())
	}
	session.stop()
}
