package devtool

import (
	"bytes"
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/well-prado/new-blok/internal/diagnostic"
)

// layoutCodes are internal/tooling/layout's diagnostic code constants by
// name, read from its source.
func layoutCodes(t *testing.T) map[string]string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join("..", "layout", "diagnostics.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	codes := map[string]string{}
	for _, decl := range file.Decls {
		general, ok := decl.(*ast.GenDecl)
		if !ok || general.Tok != token.CONST {
			continue
		}
		for _, spec := range general.Specs {
			value := spec.(*ast.ValueSpec)
			for index, name := range value.Names {
				if text, ok := stringLiteral(value.Values[index]); ok && strings.HasPrefix(name.Name, "Code") {
					codes[name.Name] = text
				}
			}
		}
	}
	if len(codes) == 0 {
		t.Fatal("no layout codes found")
	}
	return codes
}

// emittedCodes finds every diagnostic code this package's non-test source
// can emit: a Code field in a composite literal, an assignment to a .Code
// selector, or a comparison against one, as a literal or a layout.Code*
// constant. Every layout code is also emitted, passed through from
// discovery.
func emittedCodes(t *testing.T) map[string]bool {
	t.Helper()
	fromLayout := layoutCodes(t)
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	codes := map[string]bool{}
	for _, code := range fromLayout {
		codes[code] = true
	}
	record := func(expr ast.Expr) {
		if value, ok := stringLiteral(expr); ok {
			codes[value] = true
		}
		if selector, ok := expr.(*ast.SelectorExpr); ok {
			if value, ok := fromLayout[selector.Sel.Name]; ok {
				codes[value] = true
			} else {
				t.Errorf("code %s.%s is not a layout constant", selector.X, selector.Sel.Name)
			}
		}
	}
	isCode := func(expr ast.Expr) bool {
		selector, ok := expr.(*ast.SelectorExpr)
		return ok && selector.Sel.Name == "Code"
	}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") || name == "codes.go" {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.KeyValueExpr:
				if key, ok := node.Key.(*ast.Ident); ok && key.Name == "Code" {
					record(node.Value)
				}
			case *ast.AssignStmt:
				for index, left := range node.Lhs {
					if isCode(left) && index < len(node.Rhs) {
						record(node.Rhs[index])
					}
				}
			case *ast.BinaryExpr:
				if isCode(node.X) {
					record(node.Y)
				}
			}
			return true
		})
	}
	return codes
}

// TestCodeRegistryMatchesSourceAndADR pins the registry: every code the
// source can emit is registered, every registered code is emitted, codes
// follow the #28 identifier form, and ADR 0024's table lists exactly them.
func TestCodeRegistryMatchesSourceAndADR(t *testing.T) {
	registry := map[string]bool{}
	form := regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	for _, item := range Codes {
		if registry[item.Code] || !form.MatchString(item.Code) || len(item.Commands) == 0 {
			t.Fatalf("bad registry entry %+v", item)
		}
		registry[item.Code] = true
	}
	emitted := emittedCodes(t)
	for code := range emitted {
		if !registry[code] {
			t.Errorf("source emits unregistered code %q", code)
		}
	}
	for code := range registry {
		if !emitted[code] {
			t.Errorf("registered code %q is never emitted", code)
		}
	}
	adr, err := os.ReadFile(filepath.Join(repoRoot(t), "docs", "decisions", "0024-check-test-inspect-cli-contract.md"))
	if err != nil {
		t.Fatal(err)
	}
	_, table, ok := strings.Cut(string(adr), "### Diagnostic codes")
	if !ok {
		t.Fatal("ADR 0024 has no diagnostic code table")
	}
	table, _, _ = strings.Cut(table, "\n### ")
	listed := map[string]bool{}
	for _, line := range strings.Split(table, "\n") {
		if !strings.HasPrefix(line, "| `") {
			continue
		}
		first := strings.Split(line, "|")[1]
		for _, match := range regexp.MustCompile("`([a-z0-9_]+)`").FindAllStringSubmatch(first, -1) {
			listed[match[1]] = true
		}
	}
	var missing, extra []string
	for code := range registry {
		if !listed[code] {
			missing = append(missing, code)
		}
	}
	for code := range listed {
		if !registry[code] {
			extra = append(extra, code)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	if len(missing)+len(extra) > 0 {
		t.Fatalf("ADR 0024 table: missing %v, not registered %v", missing, extra)
	}
}

// TestFinishSortsAndRedacts: whatever order checks report in, the report's
// diagnostics follow diagnostic.Sort, and every field is redacted.
func TestFinishSortsAndRedacts(t *testing.T) {
	found := &diagnostics{}
	for _, item := range []diagnostic.Diagnostic{
		{Code: "go_vet_finding", Source: "b.go:1", Message: "m", Remediation: "r"},
		{Code: "bindings_stale", Source: "z.go", Message: "m", Remediation: "r"},
		{Code: "go_vet_finding", Source: "a.go:1", Message: "m", Remediation: "r"},
		{Code: "go_toolchain_error", Actual: "Authorization: Bearer fixture-0123456789abcdefghij", Message: "m", Remediation: "r"},
	} {
		found.add(item)
	}
	report := Report{Command: "check"}
	finish(&report, found, context.Background(), false)
	var order []string
	for _, item := range report.Diagnostics {
		order = append(order, item.Code+"@"+item.Source)
	}
	if strings.Join(order, ",") != "bindings_stale@z.go,go_toolchain_error@,go_vet_finding@a.go:1,go_vet_finding@b.go:1" {
		t.Fatalf("order=%v", order)
	}
	if strings.Contains(report.Diagnostics[1].Actual, "Bearer") {
		t.Fatalf("not redacted: %+v", report.Diagnostics[1])
	}
}

func TestRedactLines(t *testing.T) {
	got := redactLines([]string{"before", "-----BEGIN OPENSSH PRIVATE KEY-----", "b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW", "-----END OPENSSH PRIVATE KEY-----", "after"})
	if strings.Join(got, "|") != "before|[redacted: sensitive-looking log message]|after" {
		t.Fatalf("got %q", got)
	}
	if got := redactLines([]string{"-----BEGIN RSA PRIVATE KEY-----", "MIIEpAIBAAKCAQEA0Z3VS5JJcds3xfn"}); len(got) != 1 || strings.Contains(strings.Join(got, ""), "MIIE") {
		t.Fatalf("an unterminated block leaked: %q", got)
	}
	if got := redactLines([]string{"status=200", "AKIAIOSFODNN7EXAMPLE"}); strings.Contains(strings.Join(got, ""), "AKIA") {
		t.Fatalf("got %q", got)
	}
}

func TestGoEnvPinsToolchainProxyModeAndWorkspace(t *testing.T) {
	base := []string{"HOME=/home/dev", "GOFLAGS=-mod=mod -toolexec=/tmp/x", "GOTOOLCHAIN=auto", "GOPROXY=https://proxy.invalid", "GOWORK=/elsewhere/go.work", "gotoolchain=auto", "GOMODCACHE=/cache"}
	env := goEnv(base, t.TempDir())
	want := map[string]string{"HOME": "/home/dev", "GOMODCACHE": "/cache", "GOTOOLCHAIN": "local", "GOPROXY": "off", "GOFLAGS": "-mod=readonly", "GOWORK": "off"}
	seen := map[string]int{}
	for _, entry := range env {
		key, value, _ := strings.Cut(entry, "=")
		seen[strings.ToUpper(key)]++
		if expected, ok := want[key]; ok && value != expected {
			t.Errorf("%s=%s want %s", key, value, expected)
		}
	}
	for key := range want {
		if seen[key] != 1 {
			t.Errorf("%s set %d times in %v", key, seen[key], env)
		}
	}
	vendored := t.TempDir()
	if err := os.MkdirAll(filepath.Join(vendored, "vendor"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(vendored, "vendor", "modules.txt"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if env := goEnv(nil, vendored); !contains(env, "GOFLAGS=-mod=vendor") {
		t.Fatalf("vendored env=%v", env)
	}
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

// TestNoNetworkNoWritesUnderAHostileEnvironment: with a caller environment
// that asks for a toolchain download (GOTOOLCHAIN=auto, a toolchain line),
// module downloads from a proxy (GOPROXY, an empty module cache, a
// dependency) and go.mod rewrites (GOFLAGS=-mod=mod), check and test make
// no request to the proxy, change no file, and report the missing module as
// a positioned go_module_not_in_cache, the same way every time. A go
// directive newer than the toolchain is go_toolchain_too_old, again with no
// request. With the module in the cache, the same project passes.
func TestNoNetworkNoWritesUnderAHostileEnvironment(t *testing.T) {
	var requests atomic.Int64
	var seen atomic.Value
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		seen.Store(r.URL.Path)
		http.NotFound(w, r)
	}))
	defer proxy.Close()
	framework, err := os.ReadFile(filepath.Join(repoRoot(t), "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	var sums []string
	for _, line := range strings.Split(string(framework), "\n") {
		if strings.HasPrefix(line, "golang.org/x/sys v0.48.0") {
			sums = append(sums, line)
		}
	}
	if len(sums) != 2 {
		t.Fatalf("framework go.sum lines for golang.org/x/sys: %v", sums)
	}
	hostile := func(cache string) []string {
		return append(os.Environ(), "GOPROXY="+proxy.URL, "GOFLAGS=-mod=mod", "GOTOOLCHAIN=auto", "GOSUMDB=off", "GOMODCACHE="+cache, "GONOSUMDB=*")
	}

	dir := scaffolded(t, "classic")
	applyEdits(t, dir, []fixtureEdit{
		{File: "go.mod", Append: "\ntoolchain go1.27.9\n\nrequire golang.org/x/sys v0.48.0\n"},
		{File: "go.sum", Append: strings.Join(sums, "\n") + "\n"},
		{File: "internal/app/sys.go", Write: ptr("package app\n\nimport \"golang.org/x/sys/unix\"\n\nvar _ = unix.Getpid\n")},
	}, placeholders("classic"))
	before := treeDigest(t, dir)
	cold := hostile(t.TempDir())
	var first []byte
	for run := range 2 {
		report := Check(context.Background(), Options{Root: dir, Env: cold})
		encoded := encode(t, report)
		if count := requests.Load(); count != 0 {
			t.Fatalf("run %d: %d requests reached the proxy, the last for %v", run, count, seen.Load())
		}
		if report.ExitCode != ExitFindings || len(report.Diagnostics) != 1 {
			t.Fatalf("run %d: %s", run, encoded)
		}
		got := report.Diagnostics[0]
		if got.Code != "go_module_not_in_cache" || got.Source != "internal/app/sys.go:3:8" || got.Actual != "golang.org/x/sys v0.48.0 is not in the module cache" {
			t.Fatalf("run %d: %+v", run, got)
		}
		if run == 0 {
			first = encoded
		} else if !bytes.Equal(first, encoded) {
			t.Fatalf("cold runs differ:\n%s\n%s", first, encoded)
		}
	}
	tested := Test(context.Background(), TestOptions{Options: Options{Root: dir, Env: cold}})
	if tested.ExitCode != ExitFindings || len(tested.Diagnostics) != 1 || tested.Diagnostics[0].Code != "go_module_not_in_cache" {
		t.Fatalf("test: %s", encode(t, tested))
	}
	if effects(before, treeDigest(t, dir)) != 0 {
		t.Fatal("check or test changed the project")
	}
	if count := requests.Load(); count != 0 {
		t.Fatalf("%d requests reached the proxy, the last for %v", count, seen.Load())
	}
	warm := append(os.Environ(), "GOPROXY="+proxy.URL, "GOFLAGS=-mod=mod", "GOTOOLCHAIN=auto")
	if report := Check(context.Background(), Options{Root: dir, Env: warm}); report.ExitCode != ExitOK {
		t.Fatalf("with the module cached: %s", encode(t, report))
	}

	newer := scaffolded(t, "unified")
	applyEdits(t, newer, []fixtureEdit{{File: "go.mod", Replace: [2]string{"\ngo 1.27.0\n", "\ngo 1.99.0\n"}}}, placeholders("unified"))
	before = treeDigest(t, newer)
	for _, report := range []Report{Check(context.Background(), Options{Root: newer, Env: cold}), Test(context.Background(), TestOptions{Options: Options{Root: newer, Env: cold}})} {
		if report.ExitCode != ExitFindings || len(report.Diagnostics) != 1 || report.Diagnostics[0].Code != "go_toolchain_too_old" || report.Diagnostics[0].Source != "go.mod" {
			t.Fatalf("%s: %s", report.Command, encode(t, report))
		}
	}
	if effects(before, treeDigest(t, newer)) != 0 || requests.Load() != 0 {
		t.Fatalf("too-old toolchain: effects or %d requests", requests.Load())
	}
}

// TestColdCacheStarterIsStable: a starter needs no module from the cache,
// so an empty module cache gives the same passing report every time.
func TestColdCacheStarterIsStable(t *testing.T) {
	dir := scaffolded(t, "unified")
	env := append(os.Environ(), "GOMODCACHE="+t.TempDir())
	first := encode(t, Check(context.Background(), Options{Root: dir, Env: env}))
	second := encode(t, Check(context.Background(), Options{Root: dir, Env: env}))
	if !bytes.Equal(first, second) || !bytes.Contains(first, []byte(`"exitCode": 0`)) {
		t.Fatalf("first:\n%s\nsecond:\n%s", first, second)
	}
}
