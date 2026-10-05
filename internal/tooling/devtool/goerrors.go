package devtool

import (
	"regexp"
	"strings"

	"github.com/well-prado/new-blok/internal/diagnostic"
)

// positioned matches a compiler or vet position: path:line:col: message.
var positioned = regexp.MustCompile(`^(\S+\.go):(\d+)(?::(\d+))?: (.*)$`)

// informational go command lines report progress, not problems. Under
// GOPROXY=off "downloading" is an attempt that the next line refuses.
var informational = []string{"go: downloading ", "go: finding ", "go: extracting ", "go: found ", "go: upgraded ", "go: added "}

const (
	remediationDownload = "run go mod download in the project (blok never downloads modules), then rerun"
	remediationTidy     = "run go mod tidy in the project (blok never edits go.mod or go.sum), then rerun"
)

// goDiagnostics folds the go command's diagnostic lines — its standard
// error, or go test's build output — into diagnostics. It is fed from one
// goroutine at a time.
type goDiagnostics struct {
	root     string
	problems []diagnostic.Diagnostic
	pending  []string
	// module is the last module go said it was downloading: the one a
	// following "lookup disabled" line refers to.
	module string
}

func (g *goDiagnostics) line(raw string) {
	text := strings.TrimRight(raw, "\r\n")
	trimmed := strings.TrimSpace(text)
	switch {
	case trimmed == "" || strings.HasPrefix(text, "# "):
		g.flush()
		return
	case strings.HasPrefix(text, "\t") && len(g.pending) == 0 && len(g.problems) > 0:
		last := &g.problems[len(g.problems)-1]
		last.Actual += "; " + trimmed
		return
	}
	for _, prefix := range informational {
		if strings.HasPrefix(text, prefix) {
			if prefix == "go: downloading " {
				g.module = strings.TrimPrefix(text, prefix)
			}
			return
		}
	}
	// go vet prefixes the compile errors of a package nothing else imports
	// (a main package, a _test.go file) with "vet: "; go test does not. The
	// same error must be the same diagnostic from either (ADR 0024).
	if match := positioned.FindStringSubmatch(strings.TrimPrefix(text, "vet: ")); match != nil {
		g.flush()
		g.problems = append(g.problems, g.positionedProblem(match))
		return
	}
	message := strings.TrimPrefix(relativeText(text, g.root), "go: ")
	switch {
	case strings.Contains(message, "requires go >= "):
		g.add(diagnostic.Diagnostic{Code: "go_toolchain_too_old", Source: "go.mod", Expected: "a go directive the installed toolchain satisfies", Actual: message, Remediation: "install the Go version go.mod requires (blok runs the installed go with GOTOOLCHAIN=local and never downloads a toolchain)", Message: "the installed Go toolchain is older than go.mod requires"})
	case strings.Contains(message, "disabled by GOPROXY=off"):
		g.add(g.notInCache("go.mod", message))
	case strings.Contains(message, "updates to go.mod needed"):
		g.add(diagnostic.Diagnostic{Code: "go_mod_needs_update", Source: "go.mod", Expected: "a go.mod consistent with the code", Actual: message, Remediation: remediationTidy, Message: "go.mod needs updates the go command would have to write"})
	case strings.Contains(message, "missing go.sum entry"):
		g.add(diagnostic.Diagnostic{Code: "go_sum_missing", Source: "go.sum", Expected: "a go.sum entry for every required module", Actual: message, Remediation: remediationTidy, Message: "go.sum lacks an entry the build needs"})
	default:
		g.pending = append(g.pending, strings.TrimSpace(message))
	}
}

func (g *goDiagnostics) add(item diagnostic.Diagnostic) {
	g.flush()
	g.problems = append(g.problems, item)
}

func (g *goDiagnostics) positionedProblem(match []string) diagnostic.Diagnostic {
	item := compileError(g.root, match)
	message := item.Actual
	switch {
	case strings.Contains(message, "disabled by GOPROXY=off"):
		return g.notInCache(item.Source, message)
	case strings.Contains(message, "import lookup disabled by -mod="), strings.Contains(message, "no required module provides package"), strings.Contains(message, "cannot find module providing package"):
		item.Code, item.Expected, item.Remediation, item.Message = "go_module_missing", "a module in go.mod providing the imported package", "add the module with go get (blok never edits go.mod), then rerun", "no module in go.mod provides an imported package"
	case strings.Contains(message, "missing go.sum entry"):
		item.Code, item.Expected, item.Remediation, item.Message = "go_sum_missing", "a go.sum entry for every required module", remediationTidy, "go.sum lacks an entry the build needs"
	}
	return item
}

func (g *goDiagnostics) notInCache(source, message string) diagnostic.Diagnostic {
	actual := message
	if g.module != "" {
		actual = g.module + " is not in the module cache"
	}
	return diagnostic.Diagnostic{Code: "go_module_not_in_cache", Source: source, Expected: "every required module in the local module cache", Actual: actual, Remediation: remediationDownload, Message: "a required module is not in the module cache"}
}

func (g *goDiagnostics) flush() {
	if len(g.pending) > 0 {
		g.problems = append(g.problems, toolchainError(strings.Join(g.pending, "; ")))
		g.pending = nil
	}
}

// result returns the problems, flushing any pending text.
func (g *goDiagnostics) result() []diagnostic.Diagnostic {
	g.flush()
	return g.problems
}

func compileError(root string, match []string) diagnostic.Diagnostic {
	position := relativePath(match[1], root) + ":" + match[2]
	if match[3] != "" {
		position += ":" + match[3]
	}
	return diagnostic.Diagnostic{Code: "go_compile_error", Source: position, Expected: "Go code that type-checks", Actual: relativeText(match[4], root), Remediation: "fix the Go compile error at this position", Message: "the Go code does not compile"}
}

func toolchainError(text string) diagnostic.Diagnostic {
	return diagnostic.Diagnostic{Code: "go_toolchain_error", Expected: "the go command to load every package", Actual: text, Remediation: "fix the module or package problem the go command reports", Message: "the go command reported a problem"}
}
