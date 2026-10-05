// Package devtool is the API behind blok check, blok test and blok inspect
// (E11-T02, ADR 0024). Each command returns one versioned Report; the CLI
// only encodes it, so the CLI's machine output and this API's values are the
// same diagnostics by construction.
//
// Check and Inspect never execute application code: they read files, parse
// Go source and run the Go toolchain's type checker (go vet), which compiles
// packages but runs none of them. Test runs the application's real tests
// through go test; nothing here replaces or imitates that execution.
package devtool

import (
	"context"
	"errors"
	"fmt"

	"github.com/well-prado/new-blok/internal/diagnostic"
)

// Version names the report schema. A consumer must reject a report whose
// version it does not know. Additive fields keep this version; renaming,
// removing or retyping a field, a status, an exit code or a diagnostic code
// requires a new one (ADR 0024).
const Version = "blok-cli/v1"

// Exit codes are part of the contract. Every command that gets past argument
// parsing writes exactly one report whose ExitCode is the process exit code.
const (
	// ExitOK: the command ran and found nothing to report.
	ExitOK = 0
	// ExitFindings: the project is invalid, a check failed or a test failed.
	ExitFindings = 1
	// ExitUsage: the arguments were invalid; no report is written.
	ExitUsage = 2
	// ExitTool: the command could not run (for example, go is not on PATH).
	ExitTool = 3
	// ExitOutput: the report could not be written to standard output.
	ExitOutput = 4
	// ExitInterrupted: SIGINT, SIGTERM or a canceled context stopped the
	// command. The report written holds everything found before the stop.
	ExitInterrupted = 130
)

// Status is the outcome a report states.
type Status string

const (
	StatusPassed      Status = "passed"
	StatusFailed      Status = "failed"
	StatusError       Status = "error"
	StatusInterrupted Status = "interrupted"
)

// MaxDiagnostics bounds one report. Later diagnostics are dropped and the
// report is marked Truncated.
const MaxDiagnostics = 1000

// Report is the one document every command produces.
type Report struct {
	Version  string   `json:"version"`
	Command  string   `json:"command"`
	Status   Status   `json:"status"`
	ExitCode int      `json:"exitCode"`
	Project  *Project `json:"project,omitempty"`
	// Checks lists every check blok check ran, skipped or did not reach.
	Checks []CheckResult `json:"checks,omitempty"`
	// Tests is blok test's result.
	Tests *TestResult `json:"tests,omitempty"`
	// Catalog is blok inspect's result.
	Catalog     *Catalog                `json:"catalog,omitempty"`
	Diagnostics []diagnostic.Diagnostic `json:"diagnostics"`
	Truncated   bool                    `json:"truncated,omitempty"`
}

// Project describes the application from its blok.json and go.mod.
type Project struct {
	Name     string   `json:"name,omitempty"`
	Module   string   `json:"module,omitempty"`
	Runtime  string   `json:"runtime,omitempty"`
	Layout   string   `json:"layout,omitempty"`
	Triggers []string `json:"triggers,omitempty"`
}

// CheckState is the outcome of one check.
type CheckState string

const (
	CheckPassed      CheckState = "passed"
	CheckFailed      CheckState = "failed"
	CheckSkipped     CheckState = "skipped"
	CheckInterrupted CheckState = "interrupted"
)

// CheckResult reports one named check. Unresolved counts constructs the
// check could not decide statically; they are neither passed nor failed.
type CheckResult struct {
	Name       string     `json:"name"`
	State      CheckState `json:"state"`
	Reason     string     `json:"reason,omitempty"`
	Unresolved int        `json:"unresolved,omitempty"`
}

// diagnostics accumulates a bounded, stable diagnostic list.
type diagnostics struct {
	items     []diagnostic.Diagnostic
	truncated bool
}

func (d *diagnostics) add(item diagnostic.Diagnostic) {
	if len(d.items) >= MaxDiagnostics {
		d.truncated = true
		return
	}
	d.items = append(d.items, item)
}

// finish fixes the report's status, exit code and diagnostic order. An
// interrupted command always says so, whatever it found before the stop.
func finish(report *Report, found *diagnostics, ctx context.Context, toolFailure bool) {
	if ctx.Err() != nil {
		found.add(interruptedDiagnostic(report.Command))
	}
	report.Diagnostics = make([]diagnostic.Diagnostic, len(found.items))
	for index, item := range found.items {
		report.Diagnostics[index] = redactDiagnostic(item)
	}
	diagnostic.Sort(report.Diagnostics)
	report.Truncated = found.truncated
	report.Version = Version
	switch {
	case ctx.Err() != nil:
		report.Status, report.ExitCode = StatusInterrupted, ExitInterrupted
	case toolFailure:
		report.Status, report.ExitCode = StatusError, ExitTool
	case len(report.Diagnostics) > 0:
		report.Status, report.ExitCode = StatusFailed, ExitFindings
	default:
		report.Status, report.ExitCode = StatusPassed, ExitOK
	}
}

func interruptedDiagnostic(command string) diagnostic.Diagnostic {
	return diagnostic.Diagnostic{
		Code:        "interrupted",
		Remediation: "rerun blok " + command + " to completion; results gathered before the interruption are partial",
		Message:     "blok " + command + " was interrupted before it finished",
	}
}

// errToolUnavailable marks a failure to start the Go toolchain.
var errToolUnavailable = errors.New("go toolchain unavailable")

func toolUnavailable(goBinary string, err error) diagnostic.Diagnostic {
	return diagnostic.Diagnostic{
		Code:        "go_toolchain_unavailable",
		Expected:    "a runnable go command",
		Actual:      err.Error(),
		Remediation: fmt.Sprintf("install Go 1.27 or later and make %q runnable from PATH", goBinary),
		Message:     "the Go toolchain could not be started",
	}
}
