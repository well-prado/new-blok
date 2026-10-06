package devtool

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/well-prado/new-blok/internal/diagnostic"
)

// WriteJSON writes the report as one indented JSON document and a newline.
func WriteJSON(w io.Writer, report Report) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	_, err = w.Write(append(data, '\n'))
	return err
}

// WriteHuman writes the report for a person. Its line shapes are stable:
// one "<source>: <code>: <message>" line per diagnostic, followed by
// "  expected:", "  actual:" and "  fix:" lines, and a closing
// "blok <command>: <status>" summary.
func WriteHuman(w io.Writer, report Report) error {
	out := bufio.NewWriter(w)
	if report.Project != nil && report.Project.Module != "" {
		fmt.Fprintf(out, "project %s", report.Project.Module)
		if report.Project.Layout != "" {
			fmt.Fprintf(out, " (%s layout)", report.Project.Layout)
		}
		fmt.Fprintln(out)
	}
	for _, check := range report.Checks {
		fmt.Fprintf(out, "check %-15s %s", check.Name, check.State)
		if check.Reason != "" {
			fmt.Fprintf(out, ": %s", check.Reason)
		}
		if check.Unresolved > 0 {
			fmt.Fprintf(out, " (%d not read statically)", check.Unresolved)
		}
		fmt.Fprintln(out)
	}
	if report.Tests != nil {
		writeTests(out, *report.Tests)
	}
	if report.Catalog != nil {
		writeCatalog(out, *report.Catalog)
	}
	for _, item := range report.Diagnostics {
		writeDiagnostic(out, item)
	}
	fmt.Fprintf(out, "blok %s: %s", report.Command, report.Status)
	if count := len(report.Diagnostics); count > 0 {
		noun := "problems"
		if count == 1 {
			noun = "problem"
		}
		fmt.Fprintf(out, ", %d %s", count, noun)
		if report.Truncated {
			fmt.Fprint(out, " (truncated)")
		}
	}
	fmt.Fprintln(out)
	return out.Flush()
}

func writeDiagnostic(out io.Writer, item diagnostic.Diagnostic) {
	source := item.Source
	if source == "" {
		source = "-"
	}
	fmt.Fprintf(out, "%s: %s: %s", source, item.Code, item.Message)
	if item.Step != "" {
		fmt.Fprintf(out, " (step %s)", item.Step)
	}
	fmt.Fprintln(out)
	if item.Expected != "" {
		fmt.Fprintf(out, "  expected: %s\n", item.Expected)
	}
	if item.Actual != "" {
		fmt.Fprintf(out, "  actual: %s\n", item.Actual)
	}
	fmt.Fprintf(out, "  fix: %s\n", item.Remediation)
}

func writeTests(out io.Writer, result TestResult) {
	for _, item := range result.Packages {
		fmt.Fprintf(out, "%-12s %s\n", item.Status, item.ImportPath)
		for _, test := range item.Tests {
			if test.Status == TestPass {
				continue
			}
			fmt.Fprintf(out, "  %-10s %s\n", test.Status, test.Name)
			for _, line := range test.Output {
				fmt.Fprintf(out, "    %s\n", strings.TrimSpace(line))
			}
		}
	}
	fmt.Fprintf(out, "tests: %d passed, %d failed, %d skipped, %d incomplete\n", result.Passed, result.Failed, result.Skipped, result.Incomplete)
}

func writeCatalog(out io.Writer, catalog Catalog) {
	at := func(source string) string {
		if source == "" {
			return ""
		}
		return "  " + source
	}
	for _, item := range catalog.Nodes {
		fmt.Fprintf(out, "node %s@%s (%s)%s\n", item.ID, item.Version, item.Package, at(item.Source))
		if item.Description != "" {
			fmt.Fprintf(out, "  %s\n", item.Description)
		}
		writeReferences(out, "test", item.Tests)
		writeReferences(out, "example", item.Examples)
	}
	for _, item := range catalog.Workflows {
		fmt.Fprintf(out, "workflow %s@%s (%s)%s\n", item.Name, item.Version, item.Package, at(item.Source))
		for _, step := range item.Steps {
			fmt.Fprintf(out, "  step %s %s%s\n", step.ID, step.Kind, at(step.Source))
		}
		writeReferences(out, "test", item.Tests)
		writeReferences(out, "example", item.Examples)
	}
	for _, item := range catalog.Triggers {
		fmt.Fprintf(out, "trigger %s %s %s -> %s%s\n", item.Kind, item.Method, item.Path, item.Workflow, at(item.Source))
	}
	for _, item := range catalog.Unresolved {
		fmt.Fprintf(out, "unresolved %s%s: %s\n", item.Kind, at(item.Source), item.Reason)
	}
	if catalog.Redacted > 0 {
		fmt.Fprintf(out, "redacted %d values\n", catalog.Redacted)
	}
}

func writeReferences(out io.Writer, kind string, references []Reference) {
	for _, reference := range references {
		fmt.Fprintf(out, "  %s %s", kind, reference.Name)
		if reference.Source != "" {
			fmt.Fprintf(out, "  %s", reference.Source)
		}
		fmt.Fprintln(out)
	}
}

// WriteDevJSON writes one blok dev event as a single JSON line.
func WriteDevJSON(w io.Writer, event DevEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = w.Write(append(data, '\n'))
	return err
}

// WriteDevHuman writes one blok dev event for a person: a "blok dev: …"
// line, then the event's diagnostics in WriteHuman's line shapes.
func WriteDevHuman(w io.Writer, event DevEvent) error {
	out := bufio.NewWriter(w)
	fmt.Fprint(out, "blok dev: ")
	switch event.Event {
	case EventWatching:
		fmt.Fprintf(out, "watching %d files", event.Files)
	case EventWatchFailed:
		fmt.Fprint(out, "cannot watch the project")
	case EventChanged:
		fmt.Fprintf(out, "%d %s changed: %s", event.Files, plural(event.Files, "file", "files"), strings.Join(event.Changed, ", "))
		if more := event.Files - len(event.Changed); more > 0 {
			fmt.Fprintf(out, " and %d more", more)
		}
	case EventBuildStarted:
		fmt.Fprintf(out, "build %d started", event.Build)
	case EventBuildFailed:
		fmt.Fprintf(out, "build %d failed, %d %s; ", event.Build, len(event.Diagnostics), plural(len(event.Diagnostics), "problem", "problems"))
		if event.Running > 0 {
			fmt.Fprintf(out, "build %d keeps running", event.Running)
		} else {
			fmt.Fprint(out, "nothing is running")
		}
	case EventBuildSucceeded:
		fmt.Fprintf(out, "build %d succeeded", event.Build)
	case EventAppStarted:
		fmt.Fprintf(out, "started build %d (generation %d, pid %d)", event.Build, event.Generation, event.PID)
	case EventAppStopped:
		fmt.Fprintf(out, "stopped build %d (%s)", event.Build, event.Status)
	case EventAppExited:
		fmt.Fprintf(out, "build %d exited", event.Build)
		if event.Status != "" {
			fmt.Fprintf(out, " (%s)", event.Status)
		}
	case EventRestartScheduled:
		fmt.Fprintf(out, "restarting build %d in %dms", event.Build, event.DelayMS)
	case EventStopped:
		fmt.Fprint(out, "stopped")
		if event.ExitCode != nil {
			fmt.Fprintf(out, ", exit code %d", *event.ExitCode)
		}
	default:
		fmt.Fprint(out, event.Event)
	}
	if len(event.Regenerated) > 0 {
		fmt.Fprintf(out, " (regenerated %s)", strings.Join(event.Regenerated, ", "))
	}
	fmt.Fprintln(out)
	for _, line := range event.Output {
		fmt.Fprintf(out, "  | %s\n", line)
	}
	for _, item := range event.Diagnostics {
		writeDiagnostic(out, item)
	}
	if event.Resume != "" {
		fmt.Fprintf(out, "  resume: %s\n", event.Resume)
	}
	return out.Flush()
}

func plural(count int, one, many string) string {
	if count == 1 {
		return one
	}
	return many
}
