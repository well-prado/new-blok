package devtool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/well-prado/new-blok/contract/deployment"
	"github.com/well-prado/new-blok/internal/diagnostic"
	"github.com/well-prado/new-blok/internal/generate"
	"github.com/well-prado/new-blok/observe/redact"
)

// DevVersion names blok dev's event stream (ADR 0026). A consumer must
// reject an event whose version it does not know; the compatibility rules
// are blok-cli/v1's.
const DevVersion = "blok-dev/v1"

// Environment blok dev adds to the application's, replacing any value the
// caller set: BLOK_DEV=1 marks a development run, and BLOK_DEV_GENERATION
// is a counter that grows with every start of the application. An
// application uses it as its persistent workers' generation, so a worker
// left from an earlier start is refused at the handshake (ADR 0004)
// instead of serving new code. Neither names or claims an artifact
// identity: the application derives that from its own executable.
const (
	EnvDev           = "BLOK_DEV"
	EnvDevGeneration = "BLOK_DEV_GENERATION"
)

// Defaults for DevOptions' zero durations.
const (
	// DefaultPoll is how often the watcher scans the project, at the
	// most: the interval stretches to ScanDuty times the last scan's
	// duration, and backs off while scans fail (ADR 0026).
	DefaultPoll = 250 * time.Millisecond
	// DefaultQuiet is how long the project must stay unchanged before a
	// rebuild starts, so a burst of saves becomes one build.
	DefaultQuiet = 300 * time.Millisecond
	// DefaultMaxWait bounds how long a stream of changes can postpone a
	// rebuild.
	DefaultMaxWait = 2 * time.Second
	// DefaultStopGrace is how long the application has to exit after it is
	// asked to; then its whole process group is killed.
	DefaultStopGrace = 10 * time.Second
	// DefaultBackoff and DefaultMaxBackoff bound restarts after the
	// application exits on its own: the delay doubles from the first to the
	// second, and resets after a new build or a run of DefaultStableAfter.
	DefaultBackoff     = 500 * time.Millisecond
	DefaultMaxBackoff  = 30 * time.Second
	DefaultStableAfter = 10 * time.Second
)

// Watcher cost bounds (ADR 0026).
const (
	// ScanDuty bounds the watcher's share of one CPU: the next scan waits
	// ScanDuty times as long as the last one took (up to MaxScanBackoff), so
	// scanning uses at most 1/ScanDuty of a CPU while a scan takes up to
	// MaxScanBackoff/ScanDuty (1 s).
	ScanDuty = 10
	// MaxScanBackoff bounds how far the pause between scans grows, while
	// they fail (doubling from Poll) or after a slow or stalled one
	// (ScanDuty), unless Poll itself is longer. An edit or a repair waits
	// that pause plus the scans' own duration to be seen.
	MaxScanBackoff = 10 * time.Second
)

// KeptBuilds is how many executables of earlier successful builds blok dev
// keeps on disk until it ends, besides the current build's and the newest
// one that ran without refusing its durable state (ADR 0026), so a run
// admitted under an earlier build can be finished with it.
const KeptBuilds = 5

// Bounds on what one event carries.
const (
	// MaxChangedListed bounds the paths a changed event lists; Files
	// counts them all.
	MaxChangedListed = 20
	// MaxOutputTail bounds the last lines of the application's standard
	// error an app-exited event carries, each cut at maxTailLineBytes.
	MaxOutputTail    = 20
	maxTailLineBytes = 1024
)

// Event names, in the order a session usually emits them.
const (
	EventWatching         = "watching"
	EventWatchFailed      = "watch-failed"
	EventChanged          = "changed"
	EventBuildStarted     = "build-started"
	EventBuildFailed      = "build-failed"
	EventBuildSucceeded   = "build-succeeded"
	EventAppStarted       = "app-started"
	EventAppStopped       = "app-stopped"
	EventAppExited        = "app-exited"
	EventRestartScheduled = "restart-scheduled"
	EventStopped          = "stopped"
)

// DevEvent is one line of blok dev's stream. Paths are project-relative
// and slash-separated; every string blok dev did not choose itself passes
// observe/redact.
type DevEvent struct {
	Version string `json:"version"`
	Event   string `json:"event"`
	// Build numbers successful and failed builds alike, from 1.
	Build int `json:"build,omitempty"`
	// Generation is the BLOK_DEV_GENERATION the application was started
	// with; it grows with every start.
	Generation int `json:"generation,omitempty"`
	PID        int `json:"pid,omitempty"`
	// Files counts watched files (watching) or changed paths (changed).
	Files   int      `json:"files,omitempty"`
	Changed []string `json:"changed,omitempty"`
	// Regenerated lists generated files a build rewrote first.
	Regenerated []string `json:"regenerated,omitempty"`
	// Running is the build that keeps serving after a failed build; 0 when
	// no application is running.
	Running int `json:"running,omitempty"`
	// Status is the application's exit status, as the OS reports it.
	Status string `json:"status,omitempty"`
	// DelayMS is how long until a scheduled restart.
	DelayMS int64 `json:"delayMs,omitempty"`
	// Output is the redacted tail of the application's standard error.
	Output      []string                `json:"output,omitempty"`
	Diagnostics []diagnostic.Diagnostic `json:"diagnostics,omitempty"`
	// Resume is, on an app-exited event with dev_durable_incompatible, the
	// shell command that runs the kept executable of the newest earlier
	// build that did not refuse the durable state, so its runs can be
	// finished while blok dev keeps running (it removes the executables
	// when it ends). Empty when no such build is kept, or when the command
	// looks like it holds a credential (observe/redact): redaction would
	// leave no command, so it is omitted and the diagnostic's remediation
	// says so.
	Resume string `json:"resume,omitempty"`
	// ExitCode is blok dev's own exit code, on the final stopped event.
	ExitCode *int `json:"exitCode,omitempty"`
}

// DevOptions select the project, the application and the loop's bounds.
// Zero durations take the defaults above.
type DevOptions struct {
	Options
	// Package is the main package to build, project-relative ("./cmd/x");
	// empty means ./cmd/<blok.json name>, where blok new puts it.
	Package string
	// Args are passed to the application.
	Args []string
	// AppEnv is the application's environment; os.Environ() when nil.
	// EnvDev and EnvDevGeneration are always set by blok dev.
	AppEnv []string
	// AppStdout and AppStderr receive the application's output; nil
	// discards it. A failing writer never blocks the application.
	AppStdout, AppStderr io.Writer
	// Emit receives every event, in order, on one goroutine. An error stops
	// blok dev with ExitOutput.
	Emit func(DevEvent) error
	// Force, when it receives, kills an application blok dev is waiting
	// on to exit instead of waiting out StopGrace (a second Ctrl+C).
	Force <-chan struct{}

	Poll, Quiet, MaxWait, StopGrace  time.Duration
	Backoff, MaxBackoff, StableAfter time.Duration
}

func (o *DevOptions) defaults() {
	set := func(value *time.Duration, fallback time.Duration) {
		if *value <= 0 {
			*value = fallback
		}
	}
	set(&o.Poll, DefaultPoll)
	set(&o.Quiet, DefaultQuiet)
	set(&o.MaxWait, DefaultMaxWait)
	set(&o.StopGrace, DefaultStopGrace)
	set(&o.Backoff, DefaultBackoff)
	set(&o.MaxBackoff, DefaultMaxBackoff)
	set(&o.StableAfter, DefaultStableAfter)
	if o.MaxBackoff < o.Backoff {
		o.MaxBackoff = o.Backoff
	}
	if o.Emit == nil {
		o.Emit = func(DevEvent) error { return nil }
	}
}

// Dev builds the application, runs it, and rebuilds and restarts it when a
// watched file changes, until ctx is canceled (ADR 0026). It returns blok
// dev's exit code, and the write error behind ExitOutput.
//
//   - A failed build (layout discovery, bindings or go build) is reported
//     with its diagnostics and the running application, if any, keeps
//     serving: only a build that compiles replaces it.
//   - The application runs in its own process group under the process
//     guard check and test use, so it and every worker it starts end with
//     blok dev, however blok dev ends.
//   - Stopping asks the application alone to exit (SIGTERM) so it can
//     drain its workers, then kills its process group after StopGrace.
//   - Each build is a new executable in a private directory, never
//     overwritten while anything runs it.
func Dev(ctx context.Context, options DevOptions) (code int, writeErr error) {
	options.defaults()
	loop := &devLoop{options: options, ctx: ctx, backoff: options.Backoff}
	// Deferred calls run while a panic unwinds too: the application's group
	// is killed before the panic reaches the CLI (exit 3).
	defer loop.cleanup()
	return loop.run()
}

type devLoop struct {
	options DevOptions
	ctx     context.Context
	root    string
	// buildDir holds the executables; it is removed on exit, and by guard,
	// the session's process guard, if blok dev is killed.
	buildDir string
	guard    *processGuard

	scan       snapshot
	scanFailed bool
	// scanFailures counts consecutive failed scans; scanTook is how long
	// the last scan took. Both set the delay before the next one.
	scanFailures int
	scanTook     time.Duration
	pending      map[string]bool
	firstSeen    time.Time
	lastSeen     time.Time

	builds, generation int
	// good is the last executable that compiled, and goodBuild its number.
	good      string
	goodBuild int
	// kept are the executables of earlier successful builds still on disk,
	// oldest first; refused marks builds whose application refused its
	// durable state (dev_durable_incompatible).
	kept    []keptBuild
	refused map[int]bool
	app     *appProcess
	backoff time.Duration
	restart *time.Timer

	fatal    int
	writeErr error
}

// keptBuild is one successful build's executable.
type keptBuild struct {
	number int
	path   string
}

// scanObserver, when set (by a test), is told how long each scan took and
// whether it failed. It runs on the loop's goroutine.
var scanObserver func(took time.Duration, failed bool)

func (l *devLoop) emit(event DevEvent) {
	if l.writeErr != nil {
		return
	}
	event.Version = DevVersion
	for index, item := range event.Diagnostics {
		event.Diagnostics[index] = redactDiagnostic(item)
	}
	diagnostic.Sort(event.Diagnostics)
	if err := l.options.Emit(event); err != nil {
		l.writeErr = err
	}
}

func (l *devLoop) run() (int, error) {
	root, err := filepath.Abs(l.options.Root)
	if err == nil {
		root, err = filepath.EvalSymlinks(root)
	}
	if err == nil {
		var info os.FileInfo
		if info, err = os.Stat(root); err == nil && !info.IsDir() {
			err = errors.New(filepath.Base(root) + ": not a directory")
		}
	}
	if err != nil {
		return l.stop(ExitFindings, diagnostic.Diagnostic{Code: "project_unreadable", Expected: "a readable project directory", Actual: errorText(err), Remediation: "run blok dev from a readable application directory", Message: "the project could not be read"})
	}
	l.root = root
	if l.buildDir, err = os.MkdirTemp("", "blok-dev-"); err != nil {
		return l.stop(ExitTool, diagnostic.Diagnostic{Code: "dev_build_dir_unavailable", Expected: "a private temporary directory for executables", Actual: errorText(err), Remediation: "make the temporary directory (TMPDIR) writable", Message: "blok dev could not create a directory for its builds"})
	}
	// The session's own guard removes the build directory if blok dev is
	// killed while no application, and so no application guard, is alive.
	if l.guard, err = startGuard(nil, l.buildDir); err != nil {
		return l.stop(ExitTool, diagnostic.Diagnostic{Code: "process_guard_unavailable", Expected: "a runnable " + guardShell, Actual: errorText(err), Remediation: "make " + guardShell + " available; blok dev runs only under a guard that cleans up if blok dies", Message: "the process guard could not be started"})
	}
	// A project that cannot be watched from the start is not run at all:
	// blok dev would serve code it could never rebuild.
	started := time.Now()
	current, problem, err := scanProject(l.ctx, l.root)
	l.observeScan(time.Since(started), err != nil || problem != nil)
	if err != nil && l.ctx.Err() != nil {
		return l.finish()
	}
	if err != nil {
		return l.stop(ExitFindings, diagnostic.Diagnostic{Code: "project_unreadable", Expected: "a readable project directory", Actual: relativeText(errorText(err), l.root), Remediation: "make the project readable, then run blok dev again", Message: "the project could not be scanned"})
	}
	if problem != nil {
		problem.Remediation += "; then run blok dev again"
		return l.stop(ExitFindings, *problem)
	}
	l.scan = current
	l.emit(DevEvent{Event: EventWatching, Files: len(l.scan)})
	l.build(nil)
	poll := time.NewTimer(l.nextScan())
	defer poll.Stop()
	for l.fatal == 0 && l.writeErr == nil {
		var exited <-chan struct{}
		if l.app != nil {
			exited = l.app.exited
		}
		var restart <-chan time.Time
		if l.restart != nil {
			restart = l.restart.C
		}
		select {
		case <-l.ctx.Done():
			return l.finish()
		case <-exited:
			l.exited(true)
		case <-restart:
			l.restart = nil
			l.launch()
		case <-poll.C:
			l.poll()
			poll.Reset(l.nextScan())
		}
	}
	return l.finish()
}

// observeScan records one scan's cost and outcome.
func (l *devLoop) observeScan(took time.Duration, failed bool) {
	l.scanTook = took
	if failed {
		l.scanFailures++
	} else {
		l.scanFailures = 0
	}
	if scanObserver != nil {
		scanObserver(took, failed)
	}
}

// nextScan is the delay before the next scan: Poll, stretched to ScanDuty
// times the last scan's duration, and doubled for every consecutive failed
// scan, each up to MaxScanBackoff, so a project the watcher cannot read, or
// reads slowly, is not rescanned at full speed, and a scan that stalled
// (a stopped process, a cold cache) does not postpone the next one by ten
// times the stall.
func (l *devLoop) nextScan() time.Duration {
	delay := max(l.options.Poll, min(ScanDuty*l.scanTook, MaxScanBackoff))
	if l.scanFailures > 0 {
		backoff := l.options.Poll
		for range l.scanFailures {
			if backoff >= MaxScanBackoff {
				break
			}
			backoff *= 2
		}
		delay = max(delay, min(backoff, MaxScanBackoff))
	}
	return delay
}

// rescan replaces the snapshot. A failed scan keeps the previous one and is
// reported once, when the watcher goes from working to failing. It returns
// false only when blok dev is being stopped.
func (l *devLoop) rescan() bool {
	started := time.Now()
	current, problem, err := scanProject(l.ctx, l.root)
	l.observeScan(time.Since(started), err != nil || problem != nil)
	if err != nil && l.ctx.Err() != nil {
		return false
	}
	if err != nil {
		problem = &diagnostic.Diagnostic{Code: "project_unreadable", Expected: "a readable project directory", Actual: relativeText(errorText(err), l.root), Remediation: "make the project readable; blok dev keeps watching", Message: "the project could not be scanned"}
	}
	if problem != nil {
		if !l.scanFailed {
			l.emit(DevEvent{Event: EventWatchFailed, Running: l.running(), Diagnostics: []diagnostic.Diagnostic{*problem}})
		}
		l.scanFailed = true
		return true
	}
	l.scanFailed = false
	l.scan = current
	return true
}

// poll scans the project and starts a build once the changes seen have
// been quiet for Quiet, or have been arriving for MaxWait.
func (l *devLoop) poll() {
	before := l.scan
	if !l.rescan() {
		return
	}
	now := time.Now()
	if changed := changes(before, l.scan); len(changed) > 0 {
		if l.pending == nil {
			l.pending, l.firstSeen = map[string]bool{}, now
		}
		for _, name := range changed {
			l.pending[name] = true
		}
		l.lastSeen = now
	}
	if l.pending == nil || (now.Sub(l.lastSeen) < l.options.Quiet && now.Sub(l.firstSeen) < l.options.MaxWait) {
		return
	}
	changed := make([]string, 0, len(l.pending))
	for name := range l.pending {
		changed = append(changed, name)
	}
	l.pending = nil
	l.build(changed)
}

// build rebuilds the application and, if it compiles, replaces the running
// one. Changes made while it runs are seen by the next poll.
func (l *devLoop) build(changed []string) {
	if l.writeErr != nil {
		return
	}
	if len(changed) > 0 {
		sort.Strings(changed)
		listed := make([]string, 0, min(len(changed), MaxChangedListed))
		for _, name := range changed[:min(len(changed), MaxChangedListed)] {
			listed = append(listed, redact.String(name))
		}
		l.emit(DevEvent{Event: EventChanged, Files: len(changed), Changed: listed})
	}
	l.builds++
	number := l.builds
	l.emit(DevEvent{Event: EventBuildStarted, Build: number})
	binary, regenerated, problems, toolFailure := l.compile(number)
	if l.ctx.Err() != nil {
		return
	}
	failed := len(problems) > 0 || binary == ""
	// The application may have exited on its own while the build ran. That
	// is an exit, never a stop: it is reported as such, and restarted with
	// backoff only when no new build replaces it.
	l.collectExit(failed)
	if failed {
		if binary == "" && len(problems) == 0 {
			problems = append(problems, toolchainError("go build failed without a diagnostic"))
		}
		l.emit(DevEvent{Event: EventBuildFailed, Build: number, Running: l.running(), Regenerated: regenerated, Diagnostics: problems})
		if toolFailure {
			l.fatal = ExitTool
		}
		return
	}
	l.emit(DevEvent{Event: EventBuildSucceeded, Build: number, Regenerated: regenerated})
	l.stopApp()
	l.cancelRestart()
	if l.good != "" {
		l.kept = append(l.kept, keptBuild{number: l.goodBuild, path: l.good})
	}
	l.good, l.goodBuild, l.backoff = binary, number, l.options.Backoff
	l.prune()
	l.launch()
}

// prune deletes the executables of earlier builds beyond the KeptBuilds
// most recent, except the one a dev_durable_incompatible refusal would
// resume. None of them is running: only the current build ever runs.
func (l *devLoop) prune() {
	resume, pinned := l.resumable(l.goodBuild + 1)
	kept := l.kept[:0]
	for index, item := range l.kept {
		if index >= len(l.kept)-KeptBuilds || pinned && item.number == resume.number {
			kept = append(kept, item)
			continue
		}
		_ = os.RemoveAll(filepath.Dir(item.path))
	}
	l.kept = kept
}

// resumable is the newest kept build before build that did not refuse its
// durable state.
func (l *devLoop) resumable(build int) (keptBuild, bool) {
	for index := len(l.kept) - 1; index >= 0; index-- {
		if item := l.kept[index]; item.number < build && !l.refused[item.number] {
			return item, true
		}
	}
	return keptBuild{}, false
}

// compile runs layout discovery, regenerates stale bindings and builds the
// main package into a new executable. toolFailure reports that the go
// command or its guard could not start.
func (l *devLoop) compile(number int) (binary string, regenerated []string, problems []diagnostic.Diagnostic, toolFailure bool) {
	workspace, found, err := l.options.source().Load(l.ctx, l.root)
	if err != nil {
		if l.ctx.Err() != nil {
			return "", nil, nil, false
		}
		return "", nil, []diagnostic.Diagnostic{{Code: "project_unreadable", Expected: "a readable project directory", Actual: errorText(err), Remediation: "make the project readable; blok dev keeps watching", Message: "the project could not be read"}}, false
	}
	if len(found) > 0 {
		return "", nil, found, false
	}
	main, missing := l.mainPackage(workspace)
	if missing != nil {
		return "", nil, []diagnostic.Diagnostic{*missing}, false
	}
	plan, problem, skipped := planBindings(workspace)
	switch {
	case problem != nil:
		return "", nil, []diagnostic.Diagnostic{*problem}, false
	case skipped != "":
	case plan.readErr != nil && !errors.Is(plan.readErr, os.ErrNotExist):
		return "", nil, []diagnostic.Diagnostic{{Code: "bindings_missing", Source: plan.path, Expected: "generated bindings beside the types file", Actual: errorText(plan.readErr), Remediation: "make the bindings file readable; blok dev regenerates it", Message: "the typed bindings cannot be read"}}, false
	case plan.handWritten():
		return "", nil, []diagnostic.Diagnostic{handWrittenBindings(plan.path)}, false
	case plan.stale():
		// Regenerate exactly as blok generate would, then record the new
		// file so the write is not seen as an edit.
		if err := generate.WriteFile(filepath.Join(l.root, filepath.FromSlash(plan.path)), plan.generated); err != nil {
			return "", nil, []diagnostic.Diagnostic{{Code: "dev_bindings_write_failed", Source: plan.path, Expected: "a writable bindings file", Actual: errorText(err), Remediation: "make the bindings file writable, or run blok generate", Message: "blok dev could not regenerate the typed bindings"}}, false
		}
		l.scan.refresh(l.root, plan.path)
		regenerated = []string{plan.path}
	}
	module := workspace.Module
	if module == "" {
		module = moduleOf(filepath.Join(l.root, "go.mod"))
	}
	// The build must not read files the watcher does not see, the
	// regenerated bindings' imports included. l.root is symlink-resolved
	// (run), as layout.ClassifyLink requires.
	unwatched, err := l.scan.buildReads(l.ctx, l.root, module, main)
	if err != nil {
		if l.ctx.Err() != nil {
			return "", regenerated, nil, false
		}
		return "", regenerated, []diagnostic.Diagnostic{{Code: "project_unreadable", Expected: "a readable project directory", Actual: relativeText(errorText(err), l.root), Remediation: "make the project readable; blok dev keeps watching", Message: "the project could not be read"}}, false
	}
	if len(unwatched) > 0 {
		return "", regenerated, unwatched, false
	}
	dir := filepath.Join(l.buildDir, "build-"+strconv.Itoa(number))
	if err := os.Mkdir(dir, 0o700); err != nil {
		return "", regenerated, []diagnostic.Diagnostic{{Code: "dev_build_dir_unavailable", Expected: "a private temporary directory for executables", Actual: errorText(err), Remediation: "make the temporary directory (TMPDIR) writable", Message: "blok dev could not create a directory for this build"}}, false
	}
	output := filepath.Join(dir, path.Base(main))
	if runtime.GOOS == "windows" {
		output += ".exe"
	}
	toolchain := &goDiagnostics{root: l.root}
	run, err := runGo(l.ctx, goCommand{
		binary: l.options.goBinary(), dir: l.root, args: []string{"build", "-o", output, "./" + main}, env: l.options.Env, remove: l.buildDir,
		stderr: func(line []byte) {
			l.options.observe("stderr", line)
			toolchain.line(string(line))
		},
	})
	if errors.Is(err, errToolUnavailable) {
		_ = os.RemoveAll(dir)
		return "", regenerated, []diagnostic.Diagnostic{toolUnavailable(l.options.goBinary(), err)}, true
	}
	problems = dedupe(toolchain.result())
	if err != nil && l.ctx.Err() == nil {
		problems = append(problems, toolchainError(err.Error()))
	}
	if run.exitCode != 0 || len(problems) > 0 || l.ctx.Err() != nil {
		if run.exitCode != 0 && len(problems) == 0 && l.ctx.Err() == nil {
			problems = append(problems, toolchainError("go build exited with status "+strconv.Itoa(run.exitCode)+" without a diagnostic"))
		}
		_ = os.RemoveAll(dir)
		return "", regenerated, problems, false
	}
	return output, regenerated, nil, false
}

// mainPackage is the project-relative directory of the package to build.
func (l *devLoop) mainPackage(workspace Workspace) (string, *diagnostic.Diagnostic) {
	requested := l.options.Package
	if requested == "" && workspace.Manifest != nil && workspace.Manifest.Name != "" {
		requested = "./cmd/" + workspace.Manifest.Name
	}
	rel := strings.TrimPrefix(filepath.ToSlash(requested), "./")
	missing := func(actual string) *diagnostic.Diagnostic {
		return &diagnostic.Diagnostic{Code: "dev_main_package_missing", Source: rel, Field: "package", Expected: "a main package directory inside the project", Actual: actual, Remediation: "pass blok dev --package ./cmd/<name>, the directory of the application's main package", Message: "blok dev cannot find the main package to build"}
	}
	if !validRelative(rel) {
		return "", missing("not a clean project-relative directory")
	}
	info, err := os.Lstat(filepath.Join(l.root, filepath.FromSlash(rel)))
	switch {
	case err != nil:
		return "", missing(errorText(err))
	case !info.IsDir():
		return "", missing("not a directory (a symbolic link is never followed)")
	}
	return rel, nil
}

// validRelative is a clean, relative, slash-separated path inside the root.
func validRelative(rel string) bool {
	return rel != "" && rel != "." && !path.IsAbs(rel) && path.Clean(rel) == rel && rel != ".." && !strings.HasPrefix(rel, "../") && !strings.ContainsAny(rel, "\\:\x00")
}

func (l *devLoop) running() int {
	if l.app == nil {
		return 0
	}
	return l.app.build
}

func (l *devLoop) cancelRestart() {
	if l.restart != nil {
		l.restart.Stop()
		l.restart = nil
	}
}

// launch starts the last executable that compiled.
func (l *devLoop) launch() {
	// A stop requested while the previous application drained, or while a
	// restart was due, starts nothing.
	if l.good == "" || l.app != nil || l.writeErr != nil || l.ctx.Err() != nil {
		return
	}
	l.generation++
	app, err := startApp(l.good, l.root, l.buildDir, l.options, l.generation, l.goodBuild)
	var start *startError
	switch {
	case errors.As(err, &start) && start.guard:
		l.emit(DevEvent{Event: EventAppExited, Build: l.goodBuild, Generation: l.generation, Diagnostics: []diagnostic.Diagnostic{{Code: "process_guard_unavailable", Expected: "a runnable " + guardShell, Actual: err.Error(), Remediation: "make " + guardShell + " available; blok dev runs the application only under a guard that stops it if blok dies", Message: "the process guard could not be started, so the application was stopped before it ran"}}})
		l.fatal = ExitTool
	case err != nil:
		l.emit(DevEvent{Event: EventAppExited, Build: l.goodBuild, Generation: l.generation, Diagnostics: []diagnostic.Diagnostic{{Code: "dev_app_start_failed", Expected: "a startable application executable", Actual: relativeText(err.Error(), l.buildDir), Remediation: "fix what stops the executable from starting; blok dev retries with backoff and rebuilds on the next change", Message: "the application could not be started"}}})
		l.scheduleRestart(0)
	default:
		l.app = app
		l.emit(DevEvent{Event: EventAppStarted, Build: app.build, Generation: app.generation, PID: app.cmd.Process.Pid})
	}
}

// collectExit reports the application as exited if it has exited on its
// own, without waiting; restart says whether it may be restarted.
func (l *devLoop) collectExit(restart bool) {
	if l.app == nil {
		return
	}
	select {
	case <-l.app.exited:
		l.exited(restart)
	default:
	}
}

// exited handles an application that stopped on its own: a clean exit
// waits for the next change, and so does a refusal of the durable state
// (deployment.ExitRetainedIncompatible), which no restart of the same
// executable can change; anything else restarts with backoff, when restart
// is set.
func (l *devLoop) exited(restart bool) {
	app := l.app
	l.app = nil
	app.reap()
	status := app.cmd.ProcessState
	event := DevEvent{Event: EventAppExited, Build: app.build, Generation: app.generation, Status: status.String(), Output: app.tail.lines()}
	switch {
	case status.Success():
		l.emit(event)
		return
	case status.ExitCode() == deployment.ExitRetainedIncompatible:
		if l.refused == nil {
			l.refused = map[int]bool{}
		}
		l.refused[app.build] = true
		event.Diagnostics = []diagnostic.Diagnostic{l.durableIncompatible(app.build, &event)}
		l.emit(event)
		return
	}
	event.Diagnostics = []diagnostic.Diagnostic{{Code: "dev_app_exited", Expected: "the application to keep running", Actual: status.String(), Remediation: "fix the failure the application reports (its output is above); blok dev restarts it with backoff and rebuilds on the next change", Message: "the application exited unexpectedly"}}
	l.emit(event)
	if restart {
		l.scheduleRestart(time.Since(app.started))
	}
}

// durableIncompatible is the diagnostic for build's refusal of the durable
// state, with event.Resume set when an earlier build is kept to finish
// its runs.
func (l *devLoop) durableIncompatible(build int, event *DevEvent) diagnostic.Diagnostic {
	problem := diagnostic.Diagnostic{
		Code:     "dev_durable_incompatible",
		Expected: "an application that can adopt the runs its durable state retains",
		Actual:   "exit status " + strconv.Itoa(deployment.ExitRetainedIncompatible) + ": the application refused its retained runs as incompatible with this build",
		Message:  "the application cannot resume runs admitted under an earlier build; blok dev does not restart it",
	}
	fixes := []string{"revert the change: Go builds are reproducible, so the reverted build is the one that admitted the runs, and blok dev starts it on save"}
	if kept, ok := l.resumable(build); ok {
		keeping := "or, while blok dev keeps running (it keeps build " + strconv.Itoa(kept.number) + "'s executable until it ends), finish the runs with it in another terminal: "
		// Redaction would replace the whole command with text that is not
		// a command, so a credential-shaped one is omitted instead, and the
		// remediation says how to run the executable without showing it.
		if command := resumeCommand(l.root, kept.path, l.options.Args); !redact.Sensitive(command) {
			event.Resume = command
			fixes = append(fixes, keeping+command+" — then stop it and save to rebuild")
		} else {
			fixes = append(fixes, keeping+"run "+redact.String(kept.path)+" from the project root with the application's arguments (the command is not shown: it looks like it holds a credential) — then stop it and save to rebuild")
		}
	}
	fixes = append(fixes, "or finish or discard the retained runs with the application's own tools (for example, remove its development journal)")
	problem.Remediation = strings.Join(fixes, "; ")
	return problem
}

// resumeCommand is a POSIX shell command that runs executable with args
// from root.
func resumeCommand(root, executable string, args []string) string {
	words := []string{"cd", shellQuote(root), "&&", shellQuote(executable)}
	for _, arg := range args {
		words = append(words, shellQuote(arg))
	}
	return strings.Join(words, " ")
}

// shellQuote quotes a word for a POSIX shell.
func shellQuote(word string) string {
	if word != "" && strings.Trim(word, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_./=:,+@%") == "" {
		return word
	}
	return "'" + strings.ReplaceAll(word, "'", `'\''`) + "'"
}

// scheduleRestart waits out the current backoff, then doubles it. A run
// that lasted StableAfter starts the backoff again from the beginning.
func (l *devLoop) scheduleRestart(ran time.Duration) {
	if ran >= l.options.StableAfter {
		l.backoff = l.options.Backoff
	}
	delay := l.backoff
	l.backoff = min(2*l.backoff, l.options.MaxBackoff)
	l.cancelRestart()
	l.restart = time.NewTimer(delay)
	l.emit(DevEvent{Event: EventRestartScheduled, Build: l.goodBuild, DelayMS: delay.Milliseconds()})
}

// stopApp stops the running application, bounded by StopGrace.
func (l *devLoop) stopApp() {
	if l.collectExit(false); l.app == nil {
		return
	}
	app := l.app
	killed, done := app.stop(l.options.StopGrace, l.options.Force)
	if done {
		// It had exited before it was asked to: an exit, not a stop.
		l.exited(false)
		return
	}
	l.app = nil
	event := DevEvent{Event: EventAppStopped, Build: app.build, Generation: app.generation, Status: app.cmd.ProcessState.String()}
	if killed {
		event.Diagnostics = []diagnostic.Diagnostic{{Code: "dev_app_stop_timeout", Expected: "the application to exit within " + l.options.StopGrace.String() + " of SIGTERM", Actual: "still running; its process group was killed", Remediation: "make the application drain and exit on SIGTERM or os.Interrupt within the grace period", Message: "the application did not stop in time"}}
	}
	l.emit(event)
}

// stop ends a session before the loop starts.
func (l *devLoop) stop(code int, problem diagnostic.Diagnostic) (int, error) {
	l.fatal = code
	l.emit(DevEvent{Event: EventWatchFailed, Diagnostics: []diagnostic.Diagnostic{problem}})
	return l.finish()
}

// finish stops the application and writes the final event.
func (l *devLoop) finish() (int, error) {
	l.cancelRestart()
	l.stopApp()
	// A signal is how a dev session normally ends, so it carries no
	// diagnostic; only the exit code (130) says what stopped it.
	code := l.fatal
	switch {
	case l.writeErr != nil:
		code = ExitOutput
	case code == 0:
		code = ExitInterrupted
	}
	l.emit(DevEvent{Event: EventStopped, ExitCode: &code})
	if l.writeErr != nil {
		code = ExitOutput
	}
	return code, l.writeErr
}

// cleanup kills whatever is still running and removes the builds. It runs
// on every return and on a panic, which then continues.
func (l *devLoop) cleanup() {
	if l.app != nil {
		l.app.kill()
		l.app = nil
	}
	l.cancelRestart()
	if l.buildDir != "" {
		_ = os.RemoveAll(l.buildDir)
		l.buildDir = ""
	}
	if l.guard != nil {
		l.guard.release()
		l.guard = nil
	}
}

// appProcess is one start of the application.
type appProcess struct {
	cmd               *exec.Cmd
	guard             *processGuard
	build, generation int
	started           time.Time
	exited            chan struct{}
	tail              *outputTail
	reapOnce          sync.Once
}

// startApp starts executable in its own process group, guarded so it dies
// with blok dev (the same guard check and test give the go command); if
// blok dev is killed, the guard also removes buildDir after the group.
func startApp(executable, root, buildDir string, options DevOptions, generation, build int) (*appProcess, error) {
	command := exec.Command(executable, options.Args...)
	command.Dir = root
	env := options.AppEnv
	if env == nil {
		env = os.Environ()
	}
	command.Env = withEnv(env, EnvDev+"=1", EnvDevGeneration+"="+strconv.Itoa(generation))
	tail := &outputTail{}
	// A file (the terminal) is handed to the application as is; any other
	// writer is fed through a pipe that never blocks it.
	switch out := options.AppStdout.(type) {
	case nil:
	case *os.File:
		command.Stdout = out
	default:
		command.Stdout = tolerant{out}
	}
	command.Stderr = io.MultiWriter(tolerant{options.AppStderr}, tail)
	command.WaitDelay = pipeDrain
	isolate(command)
	if err := command.Start(); err != nil {
		return nil, err
	}
	guard, err := startGuard(command.Process, buildDir)
	if err != nil {
		_ = killGroup(command.Process)
		_ = command.Wait()
		return nil, &startError{err: fmt.Errorf("start the process guard: %w", err), guard: true}
	}
	app := &appProcess{cmd: command, guard: guard, build: build, generation: generation, started: time.Now(), exited: make(chan struct{}), tail: tail}
	go func() {
		_ = command.Wait()
		close(app.exited)
	}()
	return app, nil
}

// stop asks the application alone to exit, so it can drain the workers it
// owns, and kills its process group if it has not exited after grace or
// when force receives. It reports whether the group had to be killed, and
// done when the application had already exited and been reaped, so it was
// never asked.
func (a *appProcess) stop(grace time.Duration, force <-chan struct{}) (killed, done bool) {
	if err := terminate(a.cmd.Process); errors.Is(err, os.ErrProcessDone) {
		<-a.exited
		return false, true
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-a.exited:
	case <-timer.C:
		killed = true
	case <-force:
		killed = true
	}
	if killed {
		_ = killGroup(a.cmd.Process)
		<-a.exited
	}
	a.reap()
	return killed, false
}

// kill ends the application's group at once (a panic, or cleanup).
func (a *appProcess) kill() {
	_ = killGroup(a.cmd.Process)
	<-a.exited
	a.reap()
}

// reap sweeps the application's process group once the application has
// exited — a worker it failed to stop, or any other descendant, dies with
// it — and then retires the guard.
func (a *appProcess) reap() {
	a.reapOnce.Do(func() {
		_ = killGroup(a.cmd.Process)
		a.guard.release()
	})
}

// withEnv replaces or adds settings in env.
func withEnv(env []string, settings ...string) []string {
	result := make([]string, 0, len(env)+len(settings))
	for _, entry := range env {
		key, _, _ := strings.Cut(entry, "=")
		keep := true
		for _, setting := range settings {
			name, _, _ := strings.Cut(setting, "=")
			keep = keep && !strings.EqualFold(key, name)
		}
		if keep {
			result = append(result, entry)
		}
	}
	return append(result, settings...)
}

// tolerant writes to w and ignores its errors, so a closed terminal can
// never block the application on a full pipe.
type tolerant struct{ w io.Writer }

func (t tolerant) Write(data []byte) (int, error) {
	if t.w != nil {
		_, _ = t.w.Write(data)
	}
	return len(data), nil
}

// outputTail keeps the last MaxOutputTail lines written to it.
type outputTail struct {
	mu      sync.Mutex
	kept    []string
	pending []byte
}

func (t *outputTail) Write(data []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, b := range data {
		if b == '\n' {
			t.push()
			continue
		}
		if len(t.pending) < maxTailLineBytes {
			t.pending = append(t.pending, b)
		}
	}
	return len(data), nil
}

func (t *outputTail) push() {
	t.kept = append(t.kept, strings.TrimRight(string(t.pending), "\r"))
	if len(t.kept) > MaxOutputTail {
		t.kept = t.kept[len(t.kept)-MaxOutputTail:]
	}
	t.pending = t.pending[:0]
}

// lines returns the tail, redacted as a block (ADR 0024).
func (t *outputTail) lines() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.pending) > 0 {
		t.push()
	}
	return redactLines(t.kept)
}
