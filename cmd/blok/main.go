// Command blok provides development tooling for New Blok applications.
package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"

	"github.com/well-prado/new-blok/internal/generate"
	"github.com/well-prado/new-blok/internal/scaffold"
	"github.com/well-prado/new-blok/internal/tooling/devtool"
	"github.com/well-prado/new-blok/internal/tooling/layout"
)

const version = "0.0.0-dev"

func run(args []string, out io.Writer) error { return runWithIO(args, out, os.Stdin) }

func runWithIO(args []string, out io.Writer, in io.Reader) error {
	if len(args) == 0 || (len(args) == 1 && (args[0] == "help" || args[0] == "--help")) {
		return writeHelp(out)
	}
	switch args[0] {
	case "version":
		if len(args) != 1 {
			return fmt.Errorf("unsupported command %q; run blok help", args[0])
		}
		_, err := fmt.Fprintln(out, "blok "+version)
		return err
	case "new":
		return runNew(args[1:], out, in)
	case "generate":
		return runGenerate(args[1:], out)
	default:
		return fmt.Errorf("unsupported command %q; run blok help", args[0])
	}
}

func writeHelp(out io.Writer) error {
	_, err := fmt.Fprintln(out, "New Blok — Go application framework\n\nUsage: blok <command>\n\nCommands:\n  new       Create a conventional Go application\n  generate  Generate deterministic typed bindings\n  check     Validate the application without running it\n  test      Run the application's tests with go test\n  inspect   Describe the application's nodes, workflows and triggers\n  dev       Build, run and watch the application, restarting it on changes\n  version   Print the development version\n  help      Show this help\n\nUse blok <command> --help for command options.")
	return err
}

func runNew(args []string, out io.Writer, in io.Reader) error {
	if hasHelp(args) {
		_, err := fmt.Fprintln(out, "Usage: blok new [options] <directory>\n\nOptions:\n  --module PATH       Go module path (default: example.com/<name>)\n  --name NAME         executable name\n  --runtime go        native runtime\n  --layout classic|unified\n  --trigger http      selected trigger\n  --framework V|DIR   framework version, or a local checkout to use through replace\n                      (default: the version this blok was built from)\n  --skip-tidy         do not run go mod tidy in the new application\n  --interactive       prompt for choices\n  --non-interactive   fail instead of prompting")
		return err
	}
	flags := flag.NewFlagSet("new", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	module, name, runtime, layout, trigger := "", "", "go", "classic", "http"
	interactive, nonInteractive, skipTidy := false, false, false
	framework := ""
	flags.StringVar(&framework, "framework", "", "framework version or local checkout")
	flags.BoolVar(&skipTidy, "skip-tidy", false, "do not run go mod tidy")
	flags.StringVar(&module, "module", "", "Go module path")
	flags.StringVar(&name, "name", "", "application executable name")
	flags.StringVar(&runtime, "runtime", "go", "native runtime")
	flags.StringVar(&layout, "layout", "classic", "node layout: classic or unified")
	flags.StringVar(&trigger, "trigger", "http", "comma-separated selected triggers")
	flags.BoolVar(&interactive, "interactive", false, "ask for missing project choices")
	flags.BoolVar(&nonInteractive, "non-interactive", false, "fail instead of prompting")
	if err := flags.Parse(reorderFlags(args, map[string]bool{"module": true, "name": true, "runtime": true, "layout": true, "trigger": true, "framework": true, "skip-tidy": false, "interactive": false, "non-interactive": false})); err != nil {
		return fmt.Errorf("new: %w", err)
	}
	positionals := flags.Args()
	if len(positionals) > 1 {
		return fmt.Errorf("new: expected one target directory")
	}
	directory := ""
	if len(positionals) == 1 {
		directory = positionals[0]
	}
	if directory == "" && !nonInteractive {
		interactive = true
	}
	if interactive {
		// One reader for the whole session: a reader per prompt would read
		// ahead piped answers and discard them with itself.
		reader := bufio.NewReader(in)
		var err error
		if directory, err = prompt(reader, out, "Target directory", directory); err != nil {
			return err
		}
		// Offer the defaults new would apply, so Enter accepts them; the
		// module default follows the name chosen just before it.
		if name == "" {
			name = scaffold.DefaultName(directory)
		}
		if name, err = prompt(reader, out, "Executable name", name); err != nil {
			return err
		}
		if module == "" {
			module = scaffold.DefaultModule(name)
		}
		if module, err = prompt(reader, out, "Module path", module); err != nil {
			return err
		}
		if runtime, err = prompt(reader, out, "Runtime", runtime); err != nil {
			return err
		}
		if layout, err = prompt(reader, out, "Layout (classic or unified)", layout); err != nil {
			return err
		}
		if trigger, err = prompt(reader, out, "Triggers", trigger); err != nil {
			return err
		}
	}
	if directory == "" {
		return fmt.Errorf("new: target directory is required (pass a path or use --interactive)")
	}
	selected, err := frameworkFor(framework)
	if err != nil {
		return err
	}
	paths, err := scaffold.Create(scaffold.Options{Directory: directory, Module: module, Name: name, Runtime: runtime, Layout: layout, Triggers: splitChoices(trigger), Framework: selected})
	if err != nil {
		return err
	}
	for _, path := range paths {
		if _, err := fmt.Fprintln(out, "created "+filepath.ToSlash(filepath.Join(directory, path))); err != nil {
			return err
		}
	}
	if skipTidy {
		_, err := fmt.Fprintln(out, "skipped go mod tidy; run it in "+filepath.ToSlash(directory)+" before building")
		return err
	}
	if _, err := fmt.Fprintln(out, "running go mod tidy in "+filepath.ToSlash(directory)); err != nil {
		return err
	}
	tidy := exec.Command("go", "mod", "tidy")
	tidy.Dir = directory
	if output, err := tidy.CombinedOutput(); err != nil {
		hint := ""
		if framework == "" {
			hint = fmt.Sprintf("; the starter requires %s %s, the version this blok was built from — if that commit is not published, remove %s and rerun with --framework <version or framework checkout>, or point its go.mod at one", scaffold.FrameworkModule, selected.Version, directory)
		}
		return fmt.Errorf("new: go mod tidy failed in %s (the files were created; fix the cause and rerun it)%s: %v\n%s", directory, hint, err, bytes.TrimSpace(output))
	}
	return nil
}

// buildVersion is this binary's module version, stamped by the go command.
var buildVersion = func() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		return info.Main.Version
	}
	return ""
}

// frameworkFor resolves --framework: a local framework checkout (used
// through a replace directive) or a module version. Without it the starter
// depends on the version this blok was built from, which only works when
// that build came from a published module version.
func frameworkFor(value string) (scaffold.Framework, error) {
	if value != "" {
		if info, err := os.Stat(value); err == nil && info.IsDir() {
			return scaffold.Framework{Dir: value}, nil
		}
		return scaffold.Framework{Version: value}, nil
	}
	version := buildVersion()
	if version == "" || version == "(devel)" || strings.Contains(version, "+dirty") {
		return scaffold.Framework{}, fmt.Errorf("new: this blok was built from a local or modified checkout (version %q), so the starter cannot depend on it from the module proxy; pass --framework with a framework version or the path of a framework checkout", version)
	}
	return scaffold.Framework{Version: version}, nil
}

func prompt(in *bufio.Reader, out io.Writer, label, current string) (string, error) {
	if _, err := fmt.Fprintf(out, "%s%s: ", label, valueSuffix(current)); err != nil {
		return "", err
	}
	line, err := in.ReadString('\n')
	if errors.Is(err, io.EOF) && strings.TrimSpace(line) == "" {
		return "", scaffold.ErrCancelled
	}
	// An answer that ends the input without a newline is still an answer.
	if err != nil && !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("%w: %v", scaffold.ErrCancelled, err)
	}
	line = strings.TrimSpace(line)
	if line == "" {
		line = current
	}
	if line == "" {
		return "", scaffold.ErrCancelled
	}
	return line, nil
}

func valueSuffix(value string) string {
	if value == "" {
		return ""
	}
	return " [" + value + "]"
}

func runGenerate(args []string, out io.Writer) error {
	if hasHelp(args) {
		_, err := fmt.Fprintln(out, "Usage: blok generate [options] [input [output]]\n\nOptions:\n  --input PATH        Go source file to analyze\n  --output PATH       generated Go output file\n  --package NAME      generated package name\n  --check             fail when generated output is stale")
		return err
	}
	flags := flag.NewFlagSet("generate", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	input, output, packageName := "", "", ""
	check := false
	flags.StringVar(&input, "input", "", "Go source file to analyze")
	flags.StringVar(&output, "output", "", "generated Go output file")
	flags.StringVar(&packageName, "package", "", "generated package name")
	flags.BoolVar(&check, "check", false, "fail when generated output is stale")
	if err := flags.Parse(reorderFlags(args, map[string]bool{"input": true, "output": true, "package": true, "check": false})); err != nil {
		return fmt.Errorf("generate: %w", err)
	}
	positionals := flags.Args()
	if input == "" && len(positionals) > 0 {
		input = positionals[0]
	}
	if output == "" && len(positionals) > 1 {
		output = positionals[1]
	}
	if len(positionals) > 2 {
		return fmt.Errorf("generate: expected input and optional output")
	}
	if input == "" {
		var err error
		if input, err = defaultTypes(); err != nil {
			return fmt.Errorf("generate: %w", err)
		}
	}
	source, err := os.ReadFile(input)
	if err != nil {
		return fmt.Errorf("generate: read %s: %w", input, err)
	}
	generated, err := generate.Source(source, generate.Options{Package: packageName})
	if err != nil {
		return fmt.Errorf("generate: %w", err)
	}
	if output == "" {
		output = filepath.Join(filepath.Dir(input), "bindings_gen.go")
	}
	if existing, readErr := os.ReadFile(output); readErr == nil {
		if !bytes.HasPrefix(existing, []byte("// Code generated by new-blok generate; DO NOT EDIT.")) {
			return fmt.Errorf("generate: refusing to replace non-generated file %s", output)
		}
		if bytes.Equal(existing, generated) {
			_, err := fmt.Fprintln(out, "generated "+filepath.ToSlash(output)+" (unchanged)")
			return err
		}
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return fmt.Errorf("generate: inspect %s: %w", output, readErr)
	}
	if check {
		return fmt.Errorf("generate: %s is stale", output)
	}
	if err := generate.WriteFile(output, generated); err != nil {
		return fmt.Errorf("generate: %w", err)
	}
	_, err = fmt.Fprintln(out, "generated "+filepath.ToSlash(output))
	return err
}

// defaultTypes is the types file the application's blok.json names, so a
// bare blok generate run in the application regenerates its bindings. The
// manifest is read through layout discovery's validation: an unknown field,
// a linked manifest or a types path outside the project fails instead of
// being guessed around.
func defaultTypes() (string, error) {
	const fallback = "internal/app/types.go"
	if _, err := os.Lstat(layout.ManifestFile); errors.Is(err, fs.ErrNotExist) {
		return fallback, nil
	}
	manifest, err := layout.LoadManifest(".")
	if err != nil {
		return "", err
	}
	if manifest.Types == "" {
		return fallback, nil
	}
	return filepath.FromSlash(manifest.Types), nil
}

func splitChoices(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			result = append(result, part)
		}
	}
	return result
}

func hasHelp(args []string) bool {
	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			return true
		}
	}
	return false
}

// reorderFlags makes the CLI forgiving of the conventional "command path
// --flag value" form while retaining flag.FlagSet's diagnostics and types.
func reorderFlags(args []string, valueFlags map[string]bool) []string {
	flags := make([]string, 0, len(args))
	positionals := make([]string, 0, len(args))
	for index := 0; index < len(args); index++ {
		arg := args[index]
		if !strings.HasPrefix(arg, "--") || arg == "--" {
			positionals = append(positionals, arg)
			continue
		}
		flags = append(flags, arg)
		name := strings.TrimPrefix(arg, "--")
		if equal := strings.IndexByte(name, '='); equal >= 0 {
			name = name[:equal]
		}
		if valueFlags[name] && !strings.Contains(arg, "=") && index+1 < len(args) {
			index++
			flags = append(flags, args[index])
		}
	}
	return append(flags, positionals...)
}

func main() {
	os.Exit(execute(os.Args[1:], os.Stdout, os.Stderr, os.Stdin))
}

// execute runs one command and returns the process exit code. check, test
// and inspect follow the exit-code contract in ADR 0024; every other command
// exits 1 on any error, as before.
func execute(args []string, stdout, stderr io.Writer, stdin io.Reader) int {
	if len(args) > 0 && args[0] == "dev" {
		return executeDev(args[1:], stdout, stderr)
	}
	if len(args) > 0 && toolCommands[args[0]] {
		// Signals are caught only for these commands, so Ctrl+C still ends
		// an interactive blok new at once. Until stop, a further signal is
		// absorbed: the first one already stops the go command, bounded by
		// devtool.InterruptGrace. If blok is killed outright, devtool's
		// process guard stops the go command instead.
		ctx, stop := signal.NotifyContext(context.Background(), toolSignals...)
		defer stop()
		ignoreBrokenPipe()
		return guarded(args[0], stderr, func() int { return runTool(ctx, args[0], args[1:], stdout, stderr) })
	}
	if err := runWithIO(args, stdout, stdin); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

var toolCommands = map[string]bool{"check": true, "test": true, "inspect": true}

// guarded turns a panic into exit 3 with the panic on stderr. devtool has
// already killed any go command the panic interrupted.
func guarded(command string, stderr io.Writer, run func() int) (code int) {
	defer func() {
		if recovered := recover(); recovered != nil {
			fmt.Fprintf(stderr, "blok %s: internal error: %v\n%s", command, recovered, debug.Stack())
			code = devtool.ExitTool
		}
	}()
	return run()
}

var toolUsage = map[string]string{
	"check":   "Usage: blok check [--json] [directory]\n\nValidates the application without running any of its code: blok.json and\ngo.mod, node import independence, generated bindings, workflow step ids and\ngo vet (which type-checks every package).\n\nOptions:\n  --json   write the versioned machine-readable report\n\nExit codes: 0 passed, 1 problems found, 2 usage, 3 tool unavailable,\n4 output not written, 130 interrupted.",
	"test":    "Usage: blok test [--json] [--run REGEXP] [--race] [directory]\n\nRuns the application's tests with go test and reports every package, test\nand failure.\n\nOptions:\n  --json         write the versioned machine-readable report\n  --run REGEXP   run only the tests go test -run selects\n  --race         enable the race detector\n\nExit codes: 0 passed, 1 failures, 2 usage, 3 tool unavailable,\n4 output not written, 130 interrupted.",
	"inspect": "Usage: blok inspect [--json] [--fields LIST] [directory]\n\nDescribes the application's nodes, workflows and HTTP routes from its source,\nwithout running it, with source, test and example references.\n\nOptions:\n  --json          write the versioned machine-readable report\n  --fields LIST   comma-separated: " + strings.Join(devtool.AllFields, ",") + "\n                  (default " + strings.Join(devtool.DefaultFields, ",") + ")\n\nExit codes: 0 described, 1 project invalid, 2 usage, 4 output not written,\n130 interrupted.",
}

// runTool parses a check, test or inspect command line, runs it and writes
// its one report.
func runTool(ctx context.Context, command string, args []string, stdout, stderr io.Writer) int {
	if hasHelp(args) {
		if _, err := fmt.Fprintln(stdout, toolUsage[command]); err != nil {
			fmt.Fprintf(stderr, "blok %s: write output: %v\n", command, err)
			return devtool.ExitOutput
		}
		return devtool.ExitOK
	}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	asJSON, race := false, false
	run, fields := "", ""
	valueFlags := map[string]bool{"json": false}
	flags.BoolVar(&asJSON, "json", false, "machine-readable report")
	switch command {
	case "test":
		flags.StringVar(&run, "run", "", "go test -run pattern")
		flags.BoolVar(&race, "race", false, "race detector")
		valueFlags["run"], valueFlags["race"] = true, false
	case "inspect":
		flags.StringVar(&fields, "fields", "", "projected fields")
		valueFlags["fields"] = true
	}
	usage := func(err error) int {
		fmt.Fprintf(stderr, "blok %s: %v; run blok %s --help\n", command, err, command)
		return devtool.ExitUsage
	}
	if err := flags.Parse(reorderFlags(args, valueFlags)); err != nil {
		return usage(err)
	}
	if flags.NArg() > 1 {
		return usage(fmt.Errorf("expected at most one project directory"))
	}
	root := "."
	if flags.NArg() == 1 {
		root = flags.Arg(0)
	}
	options := devtool.Options{Root: root}
	var report devtool.Report
	switch command {
	case "check":
		report = devtool.Check(ctx, options)
	case "test":
		report = devtool.Test(ctx, devtool.TestOptions{Options: options, Run: run, Race: race})
	case "inspect":
		selected, err := devtool.ParseFields(fields)
		if err != nil {
			return usage(err)
		}
		report = devtool.Inspect(ctx, devtool.InspectOptions{Options: options, Fields: selected})
	}
	write := devtool.WriteHuman
	if asJSON {
		write = devtool.WriteJSON
	}
	if err := write(stdout, report); err != nil {
		fmt.Fprintf(stderr, "blok %s: write output: %v\n", command, err)
		return devtool.ExitOutput
	}
	return report.ExitCode
}

const devUsage = `Usage: blok dev [--json] [--package DIR] [directory] [-- application arguments]

Builds the application's main package, runs it, and watches the project.
When a watched file changes (blok.json, go.mod, go.sum, the module's Go
source outside _test.go files, and every file of a node directory) it
regenerates stale bindings, rebuilds, and replaces the running application.
A build that fails is reported and the running application keeps serving.
An application that exits on its own is restarted with backoff.

The application runs in its own process group with BLOK_DEV=1 and
BLOK_DEV_GENERATION set; stopping asks it alone to exit (SIGTERM), so it can
drain the workers it owns, and kills its whole group after 10s. Ctrl+C
stops blok dev; a second Ctrl+C kills the application at once.

Options:
  --json          write one JSON event per line (blok-dev/v1); the
                  application's output goes to standard error
  --package DIR   the main package to build (default ./cmd/<blok.json name>)

Signals blok dev was started with ignored stay ignored, so nohup blok dev &
outlives the terminal.

Exit codes: 130 stopped by a signal, 1 project unreadable or too large to
watch, 2 usage, 3 go or the process guard unavailable, 4 output not written.`

// executeDev runs blok dev. The first SIGINT, SIGTERM, SIGHUP or SIGQUIT
// stops it gracefully; any further one forces the application down. A
// signal blok was started with ignored stays ignored: nohup blok dev &
// survives the terminal closing, as nohup promises.
func executeDev(args []string, stdout, stderr io.Writer) int {
	signals := make(chan os.Signal, 4)
	// Notify with no signals would catch every signal.
	if caught := notIgnored(toolSignals); len(caught) > 0 {
		signal.Notify(signals, caught...)
	}
	defer signal.Stop(signals)
	defer catchBrokenPipe()()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	force := make(chan struct{}, 1)
	done := make(chan struct{})
	defer close(done)
	go func() {
		for first := true; ; first = false {
			select {
			case <-done:
				return
			case <-signals:
			}
			if first {
				cancel()
				continue
			}
			select {
			case force <- struct{}{}:
			default:
			}
		}
	}()
	return guarded("dev", stderr, func() int { return runDev(ctx, force, args, stdout, stderr) })
}

// notIgnored is signals without those the process inherited as ignored.
// Notify would un-ignore them.
func notIgnored(signals []os.Signal) []os.Signal {
	var result []os.Signal
	for _, item := range signals {
		if !signal.Ignored(item) {
			result = append(result, item)
		}
	}
	return result
}

func runDev(ctx context.Context, force <-chan struct{}, args []string, stdout, stderr io.Writer) int {
	if hasHelp(args) {
		if _, err := fmt.Fprintln(stdout, devUsage); err != nil {
			fmt.Fprintf(stderr, "blok dev: write output: %v\n", err)
			return devtool.ExitOutput
		}
		return devtool.ExitOK
	}
	var appArgs []string
	for index, arg := range args {
		if arg == "--" {
			args, appArgs = args[:index], append([]string(nil), args[index+1:]...)
			break
		}
	}
	flags := flag.NewFlagSet("dev", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	asJSON := false
	pkg := ""
	flags.BoolVar(&asJSON, "json", false, "machine-readable events")
	flags.StringVar(&pkg, "package", "", "main package directory")
	usage := func(err error) int {
		fmt.Fprintf(stderr, "blok dev: %v; run blok dev --help\n", err)
		return devtool.ExitUsage
	}
	if err := flags.Parse(reorderFlags(args, map[string]bool{"json": false, "package": true})); err != nil {
		return usage(err)
	}
	if flags.NArg() > 1 {
		return usage(fmt.Errorf("expected at most one project directory"))
	}
	root := "."
	if flags.NArg() == 1 {
		root = flags.Arg(0)
	}
	options := devtool.DevOptions{Options: devtool.Options{Root: root}, Package: pkg, Args: appArgs, AppStdout: stdout, AppStderr: stderr, Force: force}
	options.Emit = func(event devtool.DevEvent) error { return devtool.WriteDevHuman(stdout, event) }
	if asJSON {
		options.AppStdout = stderr
		options.Emit = func(event devtool.DevEvent) error { return devtool.WriteDevJSON(stdout, event) }
	}
	code, err := devtool.Dev(ctx, options)
	if err != nil {
		fmt.Fprintf(stderr, "blok dev: write output: %v\n", err)
	}
	return code
}
