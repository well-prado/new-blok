// Command blok provides development tooling for New Blok applications.
package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/well-prado/new-blok/internal/generate"
	"github.com/well-prado/new-blok/internal/scaffold"
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
	_, err := fmt.Fprintln(out, "New Blok — Go application framework\n\nUsage: blok <command>\n\nCommands:\n  new       Create a conventional Go application\n  generate  Generate deterministic typed bindings\n  version   Print the development version\n  help      Show this help\n\nUse blok new --help or blok generate --help for command options.")
	return err
}

func runNew(args []string, out io.Writer, in io.Reader) error {
	if hasHelp(args) {
		_, err := fmt.Fprintln(out, "Usage: blok new [options] <directory>\n\nOptions:\n  --module PATH       Go module path (default: example.com/<name>)\n  --name NAME         executable name\n  --runtime go        native runtime\n  --layout classic|unified\n  --trigger http      selected trigger\n  --interactive       prompt for choices\n  --non-interactive   fail instead of prompting")
		return err
	}
	flags := flag.NewFlagSet("new", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	module, name, runtime, layout, trigger := "", "", "go", "classic", "http"
	interactive, nonInteractive := false, false
	flags.StringVar(&module, "module", "", "Go module path")
	flags.StringVar(&name, "name", "", "application executable name")
	flags.StringVar(&runtime, "runtime", "go", "native runtime")
	flags.StringVar(&layout, "layout", "classic", "node layout: classic or unified")
	flags.StringVar(&trigger, "trigger", "http", "comma-separated selected triggers")
	flags.BoolVar(&interactive, "interactive", false, "ask for missing project choices")
	flags.BoolVar(&nonInteractive, "non-interactive", false, "fail instead of prompting")
	if err := flags.Parse(reorderFlags(args, map[string]bool{"module": true, "name": true, "runtime": true, "layout": true, "trigger": true, "interactive": false, "non-interactive": false})); err != nil {
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
		var err error
		if directory, err = prompt(in, out, "Target directory", directory); err != nil {
			return err
		}
		if module, err = prompt(in, out, "Module path", module); err != nil {
			return err
		}
		if name, err = prompt(in, out, "Executable name", name); err != nil {
			return err
		}
		if runtime, err = prompt(in, out, "Runtime [go]", runtime); err != nil {
			return err
		}
		if layout, err = prompt(in, out, "Layout [classic]", layout); err != nil {
			return err
		}
		if trigger, err = prompt(in, out, "Triggers [http]", trigger); err != nil {
			return err
		}
	}
	if directory == "" {
		return fmt.Errorf("new: target directory is required (pass a path or use --interactive)")
	}
	paths, err := scaffold.Create(scaffold.Options{Directory: directory, Module: module, Name: name, Runtime: runtime, Layout: layout, Triggers: splitChoices(trigger)})
	if err != nil {
		return err
	}
	for _, path := range paths {
		if _, err := fmt.Fprintln(out, "created "+filepath.ToSlash(filepath.Join(directory, path))); err != nil {
			return err
		}
	}
	return nil
}

func prompt(in io.Reader, out io.Writer, label, current string) (string, error) {
	if _, err := fmt.Fprintf(out, "%s%s: ", label, valueSuffix(current)); err != nil {
		return "", err
	}
	line, err := bufio.NewReader(in).ReadString('\n')
	if errors.Is(err, io.EOF) && strings.TrimSpace(line) == "" {
		return "", scaffold.ErrCancelled
	}
	if err != nil {
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
		input = "internal/app/types.go"
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
	if err := writeAtomic(output, generated); err != nil {
		return fmt.Errorf("generate: %w", err)
	}
	_, err = fmt.Fprintln(out, "generated "+filepath.ToSlash(output))
	return err
}

func writeAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".blok-generate-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0o644); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryName, path)
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
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
