// Command blok provides development tooling for New Blok applications.
package main

import (
	"fmt"
	"io"
	"os"
)

const version = "0.0.0-dev"

func run(args []string, out io.Writer) error {
	if len(args) == 0 || (len(args) == 1 && (args[0] == "help" || args[0] == "--help")) {
		_, err := fmt.Fprintln(out, "New Blok — Go application framework\n\nUsage: blok <command>\n\nCommands:\n  version  Print the development version\n  help     Show this help\n\nApplication tooling is tracked in ROADMAP.md.")
		return err
	}
	if len(args) == 1 && args[0] == "version" {
		_, err := fmt.Fprintln(out, "blok "+version)
		return err
	}
	return fmt.Errorf("unsupported command %q; run blok help", args[0])
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
