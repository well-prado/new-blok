// Package scaffold creates conventional Go applications without a hosted
// service or a foreign runtime. A starter is a real New Blok application: a
// typed node, a structural workflow composed through generated accessors,
// and the Blok HTTP trigger, owned by the application's own module.
package scaffold

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/well-prado/new-blok/internal/generate"
)

// FrameworkModule is the module every starter depends on.
const FrameworkModule = "github.com/well-prado/new-blok"

var ErrCancelled = errors.New("scaffold cancelled")

// Framework is how a starter's go.mod reaches the framework: a released or
// pseudo-version from the module proxy, or a local checkout through a
// replace directive. Exactly one is set.
type Framework struct {
	Version string
	Dir     string
}

type Options struct {
	Directory string
	Module    string
	Name      string
	Runtime   string
	Layout    string
	Triggers  []string
	Framework Framework
}

type Manifest struct {
	Name     string   `json:"name"`
	Module   string   `json:"module"`
	Runtime  string   `json:"runtime"`
	Layout   string   `json:"layout"`
	Triggers []string `json:"triggers"`
	// Types is the Go source blok generate reads by default; its bindings
	// are written beside it.
	Types string `json:"types"`
}

// NodeDir is where a layout keeps a Go node's package (architecture §3).
func NodeDir(layout, node string) string {
	if layout == "unified" {
		return "nodes/go/" + node
	}
	return "runtimes/go/nodes/" + node
}

func Create(options Options) ([]string, error) {
	options, err := normalize(options)
	if err != nil {
		return nil, err
	}
	nodeDir := NodeDir(options.Layout, "quote")
	bindings, err := generate.Source([]byte(typesSource), generate.Options{})
	if err != nil {
		return nil, fmt.Errorf("new: generate bindings: %w", err)
	}
	manifestBytes, err := json.MarshalIndent(Manifest{Name: options.Name, Module: options.Module, Runtime: options.Runtime, Layout: options.Layout, Triggers: options.Triggers, Types: nodeDir + "/types.go"}, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode manifest: %w", err)
	}
	manifestBytes = append(manifestBytes, '\n')
	files := map[string][]byte{
		"go.mod":                           goModSource(options),
		"blok.json":                        manifestBytes,
		nodeDir + "/types.go":              []byte(typesSource),
		nodeDir + "/bindings_gen.go":       bindings,
		nodeDir + "/quote.go":              []byte(nodeSource),
		"workflows/quotes/quotes.go":       []byte(workflowSource(options.Module, nodeDir)),
		"internal/app/app.go":              []byte(appSource(options.Module, nodeDir)),
		"internal/app/app_test.go":         []byte(appTestSource),
		"cmd/" + options.Name + "/main.go": []byte(mainSource(options.Module)),
		"README.md":                        []byte(readmeSource(options, nodeDir)),
	}
	if err := ensureEmptyTarget(options.Directory); err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(files))
	for path := range files {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		if err := writeNewFile(options.Directory, path, files[path]); err != nil {
			return nil, err
		}
	}
	return paths, nil
}

func normalize(options Options) (Options, error) {
	if strings.TrimSpace(options.Directory) == "" {
		return Options{}, fmt.Errorf("new: target directory is required")
	}
	options.Directory = filepath.Clean(options.Directory)
	if options.Name == "" {
		options.Name = DefaultName(options.Directory)
	}
	if !validIdentifier(options.Name) {
		return Options{}, fmt.Errorf("new: application name %q must be lower-case letters, digits and underscores, starting with a letter", options.Name)
	}
	if options.Module == "" {
		options.Module = DefaultModule(options.Name)
	}
	if strings.ContainsAny(options.Module, " \t\r\n\"\\`") || strings.HasPrefix(options.Module, ".") || strings.HasPrefix(options.Module, "/") || options.Module == FrameworkModule || strings.HasPrefix(options.Module, FrameworkModule+"/") {
		return Options{}, fmt.Errorf("new: invalid Go module path %q", options.Module)
	}
	if options.Runtime == "" {
		options.Runtime = "go"
	}
	if options.Runtime != "go" {
		return Options{}, fmt.Errorf("new: runtime %q is unsupported; native starters support go", options.Runtime)
	}
	if options.Layout == "" {
		options.Layout = "classic"
	}
	if options.Layout != "classic" && options.Layout != "unified" {
		return Options{}, fmt.Errorf("new: layout %q is unsupported; choose classic or unified", options.Layout)
	}
	if len(options.Triggers) == 0 {
		options.Triggers = []string{"http"}
	}
	for _, trigger := range options.Triggers {
		if trigger != "http" {
			return Options{}, fmt.Errorf("new: trigger %q is unsupported; the native starter supports http", trigger)
		}
	}
	options.Triggers = []string{"http"}
	switch {
	case options.Framework.Version != "" && options.Framework.Dir != "":
		return Options{}, fmt.Errorf("new: choose a framework version or a local framework directory, not both")
	case options.Framework.Version == "" && options.Framework.Dir == "":
		return Options{}, fmt.Errorf("new: a framework version or local framework directory is required")
	case options.Framework.Dir != "":
		dir, err := filepath.Abs(options.Framework.Dir)
		if err != nil {
			return Options{}, fmt.Errorf("new: framework directory: %w", err)
		}
		modFile, err := os.ReadFile(filepath.Join(dir, "go.mod"))
		if err != nil || !strings.Contains(string(modFile), "module "+FrameworkModule+"\n") {
			return Options{}, fmt.Errorf("new: %s is not a %s checkout", dir, FrameworkModule)
		}
		options.Framework.Dir = dir
	default:
		if !validVersion(options.Framework.Version) {
			return Options{}, fmt.Errorf("new: framework version %q is not a module version", options.Framework.Version)
		}
	}
	return options, nil
}

func validVersion(version string) bool {
	if !strings.HasPrefix(version, "v") || strings.ContainsAny(version, " \t\r\n\"`+") {
		return false
	}
	return len(version) > 1 && version[1] >= '0' && version[1] <= '9'
}

func ensureEmptyTarget(directory string) error {
	entries, err := os.ReadDir(directory)
	if errors.Is(err, fs.ErrNotExist) {
		return os.MkdirAll(directory, 0o755)
	}
	if err != nil {
		return fmt.Errorf("new: inspect target: %w", err)
	}
	if len(entries) != 0 {
		return fmt.Errorf("new: target %q is not empty; refusing to overwrite existing files", directory)
	}
	return nil
}

func writeNewFile(root, name string, content []byte) error {
	path := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("new: create %s: %w", name, err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("new: write %s: %w", name, err)
	}
	if _, err := file.Write(content); err != nil {
		_ = file.Close()
		return fmt.Errorf("new: write %s: %w", name, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("new: write %s: %w", name, err)
	}
	return nil
}

func validIdentifier(value string) bool {
	if value == "" || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, char := range value[1:] {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '_' {
			return false
		}
	}
	return true
}

// DefaultName is the executable name a target directory implies.
func DefaultName(directory string) string {
	return executableName(filepath.Base(filepath.Clean(directory)))
}

// DefaultModule is the module path an executable name implies.
func DefaultModule(name string) string { return "example.com/" + name }

func executableName(value string) string {
	var builder strings.Builder
	for _, char := range strings.ToLower(value) {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '_' {
			builder.WriteRune(char)
		} else {
			builder.WriteByte('_')
		}
	}
	result := strings.Trim(builder.String(), "_")
	if result == "" {
		return "app"
	}
	if result[0] >= '0' && result[0] <= '9' {
		return "app_" + result
	}
	return result
}

func goModSource(options Options) []byte {
	var source strings.Builder
	fmt.Fprintf(&source, "module %s\n\ngo 1.27.0\n\n", options.Module)
	if options.Framework.Dir != "" {
		// Always quoted: go.mod splits unquoted paths on spaces, quotes,
		// brackets and commas, and the quoted form is valid on every OS.
		fmt.Fprintf(&source, "require %s v0.0.0-00010101000000-000000000000\n\nreplace %s => %s\n", FrameworkModule, FrameworkModule, strconv.Quote(options.Framework.Dir))
	} else {
		fmt.Fprintf(&source, "require %s %s\n", FrameworkModule, options.Framework.Version)
	}
	return []byte(source.String())
}

const typesSource = `package quote

// Input is the quote node's input. Types for generated bindings live in their
// own file, with no imports, so blok generate can analyze them without
// running any package code.
type Input struct {
	SKU      string ` + "`json:\"sku\"`" + `
	Quantity int    ` + "`json:\"quantity\"`" + `
}

// Output is the quote node's output.
type Output struct {
	SKU        string ` + "`json:\"sku\"`" + `
	Quantity   int    ` + "`json:\"quantity\"`" + `
	TotalCents int64  ` + "`json:\"totalCents\"`" + `
	Currency   string ` + "`json:\"currency\"`" + `
}
`

const nodeSource = `package quote

import (
	"context"
	"fmt"

	"github.com/well-prado/new-blok/node"
)

// Catalog prices SKUs. The application injects it; the node never looks it up.
type Catalog interface {
	PriceCents(context.Context, string) (int64, error)
}

var outputSchema = []byte(` + "`" + `{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer"},"totalCents":{"type":"integer"},"currency":{"type":"string"}},"required":["sku","quantity","totalCents","currency"]}` + "`" + `)

// New defines the quote node: a typed Go function with its schemas.
func New(catalog Catalog) (node.Definition[Input, Output], error) {
	return node.Define("app/calculate-quote", "1.0.0", func(ctx context.Context, input Input) (Output, error) {
		if input.Quantity < 1 || input.Quantity > 100 {
			return Output{}, &node.DomainError{Code: "invalid_quantity", Class: "validation"}
		}
		price, err := catalog.PriceCents(ctx, input.SKU)
		if err != nil {
			return Output{}, err
		}
		return Output{SKU: input.SKU, Quantity: input.Quantity, TotalCents: price * int64(input.Quantity), Currency: "USD"}, nil
	}, node.Description("Calculates a quote from an injected catalog"), node.Schemas([]byte(` + "`" + `{"type":"object"}` + "`" + `), outputSchema))
}

// Prices is an in-memory Catalog.
type Prices map[string]int64

func (p Prices) PriceCents(_ context.Context, sku string) (int64, error) {
	price, ok := p[sku]
	if !ok {
		return 0, &node.DomainError{Code: "unknown_sku", Class: "validation", Err: fmt.Errorf("sku %q is not available", sku)}
	}
	return price, nil
}
`

func workflowSource(module, nodeDir string) string {
	return `// Package quotes holds the quote workflow. Workflows compose nodes; nodes
// never call each other.
package quotes

import (
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"

	"` + module + `/` + nodeDir + `"
)

// New records the workflow's structure: call the quote node, then return its
// total through the generated typed accessor. Building it runs no business
// logic.
func New(calculate node.Definition[quote.Input, quote.Output]) (flow.Definition[quote.Input, int64], error) {
	return flow.Define(flow.Spec{Name: "quote", Version: "1.0.0"}, func(builder *flow.Builder, input flow.Ref[quote.Input]) flow.Ref[int64] {
		result := flow.Call(builder, "calculate", calculate, input)
		return quote.OutputFields(result).TotalCents()
	})
}
`
}

func appSource(module, nodeDir string) string {
	return `// Package app is the composition root: it registers the node, the workflow
// and the selected HTTP trigger. Configuration cannot add code.
package app

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/node"
	blokhttp "github.com/well-prado/new-blok/trigger/http"

	"` + module + `/` + nodeDir + `"
	"` + module + `/workflows/quotes"
)

var quoteInputSchema = []byte(` + "`" + `{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer","minimum":1,"maximum":100}},"required":["sku","quantity"]}` + "`" + `)

// New builds the application and its HTTP handler: POST /quotes runs the
// quote workflow.
func New() (*app.Application, http.Handler, error) {
	calculate, err := quote.New(quote.Prices{"coffee": 1500})
	if err != nil {
		return nil, nil, err
	}
	workflow, err := quotes.New(calculate)
	if err != nil {
		return nil, nil, err
	}
	program, err := workflow.Lower()
	if err != nil {
		return nil, nil, err
	}
	application, err := app.New(app.Config{
		Workflows: []app.Workflow{{Name: "app/quote"}},
		Routes:    []app.Route{{Method: http.MethodPost, Path: "/quotes", Workflow: "app/quote"}},
	})
	if err != nil {
		return nil, nil, err
	}
	runner := execution.NewRunner(application, map[string]node.Any{"app/calculate-quote": calculate.Any()})
	handler, err := blokhttp.New(application, []blokhttp.Endpoint{{
		Method: http.MethodPost, Path: "/quotes", InputSchema: quoteInputSchema,
		Handle: func(ctx context.Context, input blokhttp.Input) (any, error) {
			var request quote.Input
			if err := json.Unmarshal(input.Body, &request); err != nil {
				return nil, err
			}
			result, err := runner.Run(ctx, program, request, inspection.Invocation{})
			if err != nil {
				return nil, err
			}
			return map[string]any{"totalCents": result.Output}, nil
		},
	}})
	if err != nil {
		return nil, nil, err
	}
	return application, handler, nil
}
`
}

const appTestSource = `package app

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestQuoteOverHTTP drives the real application, workflow and HTTP trigger.
func TestQuoteOverHTTP(t *testing.T) {
	application, handler, err := New()
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = application.Shutdown(context.Background()) })
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	for _, test := range []struct {
		name, body string
		status     int
		contains   string
	}{
		{"quote", ` + "`" + `{"sku":"coffee","quantity":2}` + "`" + `, http.StatusOK, ` + "`" + `"totalCents":3000` + "`" + `},
		{"quantity out of range", ` + "`" + `{"sku":"coffee","quantity":0}` + "`" + `, http.StatusBadRequest, ""},
		{"unknown sku", ` + "`" + `{"sku":"tea","quantity":1}` + "`" + `, http.StatusBadRequest, ""},
		{"malformed json", ` + "`" + `{"sku":` + "`" + `, http.StatusBadRequest, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			response, err := http.Post(server.URL+"/quotes", "application/json", strings.NewReader(test.body))
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, _ := io.ReadAll(response.Body)
			if response.StatusCode != test.status || !strings.Contains(string(body), test.contains) {
				t.Fatalf("status=%d body=%s; want %d containing %q", response.StatusCode, body, test.status, test.contains)
			}
		})
	}
}
`

func mainSource(module string) string {
	return `package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"` + module + `/internal/app"
)

func main() {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}
	application, handler, err := app.New()
	if err != nil {
		log.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		log.Fatal(err)
	}
	server := &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	served := make(chan error, 1)
	go func() { served <- server.ListenAndServe() }()
	log.Printf("listening on %s", addr)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	select {
	case err := <-served:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	case <-signals:
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		_ = application.Shutdown(ctx)
	}
}
`
}

func readmeSource(options Options, nodeDir string) string {
	return fmt.Sprintf(`# %s

A New Blok application owned by module %s: a typed node (%s), a workflow
(workflows/quotes) and the HTTP trigger, wired in internal/app.

## Run

    go test ./...
    go run ./cmd/%s

Then post {"sku":"coffee","quantity":2} to http://localhost:8080/quotes
(set ADDR to listen elsewhere).

After changing %s/types.go, regenerate the typed accessors:

    blok generate

Only Go is required: no account, registry, container, Node.js or Python.
`, options.Name, options.Module, nodeDir, options.Name, nodeDir)
}
