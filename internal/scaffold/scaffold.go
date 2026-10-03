// Package scaffold creates conventional Go applications without a hosted
// service or a foreign runtime.
package scaffold

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var ErrCancelled = errors.New("scaffold cancelled")

type Options struct {
	Directory string
	Module    string
	Name      string
	Runtime   string
	Layout    string
	Triggers  []string
}

type Manifest struct {
	Name     string   `json:"name"`
	Module   string   `json:"module"`
	Runtime  string   `json:"runtime"`
	Layout   string   `json:"layout"`
	Triggers []string `json:"triggers"`
}

func Create(options Options) ([]string, error) {
	options, err := normalize(options)
	if err != nil {
		return nil, err
	}
	if err := ensureEmptyTarget(options.Directory); err != nil {
		return nil, err
	}
	typesSource := "package app\n\ntype QuoteRequest struct {\n\tSKU string `json:\"sku\"`\n\tQuantity int `json:\"quantity\"`\n}\n\ntype QuoteResponse struct {\n\tTotalCents int64 `json:\"totalCents\"`\n}\n"
	manifestBytes, err := json.MarshalIndent(Manifest{Name: options.Name, Module: options.Module, Runtime: options.Runtime, Layout: options.Layout, Triggers: options.Triggers}, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode manifest: %w", err)
	}
	manifestBytes = append(manifestBytes, '\n')
	files := map[string][]byte{
		"go.mod":                                []byte(fmt.Sprintf("module %s\n\ngo 1.27.0\n", options.Module)),
		"blok.json":                             manifestBytes,
		"internal/app/types.go":                 []byte(typesSource),
		"cmd/" + options.Name + "/main.go":      []byte(mainSource()),
		"cmd/" + options.Name + "/main_test.go": []byte(mainTestSource()),
		"README.md":                             []byte(readmeSource(options)),
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
		options.Name = executableName(filepath.Base(options.Directory))
	}
	if !validIdentifier(options.Name) {
		return Options{}, fmt.Errorf("new: application name %q must be a lower-case Go identifier", options.Name)
	}
	if options.Module == "" {
		options.Module = "example.com/" + options.Name
	}
	if strings.ContainsAny(options.Module, " \t\r\n") || strings.HasPrefix(options.Module, ".") {
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
			return Options{}, fmt.Errorf("new: trigger %q is unsupported; native starter supports http", trigger)
		}
	}
	return options, nil
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
	defer file.Close()
	if _, err := file.Write(content); err != nil {
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

func mainSource() string {
	return `package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

type quoteRequest struct { SKU string ` + "`json:\"sku\"`" + `; Quantity int ` + "`json:\"quantity\"`" + ` }

func main() {
	http.HandleFunc("/quotes", quote)
	log.Printf("listening on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil { log.Fatal(err) }
}

func quote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
	var input quoteRequest
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil || input.SKU != "coffee" || input.Quantity < 1 || input.Quantity > 100 {
		http.Error(w, fmt.Sprintf("invalid quote request: %v", err), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]int64{"totalCents": int64(input.Quantity) * 1500})
}
`
}

func mainTestSource() string {
	return `package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestQuote(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/quotes", strings.NewReader(` + "`{\"sku\":\"coffee\",\"quantity\":2}`" + `))
	quote(recorder, request)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), ` + "`\"totalCents\":3000`" + `) { t.Fatalf("status=%d body=%q", recorder.Code, recorder.Body.String()) }
}
`
}

func readmeSource(options Options) string {
	return fmt.Sprintf(`# %s

This is a conventional Go application owned by module %s.

## Run

    go test ./...
    go run ./cmd/%s

Then post {"sku":"coffee","quantity":2} to http://localhost:8080/quotes.

The starter uses only Go's standard library and the selected %s trigger. No account, registry, container, Node.js or Python runtime is required.
`, options.Name, options.Module, options.Name, strings.Join(options.Triggers, ", "))
}
