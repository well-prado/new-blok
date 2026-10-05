//go:build unix

package layout

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestFIFOSwappedInDoesNotBlock (review L2): a FIFO replacing a listed
// regular file between the walk and the open returns at once as an
// unsupported file instead of waiting for a writer.
func TestFIFOSwappedInDoesNotBlock(t *testing.T) {
	root := t.TempDir()
	files := baseProject(Unified)
	files["nodes/go/quote/quote.go"] = "package quote\n"
	writeTree(t, root, files)
	listed, err := os.Lstat(filepath.Join(root, "nodes", "go", "quote", "quote.go"))
	if err != nil {
		t.Fatal(err)
	}
	pipe := filepath.Join(root, "nodes", "go", "quote", "pipe.go")
	if err := syscall.Mkfifo(pipe, 0o644); err != nil {
		t.Skipf("mkfifo: %v", err)
	}
	opened, err := os.OpenRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	d := &discoverer{root: opened, abs: root, c: &collector{}, nodeBytes: -1}
	done := make(chan bool, 1)
	go func() {
		_, ok := d.read("nodes/go/quote/pipe.go", listed) // listed as regular: the swap
		done <- ok
	}()
	select {
	case ok := <-done:
		if ok || len(d.c.items) != 1 || d.c.items[0].Code != CodeFileUnsupported {
			t.Fatalf("ok=%v diagnostics=%+v; want an unsupported-file refusal", ok, d.c.items)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reading a FIFO blocked discovery")
	}
	// Listed as a FIFO, it is refused by the walk without being opened.
	_, err = Discover(root)
	if got := codes(t, err); len(got) != 1 || got[0] != CodeFileUnsupported {
		t.Fatalf("codes=%v", got)
	}
}

// TestHardLinkIsRefused (review L4): a hard link is a second name for a
// file that may live outside the project; it is refused before its content
// is parsed, so nothing of it reaches a diagnostic.
func TestHardLinkIsRefused(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "project")
	outside := filepath.Join(base, "secret.go")
	if err := os.WriteFile(outside, []byte("SECRETTOKEN package\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	files := baseProject(Unified)
	files["nodes/go/quote/quote.go"] = "package quote\n\nimport \"github.com/well-prado/new-blok/node\"\n\nvar Node = node.MustDefine(\"shop/quote\", \"1.0.0\", nil)\n"
	writeTree(t, root, files)
	if err := os.Link(outside, filepath.Join(root, "nodes", "go", "quote", "linked.go")); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	_, err := Discover(root)
	layoutErr, ok := err.(*Error)
	if !ok || len(layoutErr.Diagnostics) != 1 || layoutErr.Diagnostics[0].Code != CodeFileUnsupported || layoutErr.Diagnostics[0].Source != "nodes/go/quote/linked.go" {
		t.Fatalf("err=%v", err)
	}
	if strings.Contains(err.Error()+layoutErr.Diagnostics[0].Message, "SECRETTOKEN") {
		t.Fatal("hard-linked content leaked into a diagnostic")
	}
}
