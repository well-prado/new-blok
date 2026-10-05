package engine

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/contract"
)

// boundLevel carries a large sibling at every level of a nested path. The
// omitempty pointer hop is the case where deciding presence must not mean
// encoding the member.
type boundLevel struct {
	Big  []string    `json:"big"`
	Next *boundLevel `json:"next,omitempty"`
	ID   string      `json:"id"`
}

const boundSiblingBytes = 1 << 20

func boundState(depth int) map[string]any {
	big := make([]string, boundSiblingBytes/64)
	for index := range big {
		big[index] = strings.Repeat("x", 64)
	}
	var level *boundLevel
	for range depth {
		level = &boundLevel{Big: big, Next: level, ID: "b-1"}
	}
	return map[string]any{"source": *level}
}

func boundPath(segments int) contract.Reference {
	path := make([]string, 0, segments)
	for range segments - 1 {
		path = append(path, "next")
	}
	return contract.Reference{Step: "source", Path: append(path, "id")}
}

// bytesPerResolution measures heap bytes allocated by one resolution.
func bytesPerResolution(t *testing.T, state map[string]any, reference contract.Reference) uint64 {
	t.Helper()
	const runs = 20
	if value, err := resolveReference(state, reference); err != nil || value != "b-1" {
		t.Fatalf("value=%#v err=%v", value, err)
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for range runs {
		if _, err := resolveReference(state, reference); err != nil {
			t.Fatal(err)
		}
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / runs
}

// TestReferenceWorkIsBoundedBySelectedMember: selecting a three-byte field
// beside a 1 MiB sibling allocates a small constant, at one segment and at
// five, instead of encoding the sibling at every step.
func TestReferenceWorkIsBoundedBySelectedMember(t *testing.T) {
	const limit = 4 << 10
	for _, segments := range []int{1, 5} {
		perRun := bytesPerResolution(t, boundState(segments), boundPath(segments))
		t.Logf("%d segment(s): %d bytes per resolution beside %d-byte siblings", segments, perRun, boundSiblingBytes)
		if perRun > limit {
			t.Fatalf("%d segment(s): %d bytes per resolution; want at most %d, independent of the %d-byte siblings", segments, perRun, limit, boundSiblingBytes)
		}
	}
}

func BenchmarkReferenceBesideLargeSibling(b *testing.B) {
	for _, segments := range []int{1, 5} {
		state, reference := boundState(segments), boundPath(segments)
		b.Run(fmt.Sprintf("segments=%d", segments), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := resolveReference(state, reference); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
