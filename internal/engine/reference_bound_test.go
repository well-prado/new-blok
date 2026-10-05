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

// optionLevel tags its key with an option encoding/json ignores; that must
// not push the struct onto the whole-encoding path.
type optionLevel struct {
	Big []string `json:"big"`
	ID  string   `json:"id,required"`
}

func TestIgnoredTagOptionKeepsWorkBounded(t *testing.T) {
	const limit = 4 << 10
	big := boundState(1)["source"].(boundLevel).Big
	state := map[string]any{"source": optionLevel{Big: big, ID: "b-1"}}
	perRun := bytesPerResolution(t, state, contract.Reference{Step: "source", Path: []string{"id"}})
	t.Logf("%d bytes per resolution beside a %d-byte sibling", perRun, boundSiblingBytes)
	if perRun > limit {
		t.Fatalf("%d bytes per resolution; want at most %d", perRun, limit)
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

type quotedLevel struct {
	N int64 `json:"n,string"`
}

// BenchmarkReferenceStringOption: a ,string selection encodes the scalar
// through probe structs built once with the type's key index.
func BenchmarkReferenceStringOption(b *testing.B) {
	state := map[string]any{"source": quotedLevel{N: 5}}
	reference := contract.Reference{Step: "source", Path: []string{"n"}}
	b.ReportAllocs()
	for b.Loop() {
		if value, err := resolveReference(state, reference); err != nil || value != "5" {
			b.Fatalf("value=%#v err=%v", value, err)
		}
	}
}
