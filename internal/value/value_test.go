package value

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/schema"
)

type nested struct {
	Labels []string
	Meta   map[string][]int
}

func TestCloneIsolatesNestedNativeOwnership(t *testing.T) {
	original := nested{Labels: []string{"a"}, Meta: map[string][]int{"counts": {1, 2}}}
	copy, err := Clone(original)
	if err != nil {
		t.Fatal(err)
	}
	copy.Labels[0] = "mutated"
	copy.Meta["counts"][0] = 99
	if original.Labels[0] != "a" || original.Meta["counts"][0] != 1 {
		t.Fatalf("original mutated: %+v", original)
	}
}

func TestFanoutCopiesAreRaceSafeAndIndependent(t *testing.T) {
	original := nested{Labels: []string{"shared"}, Meta: map[string][]int{"values": {1}}}
	var group sync.WaitGroup
	for i := 0; i < 16; i++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			copy, err := Clone(original)
			if err != nil {
				t.Error(err)
				return
			}
			copy.Labels[0] = string(rune('a' + index))
			copy.Meta["values"][0] = index
		}(i)
	}
	group.Wait()
	if original.Labels[0] != "shared" || original.Meta["values"][0] != 1 {
		t.Fatalf("original mutated: %+v", original)
	}
}

func TestCloneRejectsCycles(t *testing.T) {
	type linked struct{ Next *linked }
	value := &linked{}
	value.Next = value
	if _, err := Clone(value); err == nil || !strings.Contains(err.Error(), "cyclic_value") {
		t.Fatalf("got %v", err)
	}
}

func TestNativeAndWireNormalizationMatch(t *testing.T) {
	contract, err := schema.Parse([]byte(`{"type":"object","properties":{"count":{"type":"integer","wire":"int64-string","default":2},"tags":{"type":"array","items":{"type":"string"}}},"required":["count"]}`))
	if err != nil {
		t.Fatal(err)
	}
	type payload struct {
		Count *int64   `json:"count,omitempty"`
		Tags  []string `json:"tags"`
	}
	native, err := Normalize(contract, payload{Tags: []string{"x"}})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := contract.Normalize([]byte(`{"tags":["x"]}`))
	if err != nil {
		t.Fatal(err)
	}
	if native.Count == nil || *native.Count != 2 || string(wire) != `{"count":"2","tags":["x"]}` {
		t.Fatalf("native=%+v wire=%s", native, wire)
	}
}

func TestBlobPolicy(t *testing.T) {
	ref := BlobRef{Digest: "sha256:" + strings.Repeat("a", 64), SizeBytes: 10, MediaType: "application/json", ExpiresAt: time.Now().Add(time.Minute)}
	if err := ref.Validate(BlobPolicy{MaxBytes: 100, RequireExpiry: true}, time.Now()); err != nil {
		t.Fatal(err)
	}
	ref.SizeBytes = 101
	if err := ref.Validate(BlobPolicy{MaxBytes: 100}, time.Now()); err == nil {
		t.Fatal("oversized blob accepted")
	}
}
