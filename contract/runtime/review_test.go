package runtime

import (
	"context"
	"errors"
	"fmt"
	"github.com/well-prado/new-blok/node"
	"strings"
	"testing"
	"time"
)

func TestCompleteFrameMetadataBoundBeforeDispatch(t *testing.T) {
	c := Call{CallID: "metadata", AttemptID: "attempt-metadata", Node: "fixture/echo", NodeVersion: "1.0.0", Generation: 1, Deadline: time.Now().Add(time.Minute), Input: []byte(`"` + strings.Repeat("x", MaxFrameBytes-1026) + `"`)}
	for i := 0; i < 128; i++ {
		c.Capabilities = append(c.Capabilities, Capability(fmt.Sprintf("c%03d%s", i, strings.Repeat("x", 124))))
	}
	if EncodedCallBytes(c) <= MaxFrameBytes {
		t.Fatal("fixture must exceed complete frame")
	}
	if err := c.Validate(DefaultLimits(), 1); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("oversized metadata accepted: %v", err)
	}
	for len(c.Input) > 0 && EncodedCallBytes(c) > MaxFrameBytes {
		c.Input = c.Input[:len(c.Input)-1]
	}
	if err := c.Validate(DefaultLimits(), 1); err != nil {
		t.Fatalf("boundary call rejected: %v", err)
	}
	c.Principal = strings.Repeat("p", 128)
	if err := c.Validate(DefaultLimits(), 1); err != nil {
		t.Fatalf("principal reserve lost: %v", err)
	}
	c.Input = append(c.Input, 'x')
	if err := c.Validate(DefaultLimits(), 1); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("one excess byte accepted: %v", err)
	}
}

func TestCatalogUsesNativeDescriptorRules(t *testing.T) {
	d := node.MustDefine("fixture/echo", "1.0.0", func(context.Context, struct{}) (struct{}, error) { return struct{}{}, nil }, node.Description("Synthetic valid descriptor"), node.Schemas([]byte(`{"type":"object"}`), []byte(`{"type":"object"}`)), node.Pure()).Descriptor()
	if _, err := CatalogDigest([]node.Descriptor{d}); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*node.Descriptor){func(d *node.Descriptor) { d.Name = "A" }, func(d *node.Descriptor) { d.Name = "a/../b" }, func(d *node.Descriptor) { d.Version = "banana" }, func(d *node.Descriptor) { d.Description = " " }} {
		bad := d
		mutate(&bad)
		if node.ValidateDescriptor(bad) == nil {
			t.Fatal("fixture must be invalid natively")
		}
		if _, err := CatalogDigest([]node.Descriptor{bad}); err == nil {
			t.Fatalf("catalog accepted invalid native metadata %+v", bad)
		}
	}
}
