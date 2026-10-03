package runtime

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestCallSharedBounds(t *testing.T) {
	c := Call{CallID: "shared", AttemptID: "attempt-shared", Node: "fixture/echo", NodeVersion: "1.0.0", Generation: 1, Deadline: time.Now().Add(90 * time.Second), Input: []byte(`{}`)}
	for i := 0; i < 128; i++ {
		c.Capabilities = append(c.Capabilities, Capability(fmt.Sprintf("synthetic:%d", i)))
		c.Blobs = append(c.Blobs, BlobRef{Digest: artifact, Size: 0})
	}
	if err := c.Validate(DefaultLimits(), 1); err != nil {
		t.Fatal(err)
	}
	c.Deadline = time.Now().Add(6 * time.Minute)
	if err := c.Validate(DefaultLimits(), 1); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("unbounded deadline: %v", err)
	}
}

func TestLogicalOperationKeyUsesBoundedUTF8NotIdentityGrammar(t *testing.T) {
	c := Call{CallID: "key", AttemptID: "attempt-key", Node: "fixture/echo", NodeVersion: "1.0.0", Generation: 1, Deadline: time.Now().Add(time.Second), Input: []byte(`{}`)}
	for _, key := range []string{"", "order 123/ação", "\ufffd", strings.Repeat("é", 64), string(make([]byte, 128))} {
		c.IdempotencyKey = key
		if err := c.Validate(DefaultLimits(), 1); err != nil {
			t.Fatalf("bounded UTF-8 key rejected: %v", err)
		}
	}
	for _, key := range []string{string([]byte{0xff}), strings.Repeat("é", 64) + "x", string(make([]byte, 129))} {
		c.IdempotencyKey = key
		if err := c.Validate(DefaultLimits(), 1); !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("invalid key accepted: %v", err)
		}
	}
}

func TestDeclaredNegotiationFixtures(t *testing.T) {
	data, err := os.ReadFile("../../testdata/runtime/fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		ID      string `json:"id"`
		Outputs int    `json:"expectedOutput"`
		Errors  int    `json:"expectedErrors"`
		Effects int    `json:"expectedEffects"`
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.ID, func(t *testing.T) {
			c, w := hello(), hello()
			switch tc.ID {
			case "matching-contract":
			case "unsupported-version":
				c.Major++
			case "wrong-catalog":
				w.CatalogDigest = artifact
			case "wrong-artifact":
				w.ArtifactDigest = catalog
			case "stale-generation":
				w.Generation++
			case "widened-capability":
				c.Capabilities = append(c.Capabilities, "db:write")
			case "int64-boundaries-presence-null":
				TestWirePreservesMissingNullAndExactInt64(t)
				return
			default:
				t.Fatal("unknown fixture")
			}
			_, err := Negotiate(c, w)
			outputs, errs := 1, 0
			if err != nil {
				outputs, errs = 0, 1
			}
			if outputs != tc.Outputs || errs != tc.Errors || tc.Effects != 0 {
				t.Fatalf("fixture %s: outputs=%d errors=%d effects=0", tc.ID, outputs, errs)
			}
		})
	}
}

const (
	artifact = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	catalog  = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func hello() Hello {
	return Hello{Protocol: ProtocolName, Major: ProtocolMajor, Minor: ProtocolMinor, ArtifactDigest: artifact, CatalogDigest: catalog, Generation: 1, Capabilities: []Capability{"http:payments", "secret:payment-token"}, Limits: DefaultLimits()}
}

func TestNegotiateReturnsIntersectionAndStableIdentity(t *testing.T) {
	client, worker := hello(), hello()
	client.Capabilities = []Capability{"http:payments"}
	worker.Limits.MaxFrameBytes = 128
	worker.Limits.MaxBlobBytes = 256
	worker.Limits.MaxConcurrentCalls = 4
	ready, err := Negotiate(client, worker)
	if err != nil {
		t.Fatal(err)
	}
	if ready.Generation != 1 || ready.Limits.MaxFrameBytes != 128 || ready.Limits.MaxBlobBytes != 256 || ready.Limits.MaxConcurrentCalls != 4 {
		t.Fatalf("unexpected ready contract: %+v", ready)
	}
	if len(ready.Capabilities) != 1 || ready.Capabilities[0] != "http:payments" {
		t.Fatalf("worker gained capability: %+v", ready.Capabilities)
	}
}

func TestNegotiateRejectsWrongVersionCatalogArtifactAndGeneration(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Hello, *Hello)
		want   error
	}{
		{"major", func(c, w *Hello) { c.Major++ }, ErrIncompatibleProtocol},
		{"catalog", func(c, w *Hello) { w.CatalogDigest = artifact }, ErrCatalogMismatch},
		{"artifact", func(c, w *Hello) { w.ArtifactDigest = catalog }, ErrArtifactMismatch},
		{"generation", func(c, w *Hello) { w.Generation = 2 }, ErrGenerationMismatch},
		{"capability", func(c, w *Hello) { c.Capabilities = append(c.Capabilities, "db:write") }, ErrCapabilityDenied},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, w := hello(), hello()
			tc.mutate(&c, &w)
			if _, err := Negotiate(c, w); err != tc.want {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestCallRejectsStaleGenerationAndUnboundedPayload(t *testing.T) {
	call := Call{CallID: "call-1", AttemptID: "attempt-1", Generation: 1, Node: "shop/quote", NodeVersion: "1.0.0", Deadline: time.Now().Add(time.Minute), Input: []byte(`{"sku":"coffee"}`)}
	if err := call.Validate(DefaultLimits(), 2); err != ErrGenerationMismatch {
		t.Fatalf("got %v, want generation mismatch", err)
	}
	limits := DefaultLimits()
	limits.MaxFrameBytes = 4
	if err := call.Validate(limits, 1); err == nil {
		t.Fatal("oversized input accepted")
	}
}

func TestCallDeadlineAndBlobBounds(t *testing.T) {
	call := Call{CallID: "call-1", AttemptID: "attempt-1", Generation: 1, Node: "shop/quote", NodeVersion: "1.0.0", Deadline: time.Now().Add(-time.Second)}
	if err := call.Validate(DefaultLimits(), 1); err == nil {
		t.Fatal("expired deadline accepted")
	}
	call.Deadline = time.Now().Add(time.Minute)
	call.Blobs = []BlobRef{{Digest: artifact, Size: MaxBlobBytes + 1}}
	if err := call.Validate(DefaultLimits(), 1); err == nil {
		t.Fatal("oversized blob accepted")
	}
}

func TestWorkerCannotRequestOrchestrationCapability(t *testing.T) {
	h := hello()
	h.Capabilities = []Capability{"orchestrate:workflow"}
	if _, err := Negotiate(h, hello()); !errors.Is(err, ErrCapabilityDenied) {
		t.Fatalf("got %v, want capability denial", err)
	}
}

func TestRemoteUncertaintyIsNeverAutomaticallyRetryable(t *testing.T) {
	if (RemoteError{Class: ErrorUncertain, Retryable: true}).SafeToRetry() {
		t.Fatal("uncertain effect marked safe to retry")
	}
	if !(RemoteError{Class: ErrorTransient, Retryable: true}).SafeToRetry() {
		t.Fatal("transient error not retryable")
	}
}
