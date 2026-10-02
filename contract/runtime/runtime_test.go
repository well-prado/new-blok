package runtime

import (
	"errors"
	"testing"
	"time"
)

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
