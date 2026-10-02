// Package runtime defines the transport-independent contract used by a
// persistent foreign-language worker. The package deliberately contains no
// gRPC, process, or workflow-engine code; adapters encode these values on the
// wire and keep orchestration in the native engine.
package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/well-prado/new-blok/contract/schema"
)

const (
	ProtocolName       = "blok.runtime"
	ProtocolMajor      = 1
	ProtocolMinor      = 0
	MaxFrameBytes      = 1 << 20
	MaxBlobBytes       = 8 << 20
	MaxConcurrentCalls = 64
)

var (
	ErrIncompatibleProtocol = errors.New("runtime: incompatible protocol")
	ErrCatalogMismatch      = errors.New("runtime: catalog digest mismatch")
	ErrArtifactMismatch     = errors.New("runtime: artifact digest mismatch")
	ErrGenerationMismatch   = errors.New("runtime: generation mismatch")
	ErrCapabilityDenied     = errors.New("runtime: capability denied")
	ErrLimitExceeded        = errors.New("runtime: limit exceeded")
	ErrUncertain            = errors.New("runtime: effect outcome is uncertain")
)

var identityPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$`)
var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// Capability is an opaque, narrow permission such as "http:payments". It is
// metadata for admission, not an authority-bearing secret.
type Capability string

type Limits struct {
	MaxFrameBytes      int `json:"maxFrameBytes"`
	MaxBlobBytes       int `json:"maxBlobBytes"`
	MaxConcurrentCalls int `json:"maxConcurrentCalls"`
}

func DefaultLimits() Limits {
	return Limits{MaxFrameBytes: MaxFrameBytes, MaxBlobBytes: MaxBlobBytes, MaxConcurrentCalls: MaxConcurrentCalls}
}

func (l Limits) Validate() error {
	if l.MaxFrameBytes < 1 || l.MaxFrameBytes > MaxFrameBytes || l.MaxBlobBytes < 1 || l.MaxBlobBytes > MaxBlobBytes || l.MaxConcurrentCalls < 1 || l.MaxConcurrentCalls > MaxConcurrentCalls {
		return fmt.Errorf("%w: frame=%d blob=%d concurrency=%d", ErrLimitExceeded, l.MaxFrameBytes, l.MaxBlobBytes, l.MaxConcurrentCalls)
	}
	return nil
}

type Hello struct {
	Protocol       string       `json:"protocol"`
	Major          int          `json:"major"`
	Minor          int          `json:"minor"`
	ArtifactDigest string       `json:"artifactDigest"`
	CatalogDigest  string       `json:"catalogDigest"`
	Generation     uint64       `json:"generation"`
	Capabilities   []Capability `json:"capabilities,omitempty"`
	Limits         Limits       `json:"limits"`
}

type Ready struct {
	Protocol       string       `json:"protocol"`
	Major          int          `json:"major"`
	Minor          int          `json:"minor"`
	ArtifactDigest string       `json:"artifactDigest"`
	CatalogDigest  string       `json:"catalogDigest"`
	Generation     uint64       `json:"generation"`
	Capabilities   []Capability `json:"capabilities,omitempty"`
	Limits         Limits       `json:"limits"`
}

// Negotiate validates both identities and returns the common limits and
// capabilities. The worker's catalog and artifact are exact identities; a
// reconnect with a different generation is a new worker, never an implicit
// retry of an in-flight effect.
func Negotiate(client, worker Hello) (Ready, error) {
	if err := client.validate(); err != nil {
		return Ready{}, err
	}
	if err := worker.validate(); err != nil {
		return Ready{}, err
	}
	if client.Protocol != ProtocolName || worker.Protocol != ProtocolName || client.Major != ProtocolMajor || worker.Major != ProtocolMajor {
		return Ready{}, ErrIncompatibleProtocol
	}
	if worker.Minor < client.Minor {
		return Ready{}, ErrIncompatibleProtocol
	}
	if client.ArtifactDigest != worker.ArtifactDigest {
		return Ready{}, ErrArtifactMismatch
	}
	if client.CatalogDigest != worker.CatalogDigest {
		return Ready{}, ErrCatalogMismatch
	}
	if client.Generation != worker.Generation {
		return Ready{}, ErrGenerationMismatch
	}
	if !containsAll(worker.Capabilities, client.Capabilities) {
		return Ready{}, ErrCapabilityDenied
	}
	limits := Limits{
		MaxFrameBytes:      min(client.Limits.MaxFrameBytes, worker.Limits.MaxFrameBytes),
		MaxBlobBytes:       min(client.Limits.MaxBlobBytes, worker.Limits.MaxBlobBytes),
		MaxConcurrentCalls: min(client.Limits.MaxConcurrentCalls, worker.Limits.MaxConcurrentCalls),
	}
	if err := limits.Validate(); err != nil {
		return Ready{}, err
	}
	return Ready{Protocol: ProtocolName, Major: ProtocolMajor, Minor: min(client.Minor, worker.Minor), ArtifactDigest: worker.ArtifactDigest, CatalogDigest: worker.CatalogDigest, Generation: worker.Generation, Capabilities: append([]Capability(nil), client.Capabilities...), Limits: limits}, nil
}

func (h Hello) validate() error {
	if h.Protocol == "" || h.Major < 0 || h.Minor < 0 || h.Generation == 0 {
		return fmt.Errorf("%w: invalid hello", ErrIncompatibleProtocol)
	}
	if !digestPattern.MatchString(h.ArtifactDigest) || !digestPattern.MatchString(h.CatalogDigest) {
		return fmt.Errorf("%w: invalid identity digest", ErrIncompatibleProtocol)
	}
	if err := h.Limits.Validate(); err != nil {
		return err
	}
	if len(h.Capabilities) > 128 { return ErrLimitExceeded }
	seen := map[Capability]bool{}
	for _, capability := range h.Capabilities {
		if !identityPattern.MatchString(string(capability)) || strings.Contains(string(capability), "orchestrate") {
			return fmt.Errorf("%w: invalid capability", ErrCapabilityDenied)
		}
		if seen[capability] { return ErrCapabilityDenied }; seen[capability] = true
	}
	return nil
}

// Validate checks a hello before it is sent to a peer.
func (h Hello) Validate() error { return h.validate() }

func containsAll(have, want []Capability) bool {
	set := make(map[Capability]struct{}, len(have))
	for _, value := range have {
		set[value] = struct{}{}
	}
	for _, value := range want {
		if _, ok := set[value]; !ok {
			return false
		}
	}
	return true
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type BlobRef struct {
	Digest string `json:"digest"`
	Size   int    `json:"size"`
}

type Call struct {
	CallID         string    `json:"callId"`
	AttemptID      string    `json:"attemptId"`
	Generation     uint64    `json:"generation"`
	Node           string    `json:"node"`
	NodeVersion    string    `json:"nodeVersion"`
	IdempotencyKey string    `json:"idempotencyKey,omitempty"`
	Deadline       time.Time `json:"deadline"`
	Input          []byte    `json:"input"`
	Blobs          []BlobRef `json:"blobs,omitempty"`
	Principal      string    `json:"principal"`
	Capabilities   []Capability `json:"capabilities,omitempty"`
}

func (c Call) Validate(limits Limits, generation uint64) error {
	if err := limits.Validate(); err != nil {
		return err
	}
	if !identityPattern.MatchString(c.CallID) || !identityPattern.MatchString(c.AttemptID) || !identityPattern.MatchString(c.Node) || !identityPattern.MatchString(c.NodeVersion) {
		return fmt.Errorf("invalid call identity")
	}
	if generation == 0 || c.Generation != generation {
		return ErrGenerationMismatch
	}
	if c.Deadline.IsZero() || !c.Deadline.After(time.Now()) {
		return context.DeadlineExceeded
	}
	if len(c.Input) == 0 || len(c.Input) > limits.MaxFrameBytes-1024 || len(c.IdempotencyKey) > 128 || len(c.Blobs) > 128 || len(c.Capabilities) > 128 {
		return fmt.Errorf("%w: input", ErrLimitExceeded)
	}
	if c.Principal != "" && !identityPattern.MatchString(c.Principal) { return ErrCapabilityDenied }
	totalBlobBytes := 0
	for _, blob := range c.Blobs {
		if blob.Size < 0 || blob.Size > limits.MaxBlobBytes || !digestPattern.MatchString(blob.Digest) {
			return fmt.Errorf("%w: blob", ErrLimitExceeded)
		}
		totalBlobBytes += blob.Size
		if totalBlobBytes > limits.MaxBlobBytes { return ErrLimitExceeded }
	}
	return nil
}

type contextDeadlineError struct{}

func (contextDeadlineError) Error() string { return "runtime: deadline exceeded" }

type ErrorClass string

const (
	ErrorInvalidInput ErrorClass = "invalid_input"
	ErrorNode         ErrorClass = "node_error"
	ErrorTransient    ErrorClass = "transient"
	ErrorUncertain    ErrorClass = "uncertain"
	ErrorCanceled     ErrorClass = "canceled"
	ErrorDeadline     ErrorClass = "deadline_exceeded"
)

type Result struct {
	CallID     string       `json:"callId"`
	AttemptID  string       `json:"attemptId"`
	Generation uint64       `json:"generation"`
	Output     []byte       `json:"output,omitempty"`
	Error      *RemoteError `json:"error,omitempty"`
}
type RemoteError struct {
	Class          ErrorClass `json:"class"`
	Code           string     `json:"code"`
	Message        string     `json:"message"`
	Retryable      bool       `json:"retryable"`
	IdempotencyKey string     `json:"idempotencyKey,omitempty"`
}

func (e RemoteError) SafeToRetry() bool { return e.Retryable && e.Class == ErrorTransient }

type FrameKind string

const (
	FrameHello  FrameKind = "hello"
	FrameReady  FrameKind = "ready"
	FrameCall   FrameKind = "call"
	FrameResult FrameKind = "result"
	FrameCancel FrameKind = "cancel"
	FrameDrain  FrameKind = "drain"
)

func (k FrameKind) Valid() bool {
	switch k {
	case FrameHello, FrameReady, FrameCall, FrameResult, FrameCancel, FrameDrain:
		return true
	default:
		return false
	}
}

// CanonicalDigest is used by generators and drift tests for descriptor and
// catalog payloads. It does not sign data or establish trust by itself.
func CanonicalDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func ValidatePayload(data []byte, contract schema.Schema) error { _, err := contract.Normalize(data); return err }

func SortedCapabilities(values []Capability) []Capability {
	out := append([]Capability(nil), values...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
