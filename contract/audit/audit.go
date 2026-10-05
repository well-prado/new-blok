// Package audit is the framework's mandatory audit contract (ADR 0021).
//
// An audit record states who decided what, bound to the digests of what was
// decided: an approval, a reconciliation, a deployment decision. It is not
// telemetry. Records commit in the same store transaction as the decision
// they describe, so a decision without its record cannot become durable, and
// a failed audit write refuses the decision. Optional logs and telemetry may
// receive a copy afterwards through a Mirror; dropping that copy never
// touches the durable record.
//
// Records hold identities, outcomes, labels, digests and opaque reference
// names only. Raw inputs, outputs, evidence text and secret values are never
// stored: evidence and results are recorded as digests, and every string is
// refused if it is credential-shaped (observe/redact), so a record is safe to
// show to an authorized reader and to a model.
package audit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/well-prado/new-blok/contract/observe"
	"github.com/well-prado/new-blok/observe/redact"
)

const (
	// MaxRecordBytes bounds one encoded record.
	MaxRecordBytes = 16 << 10
	// MaxRecords is the hard ceiling for Config.MaxRecords.
	MaxRecords = 10_000_000
	// MaxPage bounds one List page.
	MaxPage = 200
	// maxDigests and maxRefs bound a record's collections.
	maxDigests = 16
	maxRefs    = 64
)

var (
	// ErrUnavailable reports that durable audit could not be written. The
	// operation that required it was refused and nothing was applied.
	ErrUnavailable = errors.New("audit: durable audit unavailable")
	// ErrRequired reports an operation that requires durable audit composed
	// without it.
	ErrRequired = errors.New("audit: durable audit is required")
	// ErrInvalid reports a malformed record.
	ErrInvalid = errors.New("audit: invalid record")
	// ErrSensitive reports a record carrying a credential-shaped value.
	ErrSensitive = errors.New("audit: record carries a credential-shaped value")
	// ErrConflict reports a record id reused for different content.
	ErrConflict = errors.New("audit: record id conflict")
	// ErrCapacity reports a full audit store. It is also ErrUnavailable.
	ErrCapacity = errors.New("audit: capacity exhausted")
	// ErrDenied reports a reader without authority for the tenant.
	ErrDenied = errors.New("audit: access denied")
	// ErrCorrupt reports a stored record that failed verification.
	ErrCorrupt = errors.New("audit: stored record failed verification")
)

// unavailable carries the cause for diagnosis while its message stays fixed:
// store errors never become operator-facing text.
type unavailable struct{ cause error }

func (u unavailable) Error() string    { return ErrUnavailable.Error() }
func (u unavailable) Unwrap() []error  { return []error{ErrUnavailable, u.cause} }
func unavailableErr(cause error) error { return unavailable{cause: cause} }

// Kind names the decision a record describes.
type Kind string

const (
	KindApproval       Kind = "approval.decision"
	KindReconciliation Kind = "reconciliation.decision"
	KindDeployment     Kind = "deployment.decision"
)

// Outcome is what was decided.
type Outcome string

const (
	OutcomeApproved Outcome = "approved"
	OutcomeRejected Outcome = "rejected"
	OutcomeApplied  Outcome = "applied"
	OutcomeAccepted Outcome = "accepted"
	OutcomeRefused  Outcome = "refused"
)

// Record is one immutable audit fact. ID is chosen by the owning operation
// and is deterministic for that decision, so a retried decision writes the
// same record (idempotent) and a conflicting one is refused.
type Record struct {
	ID      string            `json:"id"`
	Kind    Kind              `json:"kind"`
	Tenant  string            `json:"tenant,omitempty"`
	Actor   string            `json:"actor"`
	Subject string            `json:"subject"`
	RunID   string            `json:"runId,omitempty"`
	Action  string            `json:"action,omitempty"`
	Outcome Outcome           `json:"outcome"`
	Reason  string            `json:"reason,omitempty"`
	Digests map[string]string `json:"digests,omitempty"`
	Refs    []string          `json:"refs,omitempty"`
	At      time.Time         `json:"at"`
}

// Validate checks shape, bounds and the no-secret rule.
func (r Record) Validate() error {
	switch r.Kind {
	case KindApproval, KindReconciliation, KindDeployment:
	default:
		return ErrInvalid
	}
	switch r.Outcome {
	case OutcomeApproved, OutcomeRejected, OutcomeApplied, OutcomeAccepted, OutcomeRefused:
	default:
		return ErrInvalid
	}
	if !text(r.ID, 1024, true) || !text(r.Actor, 256, true) || !text(r.Subject, 512, true) || !text(r.RunID, 256, false) || !text(r.Action, 256, false) || r.At.IsZero() {
		return ErrInvalid
	}
	if !r.tenantValid() || (r.Reason != "" && !observe.ValidLabel(r.Reason)) {
		return ErrInvalid
	}
	if len(r.Digests) > maxDigests || len(r.Refs) > maxRefs {
		return ErrInvalid
	}
	for name, digest := range r.Digests {
		if !observe.ValidLabel(name) || !ValidDigest(digest) {
			return ErrInvalid
		}
	}
	for _, ref := range r.Refs {
		if !text(ref, 512, true) {
			return ErrInvalid
		}
	}
	for _, value := range append([]string{r.ID, r.Actor, r.Subject, r.RunID, r.Action, r.Tenant, r.Reason}, r.Refs...) {
		if value != "" && redact.Sensitive(value) {
			return ErrSensitive
		}
	}
	encoded, err := r.encode()
	if err != nil || len(encoded) > MaxRecordBytes {
		return ErrInvalid
	}
	return nil
}

func (r Record) tenantValid() bool { return r.Tenant == "" || observe.ValidLabel(r.Tenant) }

func (r Record) encode() ([]byte, error) {
	r.At = r.At.UTC()
	return json.Marshal(r)
}

func text(value string, max int, required bool) bool {
	if value == "" {
		return !required
	}
	return len(value) <= max && utf8.ValidString(value) && !strings.ContainsFunc(value, func(c rune) bool { return c < 0x20 || c == 0x7f })
}

// ValidDigest reports whether value is a canonical lowercase sha256 digest.
func ValidDigest(value string) bool {
	if len(value) != 71 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	_, err := hex.DecodeString(value[7:])
	return err == nil && value == strings.ToLower(value)
}

// Digest returns the canonical sha256 digest the records use for content
// that must be bound but never stored (evidence text, results, outputs).
func Digest(content []byte) string {
	sum := sha256.Sum256(content)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// Mirror receives committed records for optional delivery, such as an
// application log or telemetry pipeline. Offer must not block; false means
// the copy was dropped. A Mirror can neither delay nor remove a record.
type Mirror interface {
	Offer(Record) bool
}

// ReadAuthorizer authenticates the reader from a trusted channel (normally
// ctx, as the application's boundary set it) and authorizes reading the
// tenant's audit. "" is the application-wide tenant. It is never given a
// reader name from a request.
type ReadAuthorizer interface {
	AuthorizeAuditRead(ctx context.Context, tenant string) error
}

type tenantKey struct{}

// WithTenant is for the trusted boundary that authenticated the operator:
// records written under ctx carry tenant. It does not authorize anything.
func WithTenant(ctx context.Context, tenant string) context.Context {
	return context.WithValue(ctx, tenantKey{}, tenant)
}

// TenantFrom returns the tenant set by WithTenant, or "".
func TenantFrom(ctx context.Context) string {
	tenant, _ := ctx.Value(tenantKey{}).(string)
	return tenant
}
