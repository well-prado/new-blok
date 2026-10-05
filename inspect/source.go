package inspect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/well-prado/new-blok/contract/inspection"
	runtimecontract "github.com/well-prado/new-blok/contract/runtime"
)

// UnavailableSource is an optional inspection.Source extension: the same read,
// also naming what the reconstruction could not include (for example steps
// whose workflow artifact no longer matches), so a reader can tell "not
// there" from "not readable". The event stream adds the names to a
// snapshot's "unavailable" list, at most maxUnavailableNotes of them, each at
// most maxUnavailableNoteBytes.
type UnavailableSource interface {
	inspection.Source
	ReadInspectionUnavailable(ctx context.Context, principal, runID, stepID string, offset, limit int, fields map[inspection.Field]bool, maxPayload int) (inspection.Run, []inspection.Step, int, []string, error)
}

// RefusingSource is an optional inspection.Source extension that classifies
// its own errors. Refused reports an error meaning the reader may not, or
// may no longer, see the run, as opposed to a read that failed (a timeout,
// an outage). The event stream closes a recovered follower whose poll is
// refused, instead of keeping it open until MaxDuration; a failed poll is
// skipped.
type RefusingSource interface {
	inspection.Source
	Refused(error) bool
}

const (
	maxUnavailableNotes     = 8
	maxUnavailableNoteBytes = 128
)

// sourceReadError is a failed source read. It is ErrNotFound to callers, and
// keeps the source's own error for RefusingSource.
type sourceReadError struct{ cause error }

func (e *sourceReadError) Error() string        { return ErrNotFound.Error() }
func (e *sourceReadError) Is(target error) bool { return target == ErrNotFound }
func (e *sourceReadError) Unwrap() error        { return e.cause }

// InspectSource obtains an authorized bounded page from a durable source and
// applies the same redaction and response bounds as the process-local recorder.
func InspectSource(ctx context.Context, source inspection.Source, principal string, policy inspection.Policy, query inspection.Query) (inspection.Page, error) {
	page, _, err := inspectSource(ctx, source, principal, policy, query)
	var failed *sourceReadError
	if errors.As(err, &failed) {
		return inspection.Page{}, ErrNotFound
	}
	return page, err
}

// inspectSource is InspectSource that also returns an UnavailableSource's
// notes, and a failed source read as a *sourceReadError.
func inspectSource(ctx context.Context, source inspection.Source, principal string, policy inspection.Policy, query inspection.Query) (inspection.Page, []string, error) {
	if query.Version != inspection.Version {
		return inspection.Page{}, nil, inspection.ErrUnsupportedVersion
	}
	if source == nil || principal == "" || query.RunID == "" {
		return inspection.Page{}, nil, ErrNotFound
	}
	limit := policy.MaxPageSize
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if query.Limit > 0 && query.Limit < limit {
		limit = query.Limit
	}
	projectedAttempts, projectedLogs := 0, 0
	if policy.Fields[inspection.FieldInput] || policy.Fields[inspection.FieldOutput] {
		projectedAttempts = limit * inspection.MaxProjectedAttempts
	}
	if policy.Fields[inspection.FieldLogs] {
		projectedLogs = limit * inspection.MaxProjectedLogs
	}
	pagePolicy, err := boundedProjectionPolicy(policy, limit, projectedAttempts, projectedLogs)
	if err != nil {
		return inspection.Page{}, nil, err
	}
	offset, err := decodeCursor(query.Cursor, query.RunID, query.StepID)
	if err != nil {
		return inspection.Page{}, nil, err
	}
	var run inspection.Run
	var steps []inspection.Step
	var total int
	var notes []string
	if noting, ok := source.(UnavailableSource); ok {
		run, steps, total, notes, err = noting.ReadInspectionUnavailable(ctx, principal, query.RunID, query.StepID, offset, limit, policy.Fields, pagePolicy.MaxPayloadBytes)
	} else {
		run, steps, total, err = source.ReadInspection(ctx, principal, query.RunID, query.StepID, offset, limit, policy.Fields, pagePolicy.MaxPayloadBytes)
	}
	if err != nil {
		return inspection.Page{}, nil, &sourceReadError{cause: err}
	}
	sourceTruncated := len(steps) > limit
	if sourceTruncated {
		steps = steps[:limit]
	}
	responseLimit := pagePolicy.MaxResponseBytes
	if responseLimit <= 0 || responseLimit > 1<<20 {
		responseLimit = 256 << 10
	}
	page := inspection.Page{Version: inspection.Version, Run: projectRun(run, pagePolicy), Steps: projectSteps(steps, pagePolicy)}
	page.Truncated = sourceTruncated
	if offset+len(steps) < total {
		page.Next = encodeCursor(query.RunID, query.StepID, offset+len(steps))
	}
	encoded, err := json.Marshal(page)
	if err != nil || len(encoded) > responseLimit {
		return inspection.Page{}, nil, fmt.Errorf("inspection: response limit exceeded")
	}
	return page, boundedNotes(notes), nil
}

// boundedNotes keeps at most maxUnavailableNotes non-empty notes of at most
// maxUnavailableNoteBytes each.
func boundedNotes(notes []string) []string {
	var out []string
	for _, note := range notes {
		if note == "" || len(note) > maxUnavailableNoteBytes {
			continue
		}
		if len(out) == maxUnavailableNotes {
			break
		}
		out = append(out, note)
	}
	return out
}

// AuthorizedBlobReader must be bound to one authenticated session and enforce
// that session's blob:read authority; principal is checked again by this layer.
type AuthorizedBlobReader interface {
	Principal() string
	ReadBlob(runtimecontract.BlobRef, int) ([]byte, error)
}

func RetrieveBlob(reader AuthorizedBlobReader, principal string, policy inspection.Policy, ref runtimecontract.BlobRef, maxBytes int) ([]byte, error) {
	if reader == nil || principal == "" || reader.Principal() != principal || !policy.AllowBlobRead || policy.MaxBlobBytes < 1 || policy.MaxBlobBytes > 1<<20 || maxBytes < 1 || maxBytes > policy.MaxBlobBytes || ref.Size < 0 || ref.Size > maxBytes {
		return nil, ErrNotFound
	}
	value, err := reader.ReadBlob(ref, maxBytes)
	if err != nil || len(value) != ref.Size || len(value) > maxBytes || runtimecontract.CanonicalDigest(value) != ref.Digest {
		return nil, ErrNotFound
	}
	return append([]byte(nil), value...), nil
}
