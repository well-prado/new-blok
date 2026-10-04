package inspect

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/well-prado/new-blok/contract/inspection"
	runtimecontract "github.com/well-prado/new-blok/contract/runtime"
)

// InspectSource obtains an authorized bounded page from a durable source and
// applies the same redaction and response bounds as the process-local recorder.
func InspectSource(ctx context.Context, source inspection.Source, principal string, policy inspection.Policy, query inspection.Query) (inspection.Page, error) {
	if query.Version != inspection.Version {
		return inspection.Page{}, inspection.ErrUnsupportedVersion
	}
	if source == nil || principal == "" || query.RunID == "" {
		return inspection.Page{}, ErrNotFound
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
		return inspection.Page{}, err
	}
	offset, err := decodeCursor(query.Cursor, query.RunID, query.StepID)
	if err != nil {
		return inspection.Page{}, err
	}
	run, steps, total, err := source.ReadInspection(ctx, principal, query.RunID, query.StepID, offset, limit, policy.Fields, pagePolicy.MaxPayloadBytes)
	if err != nil {
		return inspection.Page{}, ErrNotFound
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
		return inspection.Page{}, fmt.Errorf("inspection: response limit exceeded")
	}
	return page, nil
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
