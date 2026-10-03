// Package policy enforces review before durable dispatch and deterministic
// validation before committing or returning a trusted tool result.
package policy

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/well-prado/new-blok/contract/approval"
	"github.com/well-prado/new-blok/contract/tool"
	"github.com/well-prado/new-blok/internal/journal"
)

// Journal is the existing effect journal port. Concrete persistence and
// provider implementations are selected by the application's composition root.
type Journal interface {
	Run(context.Context, string) (journal.Run, error)
	BeginEffect(context.Context, journal.EffectIntent) (journal.Operation, error)
	StartAttempt(context.Context, string) (journal.Attempt, error)
	CommitEffect(context.Context, journal.EffectCommit) error
	MarkUncertain(context.Context, string, string, string) error
}

type ScopeAuthorizer interface {
	ExecutionScope(context.Context) ([]string, error)
}

// Verifier is trusted deterministic application code registered with the
// target, never selected by caller input. It must validate ALL required
// assertions, evidence and provenance against this exact output/proposal.
type Verifier interface {
	Verify(context.Context, approval.Proposal, []byte, []approval.Assertion) error
}

type Target struct {
	Action         string
	Workflow       string
	ArtifactDigest string
	ToolDigest     string
	Effects        []string
	Scope          []string
	Execute        func(context.Context, []byte) ([]byte, error)
	Verifier       Verifier
}

type Invocation struct {
	RunID          string
	InvocationPath string
	IterationPath  string
	ApprovalID     string
	Input          []byte
	Assertions     []approval.Assertion
}

type Config struct {
	Journal        Journal
	Approvals      approval.Reader
	Authorizer     ScopeAuthorizer
	Clock          func() time.Time
	MaxInputBytes  int
	MaxOutputBytes int
	MaxAssertions  int
}

type Policy struct{ cfg Config }
type scopeKey struct{}

var ErrExecution = errors.New("policy: execution failed; reconciliation required")

// externalError retains only stable codes, never an external Error string or
// unwrap chain. In particular a wrapped cancellation may contain credentials.
func externalError(err, fallback error) error {
	for _, code := range []error{context.Canceled, context.DeadlineExceeded, approval.ErrDenied, approval.ErrStale, approval.ErrEvidence} {
		if errors.Is(err, code) {
			return code
		}
	}
	return fallback
}

func New(cfg Config) (*Policy, error) {
	if cfg.Journal == nil || cfg.Approvals == nil || cfg.Authorizer == nil ||
		cfg.MaxInputBytes <= 0 || cfg.MaxInputBytes > 16<<20 ||
		cfg.MaxOutputBytes <= 0 || cfg.MaxOutputBytes > 16<<20 ||
		cfg.MaxAssertions <= 0 || cfg.MaxAssertions > 64 {
		return nil, approval.ErrDenied
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	return &Policy{cfg: cfg}, nil
}

// Prepare derives the proposal from registered target metadata, admitted run,
// and exact dispatch bytes. The same method supports an approval UI without
// trusting a model's claimed artifact, effects, scope or input digest.
func (p *Policy) Prepare(ctx context.Context, target Target, call Invocation) (approval.Proposal, error) {
	if ctx == nil {
		return approval.Proposal{}, approval.ErrDenied
	}
	if err := ctx.Err(); err != nil {
		return approval.Proposal{}, err
	}
	if target.Verifier == nil || len(call.Input) > p.cfg.MaxInputBytes || !json.Valid(call.Input) || len(call.Assertions) > p.cfg.MaxAssertions {
		return approval.Proposal{}, approval.ErrDenied
	}
	for _, a := range call.Assertions {
		if len(a.Name)+len(a.Digest)+len(a.Source) > 1024 {
			return approval.Proposal{}, approval.ErrDenied
		}
	}
	run, err := p.cfg.Journal.Run(ctx, call.RunID)
	if err != nil {
		return approval.Proposal{}, err
	}
	if run.State != "accepted" || run.Workflow != target.Workflow || run.ArtifactDigest != target.ArtifactDigest {
		return approval.Proposal{}, approval.ErrStale
	}
	scope, err := p.cfg.Authorizer.ExecutionScope(ctx)
	if err != nil {
		return approval.Proposal{}, err
	}
	if !approval.Subset(target.Scope, scope) {
		return approval.Proposal{}, approval.ErrDenied
	}
	if inherited, ok := ctx.Value(scopeKey{}).([]string); ok && !approval.Subset(target.Scope, inherited) {
		return approval.Proposal{}, approval.ErrDenied
	}
	if inherited, ok := tool.Scope(ctx); ok && !approval.Subset(target.Scope, inherited.Capabilities) {
		return approval.Proposal{}, approval.ErrDenied
	}
	proposal := approval.Proposal{
		Action: target.Action, Workflow: target.Workflow, ArtifactDigest: target.ArtifactDigest,
		ToolDigest:  target.ToolDigest,
		InputDigest: approval.BytesDigest(call.Input), RunID: call.RunID,
		InvocationPath: call.InvocationPath, IterationPath: call.IterationPath,
		Effects: append([]string(nil), target.Effects...), Scope: append([]string(nil), target.Scope...),
	}
	if _, err := approval.Digest(proposal); err != nil {
		return approval.Proposal{}, err
	}
	return proposal, nil
}

// Invoke wraps a trusted executor after admission. BindCatalog connects it to
// #74's real registry-backed invocation path. Returned bytes are trusted only
// after commit; Target is application composition, never caller/model data.
func (p *Policy) Invoke(ctx context.Context, target Target, call Invocation) ([]byte, error) {
	if ctx == nil || target.Execute == nil || len(call.Input) > p.cfg.MaxInputBytes || len(call.Assertions) > p.cfg.MaxAssertions || len(target.Scope) > 64 || len(target.Effects) > 64 {
		return nil, approval.ErrDenied
	}
	// Detach all collections before deriving a binding or invoking user code.
	target.Scope = append([]string(nil), target.Scope...)
	target.Effects = append([]string(nil), target.Effects...)
	call.Input = append([]byte(nil), call.Input...)
	call.Assertions = append([]approval.Assertion(nil), call.Assertions...)
	proposal, err := p.Prepare(ctx, target, call)
	if err != nil {
		return nil, err
	}
	req := approval.Request{Proposal: proposal, ApprovalID: call.ApprovalID}
	if err := approval.Authorize(ctx, p.cfg.Approvals, p.cfg.Clock(), req); err != nil {
		return nil, err
	}
	identity := journal.OperationIdentity{RunID: proposal.RunID, ArtifactDigest: proposal.ArtifactDigest, InvocationPath: proposal.InvocationPath, IterationPath: proposal.IterationPath}
	op, err := p.cfg.Journal.BeginEffect(ctx, journal.EffectIntent{Identity: identity})
	if err != nil {
		return nil, err
	}
	// StartAttempt refuses dispatched/uncertain/committed operations. A crash
	// after this barrier must be reconciled; never blindly redispatch a retry.
	attempt, err := p.cfg.Journal.StartAttempt(ctx, op.Key)
	if err != nil {
		return nil, err
	}
	uncertain := func(cause error) ([]byte, error) {
		// Cleanup gets a short independent deadline when dispatch context expired.
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		err := p.cfg.Journal.MarkUncertain(cleanup, op.Key, attempt.ID, "policy result withheld; reconciliation required")
		if err != nil {
			err = externalError(err, ErrExecution)
		}
		return nil, errors.Join(cause, err)
	}
	// Recheck lifetime/cancellation after the durable dispatch barrier and before
	// executing the provider. Child invocations receive only this target's scope.
	if err := approval.Authorize(ctx, p.cfg.Approvals, p.cfg.Clock(), req); err != nil {
		return uncertain(err)
	}
	if err := ctx.Err(); err != nil {
		return uncertain(err)
	}
	execCtx := context.WithValue(ctx, scopeKey{}, append([]string(nil), proposal.Scope...))
	output, err := target.Execute(execCtx, append([]byte(nil), call.Input...))
	if err != nil {
		if cause := ctx.Err(); cause != nil {
			return uncertain(cause)
		}
		return uncertain(externalError(err, ErrExecution))
	}
	if len(output) > p.cfg.MaxOutputBytes || !json.Valid(output) {
		return uncertain(approval.ErrEvidence)
	}
	output = append([]byte(nil), output...)
	// Pass detached snapshots: a verifier cannot alter what is committed.
	if err := target.Verifier.Verify(execCtx, approval.CloneProposal(proposal), append([]byte(nil), output...), append([]approval.Assertion(nil), call.Assertions...)); err != nil {
		if cause := ctx.Err(); cause != nil {
			return uncertain(errors.Join(approval.ErrEvidence, cause))
		}
		return uncertain(errors.Join(approval.ErrEvidence, externalError(err, approval.ErrEvidence)))
	}
	if err := ctx.Err(); err != nil {
		return uncertain(err)
	}
	if err := approval.Authorize(ctx, p.cfg.Approvals, p.cfg.Clock(), req); err != nil {
		return uncertain(err)
	}
	if err := p.cfg.Journal.CommitEffect(ctx, journal.EffectCommit{OperationKey: op.Key, AttemptID: attempt.ID, Result: output}); err != nil {
		return uncertain(err)
	}
	return output, nil
}
