package policy

import (
	"context"
	"sort"
	"strings"
	"sync"

	"github.com/well-prado/new-blok/agent"
	"github.com/well-prado/new-blok/contract/approval"
	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/contract/tool"
	"github.com/well-prado/new-blok/node"
)

type catalogBinding struct {
	listing agent.Listing
	input   schema.Schema
	scope   []string
	verify  Verifier
}

// CatalogGate owns the catalog it guards. Register nodes/workflows through
// Catalog(), then Bind each reachable tool at trusted application startup.
// Direct Catalog.Invoke calls fail closed: only Invoke creates the private
// session linking catalog admission to a durable reviewed operation.
type CatalogGate struct {
	policy   *Policy
	catalog  *agent.Catalog
	mu       sync.RWMutex
	bindings map[string]catalogBinding
}

func BindCatalog(p *Policy, registry *node.Registry) (*CatalogGate, error) {
	if p == nil || registry == nil {
		return nil, approval.ErrDenied
	}
	g := &CatalogGate{policy: p, bindings: make(map[string]catalogBinding)}
	g.catalog = agent.NewCatalog(registry, g)
	return g, nil
}

func (g *CatalogGate) Catalog() *agent.Catalog { return g.catalog }

// Bind snapshots the actual registered catalog digest, schemas and effects.
// The exact capability set must match the catalog's capability digest; a
// broader principal grant cannot accidentally become this tool's scope.
// Verifiers come from trusted composition, never invocation JSON.
func (g *CatalogGate) Bind(name, version string, capabilities []string, verifier Verifier) error {
	if verifier == nil || len(capabilities) > 64 {
		return approval.ErrDenied
	}
	scope := append([]string(nil), capabilities...)
	sort.Strings(scope)
	for i := 1; i < len(scope); i++ {
		if scope[i] == scope[i-1] {
			return approval.ErrDenied
		}
	}
	var b catalogBinding
	for _, l := range g.catalog.List(tool.Principal{ID: "policy-composition", Capabilities: scope, MaxDepth: 64}) {
		if l.Name == name && l.Version == version {
			if l.CapabilityDigest != approval.BytesDigest([]byte(strings.Join(scope, "\n"))) {
				return approval.ErrDenied
			}
			in, err := schema.Parse(l.InputSchema)
			if err != nil {
				return approval.ErrDenied
			}
			b = catalogBinding{listing: l, input: in, scope: scope, verify: verifier}
			break
		}
	}
	if b.verify == nil {
		return approval.ErrDenied
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	key := name + "@" + version
	if _, exists := g.bindings[key]; exists {
		return approval.ErrConflict
	}
	g.bindings[key] = b
	return nil
}

func (g *CatalogGate) binding(name, version string) (catalogBinding, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	b, ok := g.bindings[name+"@"+version]
	return b, ok
}

type catalogSessionKey struct{}
type catalogSession struct {
	gate      *CatalogGate
	proposal  approval.Proposal
	approval  string
	principal string
	claims    []approval.Assertion
}

func (g *CatalogGate) target(ctx context.Context, principal tool.Principal, name, version string, call Invocation) (Target, Invocation, error) {
	b, ok := g.binding(name, version)
	if !ok || principal.ID == "" || !principal.Allows(tool.Manifest{Capabilities: b.scope}) {
		return Target{}, call, approval.ErrDenied
	}
	// Workflow/artifact come from committed admission, not model claims. The
	// tool digest is independent: a deployment may contain many catalog tools.
	run, err := g.policy.cfg.Journal.Run(ctx, call.RunID)
	if err != nil {
		return Target{}, call, err
	}
	if len(call.Input) > g.policy.cfg.MaxInputBytes {
		return Target{}, call, approval.ErrDenied
	}
	call.Input, err = b.input.Normalize(call.Input)
	if err != nil {
		return Target{}, call, approval.ErrDenied
	}
	target := Target{Action: name + "@" + version, Workflow: run.Workflow, ArtifactDigest: run.ArtifactDigest, ToolDigest: b.listing.ArtifactDigest, Scope: append([]string(nil), b.scope...), Effects: append([]string(nil), b.listing.Effects...), Verifier: b.verify}
	return target, call, nil
}

// Prepare returns the immutable normalized proposal for the review channel.
// principal is authenticated by the application, never decoded from input.
func (g *CatalogGate) Prepare(ctx context.Context, principal tool.Principal, name, version string, call Invocation) (approval.Proposal, error) {
	if ctx == nil {
		return approval.Proposal{}, approval.ErrDenied
	}
	target, call, err := g.target(ctx, principal, name, version, call)
	if err != nil {
		return approval.Proposal{}, err
	}
	return g.policy.Prepare(ctx, target, call)
}

func (g *CatalogGate) Invoke(ctx context.Context, principal tool.Principal, name, version string, call Invocation, budget tool.Budget) ([]byte, error) {
	if ctx == nil || ctx.Value(catalogSessionKey{}) != nil {
		return nil, approval.ErrDenied
	}
	if err := budget.Validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithDeadline(ctx, budget.Deadline)
	defer cancel()
	principal.Capabilities = append([]string(nil), principal.Capabilities...)
	target, call, err := g.target(ctx, principal, name, version, call)
	if err != nil {
		return nil, err
	}
	proposal, err := g.policy.Prepare(ctx, target, call)
	if err != nil {
		return nil, err
	}
	session := &catalogSession{gate: g, proposal: approval.CloneProposal(proposal), approval: call.ApprovalID, principal: principal.ID, claims: append([]approval.Assertion(nil), call.Assertions...)}
	target.Execute = func(ctx context.Context, input []byte) ([]byte, error) {
		return g.catalog.Invoke(context.WithValue(ctx, catalogSessionKey{}, session), principal, name, version, input, budget)
	}
	return g.policy.Invoke(ctx, target, call)
}

func (g *CatalogGate) admission(ctx context.Context, a tool.Admission) (*catalogSession, catalogBinding, error) {
	if ctx == nil {
		return nil, catalogBinding{}, approval.ErrDenied
	}
	s, ok := ctx.Value(catalogSessionKey{}).(*catalogSession)
	if !ok || s.gate != g || a.Principal != s.principal || a.Workflow != s.proposal.Action || a.WorkflowDigest != s.proposal.ToolDigest {
		return nil, catalogBinding{}, approval.ErrDenied
	}
	b, ok := g.binding(a.Name, a.Version)
	if !ok || a.ArtifactDigest != b.listing.ArtifactDigest || a.InputDigest != approval.BytesDigest(a.Input) ||
		a.Resources != b.listing.Resources ||
		!sameSet(a.Capabilities, b.scope) || !sameSet(a.Effects, b.listing.Effects) ||
		!approval.Subset(a.Capabilities, s.proposal.Scope) || !approval.Subset(a.Effects, s.proposal.Effects) {
		return nil, catalogBinding{}, approval.ErrDenied
	}
	if a.Name+"@"+a.Version == s.proposal.Action && a.InputDigest != s.proposal.InputDigest {
		return nil, catalogBinding{}, approval.ErrStale
	}
	if err := approval.Authorize(ctx, g.policy.cfg.Approvals, g.policy.cfg.Clock(), approval.Request{Proposal: s.proposal, ApprovalID: s.approval}); err != nil {
		return nil, catalogBinding{}, err
	}
	return s, b, nil
}

func (g *CatalogGate) Authorize(ctx context.Context, a tool.Admission) error {
	_, _, err := g.admission(ctx, a)
	return err
}

func (g *CatalogGate) Publish(ctx context.Context, a tool.Admission, output []byte) error {
	s, b, err := g.admission(ctx, a)
	if err != nil {
		return err
	}
	// Root verification runs in Policy immediately before the durable commit.
	// Child verification runs here, before the engine can use its output.
	if a.Name+"@"+a.Version == s.proposal.Action {
		return nil
	}
	p := approval.CloneProposal(s.proposal)
	p.Action, p.ToolDigest, p.InputDigest = a.Name+"@"+a.Version, a.ArtifactDigest, a.InputDigest
	p.Scope, p.Effects = append([]string(nil), a.Capabilities...), append([]string(nil), a.Effects...)
	ctx = context.WithValue(tool.WithScope(ctx, tool.Principal{ID: a.Principal, Capabilities: a.Capabilities, MaxDepth: a.Budget.MaxDepth}), scopeKey{}, append([]string(nil), a.Capabilities...))
	if err := b.verify.Verify(ctx, p, append([]byte(nil), output...), append([]approval.Assertion(nil), s.claims...)); err != nil {
		return externalError(err, approval.ErrEvidence)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return approval.Authorize(ctx, g.policy.cfg.Approvals, g.policy.cfg.Clock(), approval.Request{Proposal: s.proposal, ApprovalID: s.approval})
}

func sameSet(a, b []string) bool {
	return approval.Subset(a, b) && approval.Subset(b, a)
}
