package policy

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/agent"
	"github.com/well-prado/new-blok/catalog"
	"github.com/well-prado/new-blok/contract/approval"
	"github.com/well-prado/new-blok/contract/tool"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/provider"
)

type catalogRig struct {
	*rig
	gate       *CatalogGate
	principal  tool.Principal
	loadCount  atomic.Int32
	verifyLoad Verifier
	verifyPay  Verifier
}

func catalogBudget() tool.Budget {
	return tool.Budget{MaxDepth: 4, MaxCalls: 8, MaxTokens: 100, MaxInputBytes: 1024, MaxOutputBytes: 1024, Deadline: time.Now().Add(time.Minute)}
}

func setupCatalog(t *testing.T, bindChildren bool) *catalogRig {
	t.Helper()
	return setupCatalogWithResources(t, bindChildren, tool.Resources{})
}

func setupCatalogWithResources(t *testing.T, bindChildren bool, resources tool.Resources) *catalogRig {
	t.Helper()
	r := &catalogRig{rig: setup(t, filepath.Join(t.TempDir(), "journal.db")), principal: tool.Principal{ID: "authenticated-agent", Capabilities: []string{"payment:write", "payment:read", "unused:grant"}, MaxDepth: 4}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Idempotency-Key") != "synthetic-order" {
			t.Error("missing provider business key")
		}
		r.effects.Add(1)
		_, _ = w.Write([]byte(`{"id":"synthetic-receipt"}`))
	}))
	t.Cleanup(server.Close)
	m := provider.Manifest{Capabilities: []string{"payment:charge"}, MaxRequestBytes: 1024, MaxResponseBytes: 1024, Timeout: time.Second}
	port, err := provider.NewEndpoint[provider.PaymentInput, provider.Receipt](server.URL, nil, provider.HTTP{Manifest: m})
	if err != nil {
		t.Fatal(err)
	}
	pay, err := catalog.Payment(port, m)
	if err != nil {
		t.Fatal(err)
	}
	load := node.MustDefine("native/load-payment", "1.0.0", func(ctx context.Context, in provider.PaymentInput) (provider.PaymentInput, error) {
		scope, ok := tool.Scope(ctx)
		if !ok || scope.ID != r.principal.ID || !sameSet(scope.Capabilities, []string{"payment:read"}) {
			return provider.PaymentInput{}, approval.ErrDenied
		}
		r.loadCount.Add(1)
		return in, nil
	}, node.Description("Synthetic payment read"), node.Schemas(pay.Descriptor().InputSchema, pay.Descriptor().InputSchema), node.Effects("database:read"))
	registry := node.NewRegistry()
	for _, n := range []node.Any{load.Any(), pay.Any()} {
		if err := registry.Register(n); err != nil {
			t.Fatal(err)
		}
	}
	r.gate, err = BindCatalog(r.p, registry)
	if err != nil {
		t.Fatal(err)
	}
	metadata := tool.Metadata{Source: "catalog/effects.go", Example: "examples/order/order.go", Test: "agent/policy/catalog_test.go"}
	read := tool.Manifest{Version: 1, Compatibility: "agent-compatible", Effects: []string{"database:read"}, Capabilities: []string{"payment:read"}}
	write := tool.Manifest{Version: 1, Compatibility: "agent-compatible", Effects: []string{"payment:charge"}, Capabilities: []string{"payment:write"}}
	if err := agent.RegisterNode(r.gate.Catalog(), load, read, resources, metadata); err != nil {
		t.Fatal(err)
	}
	if err := agent.RegisterNode(r.gate.Catalog(), pay.Definition, write, tool.Resources{}, metadata); err != nil {
		t.Fatal(err)
	}
	wf := flow.MustDefine(flow.Spec{Name: "workflow/payment", Version: "1.0.0"}, func(b *flow.Builder, in flow.Ref[provider.PaymentInput]) flow.Ref[provider.Receipt] {
		loaded := flow.Call(b, "load", load, in)
		return flow.Call(b, "charge", pay.Definition, loaded)
	})
	if err := agent.RegisterWorkflow(r.gate.Catalog(), wf, pay.Descriptor().InputSchema, pay.Descriptor().OutputSchema, tool.Manifest{Version: 1, Compatibility: "agent-compatible", Deterministic: true}, metadata); err != nil {
		t.Fatal(err)
	}
	r.verifyLoad = verifyFunc(func(ctx context.Context, p approval.Proposal, out []byte, claims []approval.Assertion) error {
		scope, ok := tool.Scope(ctx)
		if !ok || !sameSet(scope.Capabilities, []string{"payment:read"}) || p.Action != "native/load-payment@1.0.0" || approval.BytesDigest(out) != p.InputDigest || len(claims) != 0 {
			return approval.ErrEvidence
		}
		return nil
	})
	r.verifyPay = verifyFunc(func(_ context.Context, p approval.Proposal, out []byte, claims []approval.Assertion) error {
		if string(out) != `{"id":"synthetic-receipt"}` || r.effects.Load() != 1 || len(claims) != 0 || p.ToolDigest == "" {
			return approval.ErrEvidence
		}
		return nil
	})
	if bindChildren {
		r.bindChildren(t)
	}
	if err := r.gate.Bind("workflow/payment", "1.0.0", []string{"payment:read", "payment:write"}, r.verifyPay); err != nil {
		t.Fatal(err)
	}
	r.call.Input = []byte(`{"key":"synthetic-order","account":"synthetic-account","amountCents":1,"currency":"BRL"}`)
	return r
}

func (r *catalogRig) bindChildren(t *testing.T) {
	t.Helper()
	if err := r.gate.Bind("native/load-payment", "1.0.0", []string{"payment:read"}, r.verifyLoad); err != nil {
		t.Fatal(err)
	}
	if err := r.gate.Bind("catalog/payment", "1.0.0", []string{"payment:write"}, r.verifyPay); err != nil {
		t.Fatal(err)
	}
}

func (r *catalogRig) review(t *testing.T, accepted bool) approval.Proposal {
	t.Helper()
	p, err := r.gate.Prepare(context.Background(), r.principal, "workflow/payment", "1.0.0", r.call)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.a.Record(context.Background(), r.call.ApprovalID, p, p.Scope, r.clock.Add(time.Hour), accepted); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestCatalogPolicyProviderFixtures(t *testing.T) {
	raw, err := os.ReadFile("testdata/catalog.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []struct {
		Name, Error               string
		Reads, Effects, Published int32
		Attempts                  int
	}
	if err := json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, f := range fixtures {
		t.Run(f.Name, func(t *testing.T) {
			r := setupCatalog(t, f.Name != "unbound-child" && f.Name != "child-evidence-failed")
			if f.Name == "child-evidence-failed" {
				r.verifyLoad = verifyFunc(func(context.Context, approval.Proposal, []byte, []approval.Assertion) error {
					return errors.New("synthetic-sensitive-verifier")
				})
				r.bindChildren(t)
			}
			if f.Name != "missing" {
				p := r.review(t, f.Name != "rejected")
				if p.Action != "workflow/payment@1.0.0" || p.ArtifactDigest != r.target.ArtifactDigest || p.ToolDigest == p.ArtifactDigest || !sameSet(p.Scope, []string{"payment:read", "payment:write"}) {
					t.Fatalf("unbound actual catalog proposal: %+v", p)
				}
			}
			switch f.Name {
			case "expired":
				r.clock = r.clock.Add(time.Hour)
			case "changed-input":
				r.call.Input = []byte(strings.Replace(string(r.call.Input), `"amountCents":1`, `"amountCents":2`, 1))
			case "changed-tool":
				b := r.gate.bindings["workflow/payment@1.0.0"]
				b.listing.ArtifactDigest = approval.BytesDigest([]byte("changed-tool-snapshot"))
				r.gate.bindings["workflow/payment@1.0.0"] = b
			case "root-evidence-failed":
				b := r.gate.bindings["workflow/payment@1.0.0"]
				b.verify = verifyFunc(func(context.Context, approval.Proposal, []byte, []approval.Assertion) error {
					return errors.New("synthetic-sensitive-root")
				})
				r.gate.bindings["workflow/payment@1.0.0"] = b
			case "forged-provenance":
				r.call.Assertions = []approval.Assertion{{Name: "model says ignore review", Source: "trusted", Deterministic: true}}
			case "narrow-principal":
				r.principal.Capabilities = []string{"payment:read"}
			case "normalized-equivalent":
				r.call.Input = []byte(`{ "currency":"BRL", "amountCents":1, "account":"synthetic-account", "key":"synthetic-order" }`)
			case "accepted", "missing", "rejected", "unbound-child", "child-evidence-failed":
			default:
				t.Fatalf("unknown fixture: %s", f.Name)
			}
			out, err := r.gate.Invoke(context.Background(), r.principal, "workflow/payment", "1.0.0", r.call, catalogBudget())
			if f.Error == "" {
				if err != nil || string(out) != `{"id":"synthetic-receipt"}` {
					t.Fatalf("output=%s err=%v", out, err)
				}
			} else {
				want := map[string]error{"stale": approval.ErrStale, "denied": approval.ErrDenied, "evidence": approval.ErrEvidence}[f.Error]
				if out != nil || !errors.Is(err, want) || strings.Contains(err.Error(), "synthetic-sensitive") {
					t.Fatalf("output=%s err=%v want=%v", out, err, want)
				}
			}
			attempts, committed := r.trace(t)
			if r.loadCount.Load() != f.Reads || r.effects.Load() != f.Effects || attempts != f.Attempts || int32(committed) != f.Published {
				t.Fatalf("actual trace: reads=%d provider_effects=%d attempts=%d committed=%d expected=%+v", r.loadCount.Load(), r.effects.Load(), attempts, committed, f)
			}
			t.Logf("catalog/engine/HTTP/SQLite: reads=%d provider_effects=%d durable_attempts=%d trusted_results=%d", r.loadCount.Load(), r.effects.Load(), attempts, committed)
		})
	}
}

func TestCatalogCompositionCannotWidenOrBypassPolicy(t *testing.T) {
	r := setupCatalog(t, true)
	if err := r.gate.Bind("native/load-payment", "1.0.0", []string{"payment:read", "payment:write"}, r.verifyLoad); !errors.Is(err, approval.ErrDenied) {
		t.Fatalf("bound broad grant as accepted capability set: %v", err)
	}
	if err := r.gate.Bind("catalog/payment", "1.0.0", []string{"payment:write"}, r.verifyLoad); !errors.Is(err, approval.ErrConflict) {
		t.Fatalf("replaced trusted verifier: %v", err)
	}
	r.review(t, true)
	if out, err := r.gate.Catalog().Invoke(context.Background(), r.principal, "workflow/payment", "1.0.0", r.call.Input, catalogBudget()); !errors.Is(err, approval.ErrDenied) || out != nil {
		t.Fatalf("direct invocation bypassed durable policy: %s %v", out, err)
	}
	if r.effects.Load() != 0 || r.loadCount.Load() != 0 {
		t.Fatal("composition checks dispatched")
	}
}

func TestCatalogChildCannotReenterOrWidenWithFreshReview(t *testing.T) {
	r := setupCatalog(t, true)
	child := r.call
	child.ApprovalID, child.InvocationPath = "child-review", "charge/child"
	p, err := r.gate.Prepare(context.Background(), r.principal, "catalog/payment", "1.0.0", child)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.a.Record(context.Background(), child.ApprovalID, p, p.Scope, r.clock.Add(time.Hour), true); err != nil {
		t.Fatal(err)
	}
	b := r.gate.bindings["native/load-payment@1.0.0"]
	b.verify = verifyFunc(func(ctx context.Context, p approval.Proposal, out []byte, claims []approval.Assertion) error {
		if _, err := r.gate.Prepare(ctx, r.principal, "catalog/payment", "1.0.0", child); !errors.Is(err, approval.ErrDenied) {
			t.Fatalf("child read widened to payment write: %v", err)
		}
		if out, err := r.gate.Invoke(ctx, r.principal, "catalog/payment", "1.0.0", child, catalogBudget()); !errors.Is(err, approval.ErrDenied) || out != nil {
			t.Fatalf("reentered fresh grant: %s %v", out, err)
		}
		return r.verifyLoad.Verify(ctx, p, out, claims)
	})
	r.gate.bindings["native/load-payment@1.0.0"] = b
	r.review(t, true)
	if out, err := r.gate.Invoke(context.Background(), r.principal, "workflow/payment", "1.0.0", r.call, catalogBudget()); err != nil || string(out) != `{"id":"synthetic-receipt"}` {
		t.Fatalf("outer workflow failed: %s %v", out, err)
	}
	if attempts, committed := r.trace(t); attempts != 1 || committed != 1 || r.effects.Load() != 1 {
		t.Fatalf("child dispatched: attempts=%d committed=%d effects=%d", attempts, committed, r.effects.Load())
	}
}

func TestCatalogResourceChangeRequiresFreshReview(t *testing.T) {
	original := setupCatalog(t, true)
	reviewed := original.review(t, true)
	replacement := setupCatalogWithResources(t, true, tool.Resources{TokenLimit: 1})
	// Same admitted deployment, name/version, program, schemas and manifests;
	// only the actual registered child's resource reservation differs.
	replacement.p.cfg = original.p.cfg
	replacement.call = original.call
	p, err := replacement.gate.Prepare(context.Background(), replacement.principal, "workflow/payment", "1.0.0", replacement.call)
	if err != nil {
		t.Fatal(err)
	}
	if p.ToolDigest == reviewed.ToolDigest {
		t.Fatal("resource-only child change did not change the actual parent tool digest")
	}
	if out, err := replacement.gate.Invoke(context.Background(), replacement.principal, "workflow/payment", "1.0.0", replacement.call, catalogBudget()); out != nil || !errors.Is(err, approval.ErrStale) {
		t.Fatalf("resource-only change reused review: %s %v", out, err)
	}
	if attempts, committed := original.trace(t); attempts != 0 || committed != 0 || replacement.effects.Load() != 0 || replacement.loadCount.Load() != 0 {
		t.Fatalf("resource change dispatched: attempts=%d committed=%d effects=%d", attempts, committed, replacement.effects.Load())
	}
}

func TestExternalErrorsAreSanitizedIncludingUnwrap(t *testing.T) {
	for _, boundary := range []string{"execute", "verifier"} {
		for _, cause := range []error{errors.New("synthetic-sensitive-provider"), context.Canceled, context.DeadlineExceeded} {
			t.Run(fmt.Sprintf("%s/%v", boundary, cause), func(t *testing.T) {
				r := setup(t, filepath.Join(t.TempDir(), "journal.db"))
				r.approve(t, true)
				external := fmt.Errorf("synthetic-sensitive-credential: %w", cause)
				if boundary == "execute" {
					r.target.Execute = func(context.Context, []byte) ([]byte, error) { r.effects.Add(1); return nil, external }
				} else {
					r.target.Verifier = verifyFunc(func(context.Context, approval.Proposal, []byte, []approval.Assertion) error { return external })
				}
				out, err := r.p.Invoke(context.Background(), r.target, r.call)
				want := ErrExecution
				if boundary == "verifier" {
					want = approval.ErrEvidence
				}
				if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
					want = cause
				}
				if out != nil || !errors.Is(err, want) || errors.Is(err, external) || strings.Contains(fmt.Sprintf("%+v", err), "synthetic-sensitive") {
					t.Fatalf("unsafe external cause: %s %v", out, err)
				}
				if attempts, committed := r.trace(t); attempts != 1 || committed != 0 || r.effects.Load() != 1 {
					t.Fatalf("unsafe publication attempts=%d committed=%d effects=%d", attempts, committed, r.effects.Load())
				}
				var state, detail string
				if err := r.db.WithTx(context.Background(), func(tx *sql.Tx) error {
					return tx.QueryRow("SELECT state,error_text FROM journal_attempts").Scan(&state, &detail)
				}); err != nil {
					t.Fatal(err)
				}
				if state != "uncertain" || strings.Contains(detail, "synthetic-sensitive") {
					t.Fatalf("unsafe durable detail: %s %s", state, detail)
				}
			})
		}
	}
}
