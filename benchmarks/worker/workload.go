// Package workerbench measures a complete order program through the real engine.
// Native and worker implementations share schemas, program, provider and fixtures.
package workerbench

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	"github.com/well-prado/new-blok/contract"
	runtimecontract "github.com/well-prado/new-blok/contract/runtime"
	"github.com/well-prado/new-blok/internal/compile"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
)

type Order struct {
	OrderID  string `json:"orderId"`
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
	Mode     string `json:"mode"`
}
type Priced struct {
	OrderID    string `json:"orderId"`
	TotalCents int    `json:"totalCents"`
	Mode       string `json:"mode"`
}
type Paid struct {
	OrderID    string `json:"orderId"`
	TotalCents int    `json:"totalCents"`
	ReceiptID  string `json:"receiptId"`
}
type Receipt struct {
	OrderID    string `json:"orderId"`
	TotalCents int    `json:"totalCents"`
	ReceiptID  string `json:"receiptId"`
	Status     string `json:"status"`
}

var OrderSchema = []byte(`{"type":"object","properties":{"orderId":{"type":"string"},"sku":{"type":"string"},"quantity":{"type":"integer","minimum":1,"maximum":100},"mode":{"type":"string"}},"required":["orderId","sku","quantity","mode"],"additionalProperties":false}`)
var PricedSchema = []byte(`{"type":"object","properties":{"orderId":{"type":"string"},"totalCents":{"type":"integer","minimum":1500,"maximum":150000},"mode":{"type":"string"}},"required":["orderId","totalCents","mode"],"additionalProperties":false}`)
var PaidSchema = []byte(`{"type":"object","properties":{"orderId":{"type":"string"},"totalCents":{"type":"integer","minimum":1500,"maximum":150000},"receiptId":{"type":"string"}},"required":["orderId","totalCents","receiptId"],"additionalProperties":false}`)
var ReceiptSchema = []byte(`{"type":"object","properties":{"orderId":{"type":"string"},"totalCents":{"type":"integer","minimum":1500,"maximum":150000},"receiptId":{"type":"string"},"status":{"type":"string"}},"required":["orderId","totalCents","receiptId","status"],"additionalProperties":false}`)

// Provider is an actual bounded HTTP endpoint with observed requests and effects.
// Uncertain deliberately commits the effect then drops the HTTP response.
//
// Two modes hold a request at a barrier instead of sleeping for a fixed time,
// so a test can act inside the window however slow the machine is:
//   - "delay" parks the request BEFORE the effect is committed;
//   - "late" commits the effect, then parks BEFORE the response is written.
//
// A parked request resumes only when Release is called. If its caller goes away
// first (the worker is killed, the context is canceled) or the provider is
// closed, the request is abandoned and never proceeds. Abandoned reports it.
type Provider struct {
	Server      *httptest.Server
	mu          sync.Mutex
	requests    int
	effects     map[string]Paid
	committed   chan string
	held        chan string
	abandoned   chan string
	release     chan struct{}
	closing     chan struct{}
	releaseOnce sync.Once
	closeOnce   sync.Once
}

func NewProvider() *Provider {
	p := &Provider{effects: make(map[string]Paid), committed: make(chan string, 1024), held: make(chan string, 1024), abandoned: make(chan string, 1024), release: make(chan struct{}), closing: make(chan struct{})}
	p.Server = httptest.NewServer(http.HandlerFunc(p.serve))
	return p
}

// Close unblocks any parked request, then stops the server.
func (p *Provider) Close() {
	p.closeOnce.Do(func() { close(p.closing) })
	p.Server.Close()
}

// Release lets every parked and future "delay"/"late" request continue. It is
// idempotent. Tests that kill or cancel the caller never need it.
func (p *Provider) Release() { p.releaseOnce.Do(func() { close(p.release) }) }

// Held yields the order ID of each "delay" request that is parked before its
// effect. The request has been counted; no effect exists yet.
func (p *Provider) Held() <-chan string { return p.held }

// Abandoned yields the order ID of each parked request whose caller went away
// (or whose provider closed) before Release: it never committed or answered.
func (p *Provider) Abandoned() <-chan string { return p.abandoned }

// park blocks until Release, reporting true, or until the request is abandoned,
// reporting false.
func (p *Provider) park(r *http.Request, id string) bool {
	select {
	case <-p.release:
		return true
	case <-r.Context().Done():
	case <-p.closing:
	}
	select {
	case p.abandoned <- id:
	default:
	}
	return false
}
func (p *Provider) Counts() (requests, effects int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.requests, len(p.effects)
}
func (p *Provider) Committed() <-chan string { return p.committed }
func (p *Provider) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/charge" {
		http.NotFound(w, r)
		return
	}
	defer r.Body.Close()
	var input Priced
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&input); err != nil || input.OrderID == "" || len(input.OrderID) > 128 || input.TotalCents < 1500 || input.TotalCents > 150000 || r.Header.Get("Idempotency-Key") != input.OrderID {
		http.Error(w, "invalid", 400)
		return
	}
	var extra any
	if dec.Decode(&extra) != io.EOF {
		http.Error(w, "invalid", 400)
		return
	}
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()
	if input.Mode == "reject" {
		http.Error(w, "declined", 422)
		return
	}
	if input.Mode == "transient" {
		http.Error(w, "unavailable", 503)
		return
	}
	if input.Mode == "delay" {
		select {
		case p.held <- input.OrderID:
		default:
		}
		if !p.park(r, input.OrderID) {
			return
		}
	}
	paid := Paid{OrderID: input.OrderID, TotalCents: input.TotalCents, ReceiptID: "receipt-" + input.OrderID}
	p.mu.Lock()
	if previous, ok := p.effects[input.OrderID]; ok {
		paid = previous
	} else {
		if len(p.effects) >= 100000 {
			p.mu.Unlock()
			http.Error(w, "capacity", 503)
			return
		}
		p.effects[input.OrderID] = paid
		select {
		case p.committed <- input.OrderID:
		default:
		}
	}
	p.mu.Unlock()
	if input.Mode == "uncertain" {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
		return
	}
	if input.Mode == "late" && !p.park(r, input.OrderID) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(paid)
}

func NativeNodes(providerURL string) map[string]node.Any {
	price := node.MustDefine("bench/price", "1.0.0", func(ctx context.Context, in Order) (Priced, error) {
		if err := ctx.Err(); err != nil {
			return Priced{}, err
		}
		if in.SKU != "coffee" {
			return Priced{}, &node.DomainError{Code: "unknown_sku", Class: "node_error"}
		}
		return Priced{OrderID: in.OrderID, TotalCents: in.Quantity * 1500, Mode: in.Mode}, nil
	}, node.Description("Price a bounded coffee order"), node.Schemas(OrderSchema, PricedSchema), node.Pure())
	client := &http.Client{Timeout: 2 * time.Second}
	pay := node.MustDefine("bench/pay", "1.0.0", func(ctx context.Context, in Priced) (Paid, error) {
		raw, err := json.Marshal(in)
		if err != nil {
			return Paid{}, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, providerURL+"/charge", bytes.NewReader(raw))
		if err != nil {
			return Paid{}, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", in.OrderID)
		resp, err := client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return Paid{}, ctx.Err()
			}
			return Paid{}, &node.DomainError{Code: "provider_uncertain", Class: "uncertain", Uncertain: true, Err: runtimecontract.ErrUncertain}
		}
		defer resp.Body.Close()
		if resp.StatusCode == 422 {
			return Paid{}, &node.DomainError{Code: "payment_declined", Class: "node_error"}
		}
		if resp.StatusCode == 503 {
			return Paid{}, &node.DomainError{Code: "provider_unavailable", Class: "transient", Retryable: true}
		}
		if resp.StatusCode != 200 {
			return Paid{}, errors.New("unexpected provider status")
		}
		var out Paid
		if err := json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&out); err != nil {
			return Paid{}, err
		}
		return out, nil
	}, node.Description("Charge the HTTP mock provider"), node.Schemas(PricedSchema, PaidSchema), node.Effects("http:orders"), node.RequiredCapabilities("http:orders"))
	receipt := node.MustDefine("bench/receipt", "1.0.0", func(ctx context.Context, in Paid) (Receipt, error) {
		if err := ctx.Err(); err != nil {
			return Receipt{}, err
		}
		return Receipt{OrderID: in.OrderID, TotalCents: in.TotalCents, ReceiptID: in.ReceiptID, Status: "paid"}, nil
	}, node.Description("Publish the paid order receipt"), node.Schemas(PaidSchema, ReceiptSchema), node.Pure())
	return map[string]node.Any{"price": price.Any(), "pay": pay.Any(), "receipt": receipt.Any()}
}

// CompileProgram uses the canonical document compiler. Both implementations run
// exactly this program, including schema validation and immutable output copies.
func CompileProgram(nodes map[string]node.Any) (contract.InternalProgram, error) {
	d := contract.Document{Version: contract.CurrentVersion, Workflow: contract.Workflow{ID: "order", Name: "order", Version: "1.0.0", Digest: runtimecontract.CanonicalDigest([]byte("issue53-order-v1")), InputSchema: OrderSchema, OutputSchema: ReceiptSchema}}
	for i, id := range []string{"price", "pay", "receipt"} {
		n, ok := nodes[id]
		if !ok {
			return contract.InternalProgram{}, fmt.Errorf("missing node %s", id)
		}
		desc := n.Descriptor()
		d.Nodes = append(d.Nodes, contract.NodeDescriptor{ID: id, Version: desc.Version, Digest: runtimecontract.CanonicalDigest(desc.InputSchema), InputSchema: desc.InputSchema, OutputSchema: desc.OutputSchema})
		step := contract.Instruction{ID: id, Kind: "call", Node: id}
		if i > 0 {
			step.References = []contract.Reference{{Step: []string{"price", "pay"}[i-1]}}
		}
		d.Workflow.Instructions = append(d.Workflow.Instructions, step)
	}
	d.Workflow.Instructions = append(d.Workflow.Instructions, contract.Instruction{ID: "output", Kind: "output", References: []contract.Reference{{Step: "receipt"}}})
	compiled, err := compile.Compile(d)
	return compiled.Program, err
}

type Workflow struct {
	engine  *engine.Engine
	program contract.InternalProgram
}

func NewWorkflow(nodes map[string]node.Any) (*Workflow, error) {
	p, err := CompileProgram(nodes)
	if err != nil {
		return nil, err
	}
	return &Workflow{engine: engine.New(nodes), program: p}, nil
}
func (w *Workflow) Run(ctx context.Context, input Order) (engine.Result, error) {
	return w.engine.Run(ctx, w.program, input)
}
