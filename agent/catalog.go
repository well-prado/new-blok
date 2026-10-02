// Package agent binds reviewed application nodes and workflows to a bounded
// tool admission path. It accepts no model-authored manifests or callbacks.
package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/contract/tool"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
)

type Manifest = tool.Manifest
type Budget = tool.Budget
type Principal = tool.Principal

var ErrInvalidManifest = tool.ErrInvalidManifest
var ErrBudget = tool.ErrBudget
var ErrNotAgentSafe = errors.New("agent: tool is not agent-safe")
var ErrDenied = errors.New("agent: capability denied")
var ErrCapacity = errors.New("agent: active invocation capacity exceeded")

type Listing struct {
	Name, Version, Description       string
	InputSchema, OutputSchema        json.RawMessage
	Effects                          []string
	CapabilityDigest, ArtifactDigest string
	Metadata                         tool.Metadata
	Resources                        tool.Resources
}

type binding struct {
	listing                 Listing
	manifest                Manifest
	in, out                 schema.Schema
	resources               tool.Resources
	native                  func(context.Context, []byte) ([]byte, error)
	program                 *contract.InternalProgram
	children                map[string]binding
	literals                map[string][]byte
	maxDepth, calls, tokens int
}

type Catalog struct {
	mu       sync.RWMutex
	registry *node.Registry
	gate     tool.Gate
	tools    map[string]binding
	active   chan struct{}
}

// NewCatalog receives only trusted application dependencies. Registry changes
// after startup are unsupported; each binding captures its registered node.
func NewCatalog(registry *node.Registry, gate tool.Gate) *Catalog {
	return &Catalog{registry: registry, gate: gate, tools: map[string]binding{}, active: make(chan struct{}, 64)}
}

// RegisterNode binds the actual registry handler, including a worker-backed
// node injected by the application. The supplied definition provides Go types;
// its handler and descriptor cannot replace the registered implementation.
func RegisterNode[I, O any](c *Catalog, definition node.Definition[I, O], m Manifest, r tool.Resources, metadata tool.Metadata) error {
	if c == nil || c.registry == nil {
		return ErrNotAgentSafe
	}
	d := definition.Descriptor()
	n, ok := c.registry.Lookup(d.Name, d.Version)
	if !ok {
		return ErrNotAgentSafe
	}
	d = n.Descriptor()
	if err := m.Validate(); err != nil {
		return err
	}
	if r.TokenLimit < 0 || r.TokenLimit > 1<<30 {
		return ErrBudget
	}
	if n.IsRemote() && r.TokenLimit > 0 {
		return ErrNotAgentSafe
	}
	if !(Principal{Capabilities: m.Capabilities}).Allows(Manifest{Capabilities: d.RequiredCapabilities}) {
		return ErrInvalidManifest
	}
	// Effects and determinism belong to the native registry, not agent claims.
	if !equalSet(m.Effects, d.Effects) || m.Deterministic != d.Deterministic {
		return ErrInvalidManifest
	}
	b, err := prepare(d.Name, d.Version, d.Description, d.InputSchema, d.OutputSchema, m, metadata)
	if err != nil {
		return err
	}
	b.resources = r
	b.listing.Resources = r
	identity, _ := json.Marshal(struct {
		Base      string
		Resources tool.Resources
	}{b.listing.ArtifactDigest, r})
	b.listing.ArtifactDigest = hash(identity)
	b.maxDepth, b.calls, b.tokens = 1, 1, r.TokenLimit
	b.native = func(ctx context.Context, input []byte) ([]byte, error) {
		var typed I
		portable, err := decode(input)
		if err != nil {
			return nil, err
		}
		native, err := json.Marshal(nativeValue(b.in, portable))
		if err != nil {
			return nil, err
		}
		decoder := json.NewDecoder(bytes.NewReader(native))
		decoder.UseNumber()
		if err := decoder.Decode(&typed); err != nil {
			return nil, err
		}
		output, err := n.Invoke(ctx, typed)
		if err != nil {
			return nil, err
		}
		return json.Marshal(output)
	}
	return c.register(b)
}

// RegisterWorkflow compiles supported structural calls, snapshots their policy,
// and aggregates every referenced child's authority. Unsupported control flow
// fails at registration instead of hiding children behind opaque callbacks.
func RegisterWorkflow[I, O any](c *Catalog, wf flow.Definition[I, O], inputSchema, outputSchema []byte, parent Manifest, metadata tool.Metadata) error {
	if c == nil {
		return ErrNotAgentSafe
	}
	if err := parent.Validate(); err != nil {
		return err
	}
	p := wf.Program()
	if len(p.Instructions) == 0 || len(p.Instructions) > 10000 {
		return ErrBudget
	}
	program := contract.InternalProgram{WorkflowID: p.Spec.Name, Version: p.Spec.Version}
	children := map[string]binding{}
	literals := map[string][]byte{}
	seen := map[string]bool{}
	m := cloneManifest(parent)
	c.mu.RLock()
	registered := make(map[string]binding, len(c.tools))
	for key, child := range c.tools {
		registered[key] = child
	}
	c.mu.RUnlock()
	maxDepth, calls, tokens := 1, 0, 0
	for _, instruction := range p.Instructions {
		if (instruction.Kind != "call" && instruction.Kind != "child") || instruction.ID == "output" || seen[instruction.ID] {
			return ErrNotAgentSafe
		}
		key := instruction.Node.Name + "@" + instruction.Node.Version
		if instruction.Kind == "child" {
			key, _ = instruction.Data["workflow"].(string)
		}
		child, ok := registered[key]
		if !ok || child.manifest.Validate() != nil || (instruction.Kind == "child" && child.program == nil) {
			return ErrNotAgentSafe
		}
		// Authoring descriptors cannot change the registry schemas/effects.
		if instruction.Kind == "call" && (!bytes.Equal(instruction.Node.InputSchema, child.listing.InputSchema) || !bytes.Equal(instruction.Node.OutputSchema, child.listing.OutputSchema)) {
			return ErrNotAgentSafe
		}
		children[key] = child
		calls += child.calls
		tokens += child.tokens
		if calls > 10000 || tokens > 1<<30 {
			return ErrBudget
		}
		if child.maxDepth+1 > maxDepth {
			maxDepth = child.maxDepth + 1
		}
		if maxDepth > 64 {
			return ErrBudget
		}
		m.Effects = append(m.Effects, child.manifest.Effects...)
		m.Capabilities = append(m.Capabilities, child.manifest.Capabilities...)
		m.SecretRefs = append(m.SecretRefs, child.manifest.SecretRefs...)
		m.Deterministic = m.Deterministic && child.manifest.Deterministic
		i := contract.InternalInstruction{Index: len(program.Instructions), ID: instruction.ID, Kind: "call", Node: key}
		switch {
		case instruction.Input == "$input":
		case instruction.Input == "$literal" && len(instruction.Literal) > 0:
			literals[instruction.ID] = append([]byte(nil), instruction.Literal...)
		default:
			ref, err := reference(instruction.Input, seen)
			if err != nil {
				return err
			}
			i.References = []contract.Reference{ref}
		}
		program.Instructions = append(program.Instructions, i)
		seen[instruction.ID] = true
	}
	ref, err := reference(p.Output, seen)
	if err != nil {
		return err
	}
	program.Instructions = append(program.Instructions, contract.InternalInstruction{ID: "output", Kind: "output", References: []contract.Reference{ref}})
	m.Effects, m.Capabilities, m.SecretRefs = unique(m.Effects), unique(m.Capabilities), unique(m.SecretRefs)
	b, err := prepare(p.Spec.Name, p.Spec.Version, "composed workflow tool", inputSchema, outputSchema, m, metadata)
	if err != nil {
		return err
	}
	b.program, b.children, b.literals = &program, children, literals
	b.maxDepth, b.calls, b.tokens = maxDepth, calls, tokens
	b.resources = tool.Resources{TokenLimit: tokens}
	b.listing.Resources = b.resources
	// Include exact program and transitive child identities in the gate digest.
	raw, _ := json.Marshal(struct {
		Program  flow.Program
		Children map[string]string
	}{p, childDigests(children)})
	b.listing.ArtifactDigest = hash(append([]byte(b.listing.ArtifactDigest), raw...))
	return c.register(b)
}

func reference(source string, seen map[string]bool) (contract.Reference, error) {
	source = strings.Replace(source, "$child.", "$step.", 1)
	if !strings.HasPrefix(source, "$step.") {
		return contract.Reference{}, ErrNotAgentSafe
	}
	parts := strings.Split(strings.TrimPrefix(source, "$step."), ".")
	if !seen[parts[0]] {
		return contract.Reference{}, ErrNotAgentSafe
	}
	return contract.Reference{Step: parts[0], Path: parts[1:]}, nil
}

func prepare(name, version, description string, input, output []byte, m Manifest, metadata tool.Metadata) (binding, error) {
	if err := metadata.Validate(); err != nil {
		return binding{}, err
	}
	if !regexp.MustCompile(`^[a-z][a-z0-9_/-]{0,127}$`).MatchString(name) || strings.Contains(name, "..") || !regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`).MatchString(version) {
		return binding{}, ErrNotAgentSafe
	}
	in, err := schema.Parse(input)
	if err != nil {
		return binding{}, err
	}
	out, err := schema.Parse(output)
	if err != nil {
		return binding{}, err
	}
	l := Listing{Name: name, Version: version, Description: description, InputSchema: append([]byte(nil), input...), OutputSchema: append([]byte(nil), output...), Effects: unique(m.Effects), CapabilityDigest: hash([]byte(strings.Join(unique(m.Capabilities), "\n"))), Metadata: metadata}
	raw, _ := json.Marshal(struct {
		Listing  Listing
		Manifest Manifest
	}{l, m})
	l.ArtifactDigest = hash(raw)
	return binding{listing: l, manifest: cloneManifest(m), in: in, out: out}, nil
}

func (c *Catalog) register(b binding) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := b.listing.Name + "@" + b.listing.Version
	if _, ok := c.tools[key]; ok {
		return fmt.Errorf("agent: duplicate tool %s", key)
	}
	c.tools[key] = b
	return nil
}

func (c *Catalog) List(p Principal) []Listing {
	if c == nil || p.ID == "" || p.MaxDepth <= 0 {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := []Listing{}
	for _, b := range c.tools {
		if b.manifest.Validate() != nil || !p.Allows(b.manifest) {
			continue
		}
		l := b.listing
		l.InputSchema, l.OutputSchema, l.Effects = append([]byte(nil), l.InputSchema...), append([]byte(nil), l.OutputSchema...), append([]string(nil), l.Effects...)
		out = append(out, l)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name+"@"+out[i].Version < out[j].Name+"@"+out[j].Version })
	return out
}

type execution struct {
	principal      Principal
	budget         Budget
	calls, tokens  int
	workflow       string
	workflowDigest string
}
type executionKey struct{}

func (c *Catalog) Invoke(ctx context.Context, p Principal, name, version string, input []byte, budget Budget) ([]byte, error) {
	if c == nil || ctx == nil {
		return nil, ErrNotAgentSafe
	}
	// Recursive calls cannot reset budgets, principal or deadline. Workflow
	// children are dispatched internally using the same execution ledger.
	if ctx.Value(executionKey{}) != nil {
		return nil, ErrDenied
	}
	if err := budget.Validate(); err != nil {
		return nil, err
	}
	if p.MaxDepth <= 0 || budget.MaxDepth > p.MaxDepth {
		return nil, ErrBudget
	}
	c.mu.RLock()
	b, ok := c.tools[name+"@"+version]
	c.mu.RUnlock()
	if !ok {
		return nil, ErrNotAgentSafe
	}
	if p.ID == "" || !p.Allows(b.manifest) {
		return nil, ErrDenied
	}
	if b.maxDepth > budget.MaxDepth || b.calls > budget.MaxCalls || b.tokens > budget.MaxTokens {
		return nil, ErrBudget
	}
	ctx, cancel := context.WithDeadline(ctx, budget.Deadline)
	defer cancel()
	select {
	case c.active <- struct{}{}:
		defer func() { <-c.active }()
	default:
		return nil, ErrCapacity
	}
	s := &execution{principal: Principal{ID: p.ID, Capabilities: append([]string(nil), b.manifest.Capabilities...), MaxDepth: p.MaxDepth}, budget: budget, workflow: name + "@" + version, workflowDigest: b.listing.ArtifactDigest}
	ctx = context.WithValue(ctx, executionKey{}, s)
	return c.execute(ctx, b, input, s, 1)
}

func (c *Catalog) execute(ctx context.Context, b binding, input []byte, s *execution, depth int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if depth > s.budget.MaxDepth || len(input) > s.budget.MaxInputBytes {
		return nil, ErrBudget
	}
	if b.manifest.Validate() != nil {
		return nil, ErrNotAgentSafe
	}
	if !s.principal.Allows(b.manifest) {
		return nil, ErrDenied
	}
	normal, err := b.in.Normalize(input)
	if err != nil {
		return nil, err
	}
	if len(normal) > s.budget.MaxInputBytes {
		return nil, ErrBudget
	}
	a := tool.Admission{Name: b.listing.Name, Version: b.listing.Version, Principal: s.principal.ID, Workflow: s.workflow, WorkflowDigest: s.workflowDigest, ArtifactDigest: b.listing.ArtifactDigest, InputDigest: hash(normal), Input: append([]byte(nil), normal...), Effects: append([]string(nil), b.manifest.Effects...), Capabilities: append([]string(nil), b.manifest.Capabilities...), Budget: s.budget, Resources: b.resources}
	if c.gate != nil {
		if err := c.gate.Authorize(ctx, a); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var output []byte
	if b.program == nil {
		if s.calls >= s.budget.MaxCalls || b.resources.TokenLimit > s.budget.MaxTokens-s.tokens {
			return nil, ErrBudget
		}
		s.calls++
		s.tokens += b.resources.TokenLimit
		scope := Principal{ID: s.principal.ID, Capabilities: append([]string(nil), b.manifest.Capabilities...), MaxDepth: s.budget.MaxDepth - depth + 1}
		output, err = b.native(tool.WithScope(tool.WithTokenLimit(ctx, b.resources.TokenLimit), scope), normal)
	} else {
		nodes := map[string]node.Any{}
		// Unique engine keys per instruction preserve literals and versions.
		program := *b.program
		program.Instructions = append([]contract.InternalInstruction(nil), b.program.Instructions...)
		for index, instruction := range program.Instructions {
			if instruction.Kind != "call" {
				continue
			}
			child := b.children[instruction.Node]
			literal := b.literals[instruction.ID]
			key := instruction.ID
			program.Instructions[index].Node = key
			n := node.MustDefine[any, any]("agent/dispatch", "1.0.0", func(ctx context.Context, value any) (any, error) {
				raw, err := json.Marshal(value)
				if err != nil {
					return nil, err
				}
				if literal != nil {
					raw = literal
				}
				result, err := c.execute(ctx, child, raw, s, depth+1)
				if err != nil {
					return nil, err
				}
				return decode(result)
			}, node.Description("admitted tool dispatch"), node.Schemas([]byte(`{"type":"object"}`), []byte(`{"type":"object"}`)))
			nodes[key] = n.Any()
		}
		value, decodeErr := decode(normal)
		if decodeErr != nil {
			return nil, decodeErr
		}
		result, runErr := engine.New(nodes).WithMaxSteps(s.budget.MaxCalls+1).Run(ctx, program, value)
		if runErr != nil {
			return nil, runErr
		}
		output, err = json.Marshal(result.Output)
	}
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(output) > s.budget.MaxOutputBytes {
		return nil, ErrBudget
	}
	output, err = b.out.Normalize(output)
	if err != nil {
		return nil, err
	}
	if len(output) > s.budget.MaxOutputBytes {
		return nil, ErrBudget
	}
	if c.gate != nil {
		if err := c.gate.Publish(ctx, a, append([]byte(nil), output...)); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return append([]byte(nil), output...), nil
}

func decode(raw []byte) (any, error) {
	var v any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	err := d.Decode(&v)
	return v, err
}

// Native Go integer fields receive exact json.Number values after portable
// schema validation. Workflows and model payloads retain the wire form.
func nativeValue(s schema.Schema, value any) any {
	if value == nil {
		return nil
	}
	if len(s.AnyOf) > 0 {
		raw, err := json.Marshal(value)
		if err != nil {
			return value
		}
		for _, candidate := range s.AnyOf {
			if _, err := candidate.Normalize(raw); err == nil {
				return nativeValue(candidate, value)
			}
		}
	}
	if s.Type == "integer" && s.Wire == "int64-string" {
		if text, ok := value.(string); ok {
			return json.Number(text)
		}
	}
	switch x := value.(type) {
	case map[string]any:
		for key, child := range x {
			if property, ok := s.Properties[key]; ok {
				x[key] = nativeValue(property, child)
			}
		}
	case []any:
		if s.Items != nil {
			for i, child := range x {
				x[i] = nativeValue(*s.Items, child)
			}
		}
	}
	return value
}
func cloneManifest(m Manifest) Manifest {
	m.Effects = append([]string(nil), m.Effects...)
	m.Capabilities = append([]string(nil), m.Capabilities...)
	m.SecretRefs = append([]string(nil), m.SecretRefs...)
	return m
}
func unique(values []string) []string {
	set := map[string]bool{}
	for _, v := range values {
		set[v] = true
	}
	out := []string{}
	for v := range set {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}
func equalSet(a, b []string) bool {
	return strings.Join(unique(a), "\n") == strings.Join(unique(b), "\n")
}
func hash(raw []byte) string { h := sha256.Sum256(raw); return "sha256:" + hex.EncodeToString(h[:]) }
func childDigests(children map[string]binding) map[string]string {
	out := map[string]string{}
	for key, b := range children {
		out[key] = b.listing.ArtifactDigest
	}
	return out
}
