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

	"github.com/well-prado/new-blok/agent/internal/catalogprogram"
	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/contract/tool"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/observe/redact"
)

type Manifest = tool.Manifest
type Budget = tool.Budget
type Principal = tool.Principal

var ErrInvalidManifest = tool.ErrInvalidManifest
var ErrBudget = tool.ErrBudget
var ErrNotAgentSafe = errors.New("agent: tool is not agent-safe")
var ErrDenied = errors.New("agent: capability denied")
var ErrCapacity = errors.New("agent: active invocation capacity exceeded")

// ErrSensitiveListing refuses a tool whose model-visible listing (its
// description, schema literals such as defaults, or its
// reviewed references) carries a credential-shaped value, encoded ones
// included (ADR 0021). Secrets reach a tool only as opaque reference names.
var ErrSensitiveListing = errors.New("agent: catalog listing carries a credential-shaped value")

type Listing struct {
	Name, Version, Description       string
	InputSchema, OutputSchema        json.RawMessage
	Effects                          []string
	CapabilityDigest, ArtifactDigest string
	Metadata                         tool.Metadata
	Resources                        tool.Resources
}

type binding struct {
	listing   Listing
	manifest  Manifest
	in, out   schema.Schema
	resources tool.Resources
	native    func(context.Context, []byte) ([]byte, error)
	// program holds a workflow tool's lowered program and its literals. It
	// runs only through catalog dispatch: package agent cannot name the raw
	// program, so it cannot run, observe or journal it without the literal
	// substitution (#260).
	program                 *catalogprogram.Program
	children                map[string]binding
	steps                   map[string]binding
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
	// One lowering for developer- and agent-authored workflows (#249): the
	// same rules as flow.Lower, plus the catalog's two extensions — a
	// literal call input, which dispatch substitutes, and a child workflow
	// call.
	instructions := make([]catalogprogram.Instruction, len(p.Instructions))
	for index, instruction := range p.Instructions {
		instructions[index] = catalogprogram.Instruction{Kind: instruction.Kind, ID: instruction.ID, Node: instruction.Node.Name, Input: instruction.Input, Literal: instruction.Literal}
		if instruction.Kind == "child" {
			instructions[index].Node, _ = instruction.Data["workflow"].(string)
		}
	}
	program, err := catalogprogram.Lower(p.Spec.Name, p.Spec.Version, instructions, p.Output)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrNotAgentSafe, err)
	}
	children := map[string]binding{}
	steps := map[string]binding{}
	m := cloneManifest(parent)
	c.mu.RLock()
	registered := make(map[string]binding, len(c.tools))
	for key, child := range c.tools {
		registered[key] = child
	}
	c.mu.RUnlock()
	literals := program.Literals()
	maxDepth, calls, tokens := 1, 0, 0
	for _, instruction := range p.Instructions {
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
		if literal, ok := literals[instruction.ID]; ok {
			if err := checkLiteral(instruction.Kind, instruction.ID, key, child, literal); err != nil {
				return err
			}
		}
		children[key] = child
		steps[instruction.ID] = child
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
	}
	m.Effects, m.Capabilities, m.SecretRefs = unique(m.Effects), unique(m.Capabilities), unique(m.SecretRefs)
	b, err := prepare(p.Spec.Name, p.Spec.Version, "composed workflow tool", inputSchema, outputSchema, m, metadata)
	if err != nil {
		return err
	}
	b.program, b.children, b.steps = program, children, steps
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

// checkLiteral runs, at registration, the admission dispatch will run on a
// literal call input (#261): the input-size bound, then the tool's own
// Normalize. A literal no budget can admit — over the 1 MiB payload limit
// tool.Budget caps MaxInputBytes at, before or after normalization — is
// ErrBudget; a literal the tool's schema refuses is ErrNotAgentSafe. Both
// name the step. The check only validates: the catalog keeps the literal as
// flow recorded it, and dispatch normalizes it exactly as it did before.
func checkLiteral(kind, id, key string, child binding, literal []byte) error {
	if len(literal) > schema.MaxPayloadBytes {
		return fmt.Errorf("%w: %s %q: literal input is %d bytes, over the %d-byte input limit", ErrBudget, kind, id, len(literal), schema.MaxPayloadBytes)
	}
	normal, err := child.in.Normalize(literal)
	if err != nil {
		return fmt.Errorf("%w: %s %q: literal input does not satisfy the input schema of %s: %w", ErrNotAgentSafe, kind, id, key, err)
	}
	if len(normal) > schema.MaxPayloadBytes {
		return fmt.Errorf("%w: %s %q: literal input normalizes to %d bytes, over the %d-byte input limit", ErrBudget, kind, id, len(normal), schema.MaxPayloadBytes)
	}
	return nil
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
	if sensitiveListing(description, input, output, metadata) {
		return binding{}, ErrSensitiveListing
	}
	l := Listing{Name: name, Version: version, Description: description, InputSchema: append([]byte(nil), input...), OutputSchema: append([]byte(nil), output...), Effects: unique(m.Effects), CapabilityDigest: hash([]byte(strings.Join(unique(m.Capabilities), "\n"))), Metadata: metadata}
	raw, _ := json.Marshal(struct {
		Listing  Listing
		Manifest Manifest
	}{l, m})
	l.ArtifactDigest = hash(raw)
	return binding{listing: l, manifest: cloneManifest(m), in: in, out: out}, nil
}

// sensitiveListing is the catalog's redaction enforcement point: everything
// a model can read in a listing is checked once, at registration.
func sensitiveListing(description string, input, output []byte, metadata tool.Metadata) bool {
	for _, text := range []string{description, metadata.Source, metadata.Example, metadata.Test} {
		if redact.Sensitive(text) {
			return true
		}
	}
	for _, raw := range [][]byte{input, output} {
		value, err := decode(raw)
		if err != nil || redact.HasSensitiveValue(value) {
			return true
		}
	}
	return false
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
		value, decodeErr := decode(normal)
		if decodeErr != nil {
			return nil, decodeErr
		}
		// The program runs only here, through catalog dispatch, which
		// substitutes each literal (#260).
		result, runErr := b.program.Run(ctx, value, s.budget.MaxCalls+1,
			func(id string) []string { return b.steps[id].manifest.Effects },
			func(ctx context.Context, id string, input []byte) (any, error) {
				result, err := c.execute(ctx, b.steps[id], input, s, depth+1)
				if err != nil {
					return nil, err
				}
				return decode(result)
			})
		if runErr != nil {
			return nil, runErr
		}
		output, err = json.Marshal(result)
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
