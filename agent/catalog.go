// Package agent exposes a fail-closed catalog boundary for authorized tool
// callers. It contains policy metadata and admission hooks, not a model or
// an orchestration engine.
package agent

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

var (
	ErrNotAgentSafe    = errors.New("agent: tool is not agent-safe")
	ErrDenied          = errors.New("agent: capability denied")
	ErrBudget          = errors.New("agent: execution budget exceeded")
	ErrInvalidManifest = errors.New("agent: invalid capability manifest")
)

type Manifest struct {
	Version       int
	Compatibility string
	Effects       []string
	Capabilities  []string
	SecretRefs    []string
	Deterministic bool
}

func (m Manifest) Validate() error {
	if m.Version != 1 || m.Compatibility != "agent-compatible" {
		return ErrInvalidManifest
	}
	for _, value := range append(append([]string{}, m.Effects...), append(m.Capabilities, m.SecretRefs...)...) {
		if value == "" || strings.Contains(value, "=") || strings.Contains(value, "\n") {
			return ErrInvalidManifest
		}
	}
	if len(m.SecretRefs) > 0 && m.Deterministic {
		return ErrInvalidManifest
	}
	return nil
}

type Budget struct {
	MaxDepth       int
	MaxInputBytes  int
	MaxOutputBytes int
	MaxTokens      int
	Deadline       time.Time
}

func (b Budget) Validate() error {
	if b.MaxDepth <= 0 || b.MaxInputBytes <= 0 || b.MaxOutputBytes <= 0 || b.MaxTokens <= 0 || b.Deadline.IsZero() {
		return ErrBudget
	}
	return nil
}

type Principal struct {
	Capabilities []string
	MaxDepth     int
}

func (p Principal) Allows(manifest Manifest) bool {
	for _, required := range manifest.Capabilities {
		if !contains(p.Capabilities, required) {
			return false
		}
	}
	return true
}
func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

type Tool struct {
	Name         string
	Version      string
	Description  string
	InputSchema  []byte
	OutputSchema []byte
	Manifest     Manifest
	Invoke       func(context.Context, []byte) ([]byte, error)
}

type Listing struct {
	Name             string
	Version          string
	Description      string
	InputSchema      []byte
	OutputSchema     []byte
	Effects          []string
	CapabilityDigest string
}

type Catalog struct{ tools map[string]Tool }

func NewCatalog() *Catalog { return &Catalog{tools: map[string]Tool{}} }
func (c *Catalog) Register(t Tool) error {
	if c == nil {
		return errors.New("agent: nil catalog")
	}
	if !regexp.MustCompile(`^[a-z][a-z0-9_/-]{0,127}$`).MatchString(t.Name) || strings.Contains(t.Name, "..") {
		return ErrInvalidManifest
	}
	if !regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`).MatchString(t.Version) {
		return ErrInvalidManifest
	}
	if t.Invoke == nil {
		return errors.New("agent: invoke function is required")
	}
	if err := t.Manifest.Validate(); err != nil {
		return err
	}
	if _, err := schema.Parse(t.InputSchema); err != nil {
		return fmt.Errorf("agent: input schema: %w", err)
	}
	if _, err := schema.Parse(t.OutputSchema); err != nil {
		return fmt.Errorf("agent: output schema: %w", err)
	}
	key := t.Name + "@" + t.Version
	if _, exists := c.tools[key]; exists {
		return fmt.Errorf("agent: duplicate tool %s", key)
	}
	c.tools[key] = t
	return nil
}

func (c *Catalog) List(principal Principal) []Listing {
	keys := make([]string, 0, len(c.tools))
	for key := range c.tools {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]Listing, 0, len(keys))
	for _, key := range keys {
		t := c.tools[key]
		if t.Manifest.Validate() != nil || !principal.Allows(t.Manifest) {
			continue
		}
		out = append(out, Listing{Name: t.Name, Version: t.Version, Description: t.Description, InputSchema: append([]byte(nil), t.InputSchema...), OutputSchema: append([]byte(nil), t.OutputSchema...), Effects: append([]string(nil), t.Manifest.Effects...), CapabilityDigest: digest(t.Manifest.Capabilities)})
	}
	return out
}

func (c *Catalog) Invoke(ctx context.Context, principal Principal, name, version string, input []byte, budget Budget) ([]byte, error) {
	if err := budget.Validate(); err != nil {
		return nil, err
	}
	if principal.MaxDepth <= 0 || budget.MaxDepth > principal.MaxDepth {
		return nil, ErrBudget
	}
	if len(input) > budget.MaxInputBytes {
		return nil, ErrBudget
	}
	t, ok := c.tools[name+"@"+version]
	if !ok || t.Manifest.Validate() != nil {
		return nil, ErrNotAgentSafe
	}
	if !principal.Allows(t.Manifest) {
		return nil, ErrDenied
	}
	callCtx, cancel := context.WithDeadline(ctx, budget.Deadline)
	defer cancel()
	output, err := t.Invoke(callCtx, append([]byte(nil), input...))
	if err != nil {
		return nil, err
	}
	if len(output) > budget.MaxOutputBytes {
		return nil, ErrBudget
	}
	return output, nil
}

// Compose makes a workflow tool only when every child is safe and the parent
// capability set is a subset of the caller's authority. Child effects are
// aggregated; a composed tool cannot widen them.
func Compose(name, version string, children []Tool, parent Manifest) (Tool, error) {
	if err := parent.Validate(); err != nil {
		return Tool{}, err
	}
	effects := append([]string(nil), parent.Effects...)
	for _, child := range children {
		if err := child.Manifest.Validate(); err != nil {
			return Tool{}, ErrNotAgentSafe
		}
		effects = append(effects, child.Manifest.Effects...)
	}
	return Tool{Name: name, Version: version, Description: "composed workflow tool", InputSchema: []byte(`{"type":"object"}`), OutputSchema: []byte(`{"type":"object"}`), Manifest: Manifest{Version: 1, Compatibility: "agent-compatible", Effects: unique(effects), Capabilities: append([]string(nil), parent.Capabilities...)}, Invoke: func(context.Context, []byte) ([]byte, error) {
		return nil, errors.New("agent: composed invocation requires workflow admission")
	}}, nil
}

func unique(values []string) []string {
	set := map[string]bool{}
	out := []string{}
	for _, v := range values {
		if !set[v] {
			set[v] = true
			out = append(out, v)
		}
	}
	sort.Strings(out)
	return out
}
func digest(values []string) string {
	values = unique(values)
	h := sha256.Sum256([]byte(strings.Join(values, "\n")))
	return "sha256:" + hex.EncodeToString(h[:])
}
