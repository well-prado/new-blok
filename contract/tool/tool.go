// Package tool owns the versioned policy and admission adapter contract.
// Values here must be supplied by trusted application composition, never JSON
// proposed by a model. A manifest declares policy; it does not sandbox code.
package tool

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"time"
)

var ErrInvalidManifest = errors.New("agent: invalid capability manifest")
var ErrBudget = errors.New("agent: execution budget exceeded")

type Manifest struct {
	Version                           int
	Compatibility                     string
	Effects, Capabilities, SecretRefs []string
	Deterministic                     bool
}

var scopeName = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_./:-]{0,127}$`)

func (m Manifest) Validate() error {
	if m.Version != 1 || m.Compatibility != "agent-compatible" {
		return ErrInvalidManifest
	}
	for _, list := range [][]string{m.Effects, m.Capabilities, m.SecretRefs} {
		if len(list) > 64 {
			return ErrInvalidManifest
		}
		seen := map[string]bool{}
		for _, value := range list {
			if !scopeName.MatchString(value) || seen[value] {
				return ErrInvalidManifest
			}
			seen[value] = true
		}
	}
	if m.Deterministic && len(m.Effects) > 0 {
		return ErrInvalidManifest
	}
	return nil
}

type Budget struct {
	MaxDepth, MaxInputBytes, MaxOutputBytes, MaxTokens, MaxCalls int
	Deadline                                                     time.Time
}

func (b Budget) Validate() error {
	if b.MaxDepth <= 0 || b.MaxDepth > 64 || b.MaxInputBytes <= 0 || b.MaxInputBytes > 1<<20 || b.MaxOutputBytes <= 0 || b.MaxOutputBytes > 1<<20 || b.MaxTokens <= 0 || b.MaxCalls <= 0 || b.MaxCalls > 10000 || !b.Deadline.After(time.Now()) {
		return ErrBudget
	}
	return nil
}

type Principal struct {
	ID           string
	Capabilities []string
	MaxDepth     int
}

func (p Principal) Allows(m Manifest) bool {
	for _, required := range m.Capabilities {
		found := false
		for _, granted := range p.Capabilities {
			if required == granted {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// Metadata contains reviewed public references, not source text or secrets.
type Metadata struct{ Source, Example, Test string }

func (m Metadata) Validate() error {
	for _, value := range []string{m.Source, m.Example, m.Test} {
		if value == "" || len(value) > 512 || strings.HasPrefix(value, "/") || strings.Contains(value, "..") || !regexp.MustCompile(`^[A-Za-z0-9_./-]+$`).MatchString(value) {
			return ErrInvalidManifest
		}
	}
	return nil
}

// Admission is constructed from registered policy and normalized input, not
// caller-supplied claims. Approval adapters (#75) own durable decisions.
type Admission struct {
	Name, Version, Workflow, ArtifactDigest, InputDigest string
	Principal, WorkflowDigest                            string
	Effects, Capabilities                                []string
	Input                                                []byte
	Budget                                               Budget
}

type scopeKey struct{}

// WithScope is for trusted admission adapters. It does not authenticate a user.
func WithScope(ctx context.Context, p Principal) context.Context {
	p.Capabilities = append([]string(nil), p.Capabilities...)
	return context.WithValue(ctx, scopeKey{}, p)
}

// Scope returns the narrowed trusted invocation scope for injected providers.
func Scope(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(scopeKey{}).(Principal)
	p.Capabilities = append([]string(nil), p.Capabilities...)
	return p, ok
}

// Gate runs before every dispatch and before returning the final result.
// Applications requiring approval must inject their contract/approval adapter.
type Gate interface {
	Authorize(context.Context, Admission) error
	Publish(context.Context, Admission, []byte) error
}

// TokenLimit is a trusted worst-case reservation, charged before each dispatch.
// Providers/workers must enforce it at their injected boundary. A token-using
// handler without such a boundary must not be registered as agent-compatible.
type Resources struct{ TokenLimit int }

type limitKey struct{}

func WithTokenLimit(ctx context.Context, limit int) context.Context {
	return context.WithValue(ctx, limitKey{}, limit)
}
func TokenLimit(ctx context.Context) int { v, _ := ctx.Value(limitKey{}).(int); return v }
