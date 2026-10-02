// Package loopback is a third-party trigger adapter written outside the New
// Blok module. It exposes an in-process call protocol and depends only on the
// public trigger and schema contracts.
package loopback

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/trigger"
)

var Declaration = trigger.Declaration{Kind: trigger.MCP, Adapter: "example.test/extension/loopback", Completion: trigger.Memory, Disconnect: trigger.CancelWork, Authentication: trigger.Caller}

type Handler func(context.Context, json.RawMessage, trigger.Principal) (json.RawMessage, error)

type Authenticator func(credential string) (trigger.Principal, error)

// CallError carries only a stable code to the caller.
type CallError struct{ Code string }

func (e *CallError) Error() string { return e.Code }

type Adapter struct {
	input        schema.Schema
	authenticate Authenticator
	handle       Handler
	mu           sync.Mutex
	started      bool
}

func New(inputSchema []byte, authenticate Authenticator, handle Handler) (*Adapter, error) {
	parsed, err := schema.Parse(inputSchema)
	if err != nil {
		return nil, err
	}
	if authenticate == nil || handle == nil {
		return nil, errors.New("loopback: authenticator and handler are required")
	}
	return &Adapter{input: parsed, authenticate: authenticate, handle: handle}, nil
}

func (a *Adapter) Start() { a.mu.Lock(); a.started = true; a.mu.Unlock() }
func (a *Adapter) Stop()  { a.mu.Lock(); a.started = false; a.mu.Unlock() }

// Call runs one invocation under the caller's context, so a caller that goes
// away cancels the work.
func (a *Adapter) Call(ctx context.Context, credential string, payload json.RawMessage) (json.RawMessage, error) {
	a.mu.Lock()
	started := a.started
	a.mu.Unlock()
	if !started {
		return nil, &CallError{Code: "unavailable"}
	}
	principal, err := a.authenticate(credential)
	if err != nil {
		return nil, &CallError{Code: "unauthorized"}
	}
	candidate := payload
	if len(candidate) == 0 {
		candidate = json.RawMessage("null")
	}
	if _, err := a.input.Normalize(candidate); err != nil {
		return nil, &CallError{Code: "invalid_input"}
	}
	output, err := a.handle(ctx, payload, principal)
	if err == nil {
		return output, nil
	}
	if errors.Is(err, trigger.ErrSaturated) {
		return nil, &CallError{Code: "saturated"}
	}
	if code, _, ok := trigger.Classify(err); ok {
		return nil, &CallError{Code: code}
	}
	return nil, &CallError{Code: "internal"}
}
