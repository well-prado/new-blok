// Package worker integrates a selected persistent foreign runtime through
// typed nodes. Applications own the worker selection and lifecycle.
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	contract "github.com/well-prado/new-blok/contract/runtime"
	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/contract/tool"
	runtime "github.com/well-prado/new-blok/internal/runtime"
	"github.com/well-prado/new-blok/node"
	"strings"
	"sync/atomic"
	"time"
)

type Config = runtime.Config
type GRPCFactory = runtime.GRPCFactory
type ProcessFactory = runtime.ProcessFactory
type Supervisor = runtime.Supervisor
type TransportPrincipal = runtime.Principal
type Credential = runtime.Credential
type TokenAuthenticator = runtime.TokenAuthenticator
type AuthenticatedSession = runtime.AuthenticatedSession
type BlobStore = runtime.BlobStore

var NewTokenAuthenticator = runtime.NewTokenAuthenticator
var NewBlobStore = runtime.NewBlobStore
var BlobHandler = runtime.BlobHandler

var New = runtime.New
var ErrCapacity = runtime.ErrCapacity

type identityKey struct{}
type Identity struct{ CallID, AttemptID, OperationKey string }

// WithIdentity binds a durable attempt supplied by the native engine. Worker
// retries use a new attempt ID but keep the same OperationKey.
func WithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, identityKey{}, id)
}

var sequence atomic.Uint64

func Define[I, O any](supervisor *Supervisor, descriptor node.Descriptor) (node.Definition[I, O], error) {
	caps := make([]contract.Capability, len(descriptor.RequiredCapabilities))
	for i, c := range descriptor.RequiredCapabilities {
		caps[i] = contract.Capability(c)
	}
	return DefineScoped[I, O](supervisor, descriptor, caps)
}

// DefineScoped binds narrow application-reviewed capabilities to this node's
// worker calls. They are not read from payload data. The authenticated
// connection still verifies they are a subset of the negotiated grant.
func DefineScoped[I, O any](supervisor *Supervisor, descriptor node.Descriptor, capabilities []contract.Capability) (node.Definition[I, O], error) {
	capabilities = append([]contract.Capability(nil), capabilities...)
	if supervisor == nil {
		return node.Definition[I, O]{}, errors.New("worker supervisor required")
	}
	in, err := schema.Parse(descriptor.InputSchema)
	if err != nil {
		return node.Definition[I, O]{}, err
	}
	out, err := schema.Parse(descriptor.OutputSchema)
	if err != nil {
		return node.Definition[I, O]{}, err
	}
	required := make([]string, len(capabilities))
	for i, c := range capabilities {
		required[i] = string(c)
	}
	for _, want := range descriptor.RequiredCapabilities {
		found := false
		for _, have := range required {
			if want == have {
				found = true
				break
			}
		}
		if !found {
			return node.Definition[I, O]{}, contract.ErrCapabilityDenied
		}
	}
	opts := []node.Option{node.Description(descriptor.Description), node.Schemas(descriptor.InputSchema, descriptor.OutputSchema), node.RequiredCapabilities(required...), node.RemoteBoundary()}
	if descriptor.Deterministic {
		opts = append(opts, node.Pure())
	} else {
		opts = append(opts, node.Effects(descriptor.Effects...))
	}
	return node.Define(descriptor.Name, descriptor.Version, func(ctx context.Context, input I) (O, error) {
		var zero O
		if tool.TokenLimit(ctx) > 0 {
			return zero, tool.ErrBudget
		}
		if scope, ok := tool.Scope(ctx); ok {
			if !scope.Allows(tool.Manifest{Capabilities: required}) {
				return zero, contract.ErrCapabilityDenied
			}
		}
		raw, err := json.Marshal(input)
		if err != nil {
			return zero, err
		}
		payload, err := in.Normalize(raw)
		if err != nil {
			return zero, err
		}
		ready, ok := supervisor.Ready()
		if !ok {
			return zero, runtime.ErrNotStarted
		}
		id, ok := ctx.Value(identityKey{}).(Identity)
		if !ok {
			n := sequence.Add(1)
			id = Identity{CallID: fmt.Sprintf("call-%d-%d", time.Now().UnixNano(), n), AttemptID: fmt.Sprintf("attempt-%d-%d", time.Now().UnixNano(), n)}
		}
		deadline, ok := ctx.Deadline()
		if !ok {
			deadline = time.Now().Add(30 * time.Second)
		}
		result, err := supervisor.Call(ctx, contract.Call{CallID: id.CallID, AttemptID: id.AttemptID, IdempotencyKey: id.OperationKey, Generation: ready.Generation, Node: descriptor.Name, NodeVersion: descriptor.Version, Deadline: deadline, Input: payload, Capabilities: capabilities})
		if err != nil {
			if len(descriptor.Effects) > 0 {
				return zero, transportFailure(ctx, err)
			}
			return zero, err
		}
		if result.Error != nil {
			e := result.Error
			if e.Class == contract.ErrorCanceled {
				return zero, context.Canceled
			}
			if e.Class == contract.ErrorDeadline {
				return zero, context.DeadlineExceeded
			}
			return zero, &node.DomainError{Class: strings.ToLower(string(e.Class)), Code: e.Code, Retryable: e.SafeToRetry(), Uncertain: e.Class == contract.ErrorUncertain}
		}
		normalized, err := out.Normalize(result.Output)
		if err != nil {
			return zero, err
		}
		native, err := nativeJSON(out, normalized)
		if err != nil {
			return zero, err
		}
		var output O
		if err := json.Unmarshal(native, &output); err != nil {
			return zero, err
		}
		return output, nil
	}, opts...)
}
func transportFailure(ctx context.Context, err error) *node.DomainError {
	failure := &node.DomainError{Class: "uncertain", Code: "worker_transport", Uncertain: true}
	if errors.Is(err, context.Canceled) {
		failure.Err = context.Canceled
	} else if errors.Is(err, context.DeadlineExceeded) {
		// Validation can observe the absolute deadline before the context
		// timer is scheduled. Preserve only the known, non-sensitive cause.
		failure.Err = context.DeadlineExceeded
	} else if ctx.Err() != nil {
		failure.Err = ctx.Err()
	}
	return failure
}
func nativeJSON(s schema.Schema, raw []byte) ([]byte, error) {
	var v any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return json.Marshal(nativeValue(s, v))
}
func nativeValue(s schema.Schema, v any) any {
	if v == nil {
		return nil
	}
	if len(s.AnyOf) > 0 {
		for _, candidate := range s.AnyOf {
			if _, err := candidate.NormalizeValue(v); err == nil {
				return nativeValue(candidate, v)
			}
		}
		return v // Normalize has already required exactly one matching branch.
	}
	if s.Type == "integer" && s.Wire == "int64-string" {
		if str, ok := v.(string); ok {
			return json.Number(str)
		}
	}
	switch x := v.(type) {
	case map[string]any:
		for key, value := range x {
			if child, ok := s.Properties[key]; ok {
				x[key] = nativeValue(child, value)
			}
		}
	case []any:
		if s.Items != nil {
			for i, value := range x {
				x[i] = nativeValue(*s.Items, value)
			}
		}
	}
	return v
}
