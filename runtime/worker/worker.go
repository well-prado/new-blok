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
	runtime "github.com/well-prado/new-blok/internal/runtime"
	"github.com/well-prado/new-blok/node"
	"sync/atomic"
	"time"
)

type Config = runtime.Config
type GRPCFactory = runtime.GRPCFactory
type ProcessFactory = runtime.ProcessFactory
type Supervisor = runtime.Supervisor

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
	opts := []node.Option{node.Description(descriptor.Description), node.Schemas(descriptor.InputSchema, descriptor.OutputSchema)}
	if descriptor.Deterministic {
		opts = append(opts, node.Pure())
	} else {
		opts = append(opts, node.Effects(descriptor.Effects...))
	}
	return node.Define(descriptor.Name, descriptor.Version, func(ctx context.Context, input I) (O, error) {
		var zero O
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
		result, err := supervisor.Call(ctx, contract.Call{CallID: id.CallID, AttemptID: id.AttemptID, IdempotencyKey: id.OperationKey, Generation: ready.Generation, Node: descriptor.Name, NodeVersion: descriptor.Version, Deadline: deadline, Input: payload})
		if err != nil {
			if len(descriptor.Effects) > 0 {
				return zero, &node.DomainError{Class: "uncertain", Code: "worker_transport", Uncertain: true}
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
			return zero, &node.DomainError{Class: string(e.Class), Code: e.Code, Retryable: e.SafeToRetry(), Uncertain: e.Class == contract.ErrorUncertain}
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
