// Package worker integrates a selected persistent foreign runtime through
// typed nodes. Applications own the worker selection and lifecycle.
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/well-prado/new-blok/contract/observe"
	contract "github.com/well-prado/new-blok/contract/runtime"
	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/contract/tool"
	runtime "github.com/well-prado/new-blok/internal/runtime"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/observe/redact"
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
		call := contract.Call{CallID: id.CallID, AttemptID: id.AttemptID, IdempotencyKey: id.OperationKey, Generation: ready.Generation, Node: descriptor.Name, NodeVersion: descriptor.Version, Deadline: deadline, Input: payload, Capabilities: capabilities, OnLog: func(entry contract.Log) { emitWorkerLog(ctx, entry) }}
		propagateTrace(ctx, &call, ready.Limits.MaxFrameBytes)
		result, err := supervisor.Call(ctx, call)
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

// propagateTrace hands the dispatching step's trace context to the worker
// (ADR 0020). Trace context is optional correlation: when adding it would push
// an otherwise admissible call over the negotiated frame ceiling it is
// omitted, so tracing never changes whether a call is dispatched.
func propagateTrace(ctx context.Context, call *contract.Call, maxFrameBytes int) {
	trace, ok := observe.TraceFrom(ctx)
	if !ok {
		return
	}
	traced := *call
	traced.Traceparent, traced.Tracestate = trace.Traceparent(), trace.State
	if contract.EncodedCallBytes(traced) > maxFrameBytes && contract.EncodedCallBytes(*call) <= maxFrameBytes {
		return
	}
	call.Traceparent, call.Tracestate = traced.Traceparent, traced.Tracestate
}

func emitWorkerLog(ctx context.Context, entry contract.Log) {
	level, ok := map[string]slog.Level{"DEBUG": slog.LevelDebug, "INFO": slog.LevelInfo, "WARN": slog.LevelWarn, "ERROR": slog.LevelError}[entry.Level]
	if !ok || len(entry.Message) > 1024 || len(entry.Attrs) > 4096 {
		return
	}
	attrs := make(map[string]any)
	if len(entry.Attrs) != 0 {
		if !json.Valid(entry.Attrs) {
			return
		}
		decoder := json.NewDecoder(bytes.NewReader(entry.Attrs))
		decoder.UseNumber()
		if decoder.Decode(&attrs) != nil || attrs == nil {
			return
		}
		var extra any
		if decoder.Decode(&extra) == nil {
			return
		}
	}
	keys := make([]string, 0, len(attrs))
	for key := range attrs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	fields := make([]slog.Attr, 0, min(len(keys), 32))
	count := 0
	for _, key := range keys {
		if count >= 32 {
			break
		}
		if !validLogAttrKey(key) {
			continue
		}
		value := attrs[key]
		if sensitiveLogKey(key) {
			value = redact.Marker
		} else {
			switch typed := value.(type) {
			case string:
				value = redact.String(typed)
			case nil, bool, json.Number, float64:
			default:
				continue
			}
		}
		fields = append(fields, slog.Any(key, value))
		count++
	}
	node.Logger(ctx).LogAttrs(ctx, level, safeLogMessage(entry.Message), fields...)
}

func validLogAttrKey(key string) bool {
	if len(key) == 0 || len(key) > 64 || key[0] < 'A' || key[0] > 'Z' && key[0] < 'a' || key[0] > 'z' {
		return false
	}
	for i := 1; i < len(key); i++ {
		c := key[i]
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '.' || c == '-') {
			return false
		}
	}
	return true
}

// sensitiveLogKey and safeLogMessage are the worker log enforcement points;
// they use the framework's single redaction boundary (ADR 0021).
func sensitiveLogKey(key string) bool { return redact.Key(key) }

func safeLogMessage(message string) string { return redact.Message(message) }

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
