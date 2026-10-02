package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/provider"
)

// EffectNode carries provider metadata without widening node.Descriptor's contract.
// Register Any(), and pass Definition to flow.Call. Manifest returns an owned copy.
type EffectNode[I provider.Keyed, O any] struct {
	node.Definition[I, O]
	manifest provider.Manifest
}

func (n EffectNode[I, O]) Manifest() provider.Manifest {
	m := n.manifest
	m.Capabilities = append([]string(nil), m.Capabilities...)
	m.SecretRefs = append([]string(nil), m.SecretRefs...)
	return m
}

func effect[I provider.Keyed, O any](name, capability string, p provider.Port[I, O], m provider.Manifest, input, output []byte) (EffectNode[I, O], error) {
	if provider.Missing(p) {
		return EffectNode[I, O]{}, errors.New("catalog: missing provider")
	}
	if err := m.Validate(); err != nil {
		return EffectNode[I, O]{}, err
	}
	if len(m.Capabilities) != 1 || m.Capabilities[0] != capability {
		return EffectNode[I, O]{}, errors.New("catalog: effect requires its narrow capability")
	}
	if m.MaxResponseBytes == 0 {
		m.MaxResponseBytes = 1 << 20
	}
	if m.Timeout == 0 {
		m.Timeout = 30 * time.Second
	}
	m.Capabilities = append([]string(nil), m.Capabilities...)
	m.SecretRefs = append([]string(nil), m.SecretRefs...)
	inSchema, err := schema.Parse(input)
	if err != nil {
		return EffectNode[I, O]{}, err
	}
	outSchema, err := schema.Parse(output)
	if err != nil {
		return EffectNode[I, O]{}, err
	}
	definition, err := node.Define(name, "1.0.0", func(ctx context.Context, in I) (result O, returned error) {
		var zero O
		key := in.EffectKey()
		defer func() {
			if recover() != nil {
				result = zero
				returned = safeError(provider.Uncertain, "provider_panic", key)
			}
		}()
		raw, err := json.Marshal(in)
		if err != nil || key == "" || len(key) > 256 || strings.ContainsAny(key, "\r\n") || len(raw) > m.MaxRequestBytes {
			return zero, safeError(provider.Invalid, "invalid_input", key)
		}
		if _, err := inSchema.Normalize(raw); err != nil {
			return zero, safeError(provider.Invalid, "invalid_input", key)
		}
		if !validEffectInput(in) {
			return zero, safeError(provider.Invalid, "invalid_input", key)
		}
		ctx, cancel := context.WithTimeout(ctx, m.Timeout)
		defer cancel()
		out, err := p.Execute(ctx, in)
		if err != nil {
			class := provider.Uncertain
			var classified *provider.Error
			if errors.As(err, &classified) {
				switch classified.Class {
				case provider.Invalid, provider.Business, provider.Transient, provider.Uncertain:
					class = classified.Class
				}
			}
			return zero, safeError(class, "effect_failed", key)
		}
		// Cancellation after provider success cannot establish a committed result.
		if ctx.Err() != nil {
			return zero, safeError(provider.Uncertain, "deadline_after_dispatch", key)
		}
		raw, err = json.Marshal(out)
		if err != nil || len(raw) > m.MaxResponseBytes {
			return zero, safeError(provider.Uncertain, "invalid_output", key)
		}
		if _, err := outSchema.Normalize(raw); err != nil {
			return zero, safeError(provider.Uncertain, "invalid_output", key)
		}
		if receipt, ok := any(out).(provider.Receipt); ok && receipt.ID == "" {
			return zero, safeError(provider.Uncertain, "invalid_output", key)
		}
		return out, nil
	}, node.Description("Injected "+capability+" effect"), node.Schemas(input, output), node.Effects(capability))
	return EffectNode[I, O]{Definition: definition, manifest: m}, err
}

func validEffectInput(input any) bool {
	switch in := input.(type) {
	case provider.EmailInput:
		return in.To != "" && in.Subject != "" && in.Text != ""
	case provider.PaymentInput:
		return in.Account != "" && len(in.Currency) == 3
	case provider.DatabaseInput:
		return in.RecordID != ""
	case provider.PublishInput:
		return in.Topic != ""
	case provider.AuditInput:
		return in.Action != "" && in.Subject != ""
	case provider.GenerateInput:
		return in.Prompt != ""
	}
	return true
}
func safeError(class provider.ErrorClass, code, key string) error {
	return &node.DomainError{Code: code, Class: string(class), Retryable: class == provider.Transient, Uncertain: class == provider.Uncertain, Err: &provider.Error{Class: class, Code: code, IdempotencyKey: key}}
}
func object(properties map[string]schema.Schema) []byte {
	required := make([]string, 0, len(properties))
	for key := range properties {
		required = append(required, key)
	}
	// JSON schemas must be deterministic across builds.
	sort.Strings(required)
	additional := false
	raw, _ := json.Marshal(schema.Schema{Type: "object", Properties: properties, Required: required, AdditionalProperties: &additional})
	return raw
}
func fields(names ...string) map[string]schema.Schema {
	p := map[string]schema.Schema{}
	for _, n := range names {
		p[n] = schema.Schema{Type: "string"}
	}
	return p
}

var receiptSchema = object(fields("id"))

func HTTPRequest(p provider.Port[provider.HTTPInput, provider.HTTPOutput], m provider.Manifest) (EffectNode[provider.HTTPInput, provider.HTTPOutput], error) {
	out := fields("body")
	min, max := int64(200), int64(299)
	out["status"] = schema.Schema{Type: "integer", Minimum: &min, Maximum: &max}
	return effect("catalog/http-request", "http:request", p, m, object(fields("key", "body")), object(out))
}
func Email(p provider.Port[provider.EmailInput, provider.Receipt], m provider.Manifest) (EffectNode[provider.EmailInput, provider.Receipt], error) {
	return effect("catalog/email", "email:send", p, m, object(fields("key", "to", "subject", "text")), receiptSchema)
}
func Payment(p provider.Port[provider.PaymentInput, provider.Receipt], m provider.Manifest) (EffectNode[provider.PaymentInput, provider.Receipt], error) {
	in := fields("key", "account", "currency")
	min := int64(1)
	in["amountCents"] = schema.Schema{Type: "integer", Minimum: &min}
	return effect("catalog/payment", "payment:charge", p, m, object(in), receiptSchema)
}
func Database(p provider.Port[provider.DatabaseInput, provider.DatabaseOutput], m provider.Manifest) (EffectNode[provider.DatabaseInput, provider.DatabaseOutput], error) {
	return effect("catalog/database", "database:write-outbox", p, m, object(fields("key", "recordId", "value")), object(fields("recordId", "eventId")))
}
func Publish(p provider.Port[provider.PublishInput, provider.Receipt], m provider.Manifest) (EffectNode[provider.PublishInput, provider.Receipt], error) {
	return effect("catalog/publish", "message:publish", p, m, object(fields("key", "topic", "payload")), receiptSchema)
}
func Audit(p provider.Port[provider.AuditInput, provider.Receipt], m provider.Manifest) (EffectNode[provider.AuditInput, provider.Receipt], error) {
	return effect("catalog/audit", "audit:append", p, m, object(fields("key", "action", "subject")), receiptSchema)
}

// Output schema is fixed by composition; model input cannot relax it.
func Generate(p provider.Port[provider.GenerateInput, provider.GenerateOutput], m provider.Manifest, valueSchema []byte) (EffectNode[provider.GenerateInput, provider.GenerateOutput], error) {
	value, err := schema.Parse(valueSchema)
	if err != nil {
		return EffectNode[provider.GenerateInput, provider.GenerateOutput]{}, err
	}
	return effect("catalog/generate", "model:generate", p, m, object(fields("key", "prompt")), object(map[string]schema.Schema{"value": value}))
}
