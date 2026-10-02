// Package trigger defines the contract every protocol adapter shares.
//
// An adapter translates authenticated protocol input into a workflow's domain
// input and hands it to an application-supplied handler. It never interprets a
// workflow program, never retries a failed workflow invocation and never
// establishes a principal from caller data. Each adapter package publishes a
// Declaration stating how work completes and what a lost caller or consumer
// means; contract/conformance verifies the adapter against that declaration.
package trigger

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/well-prado/new-blok/contract/schema"
)

// Kind names one of the nine trigger protocols.
type Kind string

const (
	HTTP      Kind = "http"
	Webhook   Kind = "webhook"
	Worker    Kind = "worker"
	Cron      Kind = "cron"
	PubSub    Kind = "pubsub"
	GRPC      Kind = "grpc"
	SSE       Kind = "sse"
	WebSocket Kind = "websocket"
	MCP       Kind = "mcp"
)

// Completion states when work is considered finished for the source.
type Completion string

const (
	// Memory work completes in band; a process crash may lose it.
	Memory Completion = "memory"
	// Durable work is acknowledged only after admission is committed.
	Durable Completion = "durable"
)

// Disconnect states what losing the caller or consumer means for work.
type Disconnect string

const (
	// CancelWork cancels in-flight memory work when the caller goes away.
	CancelWork Disconnect = "cancel"
	// StopWaiting stops waiting for the caller; committed durable work continues.
	StopWaiting Disconnect = "detach"
	// Redeliver returns an unacknowledged delivery to its source.
	Redeliver Disconnect = "redeliver"
)

// Authentication states who establishes the execution principal.
type Authentication string

const (
	// Caller means the adapter authenticates each protocol caller.
	Caller Authentication = "caller"
	// TrustedProducer means application code produced the delivery; the
	// adapter has no caller to authenticate.
	TrustedProducer Authentication = "trusted-producer"
)

type semantics struct {
	completion Completion
	disconnect Disconnect
}

// allowed lists the completion/disconnect pairs each protocol may declare.
// Memory work always cancels on disconnect; durable work never does.
var allowed = map[Kind][]semantics{
	HTTP:      {{Memory, CancelWork}, {Durable, StopWaiting}},
	Webhook:   {{Durable, StopWaiting}},
	Worker:    {{Durable, Redeliver}},
	Cron:      {{Durable, Redeliver}},
	PubSub:    {{Durable, Redeliver}},
	GRPC:      {{Memory, CancelWork}, {Durable, StopWaiting}},
	SSE:       {{Memory, CancelWork}, {Durable, StopWaiting}},
	WebSocket: {{Memory, CancelWork}, {Durable, StopWaiting}},
	MCP:       {{Memory, CancelWork}, {Durable, StopWaiting}},
}

// authentication lists who may establish the principal for each kind. Only a
// source whose producer is application code may declare trusted-producer;
// every protocol with an external caller must authenticate it.
var authentication = map[Kind][]Authentication{
	HTTP:      {Caller},
	Webhook:   {Caller},
	Worker:    {TrustedProducer},
	Cron:      {TrustedProducer},
	PubSub:    {Caller, TrustedProducer},
	GRPC:      {Caller},
	SSE:       {Caller},
	WebSocket: {Caller},
	MCP:       {Caller},
}

// Kinds returns the nine trigger kinds in stable order.
func Kinds() []Kind {
	kinds := make([]Kind, 0, len(allowed))
	for kind := range allowed {
		kinds = append(kinds, kind)
	}
	sort.Slice(kinds, func(i, j int) bool { return kinds[i] < kinds[j] })
	return kinds
}

// Declaration is the behavior an adapter promises and conformance enforces.
type Declaration struct {
	Kind           Kind           `json:"kind"`
	Adapter        string         `json:"adapter"`
	Completion     Completion     `json:"completion"`
	Disconnect     Disconnect     `json:"disconnect"`
	Authentication Authentication `json:"authentication"`
}

// Error is a stable trigger contract diagnostic.
type Error struct {
	Code    string `json:"code"`
	Path    string `json:"path,omitempty"`
	Message string `json:"message"`
}

func (e *Error) Error() string {
	if e.Path == "" {
		return e.Code + ": " + e.Message
	}
	return e.Code + " at " + e.Path + ": " + e.Message
}

func (d Declaration) Validate() error {
	pairs, ok := allowed[d.Kind]
	if !ok {
		return &Error{Code: "invalid_declaration", Path: "kind", Message: fmt.Sprintf("unknown trigger kind %q", d.Kind)}
	}
	if strings.TrimSpace(d.Adapter) == "" {
		return &Error{Code: "invalid_declaration", Path: "adapter", Message: "adapter name is required"}
	}
	permitted := false
	for _, auth := range authentication[d.Kind] {
		permitted = permitted || auth == d.Authentication
	}
	if !permitted {
		return &Error{Code: "invalid_declaration", Path: "authentication", Message: fmt.Sprintf("%s does not support %q authentication", d.Kind, d.Authentication)}
	}
	for _, pair := range pairs {
		if pair.completion == d.Completion && pair.disconnect == d.Disconnect {
			return nil
		}
	}
	return &Error{Code: "invalid_declaration", Path: "completion", Message: fmt.Sprintf("%s does not support %s completion with %s disconnect", d.Kind, d.Completion, d.Disconnect)}
}

// Principal is the verified caller identity. Adapters obtain it only from
// their authenticator, never from payload fields.
type Principal struct {
	ID    string   `json:"id"`
	Roles []string `json:"roles,omitempty"`
}

// ErrSaturated is returned by an admission handler that has no capacity.
// Adapters translate it into protocol backpressure without retrying.
var ErrSaturated = errors.New("admission_saturated")

// Classified is implemented by errors that carry a stable public code and
// class. Adapters map these without importing engine packages.
type Classified interface {
	ErrorCode() string
	ErrorClass() string
}

var stableCode = regexp.MustCompile(`^[a-z][a-z0-9_.]{0,63}$`)

// Classify returns the first stable code/class in err's chain. Codes are
// source-visible, so a code that is not a short lowercase identifier (for
// example one built from caller or secret data) is not reported.
func Classify(err error) (code, class string, ok bool) {
	var classified Classified
	if !errors.As(err, &classified) {
		return "", "", false
	}
	code, class = classified.ErrorCode(), classified.ErrorClass()
	if !stableCode.MatchString(code) || (class != "" && !stableCode.MatchString(class)) {
		return "", "", false
	}
	return code, class, true
}

// Binding connects one protocol entry point to a domain workflow. InputSchema
// describes the domain value the binding produces after mapping.
type Binding struct {
	ID          string `json:"id"`
	Kind        Kind   `json:"kind"`
	Workflow    string `json:"workflow"`
	InputSchema []byte `json:"inputSchema,omitempty"`
}

// CheckBindings validates that every binding targets workflow and that every
// value the binding can produce is accepted by the workflow input schema.
// Several bindings, of the same or different kinds, may share one workflow.
func CheckBindings(workflow string, workflowInput []byte, bindings []Binding) error {
	target, err := schema.Parse(workflowInput)
	if err != nil {
		return &Error{Code: "invalid_binding", Path: "workflow.inputSchema", Message: err.Error()}
	}
	seen := map[string]bool{}
	for index, binding := range bindings {
		path := fmt.Sprintf("bindings[%d]", index)
		if strings.TrimSpace(binding.ID) == "" {
			return &Error{Code: "invalid_binding", Path: path + ".id", Message: "binding id is required"}
		}
		if seen[binding.ID] {
			return &Error{Code: "duplicate_binding", Path: path + ".id", Message: "duplicate binding id " + binding.ID}
		}
		seen[binding.ID] = true
		if _, ok := allowed[binding.Kind]; !ok {
			return &Error{Code: "invalid_binding", Path: path + ".kind", Message: fmt.Sprintf("unknown trigger kind %q", binding.Kind)}
		}
		if binding.Workflow != workflow {
			return &Error{Code: "invalid_binding", Path: path + ".workflow", Message: "binding targets a different workflow"}
		}
		source, err := schema.Parse(binding.InputSchema)
		if err != nil {
			return &Error{Code: "invalid_binding", Path: path + ".inputSchema", Message: err.Error()}
		}
		if err := produces(source, target, path+".inputSchema"); err != nil {
			return err
		}
	}
	return nil
}

func incompatible(path, message string) error {
	return &Error{Code: "incompatible_binding", Path: path, Message: message}
}

func isOpen(s schema.Schema) bool { return s.AdditionalProperties != nil && *s.AdditionalProperties }

// produces proves that every value valid for source is valid for target.
// Anything it cannot prove is rejected; optional target fields may be omitted.
func produces(source, target schema.Schema, path string) error {
	if len(source.AnyOf) > 0 {
		for index, branch := range source.AnyOf {
			if err := produces(branch, target, fmt.Sprintf("%s.anyOf[%d]", path, index)); err != nil {
				return err
			}
		}
		return nil
	}
	if len(target.AnyOf) > 0 {
		for _, branch := range target.AnyOf {
			if produces(source, branch, path) == nil {
				return nil
			}
		}
		return incompatible(path, "no workflow alternative accepts the binding value")
	}
	if source.Type != target.Type || source.Wire != target.Wire {
		return incompatible(path, fmt.Sprintf("binding produces %s, workflow requires %s", source.Type, target.Type))
	}
	if source.Nullable && !target.Nullable {
		return incompatible(path, "binding may produce null")
	}
	if target.Format != "" && source.Format != target.Format {
		return incompatible(path, "binding does not guarantee format "+target.Format)
	}
	if target.Minimum != nil && (source.Minimum == nil || *source.Minimum < *target.Minimum) {
		return incompatible(path, "binding allows values below the workflow minimum")
	}
	if target.Maximum != nil && (source.Maximum == nil || *source.Maximum > *target.Maximum) {
		return incompatible(path, "binding allows values above the workflow maximum")
	}
	switch source.Type {
	case "object":
		if isOpen(source) && !isOpen(target) {
			return incompatible(path, "binding may produce undeclared fields the workflow rejects")
		}
		for name, child := range source.Properties {
			wanted, declared := target.Properties[name]
			if !declared {
				if !isOpen(target) {
					return incompatible(path+"."+name, "workflow rejects this field")
				}
				continue
			}
			if err := produces(child, wanted, path+"."+name); err != nil {
				return err
			}
		}
		for _, required := range target.Required {
			if !contains(source.Required, required) {
				return incompatible(path+"."+required, "workflow requires a field the binding may omit")
			}
		}
	case "array":
		if source.Items == nil || target.Items == nil {
			return incompatible(path, "array items are not declared")
		}
		return produces(*source.Items, *target.Items, path+"[]")
	}
	return nil
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
