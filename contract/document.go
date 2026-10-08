// Package contract defines the versioned, transport-independent workflow
// document. It deliberately contains no trigger, storage, or runtime code.
package contract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
)

const (
	CurrentVersion   = 1
	MaxDocumentBytes = 1 << 20
	MaxJSONDepth     = 64
)

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

type SourceSpan struct {
	File        string `json:"file"`
	StartLine   int    `json:"startLine"`
	StartColumn int    `json:"startColumn"`
	EndLine     int    `json:"endLine"`
	EndColumn   int    `json:"endColumn"`
}

type Document struct {
	Version  int              `json:"version"`
	Workflow Workflow         `json:"workflow"`
	Bindings []Binding        `json:"bindings,omitempty"`
	Nodes    []NodeDescriptor `json:"nodes,omitempty"`
}

type Workflow struct {
	ID           string          `json:"id"`
	Name         string          `json:"name"`
	Version      string          `json:"version"`
	Digest       string          `json:"digest"`
	InputSchema  json.RawMessage `json:"inputSchema"`
	OutputSchema json.RawMessage `json:"outputSchema"`
	Instructions []Instruction   `json:"instructions"`
}

type Binding struct {
	ID          string          `json:"id"`
	Kind        string          `json:"kind"`
	Workflow    string          `json:"workflow"`
	InputSchema json.RawMessage `json:"inputSchema,omitempty"`
	Source      *SourceSpan     `json:"source,omitempty"`
}

type NodeDescriptor struct {
	ID           string          `json:"id"`
	Version      string          `json:"version"`
	Digest       string          `json:"digest"`
	InputSchema  json.RawMessage `json:"inputSchema"`
	OutputSchema json.RawMessage `json:"outputSchema"`
}

type Instruction struct {
	ID         string           `json:"id"`
	Kind       string           `json:"kind"`
	Node       string           `json:"node,omitempty"`
	Wait       *WaitInstruction `json:"wait,omitempty"`
	References []Reference      `json:"references,omitempty"`
	Output     OptionalString   `json:"output,omitempty"`
	Source     *SourceSpan      `json:"source,omitempty"`
}

// WaitInstruction describes a durable signal wait with an optional timeout.
// A zero timeout waits for a signal indefinitely; positive timeouts are
// persisted once and do not restart when a run is resumed by a new owner.
type WaitInstruction struct {
	Name          string `json:"name"`
	TimeoutMillis int64  `json:"timeoutMillis,omitempty"`
}

type Reference struct {
	Step string   `json:"step"`
	Path []string `json:"path,omitempty"`
}

type Presence uint8

const (
	Missing Presence = iota
	Null
	Value
)

type OptionalString struct {
	Presence Presence
	Value    string
}

func (o *OptionalString) UnmarshalJSON(data []byte) error {
	o.Presence = Value
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		o.Presence = Null
		o.Value = ""
		return nil
	}
	if err := json.Unmarshal(data, &o.Value); err != nil {
		return err
	}
	return nil
}

func (o OptionalString) MarshalJSON() ([]byte, error) {
	switch o.Presence {
	case Missing:
		return []byte("null"), nil
	case Null:
		return []byte("null"), nil
	default:
		return json.Marshal(o.Value)
	}
}

type InternalProgram struct {
	WorkflowID string `json:"workflowId"`
	Version    string `json:"version"`
	Digest     string `json:"digest"`
	// Format is the instruction set the program uses. Zero, omitted from
	// the encoding, is the original call set (call, wait, output), so every
	// program that set describes encodes exactly as it did before #333.
	// ControlFormat adds the control instructions flow lowers (ADR 0028).
	Format       int                   `json:"format,omitempty"`
	Instructions []InternalInstruction `json:"instructions"`
	Bindings     []Binding             `json:"bindings,omitempty"`
}

// ControlFormat is the InternalProgram.Format of a program holding control
// instructions: compare, default, if, choose, try-finally, each and
// parallel (ADR 0028).
const ControlFormat = 2

type InternalInstruction struct {
	Index      int              `json:"index"`
	ID         string           `json:"id"`
	Kind       string           `json:"kind"`
	Node       string           `json:"node,omitempty"`
	Wait       *WaitInstruction `json:"wait,omitempty"`
	References []Reference      `json:"references,omitempty"`
	Output     OptionalString   `json:"output,omitempty"`
	Source     *SourceSpan      `json:"source,omitempty"`
	// Control carries a control instruction's operands and arms. It is
	// nil, and omitted from the encoding, for call, wait and output.
	Control *Control `json:"control,omitempty"`
}

// Control is the body of a control instruction (ADR 0028). Operands are
// positional: compare reads left and right, default value and fallback, if
// and choose their condition, each its items. Arms hold the nested
// instructions: if has "then" and "else", choose one "case-<n>" arm per
// case (in case order) and "default", try-finally "try" and "finally",
// each "body", parallel "0" to "<n-1>". Concurrency bounds an each's
// iterations in flight.
type Control struct {
	Operator    string    `json:"operator,omitempty"`
	Operands    []Operand `json:"operands,omitempty"`
	Arms        []Arm     `json:"arms,omitempty"`
	Concurrency int       `json:"concurrency,omitempty"`
}

// InputStep is the step a control operand's reference names to read the
// workflow input. It is outside the id grammar, so no step can take it.
const InputStep = "$input"

// Operand is a value a control instruction reads: a reference to a value
// in scope (InputStep for the workflow input), or a JSON literal. Exactly
// one is set.
type Operand struct {
	Reference *Reference      `json:"reference,omitempty"`
	Literal   json.RawMessage `json:"literal,omitempty"`
}

// Arm is one nested block of a control instruction. Name is the arm's
// segment in the invocation paths of its instructions. Match is the JSON
// value a choose case selects. Output is the arm's result; an arm without
// one (finally) yields nothing.
type Arm struct {
	Name         string                `json:"name"`
	Match        json.RawMessage       `json:"match,omitempty"`
	Instructions []InternalInstruction `json:"instructions,omitempty"`
	Output       *Operand              `json:"output,omitempty"`
}

var semver = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)
var digest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
var triggerKinds = map[string]bool{"http": true, "webhook": true, "worker": true, "cron": true, "pubsub": true, "grpc": true, "sse": true, "websocket": true, "mcp": true}
var instructionKinds = map[string]bool{"call": true, "condition": true, "output": true, "wait": true, "parallel": true}

func Parse(data []byte) (Document, error) {
	if len(data) == 0 || len(data) > MaxDocumentBytes {
		return Document{}, &Error{Code: "document_too_large", Message: "document must be between 1 byte and 1 MiB"}
	}
	if err := validateDepth(data); err != nil {
		return Document{}, err
	}
	var d Document
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&d); err != nil {
		return Document{}, &Error{Code: "invalid_json", Message: err.Error()}
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		if err == nil {
			return Document{}, &Error{Code: "trailing_data", Message: "document has trailing JSON values"}
		}
		return Document{}, &Error{Code: "invalid_json", Message: err.Error()}
	}
	if err := d.Validate(); err != nil {
		return Document{}, err
	}
	return d, nil
}

func (d Document) Validate() error {
	if d.Version != CurrentVersion {
		return &Error{Code: "unsupported_version", Path: "version", Message: fmt.Sprintf("document version %d is not supported", d.Version)}
	}
	w := d.Workflow
	if err := validID(w.ID, "workflow.id"); err != nil {
		return err
	}
	if w.Name == "" {
		return &Error{Code: "missing_name", Path: "workflow.name", Message: "workflow name is required"}
	}
	if !semver.MatchString(w.Version) {
		return &Error{Code: "invalid_version", Path: "workflow.version", Message: "version must be major.minor.patch"}
	}
	if !digest.MatchString(w.Digest) {
		return &Error{Code: "invalid_digest", Path: "workflow.digest", Message: "digest must be sha256 followed by 64 lowercase hex characters"}
	}
	if !isJSON(w.InputSchema) || !isJSON(w.OutputSchema) {
		return &Error{Code: "invalid_schema", Path: "workflow", Message: "input and output schemas must be JSON values"}
	}
	ids := map[string]bool{}
	for i, n := range d.Nodes {
		if err := validateNode(n, fmt.Sprintf("nodes[%d]", i)); err != nil {
			return err
		}
		if ids[n.ID] {
			return duplicate(n.ID, "nodes")
		}
		ids[n.ID] = true
	}
	instructionIDs := map[string]bool{}
	for i, in := range w.Instructions {
		path := fmt.Sprintf("workflow.instructions[%d]", i)
		if err := validID(in.ID, path+".id"); err != nil {
			return err
		}
		if instructionIDs[in.ID] {
			return duplicate(in.ID, "workflow.instructions")
		}
		if !instructionKinds[in.Kind] {
			return &Error{Code: "unknown_instruction", Path: path + ".kind", Message: "instruction kind is not supported"}
		}
		if in.Kind == "call" {
			if in.Node == "" {
				return &Error{Code: "missing_node", Path: path + ".node", Message: "call instruction requires a node"}
			}
			if !ids[in.Node] {
				return &Error{Code: "unknown_node", Path: path + ".node", Message: "referenced node is not declared"}
			}
		}
		if in.Kind == "wait" {
			if in.Wait == nil || in.Wait.Name == "" || len(in.Wait.Name) > 180 || in.Wait.TimeoutMillis < 0 || in.Wait.TimeoutMillis > 365*24*60*60*1000 {
				return &Error{Code: "invalid_wait", Path: path + ".wait", Message: "wait requires a bounded signal name and timeout from zero through 365 days"}
			}
		} else if in.Wait != nil {
			return &Error{Code: "unexpected_wait", Path: path + ".wait", Message: "wait metadata is only valid on wait instructions"}
		}
		for _, ref := range in.References {
			if !instructionIDs[ref.Step] {
				return &Error{Code: "invalid_reference", Path: path + ".references", Message: "references must target an earlier instruction"}
			}
		}
		instructionIDs[in.ID] = true
	}
	for i, b := range d.Bindings {
		path := fmt.Sprintf("bindings[%d]", i)
		if err := validID(b.ID, path+".id"); err != nil {
			return err
		}
		if !triggerKinds[b.Kind] {
			return &Error{Code: "invalid_binding", Path: path + ".kind", Message: "trigger kind is not supported"}
		}
		if b.Workflow != w.ID {
			return &Error{Code: "invalid_binding", Path: path + ".workflow", Message: "binding targets an unknown workflow"}
		}
	}
	return nil
}

func (d Document) Compile() (InternalProgram, error) {
	if err := d.Validate(); err != nil {
		return InternalProgram{}, err
	}
	p := InternalProgram{WorkflowID: d.Workflow.ID, Version: d.Workflow.Version, Digest: d.Workflow.Digest, Bindings: append([]Binding(nil), d.Bindings...)}
	for i, in := range d.Workflow.Instructions {
		var wait *WaitInstruction
		if in.Wait != nil {
			copy := *in.Wait
			wait = &copy
		}
		p.Instructions = append(p.Instructions, InternalInstruction{Index: i, ID: in.ID, Kind: in.Kind, Node: in.Node, Wait: wait, References: append([]Reference(nil), in.References...), Output: in.Output, Source: in.Source})
	}
	sort.SliceStable(p.Bindings, func(i, j int) bool { return p.Bindings[i].ID < p.Bindings[j].ID })
	return p, nil
}

func (d Document) Canonical() ([]byte, error) {
	p, err := d.Compile()
	if err != nil {
		return nil, err
	}
	return json.Marshal(p)
}

// IDPattern is the grammar every document id matches: workflow, instruction,
// binding and node descriptor ids. flow step ids follow it too (#251), so an
// authored step id never contains the "." that separates reference fields.
const IDPattern = `^[a-z][a-z0-9_-]{0,63}$`

var idGrammar = regexp.MustCompile(IDPattern)

// ValidID reports whether id matches IDPattern.
func ValidID(id string) bool { return idGrammar.MatchString(id) }

func validID(v, path string) error {
	if !ValidID(v) {
		return &Error{Code: "invalid_id", Path: path, Message: "id must start with a lowercase letter and contain at most 64 lowercase letters, digits, underscore or hyphen"}
	}
	return nil
}
func duplicate(id, where string) error {
	return &Error{Code: "duplicate_id", Path: where, Message: "duplicate id " + id}
}
func validateNode(n NodeDescriptor, path string) error {
	if err := validID(n.ID, path+".id"); err != nil {
		return err
	}
	if !semver.MatchString(n.Version) {
		return &Error{Code: "invalid_version", Path: path + ".version", Message: "version must be major.minor.patch"}
	}
	if !digest.MatchString(n.Digest) {
		return &Error{Code: "invalid_digest", Path: path + ".digest", Message: "digest must be sha256 followed by 64 lowercase hex characters"}
	}
	if !isJSON(n.InputSchema) || !isJSON(n.OutputSchema) {
		return &Error{Code: "invalid_schema", Path: path, Message: "node schemas must be JSON values"}
	}
	return nil
}
func isJSON(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	var v any
	return json.Unmarshal(data, &v) == nil
}
func validateDepth(data []byte) error {
	depth, inString, escaped := 0, false, false
	for _, c := range data {
		if inString {
			if escaped {
				escaped = false
			} else if c == '\\' {
				escaped = true
			} else if c == '"' {
				inString = false
			}
			continue
		}
		if c == '"' {
			inString = true
		}
		if c == '{' || c == '[' {
			depth++
			if depth > MaxJSONDepth {
				return &Error{Code: "depth_exceeded", Message: "document exceeds maximum JSON depth"}
			}
		}
		if c == '}' || c == ']' {
			depth--
			if depth < 0 {
				return &Error{Code: "invalid_json", Message: "document has an unmatched closing delimiter"}
			}
		}
	}
	return nil
}
