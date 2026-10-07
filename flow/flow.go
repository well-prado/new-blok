// Package flow records typed workflow programs without executing node effects.
package flow

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/lowering"
	"github.com/well-prado/new-blok/node"
)

type Durability string

const (
	Memory Durability = "memory"
)

type Spec struct {
	Name       string
	Version    string
	Durability Durability
}

type expression struct {
	Kind   string
	Source string
	Value  any
}

type Ref[T any] struct{ expression expression }

func (r Ref[T]) Source() string { return r.expression.Source }

func Lit[T any](value T) Ref[T] {
	return Ref[T]{expression: expression{Kind: "literal", Source: "$literal", Value: value}}
}

func Select[I, O any](input Ref[I], path string) Ref[O] {
	if path == "" {
		violate("flow: field path is required")
	}
	return Ref[O]{expression: expression{Kind: "reference", Source: input.expression.Source + "." + path}}
}

type Instruction struct {
	Kind    string          `json:"kind"`
	ID      string          `json:"id"`
	Node    node.Descriptor `json:"node"`
	Input   string          `json:"input"`
	Output  string          `json:"output"`
	Literal json.RawMessage `json:"literal,omitempty"`
	Data    map[string]any  `json:"data,omitempty"`
	// Literals holds a control instruction's literal operands, encoded, by
	// their Data key ("left", "right", "value", "fallback").
	Literals map[string]json.RawMessage `json:"literals,omitempty"`
	// Arms holds the instructions each arm of a control construct records,
	// in arm order (#333). Arm instructions are not in the enclosing list.
	Arms []Arm `json:"arms,omitempty"`
}

// Arm is one arm of a control construct: "then"/"else" for If, "case-<n>"
// (Case is its value, cases in sorted order) and "default" for Choose,
// "body" for Each, "<n>" for Parallel, "try"/"finally" for TryFinally.
// Output is the source the arm returns, empty when it returns nothing.
type Arm struct {
	Name         string        `json:"name"`
	Case         string        `json:"case,omitempty"`
	Instructions []Instruction `json:"instructions,omitempty"`
	Output       string        `json:"output,omitempty"`
}

type Program struct {
	Spec         Spec          `json:"spec"`
	Instructions []Instruction `json:"instructions"`
	Output       string        `json:"output"`
}

type Definition[I, O any] struct {
	program Program
}

// OutputID is the id of the instruction Lower appends to return the
// workflow's output. Every builder rejects it as a step id, so a lowered
// program never holds two instructions with this id (#247).
const OutputID = lowering.OutputID

func (d Definition[I, O]) Program() Program { return cloneProgram(d.program) }

// Lower converts a structurally authored definition into the engine's
// transport-independent internal program. It only lowers the instruction
// forms supported by the native execution path; it never invokes a node.
//
// Each call's input lowers to the same structural reference the canonical
// document compiler produces: the workflow input ("$input") carries no
// reference, and an earlier call's result or field ("$step.<id>[.<field>…]")
// becomes one reference with that path. Every other input — a literal, a
// field of the workflow input, a reference to a call that is not strictly
// earlier in this program — has no program form, so Lower rejects it instead
// of letting the engine fall back to the workflow input (#244).
//
// Compare, Default, If, Choose and TryFinally lower to control instructions
// whose arms nest their own instructions (ADR 0031, #333); a program holding
// one has Format contract.ControlFormat. An arm's steps are visible only
// inside that arm: later instructions read the construct's result. Each,
// Parallel, Template and Child are still rejected.
//
// The rules live in internal/lowering, which the agent catalog lowers
// composed workflows through as well, so the two cannot drift (#249). Lower
// uses it with control flow and without the catalog's extensions: a literal
// call input and a child workflow call stay rejected here.
func (d Definition[I, O]) Lower() (contract.InternalProgram, error) {
	program := d.Program()
	result, err := lowering.Lower(program.Spec.Name, program.Spec.Version, loweringInstructions(program.Instructions), program.Output, lowering.Options{Control: true})
	if err != nil {
		return contract.InternalProgram{}, err
	}
	return result.Program, nil
}

// loweringInstructions copies the recorded instructions into the shared
// lowering's form. A call names its node by descriptor name, as the
// canonical compiler does.
func loweringInstructions(recorded []Instruction) []lowering.Instruction {
	instructions := make([]lowering.Instruction, len(recorded))
	for index, instruction := range recorded {
		instructions[index] = lowering.Instruction{Kind: instruction.Kind, ID: instruction.ID, Node: instruction.Node.Name, Input: instruction.Input, Literal: instruction.Literal, Data: instruction.Data, Literals: instruction.Literals}
		for _, arm := range instruction.Arms {
			instructions[index].Arms = append(instructions[index].Arms, lowering.Arm{Name: arm.Name, Case: arm.Case, Instructions: loweringInstructions(arm.Instructions), Output: arm.Output})
		}
	}
	return instructions
}

// violation is the panic value a builder raises when a definition breaks an
// authoring rule. Define recovers exactly this type and returns it.
type violation struct{ message string }

func (v *violation) Error() string { return v.message }

func violate(message string) { panic(&violation{message: message}) }

// Define records the program build authors. A builder rule the callback
// breaks — an id outside the grammar, a reserved or duplicate id, a malformed
// construct — is returned as an error, so tooling that loads definitions gets
// a diagnostic instead of a crash. Any other panic raised while build runs is
// the application's own and propagates unchanged.
func Define[I, O any](spec Spec, build func(*Builder, Ref[I]) Ref[O]) (Definition[I, O], error) {
	return define(spec, build, true)
}

// MustDefine is Define for package-level definitions. A broken builder rule
// panics at the builder call that broke it, so the stack names the line.
func MustDefine[I, O any](spec Spec, build func(*Builder, Ref[I]) Ref[O]) Definition[I, O] {
	definition, err := define(spec, build, false)
	if err != nil {
		panic(err)
	}
	return definition
}

func define[I, O any](spec Spec, build func(*Builder, Ref[I]) Ref[O], recoverViolations bool) (definition Definition[I, O], err error) {
	if build == nil {
		return Definition[I, O]{}, fmt.Errorf("flow build callback is required")
	}
	if spec.Name == "" || spec.Version == "" {
		return Definition[I, O]{}, fmt.Errorf("flow name and version are required")
	}
	if recoverViolations {
		defer func() {
			if recovered := recover(); recovered != nil {
				authoring, ok := recovered.(*violation)
				if !ok {
					panic(recovered)
				}
				definition, err = Definition[I, O]{}, authoring
			}
		}()
	}
	builder := &Builder{program: Program{Spec: spec}, ids: make(map[string]struct{})}
	output := build(builder, Ref[I]{expression: expression{Kind: "input", Source: "$input"}})
	if output.expression.Kind == "" {
		return Definition[I, O]{}, fmt.Errorf("flow build callback must return a reference")
	}
	builder.program.Output = output.expression.Source
	return Definition[I, O]{program: cloneProgram(builder.program)}, nil
}

// Builder records one block of instructions: the workflow's, or one arm's.
// Every builder of a definition shares one id set, so step ids stay unique
// across arms.
type Builder struct {
	program Program
	ids     map[string]struct{}
	// recording is set while one of this builder's arms is being built. A
	// step recorded on this builder then would land outside the arm.
	recording bool
}

// arm records one arm on its own builder and returns it with the source the
// arm returns.
func (builder *Builder) arm(name string, build func(*ArmBuilder) string) Arm {
	if builder == nil {
		violate("flow: nil builder")
	}
	child := &Builder{ids: builder.ids}
	previous := builder.recording
	builder.recording = true
	output := build(&ArmBuilder{parent: child})
	builder.recording = previous
	return Arm{Name: name, Instructions: child.program.Instructions, Output: output}
}

// record appends one instruction to the block this builder records.
func (builder *Builder) record(instruction Instruction) {
	builder.program.Instructions = append(builder.program.Instructions, instruction)
}

func Call[I, O any](builder *Builder, id string, definition node.Definition[I, O], input Ref[I]) Ref[O] {
	if builder == nil {
		violate("flow: nil builder")
	}
	builder.reserveID(id)
	output := "$step." + id
	instruction := Instruction{
		Kind:   "call",
		ID:     id,
		Node:   definition.Descriptor(),
		Input:  input.expression.Source,
		Output: output,
	}
	if input.expression.Kind == "literal" {
		literal, err := json.Marshal(input.expression.Value)
		if err != nil {
			violate("flow: literal cannot be encoded: " + err.Error())
		}
		instruction.Literal = literal
	}
	builder.record(instruction)
	return Ref[O]{expression: expression{Kind: "reference", Source: output}}
}

// reserveID is the one place every builder's step id is checked. Ids follow
// the document id grammar, so a step id has the same meaning in flow, the
// canonical compiler and documents, and never contains the "." that a
// "$step.<id>.<field>" reference splits on (#251).
func (builder *Builder) reserveID(id string) {
	if builder == nil {
		violate("flow: nil builder")
	}
	if builder.recording {
		violate(fmt.Sprintf("flow: step %q was recorded on an enclosing builder while one of its arms was being built; record it on the arm (ArmCall, or the arm's Builder)", id))
	}
	if !contract.ValidID(id) {
		violate(fmt.Sprintf("flow: instruction id %q does not match the id grammar %s; rename the step", id, contract.IDPattern))
	}
	if id == OutputID {
		violate("flow: instruction id \"" + OutputID + "\" is reserved for the workflow output instruction Lower appends; rename the step")
	}
	if _, exists := builder.ids[id]; exists {
		violate("flow: duplicate instruction id " + id)
	}
	builder.ids[id] = struct{}{}
}

// ArmBuilder records one arm of a control construct.
type ArmBuilder struct{ parent *Builder }

// Builder returns the builder of this arm, for nesting a control construct
// inside it: flow.If(arm.Builder(), …) records the If in this arm.
func (arm *ArmBuilder) Builder() *Builder {
	if arm == nil || arm.parent == nil {
		violate("flow: nil arm builder")
	}
	return arm.parent
}

func ArmCall[I, O any](arm *ArmBuilder, id string, definition node.Definition[I, O], input Ref[I]) Ref[O] {
	if arm == nil || arm.parent == nil {
		violate("flow: nil arm builder")
	}
	return Call(arm.parent, id, definition, input)
}

func If[T any](builder *Builder, id string, condition Ref[bool], thenArm, elseArm func(*ArmBuilder) Ref[T]) Ref[T] {
	if thenArm == nil || elseArm == nil {
		violate("flow: both if arms are required")
	}
	then := builder.arm("then", func(arm *ArmBuilder) string { return thenArm(arm).expression.Source })
	otherwise := builder.arm("else", func(arm *ArmBuilder) string { return elseArm(arm).expression.Source })
	builder.reserveID(id)
	output := "$join." + id
	builder.record(Instruction{Kind: "if", ID: id, Input: condition.expression.Source, Literal: literalOf(condition.expression), Output: output, Data: map[string]any{"then": then.Output, "else": otherwise.Output, "cancellation": "cooperative"}, Arms: []Arm{then, otherwise}})
	return Ref[T]{expression: expression{Kind: "join", Source: output}}
}

func Choose[T any](builder *Builder, id string, condition Ref[string], cases map[string]func(*ArmBuilder) Ref[T], fallback func(*ArmBuilder) Ref[T]) Ref[T] {
	if len(cases) == 0 || fallback == nil {
		violate("flow: choose requires cases and a fallback")
	}
	keys := make([]string, 0, len(cases))
	for key := range cases {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	outputs := make(map[string]any, len(keys)+1)
	arms := make([]Arm, 0, len(keys)+1)
	for index, key := range keys {
		build := cases[key]
		if build == nil {
			violate("flow: choose case " + strconv.Quote(key) + " has no arm")
		}
		arm := builder.arm("case-"+strconv.Itoa(index), func(arm *ArmBuilder) string { return build(arm).expression.Source })
		arm.Case = key
		outputs[key] = arm.Output
		arms = append(arms, arm)
	}
	otherwise := builder.arm("default", func(arm *ArmBuilder) string { return fallback(arm).expression.Source })
	outputs["default"] = otherwise.Output
	builder.reserveID(id)
	output := "$join." + id
	builder.record(Instruction{Kind: "choose", ID: id, Input: condition.expression.Source, Literal: literalOf(condition.expression), Output: output, Data: outputs, Arms: append(arms, otherwise)})
	return Ref[T]{expression: expression{Kind: "join", Source: output}}
}

func Each[I, O any](builder *Builder, id string, input Ref[[]I], concurrency int, body func(*ArmBuilder, Ref[I]) Ref[O]) Ref[[]O] {
	if concurrency < 1 || concurrency > 1024 {
		violate("flow: each concurrency must be between 1 and 1024")
	}
	if body == nil {
		violate("flow: each body is required")
	}
	recorded := builder.arm("body", func(arm *ArmBuilder) string {
		return body(arm, Ref[I]{expression: expression{Kind: "iteration", Source: "$item"}}).expression.Source
	})
	builder.reserveID(id)
	output := "$join." + id
	builder.record(Instruction{Kind: "each", ID: id, Input: input.expression.Source, Output: output, Data: map[string]any{"concurrency": concurrency, "preserveOrder": true, "body": recorded.Output}, Arms: []Arm{recorded}})
	return Ref[[]O]{expression: expression{Kind: "join", Source: output}}
}

func Parallel(builder *Builder, id string, arms ...func(*ArmBuilder)) {
	if len(arms) == 0 {
		violate("flow: parallel requires at least one arm")
	}
	recorded := make([]Arm, 0, len(arms))
	for index, build := range arms {
		if build == nil {
			violate("flow: parallel arm is required")
		}
		recorded = append(recorded, builder.arm(strconv.Itoa(index), func(arm *ArmBuilder) string { build(arm); return "" }))
	}
	builder.reserveID(id)
	builder.record(Instruction{Kind: "parallel", ID: id, Data: map[string]any{"policy": "fail-fast", "cancellation": "cooperative"}, Arms: recorded})
}

func TryFinally[T any](builder *Builder, id string, tryArm func(*ArmBuilder) Ref[T], finallyArm func(*ArmBuilder)) Ref[T] {
	if tryArm == nil || finallyArm == nil {
		violate("flow: try and finally arms are required")
	}
	try := builder.arm("try", func(arm *ArmBuilder) string { return tryArm(arm).expression.Source })
	finally := builder.arm("finally", func(arm *ArmBuilder) string { finallyArm(arm); return "" })
	builder.reserveID(id)
	output := "$join." + id
	builder.record(Instruction{Kind: "try-finally", ID: id, Output: output, Data: map[string]any{"try": try.Output, "suspension": "finally-not-guaranteed-after-suspension"}, Arms: []Arm{try, finally}})
	return Ref[T]{expression: expression{Kind: "join", Source: output}}
}

func Child[I, O any](builder *Builder, id, workflow string, input Ref[I]) Ref[O] {
	if workflow == "" {
		violate("flow: child workflow name is required")
	}
	builder.reserveID(id)
	output := "$child." + id
	builder.record(Instruction{Kind: "child", ID: id, Input: input.expression.Source, Output: output, Data: map[string]any{"workflow": workflow, "wait": true}})
	return Ref[O]{expression: expression{Kind: "reference", Source: output}}
}

func Compare[T any](builder *Builder, id, operator string, left, right Ref[T]) Ref[bool] {
	if operator == "" {
		violate("flow: comparison operator is required")
	}
	builder.reserveID(id)
	output := "$op." + id
	builder.record(Instruction{Kind: "compare", ID: id, Output: output, Data: map[string]any{"operator": operator, "left": left.expression.Source, "right": right.expression.Source}, Literals: literals(map[string]expression{"left": left.expression, "right": right.expression})})
	return Ref[bool]{expression: expression{Kind: "operation", Source: output}}
}

func Default[T any](builder *Builder, id string, value, fallback Ref[T]) Ref[T] {
	builder.reserveID(id)
	output := "$op." + id
	builder.record(Instruction{Kind: "default", ID: id, Output: output, Data: map[string]any{"value": value.expression.Source, "fallback": fallback.expression.Source}, Literals: literals(map[string]expression{"value": value.expression, "fallback": fallback.expression})})
	return Ref[T]{expression: expression{Kind: "operation", Source: output}}
}

// literalOf encodes a literal expression, nil for any other.
func literalOf(value expression) json.RawMessage {
	if value.Kind != "literal" {
		return nil
	}
	encoded, err := json.Marshal(value.Value)
	if err != nil {
		violate("flow: literal cannot be encoded: " + err.Error())
	}
	return encoded
}

// literals encodes the literal operands among values, nil when none is.
func literals(values map[string]expression) map[string]json.RawMessage {
	var encoded map[string]json.RawMessage
	for key, value := range values {
		if literal := literalOf(value); literal != nil {
			if encoded == nil {
				encoded = map[string]json.RawMessage{}
			}
			encoded[key] = literal
		}
	}
	return encoded
}

func Template(builder *Builder, id, template string, values ...Ref[string]) Ref[string] {
	if strings.Contains(template, "js/") {
		violate("flow: raw expression strings are not supported")
	}
	builder.reserveID(id)
	paths := make([]string, len(values))
	for index, value := range values {
		paths[index] = value.expression.Source
	}
	output := "$op." + id
	builder.record(Instruction{Kind: "template", ID: id, Output: output, Data: map[string]any{"template": template, "values": paths}})
	return Ref[string]{expression: expression{Kind: "operation", Source: output}}
}

func cloneProgram(program Program) Program {
	program.Instructions = cloneInstructions(program.Instructions)
	return program
}

func cloneInstructions(instructions []Instruction) []Instruction {
	if instructions == nil {
		return nil
	}
	instructions = append([]Instruction(nil), instructions...)
	for index := range instructions {
		instructions[index].Literal = append([]byte(nil), instructions[index].Literal...)
		if instructions[index].Data != nil {
			data := make(map[string]any, len(instructions[index].Data))
			for key, value := range instructions[index].Data {
				data[key] = value
			}
			instructions[index].Data = data
		}
		if instructions[index].Literals != nil {
			literals := make(map[string]json.RawMessage, len(instructions[index].Literals))
			for key, value := range instructions[index].Literals {
				literals[key] = append(json.RawMessage(nil), value...)
			}
			instructions[index].Literals = literals
		}
		if instructions[index].Arms != nil {
			arms := append([]Arm(nil), instructions[index].Arms...)
			for arm := range arms {
				arms[arm].Instructions = cloneInstructions(arms[arm].Instructions)
			}
			instructions[index].Arms = arms
		}
	}
	return instructions
}
