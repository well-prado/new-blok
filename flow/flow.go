// Package flow records typed workflow programs without executing node effects.
package flow

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/well-prado/new-blok/contract"
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
		panic("flow: field path is required")
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
}

type Program struct {
	Spec         Spec          `json:"spec"`
	Instructions []Instruction `json:"instructions"`
	Output       string        `json:"output"`
}

type Definition[I, O any] struct {
	program Program
}

func (d Definition[I, O]) Program() Program { return cloneProgram(d.program) }

// Lower converts a structurally authored definition into the engine's
// transport-independent internal program. It only lowers the instruction
// forms supported by the native execution path; it never invokes a node.
func (d Definition[I, O]) Lower() (contract.InternalProgram, error) {
	program := d.Program()
	internal := contract.InternalProgram{
		WorkflowID: program.Spec.Name,
		Version:    program.Spec.Version,
	}
	for index, instruction := range program.Instructions {
		if instruction.Kind != "call" {
			return contract.InternalProgram{}, fmt.Errorf("flow: instruction %q of kind %q cannot be lowered", instruction.ID, instruction.Kind)
		}
		internal.Instructions = append(internal.Instructions, contract.InternalInstruction{
			Index: index,
			ID:    instruction.ID,
			Kind:  instruction.Kind,
			Node:  instruction.Node.Name,
		})
	}
	step, path, err := lowerReference(program.Output)
	if err != nil {
		return contract.InternalProgram{}, fmt.Errorf("flow: output: %w", err)
	}
	internal.Instructions = append(internal.Instructions, contract.InternalInstruction{
		Index: len(internal.Instructions),
		ID:    "output",
		Kind:  "output",
		References: []contract.Reference{{
			Step: step,
			Path: path,
		}},
	})
	return internal, nil
}

func lowerReference(source string) (string, []string, error) {
	const prefix = "$step."
	if !strings.HasPrefix(source, prefix) {
		return "", nil, fmt.Errorf("output %q must reference a call result", source)
	}
	parts := strings.Split(strings.TrimPrefix(source, prefix), ".")
	if len(parts) == 0 || parts[0] == "" {
		return "", nil, fmt.Errorf("output %q has no step", source)
	}
	if len(parts) == 1 {
		return parts[0], nil, nil
	}
	return parts[0], parts[1:], nil
}

func Define[I, O any](spec Spec, build func(*Builder, Ref[I]) Ref[O]) (Definition[I, O], error) {
	if build == nil {
		return Definition[I, O]{}, fmt.Errorf("flow build callback is required")
	}
	if spec.Name == "" || spec.Version == "" {
		return Definition[I, O]{}, fmt.Errorf("flow name and version are required")
	}
	builder := &Builder{program: Program{Spec: spec}, ids: make(map[string]struct{})}
	output := build(builder, Ref[I]{expression: expression{Kind: "input", Source: "$input"}})
	if output.expression.Kind == "" {
		return Definition[I, O]{}, fmt.Errorf("flow build callback must return a reference")
	}
	builder.program.Output = output.expression.Source
	return Definition[I, O]{program: cloneProgram(builder.program)}, nil
}

func MustDefine[I, O any](spec Spec, build func(*Builder, Ref[I]) Ref[O]) Definition[I, O] {
	definition, err := Define(spec, build)
	if err != nil {
		panic(err)
	}
	return definition
}

type Builder struct {
	program Program
	ids     map[string]struct{}
}

func Call[I, O any](builder *Builder, id string, definition node.Definition[I, O], input Ref[I]) Ref[O] {
	if builder == nil {
		panic("flow: nil builder")
	}
	if id == "" {
		panic("flow: call id is required")
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
			panic("flow: literal cannot be encoded: " + err.Error())
		}
		instruction.Literal = literal
	}
	builder.program.Instructions = append(builder.program.Instructions, instruction)
	return Ref[O]{expression: expression{Kind: "reference", Source: output}}
}

func (builder *Builder) reserveID(id string) {
	if _, exists := builder.ids[id]; exists {
		panic("flow: duplicate instruction id " + id)
	}
	builder.ids[id] = struct{}{}
}

type ArmBuilder struct{ parent *Builder }

func ArmCall[I, O any](arm *ArmBuilder, id string, definition node.Definition[I, O], input Ref[I]) Ref[O] {
	if arm == nil || arm.parent == nil {
		panic("flow: nil arm builder")
	}
	return Call(arm.parent, id, definition, input)
}

func If[T any](builder *Builder, id string, condition Ref[bool], thenArm, elseArm func(*ArmBuilder) Ref[T]) Ref[T] {
	if thenArm == nil || elseArm == nil {
		panic("flow: both if arms are required")
	}
	arm := &ArmBuilder{parent: builder}
	thenOutput := thenArm(arm)
	elseOutput := elseArm(arm)
	builder.reserveID(id)
	output := "$join." + id
	builder.program.Instructions = append(builder.program.Instructions, Instruction{Kind: "if", ID: id, Input: condition.expression.Source, Output: output, Data: map[string]any{"then": thenOutput.expression.Source, "else": elseOutput.expression.Source, "cancellation": "cooperative"}})
	return Ref[T]{expression: expression{Kind: "join", Source: output}}
}

func Choose[T any](builder *Builder, id string, condition Ref[string], cases map[string]func(*ArmBuilder) Ref[T], fallback func(*ArmBuilder) Ref[T]) Ref[T] {
	if len(cases) == 0 || fallback == nil {
		panic("flow: choose requires cases and a fallback")
	}
	keys := make([]string, 0, len(cases))
	for key := range cases {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	arm := &ArmBuilder{parent: builder}
	outputs := make(map[string]any, len(keys)+1)
	for _, key := range keys {
		outputs[key] = cases[key](arm).expression.Source
	}
	outputs["default"] = fallback(arm).expression.Source
	builder.reserveID(id)
	output := "$join." + id
	builder.program.Instructions = append(builder.program.Instructions, Instruction{Kind: "choose", ID: id, Input: condition.expression.Source, Output: output, Data: outputs})
	return Ref[T]{expression: expression{Kind: "join", Source: output}}
}

func Each[I, O any](builder *Builder, id string, input Ref[[]I], concurrency int, body func(*ArmBuilder, Ref[I]) Ref[O]) Ref[[]O] {
	if concurrency < 1 || concurrency > 1024 {
		panic("flow: each concurrency must be between 1 and 1024")
	}
	if body == nil {
		panic("flow: each body is required")
	}
	arm := &ArmBuilder{parent: builder}
	bodyOutput := body(arm, Ref[I]{expression: expression{Kind: "iteration", Source: "$item"}})
	builder.reserveID(id)
	output := "$join." + id
	builder.program.Instructions = append(builder.program.Instructions, Instruction{Kind: "each", ID: id, Input: input.expression.Source, Output: output, Data: map[string]any{"concurrency": concurrency, "preserveOrder": true, "body": bodyOutput.expression.Source}})
	return Ref[[]O]{expression: expression{Kind: "join", Source: output}}
}

func Parallel(builder *Builder, id string, arms ...func(*ArmBuilder)) {
	if len(arms) == 0 {
		panic("flow: parallel requires at least one arm")
	}
	arm := &ArmBuilder{parent: builder}
	for _, build := range arms {
		if build == nil {
			panic("flow: parallel arm is required")
		}
		build(arm)
	}
	builder.reserveID(id)
	builder.program.Instructions = append(builder.program.Instructions, Instruction{Kind: "parallel", ID: id, Data: map[string]any{"policy": "fail-fast", "cancellation": "cooperative"}})
}

func TryFinally[T any](builder *Builder, id string, tryArm func(*ArmBuilder) Ref[T], finallyArm func(*ArmBuilder)) Ref[T] {
	if tryArm == nil || finallyArm == nil {
		panic("flow: try and finally arms are required")
	}
	arm := &ArmBuilder{parent: builder}
	tryOutput := tryArm(arm)
	finallyArm(arm)
	builder.reserveID(id)
	output := "$join." + id
	builder.program.Instructions = append(builder.program.Instructions, Instruction{Kind: "try-finally", ID: id, Output: output, Data: map[string]any{"try": tryOutput.expression.Source, "suspension": "finally-not-guaranteed-after-suspension"}})
	return Ref[T]{expression: expression{Kind: "join", Source: output}}
}

func Child[I, O any](builder *Builder, id, workflow string, input Ref[I]) Ref[O] {
	if workflow == "" {
		panic("flow: child workflow name is required")
	}
	builder.reserveID(id)
	output := "$child." + id
	builder.program.Instructions = append(builder.program.Instructions, Instruction{Kind: "child", ID: id, Input: input.expression.Source, Output: output, Data: map[string]any{"workflow": workflow, "wait": true}})
	return Ref[O]{expression: expression{Kind: "reference", Source: output}}
}

func Compare[T any](builder *Builder, id, operator string, left, right Ref[T]) Ref[bool] {
	if operator == "" {
		panic("flow: comparison operator is required")
	}
	builder.reserveID(id)
	output := "$op." + id
	builder.program.Instructions = append(builder.program.Instructions, Instruction{Kind: "compare", ID: id, Output: output, Data: map[string]any{"operator": operator, "left": left.expression.Source, "right": right.expression.Source}})
	return Ref[bool]{expression: expression{Kind: "operation", Source: output}}
}

func Default[T any](builder *Builder, id string, value, fallback Ref[T]) Ref[T] {
	builder.reserveID(id)
	output := "$op." + id
	builder.program.Instructions = append(builder.program.Instructions, Instruction{Kind: "default", ID: id, Output: output, Data: map[string]any{"value": value.expression.Source, "fallback": fallback.expression.Source}})
	return Ref[T]{expression: expression{Kind: "operation", Source: output}}
}

func Template(builder *Builder, id, template string, values ...Ref[string]) Ref[string] {
	if strings.Contains(template, "js/") {
		panic("flow: raw expression strings are not supported")
	}
	builder.reserveID(id)
	paths := make([]string, len(values))
	for index, value := range values {
		paths[index] = value.expression.Source
	}
	output := "$op." + id
	builder.program.Instructions = append(builder.program.Instructions, Instruction{Kind: "template", ID: id, Output: output, Data: map[string]any{"template": template, "values": paths}})
	return Ref[string]{expression: expression{Kind: "operation", Source: output}}
}

func cloneProgram(program Program) Program {
	program.Instructions = append([]Instruction(nil), program.Instructions...)
	for index := range program.Instructions {
		program.Instructions[index].Literal = append([]byte(nil), program.Instructions[index].Literal...)
		if program.Instructions[index].Data != nil {
			data := make(map[string]any, len(program.Instructions[index].Data))
			for key, value := range program.Instructions[index].Data {
				data[key] = value
			}
			program.Instructions[index].Data = data
		}
	}
	return program
}
