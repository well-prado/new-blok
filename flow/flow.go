// Package flow records typed workflow programs without executing node effects.
package flow

import (
	"encoding/json"
	"fmt"

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

type Instruction struct {
	Kind    string          `json:"kind"`
	ID      string          `json:"id"`
	Node    node.Descriptor `json:"node"`
	Input   string          `json:"input"`
	Output  string          `json:"output"`
	Literal json.RawMessage `json:"literal,omitempty"`
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

func Define[I, O any](spec Spec, build func(*Builder, Ref[I]) Ref[O]) (Definition[I, O], error) {
	if build == nil {
		return Definition[I, O]{}, fmt.Errorf("flow build callback is required")
	}
	if spec.Name == "" || spec.Version == "" {
		return Definition[I, O]{}, fmt.Errorf("flow name and version are required")
	}
	builder := &Builder{program: Program{Spec: spec}}
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
}

func Call[I, O any](builder *Builder, id string, definition node.Definition[I, O], input Ref[I]) Ref[O] {
	if builder == nil {
		panic("flow: nil builder")
	}
	if id == "" {
		panic("flow: call id is required")
	}
	for _, instruction := range builder.program.Instructions {
		if instruction.ID == id {
			panic("flow: duplicate call id " + id)
		}
	}
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

func cloneProgram(program Program) Program {
	program.Instructions = append([]Instruction(nil), program.Instructions...)
	for index := range program.Instructions {
		program.Instructions[index].Literal = append([]byte(nil), program.Instructions[index].Literal...)
	}
	return program
}
