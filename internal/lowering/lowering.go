// Package lowering is the one lowering from a flow definition's recorded
// call instructions to the engine's internal program. flow.Definition.Lower
// and the agent catalog's RegisterWorkflow both call it, so a workflow
// lowers by the same rules whether a developer or an agent catalog lowers it
// (#249). It does not import flow, which imports it: callers copy the
// recorded instructions into Instruction.
package lowering

import (
	"fmt"
	"strings"

	"github.com/well-prado/new-blok/contract"
)

// OutputID is the id of the instruction Lower appends to return the
// workflow's output. flow.OutputID is this constant.
const OutputID = "output"

// Recorded input sources with a fixed meaning.
const (
	inputSource   = "$input"
	literalSource = "$literal"
	stepPrefix    = "$step."
	childPrefix   = "$child."
)

// Instruction is one recorded flow instruction.
type Instruction struct {
	// Kind is the flow instruction kind: "call", "child", or a control
	// construct ("if", "each", …), which never lowers.
	Kind string
	ID   string
	// Node is the engine node key the lowered call names: the node name for
	// a call, the child workflow key for a child.
	Node string
	// Input is the recorded input source: "$input", "$literal", or
	// "$step.<id>[.<field>…]" / "$child.<id>[.<field>…]".
	Input string
	// Literal is the encoded literal when Input is "$literal".
	Literal []byte
}

// Options select the two catalog extensions. The zero value is
// flow.Definition.Lower: calls only, no literals.
type Options struct {
	// Literals lets a call take a recorded literal as its input. The engine
	// program has no literal form, so the call lowers with no references and
	// the literal is returned in Result.Literals for the caller to substitute
	// when it dispatches that call. Only a caller that owns dispatch may set
	// it: agent/internal/catalogprogram, which keeps the program where only
	// catalog dispatch can run it (#260). flow.Lower never does (ADR 0001,
	// #249).
	Literals bool
	// Children lowers a "child" instruction as a call of the child workflow
	// key in Node, referenced by later instructions as "$child.<id>".
	Children bool
}

// Result is a lowered program and the literals its caller substitutes.
type Result struct {
	Program contract.InternalProgram
	// Literals holds each literal call input by call id. It is nil unless
	// Options.Literals is set and a call takes a literal.
	Literals map[string][]byte
}

// Lower converts recorded instructions into the engine's internal program.
//
// Each call's input lowers to the structural reference the canonical
// document compiler produces: the workflow input ("$input") carries no
// reference, and an earlier call's result or field becomes one reference
// whose path is nil for the whole value. Every other input — a literal
// (unless Options.Literals), a field of the workflow input, an empty field
// segment, a reference to an instruction that is not strictly earlier — has
// no program form and is rejected, as is every control construct. Ids follow
// the document id grammar, are unique, and never take OutputID. Errors read
// "flow: …" because they describe the flow definition being lowered.
func Lower(workflowID, version string, instructions []Instruction, output string, options Options) (Result, error) {
	for _, instruction := range instructions {
		if instruction.Kind != "call" && !(options.Children && instruction.Kind == "child") {
			return Result{}, fmt.Errorf("flow: instruction %q of kind %q cannot be lowered", instruction.ID, instruction.Kind)
		}
	}
	result := Result{Program: contract.InternalProgram{WorkflowID: workflowID, Version: version}}
	// earlier maps each lowered id to the prefix its result is referenced by.
	earlier := make(map[string]string, len(instructions))
	for index, instruction := range instructions {
		if err := checkID(instruction.ID, earlier); err != nil {
			return Result{}, err
		}
		references, literal, err := lowerInput(instruction, earlier, options)
		if err != nil {
			return Result{}, fmt.Errorf("flow: %s %q: %w", instruction.Kind, instruction.ID, err)
		}
		if literal != nil {
			if result.Literals == nil {
				result.Literals = map[string][]byte{}
			}
			result.Literals[instruction.ID] = literal
		}
		result.Program.Instructions = append(result.Program.Instructions, contract.InternalInstruction{
			Index:      index,
			ID:         instruction.ID,
			Kind:       "call",
			Node:       instruction.Node,
			References: references,
		})
		earlier[instruction.ID] = stepPrefix
		if instruction.Kind == "child" {
			earlier[instruction.ID] = childPrefix
		}
	}
	reference, err := lowerReference(output, earlier, options.Children)
	if err != nil {
		return Result{}, fmt.Errorf("flow: output: %w", err)
	}
	result.Program.Instructions = append(result.Program.Instructions, contract.InternalInstruction{
		Index:      len(result.Program.Instructions),
		ID:         OutputID,
		Kind:       "output",
		References: []contract.Reference{reference},
	})
	return result, nil
}

// checkID repeats the builder's id rules (flow.Builder.reserveID), so the
// program Lower returns never holds an id the canonical compiler would
// reject, whoever recorded the instructions.
func checkID(id string, earlier map[string]string) error {
	switch {
	case !contract.ValidID(id):
		return fmt.Errorf("flow: instruction id %q does not match the id grammar %s", id, contract.IDPattern)
	case id == OutputID:
		return fmt.Errorf("flow: instruction id %q is reserved for the workflow output instruction", OutputID)
	case earlier[id] != "":
		return fmt.Errorf("flow: duplicate instruction id %s", id)
	}
	return nil
}

// lowerInput returns the references for one instruction's input, and the
// literal the caller substitutes when Options.Literals admits one. An
// instruction without references receives the workflow input, so only
// "$input" and an admitted literal lower to none.
func lowerInput(instruction Instruction, earlier map[string]string, options Options) ([]contract.Reference, []byte, error) {
	switch {
	case instruction.Input == inputSource:
		return nil, nil, nil
	case instruction.Input == literalSource && !options.Literals:
		return nil, nil, fmt.Errorf("literal input cannot be lowered: the engine program has no literal form")
	case instruction.Input == literalSource && len(instruction.Literal) == 0:
		return nil, nil, fmt.Errorf("literal input has no recorded value")
	case instruction.Input == literalSource:
		return nil, append([]byte(nil), instruction.Literal...), nil
	}
	reference, err := lowerReference(instruction.Input, earlier, options.Children)
	if err != nil {
		return nil, nil, fmt.Errorf("input %w", err)
	}
	return []contract.Reference{reference}, nil, nil
}

// lowerReference converts "$step.<id>[.<field>…]" naming an earlier call, or
// with children "$child.<id>[.<field>…]" naming an earlier child, into a
// structural reference with a nil path for the whole value.
func lowerReference(source string, earlier map[string]string, children bool) (contract.Reference, error) {
	var prefix string
	switch {
	case strings.HasPrefix(source, stepPrefix):
		prefix = stepPrefix
	case strings.HasPrefix(source, childPrefix) && children:
		prefix = childPrefix
	default:
		return contract.Reference{}, fmt.Errorf("%q cannot be lowered: it does not name a call result", source)
	}
	parts := strings.Split(strings.TrimPrefix(source, prefix), ".")
	for _, part := range parts {
		if part == "" {
			return contract.Reference{}, fmt.Errorf("%q has an empty field", source)
		}
	}
	if earlier[parts[0]] != prefix {
		return contract.Reference{}, fmt.Errorf("%q does not reference an earlier call", source)
	}
	if len(parts) == 1 {
		return contract.Reference{Step: parts[0]}, nil
	}
	return contract.Reference{Step: parts[0], Path: parts[1:]}, nil
}
