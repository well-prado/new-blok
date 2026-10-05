// Package catalogprogram holds an agent workflow tool's lowered program so
// that it can only run through catalog dispatch (#260).
//
// The engine program has no literal form (ADR 0001, #249): a call that takes
// a literal lowers with no references, and an engine running the program on
// its own would hand that call the workflow input, and an observer or a
// journal would record the workflow input as the call's input. The literal
// reaches the call only when dispatch substitutes it. Program therefore keeps
// the lowered program and its literals in unexported fields, and its one run
// method builds the dispatch nodes, substitutes every literal, and runs the
// engine without an observer or a journal. Package agent cannot name the raw
// program, so it cannot run it, observe it or journal it any other way.
package catalogprogram

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/internal/lowering"
	"github.com/well-prado/new-blok/node"
)

// Instruction is one recorded flow instruction, in the lowering's form.
type Instruction = lowering.Instruction

// Program is a lowered workflow tool. The zero value is not usable; Lower
// returns one.
type Program struct {
	program  contract.InternalProgram
	literals map[string][]byte
}

// Lower lowers recorded flow instructions with both catalog extensions,
// literal call inputs and child workflow calls, through the one lowering
// flow.Lower uses (#249).
func Lower(workflowID, version string, instructions []Instruction, output string) (*Program, error) {
	lowered, err := lowering.Lower(workflowID, version, instructions, output, lowering.Options{Literals: true, Children: true})
	if err != nil {
		return nil, err
	}
	return &Program{program: lowered.Program, literals: lowered.Literals}, nil
}

// Literals returns a copy of each literal call input by call id, or nil when
// no call takes one. A literal is the value dispatch hands the call, not a
// program: it cannot run anything.
func (p *Program) Literals() map[string][]byte {
	if p.literals == nil {
		return nil
	}
	out := make(map[string][]byte, len(p.literals))
	for id, literal := range p.literals {
		out[id] = append([]byte(nil), literal...)
	}
	return out
}

// Equal reports whether the stored program is exactly want. It lets tests
// compare the catalog's program with flow.Lower's without handing it out.
func (p *Program) Equal(want contract.InternalProgram) bool {
	return reflect.DeepEqual(p.program, want)
}

// GoString describes the whole Program for diagnostics: the lowered program
// and, because the program alone does not show what a literal call
// receives, the literals as text.
func (p *Program) GoString() string {
	var literals map[string]string
	if p.literals != nil {
		literals = make(map[string]string, len(p.literals))
		for id, literal := range p.literals {
			literals[id] = string(literal)
		}
	}
	return fmt.Sprintf("catalogprogram.Program{program:%#v, literals:%#v}", p.program, literals)
}

// Dispatch admits and runs one call: id is the call's instruction id and
// input the bytes the call is handed, which for a literal call is the
// literal. It returns the call's decoded result.
type Dispatch func(ctx context.Context, id string, input []byte) (any, error)

// Run executes the program with every call routed through dispatch and every
// literal substituted, and returns the program output. effects gives each
// call's declared effects, so the engine knows when a later failure is no
// longer safe to retry (#190). The engine runs with maxSteps and no observer
// or journal: Run accepts neither, because an engine event or journal entry
// for a literal call would carry the workflow input, not the literal.
// source_guard_test.go keeps it that way: it allows only engine.New,
// WithMaxSteps and Run, forbids importing anything that observes or
// journals, and requires this to be the package's one engine run.
func (p *Program) Run(ctx context.Context, input any, maxSteps int, effects func(id string) []string, dispatch Dispatch) (any, error) {
	nodes := map[string]node.Any{}
	// Unique engine keys per instruction preserve literals and versions.
	program := p.program
	program.Instructions = append([]contract.InternalInstruction(nil), p.program.Instructions...)
	for index, instruction := range program.Instructions {
		if instruction.Kind != "call" {
			continue
		}
		id := instruction.ID
		literal := p.literals[id]
		program.Instructions[index].Node = id
		n := node.MustDefine[any, any]("agent/dispatch", "1.0.0", func(ctx context.Context, value any) (any, error) {
			raw, err := json.Marshal(value)
			if err != nil {
				return nil, err
			}
			if literal != nil {
				raw = literal
			}
			return dispatch(ctx, id, raw)
		}, node.Description("admitted tool dispatch"), node.Schemas([]byte(`{"type":"object"}`), []byte(`{"type":"object"}`)),
			// The call's effects, so the engine knows a later step's
			// saturation is no longer safe to retry (#190).
			node.Effects(effects(id)...))
		nodes[id] = n.Any()
	}
	result, err := engine.New(nodes).WithMaxSteps(maxSteps).Run(ctx, program, input)
	if err != nil {
		return nil, err
	}
	return result.Output, nil
}
