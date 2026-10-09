// Package lowering is the one lowering from a flow definition's recorded
// call instructions to the engine's internal program. flow.Definition.Lower
// and the agent catalog's RegisterWorkflow both call it, so a workflow
// lowers by the same rules whether a developer or an agent catalog lowers it
// (#249). It does not import flow, which imports it: callers copy the
// recorded instructions into Instruction.
package lowering

import (
	"encoding/json"
	"fmt"
	"strconv"
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
	// Control constructs (Options.Control): an operation's result
	// (compare, default) and a construct's join (if, choose, try-finally).
	opPrefix   = "$op."
	joinPrefix = "$join."
	// itemPrefix reads the current item of the each it names, only inside
	// that each's body: "$item.<each id>[.<field>…]".
	itemPrefix = "$item."
)

// MaxNesting bounds how deeply control constructs nest (ADR 0028).
const MaxNesting = 64

// MaxConcurrency bounds an each's iterations in flight, as flow.Each does.
const MaxConcurrency = 1024

// operators are the comparisons a compare instruction lowers with.
var operators = map[string]bool{"eq": true, "ne": true, "gt": true, "gte": true, "lt": true, "lte": true}

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
	// Data, Literals and Arms describe a control construct, as flow
	// records them: its sources by key, its literal operands by key, and
	// its arms in order.
	Data     map[string]any
	Literals map[string]json.RawMessage
	Arms     []Arm
}

// Arm is one recorded arm of a control construct.
type Arm struct {
	Name         string
	Case         string
	Instructions []Instruction
	Output       string
}

// Options select the catalog extensions and control flow. The zero value
// lowers calls only, without literals.
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
	// Without it, Control lowers a child as a durable child run instead
	// (contract kind "child", the workflow in Node).
	Children bool
	// Control lowers compare, default, if, choose, try-finally, each and
	// parallel into control instructions (ADR 0028). flow.Definition.Lower sets it; the
	// agent catalog does not, so a composed workflow with control flow
	// stays not agent-safe.
	Control bool
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
	if err := checkKinds(instructions, options); err != nil {
		return Result{}, err
	}
	l := &lowerer{options: options, seen: map[string]bool{}, located: map[string]string{}}
	root := &scope{earlier: make(map[string]string, len(instructions))}
	lowered, err := l.block(instructions, root, 0)
	if err != nil {
		return Result{}, err
	}
	result := Result{Program: contract.InternalProgram{WorkflowID: workflowID, Version: version, Instructions: lowered}, Literals: l.literals}
	reference, err := l.reference(output, root)
	if err != nil {
		return Result{}, fmt.Errorf("flow: output: %w", err)
	}
	result.Program.Instructions = append(result.Program.Instructions, contract.InternalInstruction{
		Index:      len(result.Program.Instructions),
		ID:         OutputID,
		Kind:       "output",
		References: []contract.Reference{reference},
	})
	if l.controlled {
		result.Program.Format = contract.ControlFormat
	}
	return result, nil
}

// checkKinds rejects, before anything else is checked, every instruction
// whose kind the options do not lower, wherever it is nested.
func checkKinds(instructions []Instruction, options Options) error {
	for _, instruction := range instructions {
		supported := instruction.Kind == "call" || (options.Children || options.Control) && instruction.Kind == "child" || options.Control && instruction.Kind == "wait"
		switch instruction.Kind {
		case "compare", "default", "if", "choose", "try-finally", "each", "parallel":
			supported = options.Control
		}
		if !supported {
			return fmt.Errorf("flow: instruction %q of kind %q cannot be lowered", instruction.ID, instruction.Kind)
		}
		for _, arm := range instruction.Arms {
			if err := checkKinds(arm.Instructions, options); err != nil {
				return err
			}
		}
	}
	return nil
}

// scope is one block's view of earlier results: each visible id maps to
// the prefix its result is referenced by. An arm's scope sees its own
// earlier instructions and everything its enclosing scopes saw when the
// construct began; nothing inside an arm is visible outside it.
type scope struct {
	earlier map[string]string
	parent  *scope
}

func (s *scope) prefix(id string) string {
	for current := s; current != nil; current = current.parent {
		if prefix, ok := current.earlier[id]; ok {
			return prefix
		}
	}
	return ""
}

type lowerer struct {
	options  Options
	literals map[string][]byte
	// seen holds every id lowered so far, at any depth: ids are one flat
	// namespace across arms, as the builder keeps them.
	seen map[string]bool
	// located names, for each id lowered inside an arm, that arm, so a
	// reference from outside it can say where the step is.
	located map[string]string
	// controlled is set once a control instruction has been lowered.
	controlled bool
}

func (l *lowerer) block(instructions []Instruction, current *scope, depth int) ([]contract.InternalInstruction, error) {
	lowered := make([]contract.InternalInstruction, 0, len(instructions))
	for index, instruction := range instructions {
		if err := checkID(instruction.ID, l.seen); err != nil {
			return nil, err
		}
		next := contract.InternalInstruction{Index: index, ID: instruction.ID, Kind: "call", Node: instruction.Node}
		prefix := stepPrefix
		switch instruction.Kind {
		case "wait":
			// A durable wait (flow.Wait, ADR 0028 slice 5): its result is
			// the signal, read later as "$step.<id>".
			name, _ := instruction.Data["name"].(string)
			timeout, ok := wholeNumber(instruction.Data["timeoutMillis"])
			if name == "" || !ok || timeout < 0 {
				return nil, fmt.Errorf("flow: wait %q needs a signal name and a timeout of zero or more", instruction.ID)
			}
			next = contract.InternalInstruction{Index: index, ID: instruction.ID, Kind: "wait", Wait: &contract.WaitInstruction{Name: name, TimeoutMillis: int64(timeout)}}
			l.controlled = true
		case "call", "child":
			references, literal, err := lowerInput(instruction, func(source string) (contract.Reference, error) { return l.reference(source, current) }, l.options)
			if err != nil {
				return nil, fmt.Errorf("flow: %s %q: %w", instruction.Kind, instruction.ID, err)
			}
			if literal != nil {
				if l.literals == nil {
					l.literals = map[string][]byte{}
				}
				l.literals[instruction.ID] = literal
			}
			next.References = references
			if instruction.Kind == "child" {
				prefix = childPrefix
				if !l.options.Children {
					// With control flow (and not the catalog's child
					// calls), a child is a durable child run of the
					// workflow it names (ADR 0028, slice 4).
					workflow, _ := instruction.Data["workflow"].(string)
					if workflow == "" {
						return nil, fmt.Errorf("flow: child %q names no workflow", instruction.ID)
					}
					if literal != nil {
						return nil, fmt.Errorf("flow: child %q: a literal input cannot be lowered", instruction.ID)
					}
					next.Kind, next.Node = "child", workflow
					l.controlled = true
				}
			}
		default:
			control, err := l.control(instruction, current, depth)
			if err != nil {
				return nil, err
			}
			next = contract.InternalInstruction{Index: index, ID: instruction.ID, Kind: instruction.Kind, Control: control}
			prefix = joinPrefix
			switch instruction.Kind {
			case "compare", "default":
				prefix = opPrefix
			case "parallel":
				// A parallel has no result of its own; its arms' steps
				// were made visible instead (control).
				prefix = ""
			}
		}
		lowered = append(lowered, next)
		l.seen[instruction.ID] = true
		if prefix != "" {
			current.earlier[instruction.ID] = prefix
		}
	}
	return lowered, nil
}

// control lowers one control construct: its operands and, in their own
// scopes, its arms.
func (l *lowerer) control(instruction Instruction, current *scope, depth int) (*contract.Control, error) {
	l.controlled = true
	fail := func(err error) (*contract.Control, error) {
		return nil, fmt.Errorf("flow: %s %q: %w", instruction.Kind, instruction.ID, err)
	}
	if depth >= MaxNesting {
		return fail(fmt.Errorf("control flow nests deeper than %d levels", MaxNesting))
	}
	operand := func(key string) (contract.Operand, error) {
		source, _ := instruction.Data[key].(string)
		value, err := l.operand(source, instruction.Literals[key], current)
		if err != nil {
			return contract.Operand{}, fmt.Errorf("%s %w", key, err)
		}
		return value, nil
	}
	control := &contract.Control{}
	var keys []string
	switch instruction.Kind {
	case "compare":
		operator, _ := instruction.Data["operator"].(string)
		if !operators[operator] {
			return fail(fmt.Errorf("operator %q is not one of eq, ne, gt, gte, lt, lte", operator))
		}
		control.Operator, keys = operator, []string{"left", "right"}
	case "default":
		keys = []string{"value", "fallback"}
	case "if", "choose":
		condition, err := l.operand(instruction.Input, instruction.Literal, current)
		if err != nil {
			return fail(fmt.Errorf("condition %w", err))
		}
		control.Operands = append(control.Operands, condition)
	case "each":
		items, err := l.operand(instruction.Input, instruction.Literal, current)
		if err != nil {
			return fail(fmt.Errorf("items %w", err))
		}
		control.Operands = append(control.Operands, items)
		concurrency, ok := wholeNumber(instruction.Data["concurrency"])
		if !ok || concurrency < 1 || concurrency > MaxConcurrency {
			return fail(fmt.Errorf("concurrency %v is not between 1 and %d", instruction.Data["concurrency"], MaxConcurrency))
		}
		control.Concurrency = concurrency
	}
	for _, key := range keys {
		value, err := operand(key)
		if err != nil {
			return fail(err)
		}
		control.Operands = append(control.Operands, value)
	}
	if err := checkArms(instruction); err != nil {
		return fail(err)
	}
	var parallel []*scope
	for _, arm := range instruction.Arms {
		inner := &scope{earlier: map[string]string{}, parent: current}
		if instruction.Kind == "each" {
			inner.earlier[instruction.ID] = itemPrefix
		}
		instructions, err := l.block(arm.Instructions, inner, depth+1)
		if err != nil {
			return nil, err
		}
		for id := range inner.earlier {
			if id != instruction.ID {
				l.located[id] = fmt.Sprintf("arm %q of %s %q", arm.Name, instruction.Kind, instruction.ID)
			}
		}
		if instruction.Kind == "parallel" {
			parallel = append(parallel, inner)
		}
		lowered := contract.Arm{Name: arm.Name, Instructions: instructions}
		if instruction.Kind == "choose" && arm.Name != "default" {
			lowered.Match, _ = json.Marshal(arm.Case)
		}
		if arm.Output != "" {
			output, err := l.operand(arm.Output, nil, inner)
			if err != nil {
				return fail(fmt.Errorf("arm %q output %w", arm.Name, err))
			}
			lowered.Output = &output
		}
		control.Arms = append(control.Arms, lowered)
	}
	// Every arm of a parallel has completed when it does, so its arms'
	// steps are readable after it (never in a sibling arm, which runs
	// concurrently): they join the enclosing scope once all are lowered.
	for _, inner := range parallel {
		for id, prefix := range inner.earlier {
			current.earlier[id] = prefix
			delete(l.located, id)
		}
	}
	return control, nil
}

// wholeNumber reads a recorded count: an int as flow records it, or the
// float64 or json.Number a decoded recording holds, if it is whole.
func wholeNumber(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int64:
		return int(typed), int64(int(typed)) == typed
	case float64:
		return int(typed), float64(int(typed)) == typed
	case json.Number:
		parsed, err := typed.Int64()
		return int(parsed), err == nil && int64(int(parsed)) == parsed
	}
	return 0, false
}

// checkArms checks a construct has the arms its kind lowers with, named
// as the builder names them, each returning a result unless it is finally.
func checkArms(instruction Instruction) error {
	var want []string
	switch instruction.Kind {
	case "if":
		want = []string{"then", "else"}
	case "try-finally":
		want = []string{"try", "finally"}
	case "choose":
		for index := 0; index+1 < len(instruction.Arms); index++ {
			want = append(want, "case-"+strconv.Itoa(index))
		}
		want = append(want, "default")
	case "each":
		want = []string{"body"}
	case "parallel":
		for index := range instruction.Arms {
			want = append(want, strconv.Itoa(index))
		}
	}
	if len(instruction.Arms) != len(want) || instruction.Kind == "choose" && len(want) < 2 || instruction.Kind == "parallel" && len(want) < 1 {
		return fmt.Errorf("has %d arms; %s lowers with %s", len(instruction.Arms), instruction.Kind, strings.Join(want, ", "))
	}
	for index, arm := range instruction.Arms {
		if arm.Name != want[index] {
			return fmt.Errorf("arm %d is named %q; want %q", index, arm.Name, want[index])
		}
		if (arm.Output == "") != (arm.Name == "finally" || instruction.Kind == "parallel") {
			return fmt.Errorf("arm %q: only finally and parallel arms return no result", arm.Name)
		}
	}
	return nil
}

// operand lowers a control operand: a recorded literal, the workflow input
// or a field of it, or a reference.
func (l *lowerer) operand(source string, literal []byte, current *scope) (contract.Operand, error) {
	if source == literalSource {
		if len(literal) == 0 || !json.Valid(literal) {
			return contract.Operand{}, fmt.Errorf("literal has no recorded value")
		}
		return contract.Operand{Literal: append(json.RawMessage(nil), literal...)}, nil
	}
	if source == inputSource || strings.HasPrefix(source, inputSource+".") {
		reference := contract.Reference{Step: contract.InputStep}
		if source != inputSource {
			reference.Path = strings.Split(strings.TrimPrefix(source, inputSource+"."), ".")
		}
		for _, part := range reference.Path {
			if part == "" {
				return contract.Operand{}, fmt.Errorf("%q has an empty field", source)
			}
		}
		return contract.Operand{Reference: &reference}, nil
	}
	reference, err := l.reference(source, current)
	if err != nil {
		return contract.Operand{}, err
	}
	return contract.Operand{Reference: &reference}, nil
}

// reference lowers a reference visible in current, explaining a step that
// exists but sits inside an arm the reference cannot see.
func (l *lowerer) reference(source string, current *scope) (contract.Reference, error) {
	reference, err := lowerReference(source, current, l.options)
	if err != nil && reference.Step != "" && l.located[reference.Step] != "" {
		return contract.Reference{}, fmt.Errorf("%q names a step inside %s, which is not visible here: read the construct's result instead", source, l.located[reference.Step])
	}
	return reference, err
}

// checkID repeats the builder's id rules (flow.Builder.reserveID), so the
// program Lower returns never holds an id the canonical compiler would
// reject, whoever recorded the instructions.
func checkID(id string, seen map[string]bool) error {
	switch {
	case !contract.ValidID(id):
		return fmt.Errorf("flow: instruction id %q does not match the id grammar %s", id, contract.IDPattern)
	case id == OutputID:
		return fmt.Errorf("flow: instruction id %q is reserved for the workflow output instruction", OutputID)
	case seen[id]:
		return fmt.Errorf("flow: duplicate instruction id %s", id)
	}
	return nil
}

// lowerInput returns the references for one instruction's input, and the
// literal the caller substitutes when Options.Literals admits one. An
// instruction without references receives the workflow input, so only
// "$input" and an admitted literal lower to none.
func lowerInput(instruction Instruction, reference func(string) (contract.Reference, error), options Options) ([]contract.Reference, []byte, error) {
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
	lowered, err := reference(instruction.Input)
	if err != nil {
		return nil, nil, fmt.Errorf("input %w", err)
	}
	return []contract.Reference{lowered}, nil, nil
}

// lowerReference converts "$step.<id>[.<field>…]" naming an earlier call,
// with children "$child.<id>[.<field>…]" naming an earlier child, and with
// control "$op.<id>…" or "$join.<id>…" naming an earlier operation or
// construct, into a structural reference with a nil path for the whole
// value. On a failure after the prefix it still returns the reference it
// parsed, without a path, so the caller can explain where the id is.
func lowerReference(source string, current *scope, options Options) (contract.Reference, error) {
	var prefix string
	switch {
	case strings.HasPrefix(source, stepPrefix):
		prefix = stepPrefix
	case strings.HasPrefix(source, childPrefix) && (options.Children || options.Control):
		prefix = childPrefix
	case strings.HasPrefix(source, opPrefix) && options.Control:
		prefix = opPrefix
	case strings.HasPrefix(source, joinPrefix) && options.Control:
		prefix = joinPrefix
	case strings.HasPrefix(source, itemPrefix) && options.Control:
		prefix = itemPrefix
	default:
		return contract.Reference{}, fmt.Errorf("%q cannot be lowered: it does not name a call result", source)
	}
	parts := strings.Split(strings.TrimPrefix(source, prefix), ".")
	for _, part := range parts {
		if part == "" {
			return contract.Reference{}, fmt.Errorf("%q has an empty field", source)
		}
	}
	if current.prefix(parts[0]) != prefix {
		if prefix == itemPrefix {
			return contract.Reference{}, fmt.Errorf("%q reads the item of each %q, which is readable only inside that each's body", source, parts[0])
		}
		if prefix == opPrefix || prefix == joinPrefix {
			return contract.Reference{Step: parts[0]}, fmt.Errorf("%q does not reference an earlier operation or construct", source)
		}
		return contract.Reference{Step: parts[0]}, fmt.Errorf("%q does not reference an earlier call", source)
	}
	if len(parts) == 1 {
		return contract.Reference{Step: parts[0]}, nil
	}
	return contract.Reference{Step: parts[0], Path: parts[1:]}, nil
}
