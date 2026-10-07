package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"reflect"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/observe"
)

// Control instructions flow lowers (ADR 0028, #333). The interpreter runs
// them in memory; a durable runner refuses them until their scopes, joins
// and children are journaled.

// maxNesting bounds how deeply control instructions nest, as lowering does.
const maxNesting = 64

// frame is one scope of a run's results: the run's root, or one arm of a
// control instruction. It reads its own results and, through parent, those
// of the scopes enclosing it; it writes only its own, so an arm's results
// never leak into the scope that follows the construct.
type frame struct {
	values map[string]any
	parent *frame
	// prefix is the invocation path of the arm this frame runs, empty at
	// the root; iteration is the iteration path, "root" outside each.
	prefix    string
	iteration string
	// span is the trace span the frame's steps are children of: the run's
	// at the root, the construct's inside an arm.
	span observe.Span
}

// invocation is the invocation path of the instruction id in this frame:
// its id at the root, "<construct path>/<arm>/<id>" inside an arm.
func (f *frame) invocation(id string) string {
	if f.prefix == "" {
		return id
	}
	return f.prefix + "/" + id
}

// arm returns the frame that runs arm of the construct id in this frame.
func (f *frame) arm(id string, arm contract.Arm) *frame {
	return &frame{values: map[string]any{}, parent: f, prefix: f.invocation(id) + "/" + arm.Name, iteration: f.iteration, span: f.span}
}

func (f *frame) resolve(reference contract.Reference) (any, error) {
	for current := f; current != nil; current = current.parent {
		if _, ok := current.values[reference.Step]; ok {
			return resolveReference(current.values, reference)
		}
	}
	return resolveReference(f.values, reference)
}

func (f *frame) operand(operand contract.Operand) (any, error) {
	if operand.Reference != nil {
		return f.resolve(*operand.Reference)
	}
	return decodeLiteral(operand.Literal)
}

func decodeLiteral(literal json.RawMessage) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(literal))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

// checkProgram rejects a program the interpreter cannot run before any
// instruction runs: an unknown format, control instructions outside the
// control format or shaped other than lowering shapes them, and more
// instructions, counted through every arm, than the step budget.
func checkProgram(program contract.InternalProgram, maxSteps int) error {
	switch program.Format {
	case 0, contract.ControlFormat:
	default:
		return &Error{Code: "unsupported_program_format", Class: "configuration", Err: fmt.Errorf("program format %d is not supported", program.Format)}
	}
	count := 0
	var check func([]contract.InternalInstruction, int) error
	check = func(instructions []contract.InternalInstruction, depth int) error {
		for _, instruction := range instructions {
			count++
			if count > maxSteps {
				return &Error{Code: "step_budget_exceeded", Class: "admission"}
			}
			wanted, control := controlShapes[instruction.Kind]
			if depth > 0 && (instruction.Kind == "output" || instruction.Kind == "wait") {
				return invalidControl(instruction.ID, fmt.Errorf("an arm cannot hold a %s instruction", instruction.Kind))
			}
			if !control {
				if instruction.Control != nil {
					return invalidControl(instruction.ID, fmt.Errorf("a %s instruction has no control body", instruction.Kind))
				}
				continue
			}
			if program.Format != contract.ControlFormat {
				return &Error{Code: "unsupported_instruction", Class: "configuration", Step: instruction.ID, Err: fmt.Errorf("%s needs program format %d", instruction.Kind, contract.ControlFormat)}
			}
			if depth >= maxNesting {
				return invalidControl(instruction.ID, fmt.Errorf("control flow nests deeper than %d levels", maxNesting))
			}
			if err := wanted.check(instruction); err != nil {
				return invalidControl(instruction.ID, err)
			}
			for _, arm := range instruction.Control.Arms {
				if err := check(arm.Instructions, depth+1); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return check(program.Instructions, 0)
}

func invalidControl(step string, err error) error {
	return &Error{Code: "invalid_control", Class: "configuration", Step: step, Err: err}
}

// shape is what lowering produces for one control kind.
type shape struct {
	operands int
	// arms names the arms in order; choose has any number of cases first.
	arms []string
}

var controlShapes = map[string]shape{
	"compare":     {operands: 2},
	"default":     {operands: 2},
	"if":          {operands: 1, arms: []string{"then", "else"}},
	"choose":      {operands: 1, arms: []string{"default"}},
	"try-finally": {arms: []string{"try", "finally"}},
}

func (s shape) check(instruction contract.InternalInstruction) error {
	control := instruction.Control
	if control == nil {
		return fmt.Errorf("%s has no control body", instruction.Kind)
	}
	if len(control.Operands) != s.operands {
		return fmt.Errorf("%s has %d operands, want %d", instruction.Kind, len(control.Operands), s.operands)
	}
	for _, operand := range control.Operands {
		if err := checkOperand(operand); err != nil {
			return err
		}
	}
	if instruction.Kind == "compare" {
		switch control.Operator {
		case "eq", "ne", "gt", "gte", "lt", "lte":
		default:
			return fmt.Errorf("compare operator %q is not supported", control.Operator)
		}
	} else if control.Operator != "" {
		return fmt.Errorf("%s has an operator", instruction.Kind)
	}
	want := s.arms
	if instruction.Kind == "choose" {
		if len(control.Arms) < 2 {
			return fmt.Errorf("choose needs at least one case and a default")
		}
		want = nil
		for index := 0; index+1 < len(control.Arms); index++ {
			want = append(want, fmt.Sprintf("case-%d", index))
		}
		want = append(want, "default")
	}
	if len(control.Arms) != len(want) {
		return fmt.Errorf("%s has %d arms, want %d", instruction.Kind, len(control.Arms), len(want))
	}
	for index, arm := range control.Arms {
		if arm.Name != want[index] {
			return fmt.Errorf("arm %d is named %q, want %q", index, arm.Name, want[index])
		}
		var match string
		if (len(arm.Match) > 0) != (instruction.Kind == "choose" && arm.Name != "default") || len(arm.Match) > 0 && json.Unmarshal(arm.Match, &match) != nil {
			return fmt.Errorf("arm %q has an invalid case value", arm.Name)
		}
		if (arm.Output == nil) != (arm.Name == "finally") {
			return fmt.Errorf("arm %q: only a finally arm returns no result", arm.Name)
		}
		if arm.Output != nil {
			if err := checkOperand(*arm.Output); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkOperand(operand contract.Operand) error {
	if (operand.Reference == nil) == (len(operand.Literal) == 0) {
		return fmt.Errorf("an operand needs exactly one of a reference and a literal")
	}
	if len(operand.Literal) > 0 && !json.Valid(operand.Literal) {
		return fmt.Errorf("an operand literal is not JSON")
	}
	return nil
}

// runControl runs one checked control instruction in f. runArm runs one of
// its arms in that arm's frame and returns the arm's result. Branches and
// try-finally run through the engine's control path (control.go).
func runControl(ctx context.Context, f *frame, instruction contract.InternalInstruction, runArm func(context.Context, contract.Arm) (any, error)) (any, error) {
	control := instruction.Control
	operands := make([]any, len(control.Operands))
	for index, operand := range control.Operands {
		value, err := f.operand(operand)
		if err != nil && !(instruction.Kind == "default" && index == 0 && isAbsent(err)) {
			return nil, &Error{Code: "invalid_operand", Class: "validation", Step: instruction.ID, Err: err}
		}
		if instruction.Kind == "if" || instruction.Kind == "choose" {
			// A condition is read as JSON, so a named bool or string type
			// selects like the plain one.
			if value, err = normalize(value); err != nil {
				return nil, &Error{Code: "invalid_condition", Class: "validation", Step: instruction.ID, Err: err}
			}
		}
		operands[index] = value
	}
	action := func(arm contract.Arm) []Action {
		return []Action{{ID: instruction.ID, Run: func(ctx context.Context) (any, error) { return runArm(ctx, arm) }}}
	}
	step := ControlStep{ID: instruction.ID, Kind: ControlAction}
	switch instruction.Kind {
	case "compare":
		matched, err := compareValues(control.Operator, operands[0], operands[1])
		if err != nil {
			return nil, &Error{Code: "invalid_comparison", Class: "validation", Step: instruction.ID, Err: err}
		}
		return matched, nil
	case "default":
		if isNull(operands[0]) {
			return operands[1], nil
		}
		return operands[0], nil
	case "if":
		selected, ok := operands[0].(bool)
		if !ok {
			return nil, &Error{Code: "invalid_condition", Class: "validation", Step: instruction.ID, Err: fmt.Errorf("if condition is %s, not a boolean", describe(operands[0]))}
		}
		step.Kind = ControlBranch
		step.Branch = &BranchPlan{When: func(context.Context) (bool, error) { return selected, nil }, Then: action(control.Arms[0]), Else: action(control.Arms[1])}
	case "choose":
		key, ok := operands[0].(string)
		if !ok {
			return nil, &Error{Code: "invalid_condition", Class: "validation", Step: instruction.ID, Err: fmt.Errorf("choose condition is %s, not a string", describe(operands[0]))}
		}
		selected := control.Arms[len(control.Arms)-1]
		for _, arm := range control.Arms[:len(control.Arms)-1] {
			var match string
			if json.Unmarshal(arm.Match, &match) == nil && match == key {
				selected = arm
				break
			}
		}
		step.Action = action(selected)[0].Run
	case "try-finally":
		finally := control.Arms[1]
		step.Kind = ControlTry
		step.Try = &TryPlan{Try: action(control.Arms[0]), Finally: []Action{{ID: instruction.ID, Run: func(ctx context.Context) (any, error) {
			_, err := runArm(ctx, finally)
			return nil, err
		}}}}
	}
	return runControlStep(ctx, step, 0, maxNesting)
}

// isNull reports whether value's JSON form is null: nil, or a nil pointer,
// slice, map or interface of any type, or a value whose own encoding is
// null. A reference hands on Go values, so an unset optional field is a
// typed nil, never nil itself.
func isNull(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Pointer, reflect.Interface:
		if reflected.IsNil() {
			return true
		}
		if _, ok := value.(json.Marshaler); !ok {
			// A non-nil pointer encodes as what it points to.
			return isNull(reflected.Elem().Interface())
		}
	case reflect.Slice, reflect.Map:
		if reflected.IsNil() {
			return true
		}
	}
	if _, ok := value.(json.Marshaler); ok {
		encoded, err := json.Marshal(value)
		return err == nil && string(encoded) == "null"
	}
	return false
}

// controlIDs adds the id of every control instruction in instructions, at
// any depth, to ids: the steps whose values a construct produced.
func controlIDs(instructions []contract.InternalInstruction, ids map[string]bool) map[string]bool {
	if ids == nil {
		ids = map[string]bool{}
	}
	for _, instruction := range instructions {
		if instruction.Control == nil {
			continue
		}
		ids[instruction.ID] = true
		for _, arm := range instruction.Control.Arms {
			controlIDs(arm.Instructions, ids)
		}
	}
	return ids
}

func isAbsent(err error) bool {
	var absent absentError
	return errors.As(err, &absent)
}

// compareValues compares two values as JSON: eq and ne by value, numbers
// by magnitude whatever their encoding; gt, gte, lt and lte two numbers or
// two strings.
func compareValues(operator string, left, right any) (bool, error) {
	l, err := normalize(left)
	if err != nil {
		return false, err
	}
	r, err := normalize(right)
	if err != nil {
		return false, err
	}
	switch operator {
	case "eq":
		return jsonEqual(l, r), nil
	case "ne":
		return !jsonEqual(l, r), nil
	}
	var order int
	switch {
	case isNumber(l) && isNumber(r):
		left, err := number(l.(json.Number))
		if err != nil {
			return false, err
		}
		right, err := number(r.(json.Number))
		if err != nil {
			return false, err
		}
		order = left.Cmp(right)
	case isString(l) && isString(r):
		order = compareStrings(l.(string), r.(string))
	default:
		return false, fmt.Errorf("%s compares two numbers or two strings, not %s and %s", operator, describe(l), describe(r))
	}
	switch operator {
	case "gt":
		return order > 0, nil
	case "gte":
		return order >= 0, nil
	case "lt":
		return order < 0, nil
	default:
		return order <= 0, nil
	}
}

func normalize(value any) (any, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return decodeLiteral(data)
}

func jsonEqual(left, right any) bool {
	switch l := left.(type) {
	case json.Number:
		r, ok := right.(json.Number)
		if !ok {
			return false
		}
		left, leftErr := number(l)
		right, rightErr := number(r)
		if leftErr != nil || rightErr != nil {
			return l == r
		}
		return left.Cmp(right) == 0
	case []any:
		r, ok := right.([]any)
		if !ok || len(l) != len(r) {
			return false
		}
		for index := range l {
			if !jsonEqual(l[index], r[index]) {
				return false
			}
		}
		return true
	case map[string]any:
		r, ok := right.(map[string]any)
		if !ok || len(l) != len(r) {
			return false
		}
		for key, value := range l {
			other, ok := r[key]
			if !ok || !jsonEqual(value, other) {
				return false
			}
		}
		return true
	default:
		return left == right
	}
}

// number parses a JSON number at a fixed precision, so two encodings of
// one value compare equal and no exponent can make parsing unbounded.
func number(value json.Number) (*big.Float, error) {
	parsed, _, err := big.ParseFloat(string(value), 10, 512, big.ToNearestEven)
	if err != nil {
		return nil, fmt.Errorf("number %s cannot be compared: %w", value, err)
	}
	return parsed, nil
}

func compareStrings(left, right string) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	}
	return 0
}

func isNumber(value any) bool { _, ok := value.(json.Number); return ok }
func isString(value any) bool { _, ok := value.(string); return ok }

func describe(value any) string {
	switch value.(type) {
	case nil:
		return "null"
	case bool:
		return "a boolean"
	case json.Number, float64, float32, int, int64, int32, uint, uint64, uint32:
		return "a number"
	case string:
		return "a string"
	case []any:
		return "an array"
	case map[string]any:
		return "an object"
	}
	return fmt.Sprintf("a %T", value)
}

// rootIteration is the iteration path outside every each, as the journal
// keys a top-level step's effects and waits.
const rootIteration = "root"
