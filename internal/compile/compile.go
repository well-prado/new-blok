// Package compile validates structural workflow references and lowers them to indexed programs.
package compile

import (
	"fmt"
	"sort"
	"strings"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/internal/diagnostic"
)

type Result struct {
	Program            contract.InternalProgram
	RuntimeValidations []RuntimeValidation
}

type RuntimeValidation struct {
	Step string
	Path string
	Why  string
}

type Errors []diagnostic.Diagnostic

func (e Errors) Error() string {
	if len(e) == 0 {
		return ""
	}
	return e[0].Error()
}

func Compile(document contract.Document) (Result, error) {
	if err := document.Validate(); err != nil {
		return Result{}, err
	}
	program, err := document.Compile()
	if err != nil {
		return Result{}, err
	}
	nodes := make(map[string]contract.NodeDescriptor, len(document.Nodes))
	for _, item := range document.Nodes {
		nodes[item.ID] = item
	}
	steps := make(map[string]stepSchema, len(document.Workflow.Instructions))
	var diagnostics []diagnostic.Diagnostic
	var runtime []RuntimeValidation
	for index, instruction := range document.Workflow.Instructions {
		step := stepSchema{}
		if instruction.Kind == "call" {
			node, ok := nodes[instruction.Node]
			if !ok {
				diagnostics = append(diagnostics, failure("unknown_node", instruction, "node", instruction.Node, "declared node", "register the node before compiling"))
			} else {
				parsed, parseErr := schema.Parse(node.OutputSchema)
				if parseErr != nil {
					diagnostics = append(diagnostics, failure("invalid_node_output_schema", instruction, "node", parseErr.Error(), "supported schema", "fix the registered node output schema"))
				} else {
					step.output = parsed
					step.known = true
					step.dynamic = len(parsed.AnyOf) > 0
				}
			}
		}
		for _, reference := range instruction.References {
			resolved, ok := steps[reference.Step]
			if !ok {
				diagnostics = append(diagnostics, failure("unresolved_reference", instruction, "reference", reference.Step, "earlier instruction", "reference an earlier sibling or declare the missing step"))
				continue
			}
			if !resolved.known || resolved.dynamic {
				runtime = append(runtime, RuntimeValidation{Step: instruction.ID, Path: referencePath(reference), Why: "source schema is dynamic"})
				continue
			}
			if _, resolveErr := resolvePath(resolved.output, reference.Path); resolveErr != nil {
				diagnostics = append(diagnostics, failure("unresolved_field", instruction, "reference", referencePath(reference), "declared output field", resolveErr.Error()))
			}
		}
		steps[instruction.ID] = step
		_ = index
	}
	workflowInput, inputErr := schema.Parse(document.Workflow.InputSchema)
	if inputErr == nil {
		for _, binding := range document.Bindings {
			if len(binding.InputSchema) == 0 {
				continue
			}
			bindingInput, err := schema.Parse(binding.InputSchema)
			if err != nil {
				diagnostics = append(diagnostics, diagnostic.Diagnostic{Code: "invalid_trigger_schema", Source: sourceOf(binding.Source), Step: binding.ID, Expected: "supported schema", Actual: err.Error(), Remediation: "fix the trigger input schema", Message: "trigger input schema is invalid"})
				continue
			}
			if !compatible(bindingInput, workflowInput) {
				diagnostics = append(diagnostics, diagnostic.Diagnostic{Code: "incompatible_trigger_mapping", Source: sourceOf(binding.Source), Step: binding.ID, Expected: "workflow input schema", Actual: "trigger input schema", Remediation: "map or normalize the trigger payload before admission", Message: "trigger input cannot safely map to workflow input"})
			}
		}
	}
	if len(diagnostics) > 0 {
		diagnostic.Sort(diagnostics)
		return Result{}, Errors(diagnostics)
	}
	sort.SliceStable(runtime, func(i, j int) bool { return runtime[i].Step < runtime[j].Step })
	return Result{Program: program, RuntimeValidations: runtime}, nil
}

type stepSchema struct {
	output  schema.Schema
	known   bool
	dynamic bool
}

func resolvePath(current schema.Schema, path []string) (schema.Schema, error) {
	for _, part := range path {
		switch current.Type {
		case "object":
			next, ok := current.Properties[part]
			if !ok {
				return schema.Schema{}, fmt.Errorf("field %q is not declared", part)
			}
			current = next
		case "array":
			if part != "*" {
				return schema.Schema{}, fmt.Errorf("array field %q requires * path segment", part)
			}
			current = *current.Items
		default:
			return schema.Schema{}, fmt.Errorf("cannot descend into %s", current.Type)
		}
	}
	return current, nil
}

func compatible(source, target schema.Schema) bool {
	if len(source.AnyOf) > 0 || len(target.AnyOf) > 0 {
		return false
	}
	if source.Type != target.Type {
		return false
	}
	if source.Type == "object" {
		for _, required := range target.Required {
			if !contains(source.Required, required) {
				return false
			}
		}
		for name, wanted := range target.Properties {
			provided, ok := source.Properties[name]
			if ok && !compatible(provided, wanted) {
				return false
			}
		}
	}
	if source.Type == "array" && source.Items != nil && target.Items != nil {
		return compatible(*source.Items, *target.Items)
	}
	return true
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func referencePath(reference contract.Reference) string {
	if len(reference.Path) == 0 {
		return reference.Step
	}
	return reference.Step + "." + strings.Join(reference.Path, ".")
}

func sourceOf(source *contract.SourceSpan) string {
	if source == nil {
		return ""
	}
	return fmt.Sprintf("%s:%d:%d", source.File, source.StartLine, source.StartColumn)
}

func failure(code string, instruction contract.Instruction, field, actual, expected, remediation string) diagnostic.Diagnostic {
	return diagnostic.Diagnostic{Code: code, Source: sourceOf(instruction.Source), Step: instruction.ID, Field: field, Actual: actual, Expected: expected, Remediation: remediation, Message: code}
}
