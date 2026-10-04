// Package migration translates the deliberately small, structural subset of
// Blok v2 JSON workflows that New Blok can represent without evaluating source.
// Unsupported input expressions are returned as diagnostics; they are never
// copied into the target program as literals.
package migration

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/well-prado/new-blok/contract"
)

type Diagnostic struct {
	Code        string `json:"code"`
	Path        string `json:"path"`
	Message     string `json:"message"`
	Remediation string `json:"remediation"`
}

func (d Diagnostic) Error() string {
	return fmt.Sprintf("%s at %s: %s; %s", d.Code, d.Path, d.Message, d.Remediation)
}

type Diagnostics []Diagnostic

func (d Diagnostics) Error() string {
	lines := make([]string, len(d))
	for i := range d {
		lines[i] = d[i].Error()
	}
	return strings.Join(lines, "\n")
}

type legacyWorkflow struct {
	SchemaVersion string                     `json:"schemaVersion"`
	Name          string                     `json:"name"`
	Version       string                     `json:"version"`
	Trigger       map[string]json.RawMessage `json:"trigger"`
	Steps         []legacyStep               `json:"steps"`
}

type legacyStep struct {
	ID     string          `json:"id"`
	Use    string          `json:"use"`
	Inputs json.RawMessage `json:"inputs"`
}

type structuralReference struct {
	Step string   `json:"step"`
	Path []string `json:"path"`
}

var validID = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
var supportedTriggerKinds = map[string]bool{"http": true, "webhook": true, "worker": true, "cron": true, "pubsub": true, "grpc": true, "sse": true, "websocket": true, "mcp": true}

const (
	maxSourceBytes  = 1 << 20
	maxWorkflowStep = 256
	maxInventory    = 4096
	maxJSONTokens   = 131072
)

type duplicateJSONKeyError struct{ key string }

func (e duplicateJSONKeyError) Error() string {
	return "duplicate JSON object member " + shortDiagnosticValue(e.key, 80)
}

// Convert accepts workflows whose calls pass the complete trigger input or a
// complete earlier step output. Node schemas and immutable identities come
// from the caller's inventory, keyed by the source workflow's `use` value.
// Source and collection bounds are enforced before any target document is
// emitted; unsupported mappings are returned as Diagnostic values.
func Convert(source []byte, inventory map[string]contract.NodeDescriptor) (contract.Document, error) {
	if len(source) > maxSourceBytes {
		return contract.Document{}, Diagnostic{Code: "source_too_large", Path: "$", Message: fmt.Sprintf("source is %d bytes; the migration limit is %d bytes", len(source), maxSourceBytes), Remediation: "split or reduce the source workflow before conversion; no partial document was emitted"}
	}
	if len(inventory) > maxInventory {
		return contract.Document{}, Diagnostic{Code: "inventory_too_large", Path: "inventory", Message: fmt.Sprintf("inventory has %d nodes; the migration limit is %d", len(inventory), maxInventory), Remediation: "pass only the pinned node descriptors referenced by this workflow"}
	}
	var inventoryBytes int64
	for key, descriptor := range inventory {
		inventoryBytes += int64(len(key)) + int64(len(descriptor.ID)) + int64(len(descriptor.Version)) + int64(len(descriptor.Digest)) + int64(len(descriptor.InputSchema)) + int64(len(descriptor.OutputSchema))
		if inventoryBytes > maxSourceBytes {
			return contract.Document{}, Diagnostic{Code: "inventory_too_large", Path: "inventory", Message: "pinned node identities and schemas exceed the 1 MiB migration inventory budget", Remediation: "pass only referenced descriptors with bounded schemas"}
		}
	}
	if err := validateSourceJSON(source); err != nil {
		var duplicate duplicateJSONKeyError
		if errors.As(err, &duplicate) {
			return contract.Document{}, Diagnostic{Code: "duplicate_json_key", Path: "$", Message: duplicate.Error(), Remediation: "remove duplicate object members; conversion rejects ambiguous source instead of choosing a value"}
		}
		return contract.Document{}, Diagnostic{Code: "invalid_source", Path: "$", Message: "source is not one bounded JSON value", Remediation: "provide exactly one valid JSON workflow object"}
	}
	var sourceFields map[string]json.RawMessage
	if err := json.Unmarshal(source, &sourceFields); err != nil {
		return contract.Document{}, Diagnostic{Code: "invalid_source", Path: "$", Message: err.Error(), Remediation: "provide a Blok v2 workflow JSON document"}
	}
	if sourceFields == nil {
		return contract.Document{}, Diagnostic{Code: "invalid_source", Path: "$", Message: "workflow source must be a JSON object", Remediation: "provide one Blok v2 workflow JSON object"}
	}
	for key := range sourceFields {
		if key != "schemaVersion" && key != "name" && key != "version" && key != "trigger" && key != "steps" {
			path := key
			if len(path) > 80 {
				path = "$"
			}
			return contract.Document{}, Diagnostic{Code: "unsupported_workflow_field", Path: path, Message: "this workflow-level field has no equivalent in the current target contract", Remediation: "migrate the field explicitly in application composition before converting the workflow"}
		}
	}
	var rawSteps []json.RawMessage
	if raw := sourceFields["steps"]; len(raw) > 0 {
		if err := json.Unmarshal(raw, &rawSteps); err != nil {
			return contract.Document{}, Diagnostic{Code: "invalid_steps", Path: "steps", Message: err.Error(), Remediation: "provide a JSON array of supported step objects"}
		}
	}
	if len(rawSteps) > maxWorkflowStep {
		return contract.Document{}, Diagnostic{Code: "too_many_steps", Path: "steps", Message: fmt.Sprintf("workflow has %d steps; the migration limit is %d", len(rawSteps), maxWorkflowStep), Remediation: "split the workflow into smaller independently reviewed workflows"}
	}
	var legacy legacyWorkflow
	decoder := json.NewDecoder(bytes.NewReader(source))
	if err := decoder.Decode(&legacy); err != nil {
		return contract.Document{}, Diagnostic{Code: "invalid_source", Path: "$", Message: err.Error(), Remediation: "provide a Blok v2 workflow JSON document"}
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return contract.Document{}, Diagnostic{Code: "trailing_source", Path: "$", Message: "source contains more than one JSON value", Remediation: "provide exactly one workflow document"}
	} else if !errors.Is(err, io.EOF) {
		return contract.Document{}, Diagnostic{Code: "invalid_source", Path: "$", Message: err.Error(), Remediation: "fix the trailing JSON data"}
	}
	if legacy.SchemaVersion != "2" {
		return contract.Document{}, Diagnostic{Code: "unsupported_source_version", Path: "schemaVersion", Message: "only Blok workflow schema version 2 is supported", Remediation: "convert the workflow to schemaVersion 2 with blokctl migrate workflows before retrying"}
	}
	if strings.TrimSpace(legacy.Name) == "" || !versionPattern.MatchString(legacy.Version) {
		return contract.Document{}, Diagnostic{Code: "invalid_workflow_identity", Path: "workflow", Message: "workflow name and semantic version are required", Remediation: "set a non-empty name and a major.minor.patch version"}
	}
	if len(legacy.Steps) == 0 {
		return contract.Document{}, Diagnostic{Code: "empty_workflow", Path: "steps", Message: "workflow has no steps", Remediation: "add at least one supported node call"}
	}
	if len(legacy.Trigger) > 1 {
		return contract.Document{}, Diagnostic{Code: "multiple_triggers", Path: "trigger", Message: "the target binding model accepts one trigger per migrated workflow", Remediation: "split the source workflow or migrate each binding explicitly"}
	}
	if raw, present := sourceFields["trigger"]; present {
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || legacy.Trigger == nil || len(legacy.Trigger) == 0 {
			return contract.Document{}, Diagnostic{Code: "invalid_trigger", Path: "trigger", Message: "a declared trigger must be a non-empty object", Remediation: "remove the trigger field or provide exactly one supported trigger kind"}
		}
	}
	for kind, raw := range legacy.Trigger {
		if kind == "queue" {
			return contract.Document{}, Diagnostic{Code: "removed_trigger_kind", Path: "trigger.queue", Message: "`queue` is a removed trigger alias", Remediation: "migrate it to `worker` and verify its delivery contract"}
		}
		if !supportedTriggerKinds[kind] {
			return contract.Document{}, Diagnostic{Code: "unsupported_trigger_kind", Path: triggerDiagnosticPath(kind), Message: "trigger kind is not supported by the target contract", Remediation: "choose a supported target trigger and migrate its protocol and delivery semantics explicitly"}
		}
		trimmed := bytes.TrimSpace(raw)
		if !bytes.Equal(trimmed, []byte("{}")) {
			return contract.Document{}, Diagnostic{Code: "unsupported_trigger_configuration", Path: triggerDiagnosticPath(kind), Message: "the target document records trigger kind but not provider-specific route, authentication, or delivery settings", Remediation: "configure the target adapter explicitly and verify its admission, authentication, acknowledgment, and duplicate-delivery contract"}
		}
	}

	wfID := migratedWorkflowID(legacy.Name)
	stepIDs := make(map[string]bool, len(legacy.Steps)+1)
	nodeIDs := make(map[string]contract.NodeDescriptor, len(inventory))
	priorSteps := make(map[string]bool, len(legacy.Steps))
	doc := contract.Document{Version: contract.CurrentVersion}
	doc.Workflow = contract.Workflow{ID: wfID, Name: legacy.Name, Version: legacy.Version}

	for index, sourceStep := range legacy.Steps {
		path := fmt.Sprintf("steps[%d]", index)
		var stepFields map[string]json.RawMessage
		if index >= len(rawSteps) {
			return contract.Document{}, Diagnostic{Code: "invalid_step", Path: path, Message: "step source is missing", Remediation: "provide an object with id, use, and optional structural inputs"}
		}
		if err := json.Unmarshal(rawSteps[index], &stepFields); err != nil || stepFields == nil {
			return contract.Document{}, Diagnostic{Code: "invalid_step", Path: path, Message: "step must be a JSON object", Remediation: "provide an object with id, use, and optional structural inputs"}
		}
		for key := range stepFields {
			if key != "id" && key != "use" && key != "inputs" {
				fieldPath := path
				if len(key) <= 80 {
					fieldPath += "." + key
				}
				return contract.Document{}, Diagnostic{Code: "unsupported_step_option", Path: fieldPath, Message: "step options such as retry, timeout, and idempotency do not map to the current native executor", Remediation: "preserve the behavior in application composition or keep this workflow on the source engine until a target contract exists"}
			}
		}
		if !validID.MatchString(sourceStep.ID) {
			return contract.Document{}, Diagnostic{Code: "unsupported_step_id", Path: path + ".id", Message: fmt.Sprintf("step id %s is outside the target identifier grammar", shortDiagnosticValue(sourceStep.ID, 80)), Remediation: "rename it to lowercase letters, digits, underscores, or hyphens, starting with a letter"}
		}
		if stepIDs[sourceStep.ID] {
			return contract.Document{}, Diagnostic{Code: "duplicate_step_id", Path: path + ".id", Message: "step ids must be unique", Remediation: "give every step a distinct id"}
		}
		stepIDs[sourceStep.ID] = true
		descriptor, ok := inventory[sourceStep.Use]
		if !ok {
			return contract.Document{}, Diagnostic{Code: "node_not_in_inventory", Path: path + ".use", Message: fmt.Sprintf("node %s has no pinned descriptor", shortDiagnosticValue(sourceStep.Use, 80)), Remediation: "add the exact node version, digest, and schemas to the migration inventory"}
		}
		if descriptor.ID == "" {
			descriptor.ID = slug(sourceStep.Use)
		}
		if !validID.MatchString(descriptor.ID) {
			return contract.Document{}, Diagnostic{Code: "invalid_target_node_id", Path: path + ".use", Message: fmt.Sprintf("inventory id %s is invalid", shortDiagnosticValue(descriptor.ID, 80)), Remediation: "assign a unique target-safe id in the inventory; keep the source `use` key as its inventory key"}
		}
		if existing, exists := nodeIDs[descriptor.ID]; exists && !sameNodeDescriptor(existing, descriptor) {
			return contract.Document{}, Diagnostic{Code: "target_node_identity_collision", Path: path + ".use", Message: "distinct source nodes resolve to the same target id with different pinned identity or schemas", Remediation: "assign distinct target ids or provide identical pinned descriptors before converting"}
		}
		if _, exists := nodeIDs[descriptor.ID]; !exists {
			doc.Nodes = append(doc.Nodes, cloneNodeDescriptor(descriptor))
			nodeIDs[descriptor.ID] = descriptor
		}
		if index == 0 {
			doc.Workflow.InputSchema = append(json.RawMessage(nil), descriptor.InputSchema...)
		}
		doc.Workflow.OutputSchema = append(json.RawMessage(nil), descriptor.OutputSchema...)

		instruction := contract.Instruction{ID: sourceStep.ID, Kind: "call", Node: descriptor.ID}
		if _, hasInputs := stepFields["inputs"]; hasInputs {
			if bytes.Equal(bytes.TrimSpace(sourceStep.Inputs), []byte("null")) {
				return contract.Document{}, Diagnostic{Code: "unsupported_mapper_expression", Path: path + ".inputs", Message: "explicit null inputs are distinct from an omitted input and are outside the supported subset", Remediation: "express a supported whole-value `$ref` or keep this workflow on its source engine"}
			}
			ref, err := parseInputReference(sourceStep.Inputs, path+".inputs")
			if err != nil {
				return contract.Document{}, err
			}
			if ref.Step == "@trigger" {
				if len(ref.Path) != 0 {
					return contract.Document{}, Diagnostic{Code: "unsupported_trigger_projection", Path: path + ".inputs", Message: "the target executor currently accepts the complete trigger value, not a projected trigger field", Remediation: "normalize the input in a typed node before migration or pass the complete trigger body"}
				}
			} else {
				if !priorSteps[ref.Step] {
					return contract.Document{}, Diagnostic{Code: "forward_or_missing_reference", Path: path + ".inputs", Message: fmt.Sprintf("referenced step %s is missing or not earlier in the workflow", shortDiagnosticValue(ref.Step, 80)), Remediation: "reference only a completed earlier step or pass the trigger value"}
				}
				instruction.References = []contract.Reference{{Step: ref.Step, Path: ref.Path}}
			}
		}
		doc.Workflow.Instructions = append(doc.Workflow.Instructions, instruction)
		priorSteps[sourceStep.ID] = true
	}

	outputID := "result"
	if stepIDs[outputID] {
		outputID = "return"
	}
	if stepIDs[outputID] {
		return contract.Document{}, Diagnostic{Code: "reserved_step_id", Path: "steps", Message: "the target output instruction needs a reserved result id", Remediation: "rename the step `result` or `return` and retry"}
	}
	last := legacy.Steps[len(legacy.Steps)-1].ID
	doc.Workflow.Instructions = append(doc.Workflow.Instructions, contract.Instruction{ID: outputID, Kind: "output", References: []contract.Reference{{Step: last}}})

	if len(legacy.Trigger) == 1 {
		for kind := range legacy.Trigger {
			doc.Bindings = []contract.Binding{{ID: "entry", Kind: kind, Workflow: wfID, InputSchema: append(json.RawMessage(nil), doc.Workflow.InputSchema...)}}
		}
	}
	canonical, err := canonicalJSON(source)
	if err != nil {
		return contract.Document{}, Diagnostic{Code: "invalid_source", Path: "$", Message: err.Error(), Remediation: "provide valid JSON"}
	}
	digest := sha256.Sum256(canonical)
	doc.Workflow.Digest = "sha256:" + hex.EncodeToString(digest[:])
	if err := doc.Validate(); err != nil {
		return contract.Document{}, Diagnostic{Code: "invalid_target_document", Path: "$", Message: err.Error(), Remediation: "correct the source workflow or pinned node inventory; no target document was emitted"}
	}
	encodedDocument, err := json.Marshal(doc)
	if err != nil {
		return contract.Document{}, Diagnostic{Code: "invalid_target_document", Path: "$", Message: "converted document could not be encoded", Remediation: "correct the source workflow or pinned node inventory"}
	}
	if len(encodedDocument) > contract.MaxDocumentBytes {
		return contract.Document{}, Diagnostic{Code: "target_document_too_large", Path: "$", Message: fmt.Sprintf("converted document is %d bytes; the target limit is %d", len(encodedDocument), contract.MaxDocumentBytes), Remediation: "reduce schema metadata or split the workflow and retry"}
	}
	return doc, nil
}

// Export writes the canonical structural subset accepted by Convert. It does
// not recreate source code, trigger configuration, or unsupported mappings.
func Export(doc contract.Document) ([]byte, error) {
	if err := checkExportBudget(doc); err != nil {
		return nil, err
	}
	if err := doc.Validate(); err != nil {
		return nil, err
	}
	if doc.Workflow.ID != migratedWorkflowID(doc.Workflow.Name) {
		return nil, exportDiagnostic("workflow.id", "workflow identity is not the deterministic ID produced by Convert", "export only a document returned by Convert or restore its converted workflow id")
	}
	if len(doc.Bindings) > 1 {
		return nil, exportDiagnostic("bindings", "the source subset can represent at most one trigger binding", "split bindings into separate source workflows")
	}
	if len(doc.Workflow.Instructions) < 2 {
		return nil, exportDiagnostic("workflow.instructions", "export requires at least one call followed by one output instruction", "convert a non-empty supported source workflow")
	}
	nodes := make(map[string]contract.NodeDescriptor, len(doc.Nodes))
	for _, descriptor := range doc.Nodes {
		nodes[descriptor.ID] = descriptor
	}
	if len(doc.Bindings) == 1 {
		binding := doc.Bindings[0]
		if binding.ID != "entry" || binding.Workflow != doc.Workflow.ID || binding.Source != nil || !bytes.Equal(binding.InputSchema, doc.Workflow.InputSchema) {
			return nil, exportDiagnostic("bindings[0]", "binding identity, source span, or input schema is not represented in source JSON", "export only bindings produced by Convert")
		}
	}
	legacy := map[string]any{"schemaVersion": "2", "name": doc.Workflow.Name, "version": doc.Workflow.Version}
	if len(doc.Bindings) > 0 {
		legacy["trigger"] = map[string]any{doc.Bindings[0].Kind: map[string]any{}}
	}
	steps := make([]map[string]any, 0, len(doc.Workflow.Instructions)-1)
	lastCall := ""
	usedNodes := make(map[string]bool, len(doc.Nodes))
	for index, instruction := range doc.Workflow.Instructions {
		if instruction.Source != nil || instruction.Output.Presence != contract.Missing {
			return nil, exportDiagnostic(fmt.Sprintf("workflow.instructions[%d]", index), "source spans and instruction output annotations are not represented in source JSON", "remove unsupported instruction metadata before exporting")
		}
		if instruction.Kind == "output" {
			if index != len(doc.Workflow.Instructions)-1 || len(instruction.References) != 1 || instruction.References[0].Step != lastCall || len(instruction.References[0].Path) != 0 {
				return nil, exportDiagnostic(fmt.Sprintf("workflow.instructions[%d]", index), "output must be the final instruction and return the complete value of the last call", "restore the exact output edge emitted by Convert")
			}
			expectedOutputID := "result"
			for _, call := range steps {
				if call["id"] == expectedOutputID {
					expectedOutputID = "return"
					break
				}
			}
			if instruction.ID != expectedOutputID {
				return nil, exportDiagnostic(fmt.Sprintf("workflow.instructions[%d].id", index), "output instruction identity is not the reserved id emitted by Convert", "restore `result` or `return` as the output instruction id")
			}
			continue
		}
		if instruction.Kind != "call" || index == len(doc.Workflow.Instructions)-1 {
			return nil, exportDiagnostic(fmt.Sprintf("workflow.instructions[%d]", index), "only calls followed by one final output instruction are representable", "remove unsupported control flow or keep the workflow on its source engine")
		}
		if len(instruction.References) > 1 {
			return nil, exportDiagnostic(fmt.Sprintf("workflow.instructions[%d].references", index), "the source subset accepts one whole-value reference per call", "move input composition into a typed node before exporting")
		}
		node, ok := nodes[instruction.Node]
		if !ok {
			return nil, exportDiagnostic(fmt.Sprintf("workflow.instructions[%d].node", index), "call node descriptor is missing", "restore the pinned node descriptor before exporting")
		}
		usedNodes[node.ID] = true
		if lastCall == "" && !bytes.Equal(doc.Workflow.InputSchema, node.InputSchema) {
			return nil, exportDiagnostic("workflow.inputSchema", "input schema does not match the first call descriptor", "restore the schema carried by Convert")
		}
		lastCall = instruction.ID
		step := map[string]any{"id": instruction.ID, "use": node.ID}
		if len(instruction.References) > 0 {
			ref := instruction.References[0]
			reference := map[string]any{"step": ref.Step}
			if ref.Path != nil {
				reference["path"] = ref.Path
			}
			step["inputs"] = map[string]any{"$ref": reference}
		}
		steps = append(steps, step)
	}
	if len(steps) == 0 {
		return nil, exportDiagnostic("workflow.instructions", "document has no call instruction", "convert a non-empty supported source workflow")
	}
	if len(usedNodes) != len(nodes) {
		return nil, exportDiagnostic("nodes", "unreferenced node descriptors have no representation in source workflows", "export a document containing only descriptors used by call instructions")
	}
	lastDescriptor := nodes[doc.Workflow.Instructions[len(doc.Workflow.Instructions)-2].Node]
	if !bytes.Equal(doc.Workflow.OutputSchema, lastDescriptor.OutputSchema) {
		return nil, exportDiagnostic("workflow.outputSchema", "output schema does not match the final call descriptor", "restore the schema carried by Convert")
	}
	legacy["steps"] = steps
	encoded, err := json.MarshalIndent(legacy, "", "  ")
	if err != nil {
		return nil, err
	}
	if len(encoded) > maxSourceBytes {
		return nil, exportDiagnostic("$", "exported source exceeds the 1 MiB source limit", "reduce metadata/path lengths or split the workflow")
	}
	return encoded, nil
}

func checkExportBudget(doc contract.Document) error {
	if len(doc.Workflow.Instructions) > maxWorkflowStep+1 || len(doc.Nodes) > maxWorkflowStep || len(doc.Bindings) > 1 {
		return exportDiagnostic("$", "document exceeds the supported workflow collection bounds", "export at most 256 calls, 256 referenced node descriptors and one binding")
	}
	var total int64
	pathSegments := 0
	add := func(size int) bool {
		total += int64(size)
		return total <= maxSourceBytes
	}
	for _, value := range []string{doc.Workflow.ID, doc.Workflow.Name, doc.Workflow.Version, doc.Workflow.Digest} {
		if !add(len(value)) {
			return exportDiagnostic("$", "document exceeds the 1 MiB export input budget", "reduce names, identifiers, paths or schemas before exporting")
		}
	}
	if !add(len(doc.Workflow.InputSchema)) || !add(len(doc.Workflow.OutputSchema)) {
		return exportDiagnostic("$", "document exceeds the 1 MiB export input budget", "reduce names, identifiers, paths or schemas before exporting")
	}
	for _, descriptor := range doc.Nodes {
		for _, value := range []int{len(descriptor.ID), len(descriptor.Version), len(descriptor.Digest), len(descriptor.InputSchema), len(descriptor.OutputSchema)} {
			if !add(value) {
				return exportDiagnostic("nodes", "document exceeds the 1 MiB export input budget", "reduce descriptor metadata or schema sizes")
			}
		}
	}
	for _, binding := range doc.Bindings {
		if !add(len(binding.ID)) || !add(len(binding.Kind)) || !add(len(binding.Workflow)) || !add(len(binding.InputSchema)) {
			return exportDiagnostic("bindings", "document exceeds the 1 MiB export input budget", "reduce binding metadata or schema sizes")
		}
	}
	for _, instruction := range doc.Workflow.Instructions {
		if len(instruction.References) > 1 {
			return exportDiagnostic("workflow.instructions.references", "more than one reference per instruction is outside the migration subset", "compose inputs in a typed node before exporting")
		}
		if !add(len(instruction.ID)) || !add(len(instruction.Kind)) || !add(len(instruction.Node)) {
			return exportDiagnostic("workflow.instructions", "document exceeds the 1 MiB export input budget", "reduce instruction metadata and path lengths")
		}
		for _, ref := range instruction.References {
			pathSegments += len(ref.Path)
			if pathSegments > maxJSONTokens || !add(len(ref.Step)) {
				return exportDiagnostic("workflow.instructions.references", "reference metadata exceeds migration bounds", "reduce the number or size of reference path segments")
			}
			for _, segment := range ref.Path {
				if !add(len(segment)) {
					return exportDiagnostic("workflow.instructions.references", "document exceeds the 1 MiB export input budget", "reduce path lengths or split the workflow")
				}
			}
		}
	}
	return nil
}

func cloneNodeDescriptor(descriptor contract.NodeDescriptor) contract.NodeDescriptor {
	descriptor.InputSchema = append(json.RawMessage(nil), descriptor.InputSchema...)
	descriptor.OutputSchema = append(json.RawMessage(nil), descriptor.OutputSchema...)
	return descriptor
}

func exportDiagnostic(path, message, remediation string) error {
	return Diagnostic{Code: "unsupported_export_document", Path: path, Message: message, Remediation: remediation}
}

func parseInputReference(raw json.RawMessage, path string) (structuralReference, error) {
	var members map[string]json.RawMessage
	if err := json.Unmarshal(raw, &members); err != nil || members == nil || len(members) != 1 || len(members["$ref"]) == 0 {
		message := "input must be one structural `$ref` covering the complete value"
		if err != nil {
			message = err.Error()
		}
		return structuralReference{}, Diagnostic{Code: "unsupported_mapper_expression", Path: path, Message: message, Remediation: "rewrite it as one whole-value `$ref`, or move the transformation into a typed node; JavaScript mapper strings are never evaluated or rewritten"}
	}
	var referenceFields map[string]json.RawMessage
	if err := json.Unmarshal(members["$ref"], &referenceFields); err != nil || referenceFields == nil {
		return structuralReference{}, unsupportedReference(path, "`$ref` must contain only a step name and optional path")
	}
	for key := range referenceFields {
		if key != "step" && key != "path" {
			return structuralReference{}, unsupportedReference(path, "nested `$ref` member "+shortDiagnosticValue(key, 80)+" is not supported")
		}
	}
	if len(referenceFields["step"]) == 0 {
		return structuralReference{}, unsupportedReference(path, "`$ref.step` is required")
	}
	if pathValue, present := referenceFields["path"]; present && bytes.Equal(bytes.TrimSpace(pathValue), []byte("null")) {
		return structuralReference{}, unsupportedReference(path, "`$ref.path` must be omitted or a string array, not null")
	}
	var ref structuralReference
	if err := json.Unmarshal(members["$ref"], &ref); err != nil || strings.TrimSpace(ref.Step) == "" {
		return structuralReference{}, unsupportedReference(path, "`$ref.step` must be a non-empty string and `path` must be a string array")
	}
	return ref, nil
}

func unsupportedReference(path, message string) error {
	return Diagnostic{Code: "unsupported_mapper_expression", Path: path, Message: message, Remediation: "use exactly `{\"$ref\":{\"step\":\"@trigger\"}}` or a reference to a completed earlier step; move transformations into a typed node"}
}

func validateSourceJSON(source []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(source))
	decoder.UseNumber()
	tokens := 0
	var walk func(json.Token, int) error
	walk = func(token json.Token, depth int) error {
		tokens++
		if tokens > maxJSONTokens || depth > contract.MaxJSONDepth {
			return errors.New("JSON token or nesting limit exceeded")
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			keys := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return errors.New("invalid object member")
				}
				if _, exists := keys[key]; exists {
					return duplicateJSONKeyError{key: key}
				}
				keys[key] = struct{}{}
				value, err := decoder.Token()
				if err != nil {
					return err
				}
				if err := walk(value, depth+1); err != nil {
					return err
				}
			}
			_, err := decoder.Token()
			return err
		case '[':
			for decoder.More() {
				value, err := decoder.Token()
				if err != nil {
					return err
				}
				if err := walk(value, depth+1); err != nil {
					return err
				}
			}
			_, err := decoder.Token()
			return err
		default:
			return errors.New("unexpected closing delimiter")
		}
	}
	first, err := decoder.Token()
	if err != nil {
		return err
	}
	if err := walk(first, 1); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func canonicalJSON(source []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(source))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func shortDiagnosticValue(value string, limit int) string {
	quoted := fmt.Sprintf("%q", value)
	if len(quoted) <= limit {
		return quoted
	}
	return fmt.Sprintf("%q…", value[:limit/2])
}

func triggerDiagnosticPath(kind string) string {
	if len(kind) > 64 {
		return "trigger"
	}
	return "trigger." + kind
}

func migratedWorkflowID(name string) string {
	digest := sha256.Sum256([]byte(name))
	suffix := hex.EncodeToString(digest[:8])
	base := slug(name)
	if len(base) > 47 {
		base = strings.TrimRight(base[:47], "-")
	}
	return base + "-" + suffix
}

func sameNodeDescriptor(left, right contract.NodeDescriptor) bool {
	return left.ID == right.ID && left.Version == right.Version && left.Digest == right.Digest && bytes.Equal(left.InputSchema, right.InputSchema) && bytes.Equal(left.OutputSchema, right.OutputSchema)
}

var versionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)

func slug(value string) string {
	var out strings.Builder
	separator := false
	for _, char := range strings.ToLower(value) {
		if char >= 'a' && char <= 'z' || char >= '0' && char <= '9' {
			if separator && out.Len() > 0 {
				out.WriteByte('-')
			}
			out.WriteRune(char)
			separator = false
		} else {
			separator = true
		}
	}
	result := strings.Trim(out.String(), "-")
	if result == "" || result[0] < 'a' || result[0] > 'z' {
		result = "workflow-" + result
	}
	if len(result) > 64 {
		result = result[:64]
	}
	return result
}
