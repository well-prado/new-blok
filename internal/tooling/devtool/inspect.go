package devtool

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/well-prado/new-blok/internal/diagnostic"
	"github.com/well-prado/new-blok/observe/redact"
)

// Inspection fields. A field that is not selected is absent from the
// catalog; Catalog.Fields lists the ones that were projected.
const (
	FieldNodes        = "nodes"
	FieldWorkflows    = "workflows"
	FieldTriggers     = "triggers"
	FieldDescriptions = "descriptions"
	FieldSources      = "sources"
	FieldTests        = "tests"
	FieldExamples     = "examples"
	// FieldSchemas projects schema literals. It is never selected by
	// default: a schema is application content, not catalog metadata.
	FieldSchemas = "schemas"
)

// AllFields are every field inspect knows, in projection order.
var AllFields = []string{FieldNodes, FieldWorkflows, FieldTriggers, FieldDescriptions, FieldSources, FieldTests, FieldExamples, FieldSchemas}

// DefaultFields are projected when no field is selected.
var DefaultFields = []string{FieldNodes, FieldWorkflows, FieldTriggers, FieldDescriptions, FieldSources, FieldTests, FieldExamples}

// ParseFields validates a comma-separated field list. An empty list selects
// DefaultFields; an unknown name is an error, never silently dropped.
func ParseFields(list string) ([]string, error) {
	if strings.TrimSpace(list) == "" {
		return append([]string(nil), DefaultFields...), nil
	}
	selected := map[string]bool{}
	for _, name := range strings.Split(list, ",") {
		name = strings.TrimSpace(name)
		known := false
		for _, field := range AllFields {
			known = known || field == name
		}
		if !known {
			return nil, fmt.Errorf("unknown inspect field %q; choose from %s", name, strings.Join(AllFields, ","))
		}
		selected[name] = true
	}
	var fields []string
	for _, field := range AllFields {
		if selected[field] {
			fields = append(fields, field)
		}
	}
	return fields, nil
}

// InspectOptions select the project and the projected fields.
type InspectOptions struct {
	Options
	// Fields from ParseFields; DefaultFields when nil.
	Fields []string
}

// Catalog describes the project's nodes, workflows and triggers as their
// source declares them. It is read statically: a declaration whose identity
// is not a literal is listed in Unresolved, never guessed.
type Catalog struct {
	Fields     []string        `json:"fields"`
	Nodes      []NodeEntry     `json:"nodes,omitempty"`
	Workflows  []WorkflowEntry `json:"workflows,omitempty"`
	Triggers   []TriggerEntry  `json:"triggers,omitempty"`
	Unresolved []Unresolved    `json:"unresolved,omitempty"`
	// Redacted counts the values replaced by observe/redact.
	Redacted int `json:"redacted,omitempty"`
}

type NodeEntry struct {
	ID                   string          `json:"id"`
	Version              string          `json:"version"`
	Runtime              string          `json:"runtime"`
	Package              string          `json:"package"`
	Description          string          `json:"description,omitempty"`
	Deterministic        bool            `json:"deterministic"`
	RemoteBoundary       bool            `json:"remoteBoundary,omitempty"`
	Effects              []string        `json:"effects,omitempty"`
	RequiredCapabilities []string        `json:"requiredCapabilities,omitempty"`
	InputSchema          json.RawMessage `json:"inputSchema,omitempty"`
	OutputSchema         json.RawMessage `json:"outputSchema,omitempty"`
	Source               string          `json:"source,omitempty"`
	Tests                []Reference     `json:"tests,omitempty"`
	Examples             []Reference     `json:"examples,omitempty"`
}

type WorkflowEntry struct {
	Name       string      `json:"name"`
	Version    string      `json:"version"`
	Durability string      `json:"durability,omitempty"`
	Package    string      `json:"package"`
	Steps      []StepEntry `json:"steps"`
	Source     string      `json:"source,omitempty"`
	Tests      []Reference `json:"tests,omitempty"`
	Examples   []Reference `json:"examples,omitempty"`
}

type StepEntry struct {
	ID     string `json:"id"`
	Kind   string `json:"kind"`
	Source string `json:"source,omitempty"`
}

type TriggerEntry struct {
	Kind     string `json:"kind"`
	Method   string `json:"method,omitempty"`
	Path     string `json:"path,omitempty"`
	Workflow string `json:"workflow,omitempty"`
	Source   string `json:"source,omitempty"`
}

// Reference names a test or example function and where it is declared.
type Reference struct {
	Name   string `json:"name"`
	Source string `json:"source,omitempty"`
}

// Unresolved is a declaration inspect found but could not describe without
// running code.
type Unresolved struct {
	Kind   string `json:"kind"`
	Source string `json:"source,omitempty"`
	Reason string `json:"reason"`
}

// Inspect describes the project from its source without executing it. Every
// projected string passes the framework's redaction boundary, and schema
// literals are projected only when FieldSchemas is selected.
func Inspect(ctx context.Context, options InspectOptions) (report Report) {
	report = Report{Command: "inspect"}
	found := &diagnostics{}
	defer func() { finish(&report, found, ctx, false) }()
	fields := options.Fields
	if fields == nil {
		fields = append([]string(nil), DefaultFields...)
	}
	selected := map[string]bool{}
	for _, field := range fields {
		selected[field] = true
	}

	workspace, problems, err := options.source().Load(ctx, options.Root)
	report.Project = workspace.project()
	if err != nil {
		if ctx.Err() == nil {
			found.add(diagnostic.Diagnostic{Code: "project_unreadable", Actual: errorText(err), Expected: "a readable project directory", Remediation: "run blok from a readable application directory", Message: "the project could not be read"})
		}
		return report
	}
	for _, problem := range problems {
		found.add(problem)
	}
	if workspace.Module == "" {
		return report
	}
	parsed, err := parseWorkspace(ctx, workspace)
	if err != nil {
		return report
	}
	for _, problem := range parsed.parseErrors {
		found.add(problem)
	}
	extracted := parsed.extract()
	projector := &projection{selected: selected}
	catalog := &Catalog{Fields: fields}
	runtime := "go"
	if workspace.Manifest != nil && workspace.Manifest.Runtime != "" {
		runtime = workspace.Manifest.Runtime
	}
	if selected[FieldNodes] {
		for _, item := range extracted.nodes {
			source := projector.source(parsed.position(item.pos))
			if item.id == "" || item.version == "" {
				catalog.Unresolved = append(catalog.Unresolved, Unresolved{Kind: "node", Source: source, Reason: strings.Join(item.unresolved, "; ")})
				continue
			}
			for _, reason := range item.unresolved {
				catalog.Unresolved = append(catalog.Unresolved, Unresolved{Kind: "node-option", Source: source, Reason: reason})
			}
			entry := NodeEntry{ID: projector.text(item.id), Version: projector.text(item.version), Runtime: runtime, Package: item.pkg.Dir, Deterministic: item.deterministic, RemoteBoundary: item.remote, Source: source}
			entry.Effects = projector.texts(item.effects)
			entry.RequiredCapabilities = projector.texts(item.capabilities)
			if selected[FieldDescriptions] {
				entry.Description = projector.message(item.description)
			}
			if selected[FieldSchemas] && item.hasSchemas {
				entry.InputSchema = projector.schema(item.input)
				entry.OutputSchema = projector.schema(item.output)
			}
			tests, examples := extracted.testsFor(item.pkg)
			entry.Tests, entry.Examples = projector.references(FieldTests, tests), projector.references(FieldExamples, examples)
			catalog.Nodes = append(catalog.Nodes, entry)
		}
		sort.SliceStable(catalog.Nodes, func(i, j int) bool {
			return catalog.Nodes[i].ID+"@"+catalog.Nodes[i].Version < catalog.Nodes[j].ID+"@"+catalog.Nodes[j].Version
		})
	}
	if selected[FieldWorkflows] {
		for _, item := range extracted.workflows {
			source := projector.source(parsed.position(item.pos))
			if item.name == "" || item.version == "" {
				catalog.Unresolved = append(catalog.Unresolved, Unresolved{Kind: "workflow", Source: source, Reason: strings.Join(item.unresolved, "; ")})
				continue
			}
			for _, reason := range item.unresolved {
				catalog.Unresolved = append(catalog.Unresolved, Unresolved{Kind: "workflow-step", Source: source, Reason: reason})
			}
			entry := WorkflowEntry{Name: projector.text(item.name), Version: projector.text(item.version), Durability: item.durability, Package: item.pkg.Dir, Source: source, Steps: []StepEntry{}}
			for _, step := range item.steps {
				entry.Steps = append(entry.Steps, StepEntry{ID: projector.text(step.id), Kind: step.kind, Source: projector.source(parsed.position(step.pos))})
			}
			tests, examples := extracted.testsFor(item.pkg)
			entry.Tests, entry.Examples = projector.references(FieldTests, tests), projector.references(FieldExamples, examples)
			catalog.Workflows = append(catalog.Workflows, entry)
		}
		sort.SliceStable(catalog.Workflows, func(i, j int) bool {
			return catalog.Workflows[i].Name+"@"+catalog.Workflows[i].Version < catalog.Workflows[j].Name+"@"+catalog.Workflows[j].Version
		})
	}
	if selected[FieldTriggers] {
		for _, route := range extracted.routes {
			source := projector.source(parsed.position(route.pos))
			for _, reason := range route.unresolved {
				catalog.Unresolved = append(catalog.Unresolved, Unresolved{Kind: "trigger", Source: source, Reason: reason})
			}
			catalog.Triggers = append(catalog.Triggers, TriggerEntry{Kind: "http", Method: projector.text(route.method), Path: projector.text(route.path), Workflow: projector.text(route.workflow), Source: source})
		}
	}
	sort.SliceStable(catalog.Unresolved, func(i, j int) bool {
		if catalog.Unresolved[i].Source != catalog.Unresolved[j].Source {
			return catalog.Unresolved[i].Source < catalog.Unresolved[j].Source
		}
		return catalog.Unresolved[i].Reason < catalog.Unresolved[j].Reason
	})
	catalog.Redacted = projector.redacted
	report.Catalog = catalog
	return report
}

// projection applies the field policy and the redaction boundary.
type projection struct {
	selected map[string]bool
	redacted int
}

func (p *projection) text(value string) string {
	projected := redact.String(value)
	if projected != value {
		p.redacted++
	}
	return projected
}

func (p *projection) message(value string) string {
	projected := redact.Message(value)
	if projected != value {
		p.redacted++
	}
	return projected
}

func (p *projection) texts(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	projected := make([]string, len(values))
	for index, value := range values {
		projected[index] = p.text(value)
	}
	return projected
}

func (p *projection) source(position string) string {
	if !p.selected[FieldSources] {
		return ""
	}
	return position
}

// schema projects a schema literal through structured redaction; text that
// is not JSON is withheld rather than projected raw.
func (p *projection) schema(literal string) json.RawMessage {
	if literal == "" {
		return nil
	}
	projected, err := redact.JSON([]byte(literal))
	if err != nil {
		p.redacted++
		return json.RawMessage(`"` + redact.Marker + `"`)
	}
	var original, compact any
	if json.Unmarshal([]byte(literal), &original) == nil && json.Unmarshal(projected, &compact) == nil {
		before, _ := json.Marshal(original)
		after, _ := json.Marshal(compact)
		if string(before) != string(after) {
			p.redacted++
		}
	}
	return projected
}

func (p *projection) references(field string, items []staticTest) []Reference {
	if !p.selected[field] || len(items) == 0 {
		return nil
	}
	references := make([]Reference, len(items))
	for index, item := range items {
		references[index] = Reference{Name: item.name, Source: p.source(item.source)}
	}
	return references
}
