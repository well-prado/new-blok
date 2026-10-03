// Package inspect builds read-only authorized projections from engine events.
package inspect

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/well-prado/new-blok/contract/inspection"
)

var ErrNotFound = errors.New("inspection: run not found")

type runRecord struct {
	owner  string
	value  inspection.Run
	steps  []inspection.Step
	byStep map[string]int
}

// Recorder accepts observations from the real engine and keeps immutable
// snapshots for development inspection. It is intentionally process-local.
type Recorder struct {
	mu   sync.RWMutex
	runs map[string]*runRecord
}

func NewRecorder() *Recorder { return &Recorder{runs: make(map[string]*runRecord)} }

func (r *Recorder) Observe(event inspection.Event) {
	if r == nil || event.RunID == "" || event.At.IsZero() {
		return
	}
	event = cloneEvent(event)
	r.mu.Lock()
	defer r.mu.Unlock()
	switch event.Kind {
	case inspection.RunStarted:
		if event.Principal == "" || r.runs[event.RunID] != nil {
			return
		}
		r.runs[event.RunID] = &runRecord{
			owner:  event.Principal,
			value:  inspection.Run{ID: event.RunID, Workflow: event.Workflow, ParentRun: event.ParentRun, ParentStep: event.ParentStep, Status: inspection.StatusRunning, StartedAt: event.At, Input: cloneRaw(event.Input)},
			byStep: make(map[string]int),
		}
	case inspection.StepProcessing, inspection.StepCompleted, inspection.StepFailed, inspection.StepCanceled, inspection.StepUncertain:
		run := r.runs[event.RunID]
		if run == nil || run.owner != event.Principal || event.StepID == "" {
			return
		}
		index, exists := run.byStep[event.StepID]
		if !exists {
			index = len(run.steps)
			run.byStep[event.StepID] = index
			run.steps = append(run.steps, inspection.Step{ID: event.StepID})
		}
		step := &run.steps[index]
		if event.Attempt > step.Attempt {
			step.Attempt = event.Attempt
		}
		if event.Kind == inspection.StepProcessing {
			step.Status = inspection.StatusRunning
			step.StartedAt = event.At
			step.Input = cloneRaw(event.Input)
			return
		}
		step.FinishedAt = event.At
		step.Output = cloneRaw(event.Output)
		step.ErrorCode, step.ErrorClass = event.ErrorCode, event.ErrorClass
		switch event.Kind {
		case inspection.StepCompleted:
			step.Status = inspection.StatusCompleted
		case inspection.StepCanceled:
			step.Status = inspection.StatusCanceled
		case inspection.StepUncertain:
			step.Status = inspection.StatusUncertain
		default:
			step.Status = inspection.StatusFailed
		}
	case inspection.RunCompleted, inspection.RunFailed, inspection.RunCanceled, inspection.RunSuspended, inspection.RunUncertain:
		run := r.runs[event.RunID]
		if run == nil || run.owner != event.Principal {
			return
		}
		run.value.FinishedAt = event.At
		run.value.Output = cloneRaw(event.Output)
		run.value.ErrorCode, run.value.ErrorClass = event.ErrorCode, event.ErrorClass
		switch event.Kind {
		case inspection.RunCompleted:
			run.value.Status = inspection.StatusCompleted
		case inspection.RunCanceled:
			run.value.Status = inspection.StatusCanceled
		case inspection.RunSuspended:
			run.value.Status = inspection.StatusSuspended
		case inspection.RunUncertain:
			run.value.Status = inspection.StatusUncertain
		default:
			run.value.Status = inspection.StatusFailed
		}
	}
}

// Inspect authorizes by the authenticated caller principal. An unauthorized
// caller receives the same result as an unknown run.
func (r *Recorder) Inspect(principal string, policy inspection.Policy, query inspection.Query) (inspection.Page, error) {
	if query.Version != inspection.Version {
		return inspection.Page{}, inspection.ErrUnsupportedVersion
	}
	if r == nil || principal == "" || query.RunID == "" {
		return inspection.Page{}, ErrNotFound
	}
	r.mu.RLock()
	run := r.runs[query.RunID]
	if run == nil || run.owner != principal {
		r.mu.RUnlock()
		return inspection.Page{}, ErrNotFound
	}
	value := cloneRun(run.value)
	steps := cloneSteps(run.steps)
	r.mu.RUnlock()

	limit := policy.MaxPageSize
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if query.Limit > 0 && query.Limit < limit {
		limit = query.Limit
	}
	offset, err := decodeCursor(query.Cursor, query.RunID, query.StepID)
	if err != nil {
		return inspection.Page{}, err
	}
	if query.StepID != "" {
		filtered := steps[:0]
		for _, item := range steps {
			if item.ID == query.StepID {
				filtered = append(filtered, item)
			}
		}
		steps = filtered
	}
	if offset > len(steps) {
		return inspection.Page{}, fmt.Errorf("inspection: invalid cursor")
	}
	end := min(offset+limit, len(steps))
	pageSteps := steps[offset:end]
	responseLimit := policy.MaxResponseBytes
	if responseLimit <= 0 || responseLimit > 1<<20 {
		responseLimit = 256 << 10
	}
	payloadLimit := responseLimit / (2*len(pageSteps) + 2)
	if policy.MaxPayloadBytes > 0 && policy.MaxPayloadBytes < payloadLimit {
		payloadLimit = policy.MaxPayloadBytes
	}
	if payloadLimit < 64 {
		payloadLimit = 64
	}
	policy.MaxPayloadBytes = payloadLimit
	page := inspection.Page{Version: inspection.Version, Run: projectRun(value, policy), Steps: projectSteps(pageSteps, policy)}
	if end < len(steps) {
		page.Next = encodeCursor(query.RunID, query.StepID, end)
	}
	encoded, err := json.Marshal(page)
	if err != nil {
		return inspection.Page{}, fmt.Errorf("inspection: cannot encode projection")
	}
	if len(encoded) > responseLimit {
		return inspection.Page{}, fmt.Errorf("inspection: response limit exceeded")
	}
	return page, nil
}

func projectRun(value inspection.Run, policy inspection.Policy) inspection.Run {
	if !policy.Fields[inspection.FieldInput] {
		value.Input = nil
	}
	if !policy.Fields[inspection.FieldOutput] {
		value.Output = nil
	}
	if !policy.Fields[inspection.FieldError] {
		value.ErrorCode, value.ErrorClass = "", ""
	}
	value.Input = bound(value.Input, policy.MaxPayloadBytes)
	value.Output = bound(value.Output, policy.MaxPayloadBytes)
	return value
}

func projectSteps(steps []inspection.Step, policy inspection.Policy) []inspection.Step {
	out := make([]inspection.Step, len(steps))
	for index, item := range steps {
		if !policy.Fields[inspection.FieldInput] {
			item.Input = nil
		}
		if !policy.Fields[inspection.FieldOutput] {
			item.Output = nil
		}
		if !policy.Fields[inspection.FieldError] {
			item.ErrorCode, item.ErrorClass = "", ""
		}
		item.Input = bound(item.Input, policy.MaxPayloadBytes)
		item.Output = bound(item.Output, policy.MaxPayloadBytes)
		out[index] = item
	}
	return out
}

func bound(raw json.RawMessage, maxBytes int) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	value, err := decode(raw)
	if err != nil {
		return json.RawMessage(`"[redacted: invalid payload]"`)
	}
	value = redact(value)
	encoded, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`"[redacted: unencodable payload]"`)
	}
	if maxBytes <= 0 || maxBytes > 1<<20 {
		maxBytes = 64 << 10
	}
	if len(encoded) > maxBytes {
		encoded, _ = json.Marshal(map[string]any{"$truncated": true, "originalBytes": len(encoded)})
	}
	return encoded
}

func decode(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

func redact(value any) any {
	switch item := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(item))
		for key, child := range item {
			if sensitive(key) {
				out[key] = "[redacted]"
			} else {
				out[key] = redact(child)
			}
		}
		return out
	case []any:
		out := make([]any, len(item))
		for index, child := range item {
			out[index] = redact(child)
		}
		return out
	default:
		return value
	}
}

func sensitive(key string) bool {
	key = strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(key, "_", ""), "-", ""))
	for _, marker := range []string{"password", "secret", "token", "authorization", "credential", "apikey", "privatekey"} {
		if strings.Contains(key, marker) {
			return true
		}
	}
	return false
}

type cursor struct {
	RunID  string `json:"r"`
	StepID string `json:"s"`
	Offset int    `json:"o"`
}

func encodeCursor(runID, stepID string, offset int) string {
	data, _ := json.Marshal(cursor{RunID: runID, StepID: stepID, Offset: offset})
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodeCursor(value, runID, stepID string) (int, error) {
	if value == "" {
		return 0, nil
	}
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return 0, fmt.Errorf("inspection: invalid cursor")
	}
	var decoded cursor
	if err := json.Unmarshal(data, &decoded); err != nil || decoded.RunID != runID || decoded.StepID != stepID || decoded.Offset < 0 {
		return 0, fmt.Errorf("inspection: invalid cursor")
	}
	return decoded.Offset, nil
}

func cloneEvent(event inspection.Event) inspection.Event {
	event.Input = cloneRaw(event.Input)
	event.Output = cloneRaw(event.Output)
	return event
}

func cloneRaw(raw json.RawMessage) json.RawMessage { return append(json.RawMessage(nil), raw...) }

func cloneRun(run inspection.Run) inspection.Run {
	run.Input, run.Output = cloneRaw(run.Input), cloneRaw(run.Output)
	return run
}

func cloneSteps(steps []inspection.Step) []inspection.Step {
	out := make([]inspection.Step, len(steps))
	for index, step := range steps {
		step.Input, step.Output = cloneRaw(step.Input), cloneRaw(step.Output)
		out[index] = step
	}
	return out
}
