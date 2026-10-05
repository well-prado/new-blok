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
	"github.com/well-prado/new-blok/contract/observe"
)

var ErrNotFound = errors.New("inspection: run not found")

const maxCursorEncodedBytes = 1024

type runRecord struct {
	owner     string
	value     inspection.Run
	steps     []inspection.Step
	byStep    map[string]int
	truncated bool
}

type RecorderLimits struct{ MaxRuns, MaxEvents, MaxRetainedBytes, MaxStepsPerRun, MaxEventBytes int }
type RecorderStats struct {
	Runs, Events, RetainedBytes, DroppedEvents int
	Saturated                                  bool
}

// Recorder accepts observations from the real engine and keeps immutable
// snapshots for development inspection. It is intentionally process-local.
type Recorder struct {
	mu     sync.RWMutex
	runs   map[string]*runRecord
	limits RecorderLimits
	stats  RecorderStats
}

func NewRecorder() *Recorder { return NewRecorderWithLimits(RecorderLimits{}) }
func NewRecorderWithLimits(l RecorderLimits) *Recorder {
	if l.MaxRuns <= 0 {
		l.MaxRuns = 1024
	}
	if l.MaxEvents <= 0 {
		l.MaxEvents = 100000
	}
	if l.MaxRetainedBytes <= 0 {
		l.MaxRetainedBytes = 64 << 20
	}
	if l.MaxStepsPerRun <= 0 {
		l.MaxStepsPerRun = 4096
	}
	if l.MaxEventBytes <= 0 {
		l.MaxEventBytes = 64 << 10
	}
	return &Recorder{runs: make(map[string]*runRecord), limits: l}
}
func (r *Recorder) Stats() RecorderStats {
	if r == nil {
		return RecorderStats{}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	stats := r.stats
	stats.Runs = len(r.runs)
	return stats
}

func (r *Recorder) Observe(event inspection.Event) {
	if r == nil || event.RunID == "" || event.At.IsZero() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, identity := range []string{event.RunID, event.Principal, event.Workflow, event.ParentRun, event.ParentStep, event.StepID, event.AttemptID} {
		if len(identity) > 256 {
			r.saturate(event.RunID)
			return
		}
	}
	if len(event.Kind) > 32 {
		r.saturate(event.RunID)
		return
	}
	event.ErrorCode = safeLabel(event.ErrorCode, 64)
	event.ErrorClass = safeLabel(event.ErrorClass, 64)
	event.LogLevel = safeLabel(event.LogLevel, 16)
	event.LogMessage = redactLogMessage(boundedText(event.LogMessage, 1024))
	if len(event.Input) > r.limits.MaxEventBytes/2 {
		event.Input = json.RawMessage(`{"$truncated":true}`)
	}
	if len(event.Output) > r.limits.MaxEventBytes/2 {
		event.Output = json.RawMessage(`{"$truncated":true}`)
	}
	if len(event.LogAttrs) > r.limits.MaxEventBytes/2 {
		event.LogAttrs = json.RawMessage(`{"$truncated":true}`)
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		r.saturate(event.RunID)
		return
	}
	if len(encoded) > r.limits.MaxEventBytes {
		event.Input = nil
		event.Output = nil
		event.LogAttrs = nil
		event.LogMessage = "[truncated: event exceeded ingestion limit]"
		encoded, _ = json.Marshal(event)
	}
	cost := len(encoded) * 3
	if r.stats.Events >= r.limits.MaxEvents || len(encoded) > r.limits.MaxEventBytes || r.stats.RetainedBytes+cost > r.limits.MaxRetainedBytes {
		r.saturate(event.RunID)
		return
	}
	event.Input = sanitizeRaw(event.Input)
	event.Output = sanitizeRaw(event.Output)
	event.LogAttrs = sanitizeRaw(event.LogAttrs)
	event = cloneEvent(event)
	r.stats.Events++
	r.stats.RetainedBytes += cost
	switch event.Kind {
	case inspection.RunStarted:
		if event.Principal == "" || r.runs[event.RunID] != nil || len(r.runs) >= r.limits.MaxRuns {
			r.saturate(event.RunID)
			return
		}
		r.runs[event.RunID] = &runRecord{
			owner:  event.Principal,
			value:  inspection.Run{ID: event.RunID, Workflow: event.Workflow, ParentRun: event.ParentRun, ParentStep: event.ParentStep, Status: inspection.StatusRunning, StartedAt: event.At, Input: cloneRaw(event.Input)},
			byStep: make(map[string]int),
		}
	case inspection.StepProcessing, inspection.StepLog, inspection.StepCompleted, inspection.StepFailed, inspection.StepCanceled, inspection.StepUncertain:
		run := r.runs[event.RunID]
		if run == nil || run.owner != event.Principal || event.StepID == "" {
			return
		}
		index, exists := run.byStep[event.StepID]
		if !exists {
			if len(run.steps) >= r.limits.MaxStepsPerRun {
				run.truncated = true
				r.stats.DroppedEvents++
				r.stats.Saturated = true
				return
			}
			index = len(run.steps)
			run.byStep[event.StepID] = index
			run.steps = append(run.steps, inspection.Step{ID: event.StepID})
		}
		step := &run.steps[index]
		if event.Kind == inspection.StepLog {
			if len(step.Logs) < 100 {
				step.Logs = append(step.Logs, inspection.Log{At: event.At, Level: event.LogLevel, Message: boundedText(event.LogMessage, 1024), Attrs: cloneRaw(event.LogAttrs)})
			} else {
				step.LogsTruncated = true
				r.saturate(event.RunID)
			}
			return
		}
		if event.Attempt > step.Attempt {
			step.Attempt = event.Attempt
		}
		if event.Kind == inspection.StepProcessing {
			step.Status = inspection.StatusRunning
			step.StartedAt = event.At
			step.Input = cloneRaw(event.Input)
			attemptID := event.AttemptID
			if attemptID == "" {
				attemptID = fmt.Sprintf("%s/%d", event.StepID, event.Attempt)
			}
			if len(step.Attempts) < 100 {
				step.Attempts = append(step.Attempts, inspection.Attempt{ID: boundedText(attemptID, 160), Number: event.Attempt, Status: inspection.StatusRunning, StartedAt: event.At, Input: cloneRaw(event.Input)})
			} else {
				step.AttemptsTruncated = true
				run.truncated = true
				r.stats.Saturated = true
			}
			return
		}
		step.FinishedAt = event.At
		step.Output = cloneRaw(event.Output)
		step.ErrorCode, step.ErrorClass = boundedText(event.ErrorCode, 64), boundedText(event.ErrorClass, 64)
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
		for i := len(step.Attempts) - 1; i >= 0; i-- {
			if step.Attempts[i].ID == event.AttemptID || event.AttemptID == "" && step.Attempts[i].Number == event.Attempt {
				attempt := &step.Attempts[i]
				attempt.FinishedAt = event.At
				attempt.Output = cloneRaw(event.Output)
				attempt.ErrorCode = step.ErrorCode
				attempt.ErrorClass = step.ErrorClass
				attempt.Status = step.Status
				break
			}
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

func boundedText(value string, max int) string {
	if len(value) > max {
		return value[:max]
	}
	return value
}

func redactLogMessage(message string) string { return observe.RedactLogMessage(message) }

func safeLabel(value string, max int) string {
	if value == "" {
		return ""
	}
	if len(value) > max {
		return "untrusted_label"
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.') {
			return "untrusted_label"
		}
	}
	return value
}

func (r *Recorder) saturate(runID string) {
	r.stats.DroppedEvents++
	r.stats.Saturated = true
	if run := r.runs[runID]; run != nil {
		run.truncated = true
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
	limit := policy.MaxPageSize
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if query.Limit > 0 && query.Limit < limit {
		limit = query.Limit
	}
	offset, err := decodeCursor(query.Cursor, query.RunID, query.StepID)
	if err != nil {
		r.mu.RUnlock()
		return inspection.Page{}, err
	}
	count := 0
	for _, item := range run.steps {
		if query.StepID == "" || item.ID == query.StepID {
			count++
		}
	}
	if offset > count {
		r.mu.RUnlock()
		return inspection.Page{}, fmt.Errorf("inspection: invalid cursor")
	}
	end := min(offset+limit, count)
	selected := make([]*inspection.Step, 0, end-offset)
	projectedAttempts, projectedLogs := 0, 0
	position := 0
	for i := range run.steps {
		item := &run.steps[i]
		if query.StepID != "" && item.ID != query.StepID {
			continue
		}
		if position >= offset && position < end {
			selected = append(selected, item)
			projectedAttempts += min(len(item.Attempts), inspection.MaxProjectedAttempts)
			projectedLogs += min(len(item.Logs), inspection.MaxProjectedLogs)
		}
		position++
	}
	pagePolicy, err := boundedProjectionPolicy(policy, len(selected), projectedAttempts, projectedLogs)
	if err != nil {
		r.mu.RUnlock()
		return inspection.Page{}, err
	}
	responseLimit := pagePolicy.MaxResponseBytes
	if responseLimit <= 0 || responseLimit > 1<<20 {
		responseLimit = 256 << 10
	}
	pageSteps := make([]inspection.Step, len(selected))
	for i, item := range selected {
		pageSteps[i] = projectStep(*item, pagePolicy)
	}
	value := projectRun(run.value, pagePolicy)
	truncated := run.truncated
	r.mu.RUnlock()
	page := inspection.Page{Version: inspection.Version, Run: value, Steps: pageSteps}
	page.Truncated = truncated
	if end < count {
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
		out[index] = projectStep(item, policy)
	}
	return out
}

func projectStep(item inspection.Step, policy inspection.Policy) inspection.Step {
	if len(item.Attempts) > inspection.MaxProjectedAttempts {
		item.AttemptsTruncated = true
		item.Attempts = item.Attempts[len(item.Attempts)-inspection.MaxProjectedAttempts:]
	}
	if len(item.Logs) > inspection.MaxProjectedLogs {
		item.LogsTruncated = true
		item.Logs = item.Logs[len(item.Logs)-inspection.MaxProjectedLogs:]
	}
	out := inspection.Step{
		ID: item.ID, Status: item.Status, Attempt: item.Attempt,
		StartedAt: item.StartedAt, FinishedAt: item.FinishedAt,
		AttemptsTruncated: item.AttemptsTruncated, LogsTruncated: item.LogsTruncated,
	}
	if policy.Fields[inspection.FieldInput] {
		out.Input = bound(item.Input, policy.MaxPayloadBytes)
	}
	if policy.Fields[inspection.FieldOutput] {
		out.Output = bound(item.Output, policy.MaxPayloadBytes)
	}
	if policy.Fields[inspection.FieldError] {
		out.ErrorCode, out.ErrorClass = item.ErrorCode, item.ErrorClass
	}
	out.Attempts = make([]inspection.Attempt, len(item.Attempts))
	for i, attempt := range item.Attempts {
		out.Attempts[i] = attempt
		if policy.Fields[inspection.FieldInput] {
			out.Attempts[i].Input = bound(attempt.Input, policy.MaxPayloadBytes)
		} else {
			out.Attempts[i].Input = nil
		}
		if policy.Fields[inspection.FieldOutput] {
			out.Attempts[i].Output = bound(attempt.Output, policy.MaxPayloadBytes)
		} else {
			out.Attempts[i].Output = nil
		}
		if !policy.Fields[inspection.FieldError] {
			out.Attempts[i].ErrorCode, out.Attempts[i].ErrorClass = "", ""
		}
	}
	if policy.Fields[inspection.FieldLogs] {
		out.Logs = make([]inspection.Log, len(item.Logs))
		for i, log := range item.Logs {
			out.Logs[i] = log
			out.Logs[i].Message = boundedText(log.Message, min(1024, policy.MaxPayloadBytes))
			out.Logs[i].Attrs = bound(log.Attrs, policy.MaxPayloadBytes)
		}
	}
	return out
}

func boundedProjectionPolicy(policy inspection.Policy, stepCount, attemptCount, logCount int) (inspection.Policy, error) {
	if policy.MaxPayloadBytes > 0 && policy.MaxPayloadBytes < inspection.MinPayloadBytes {
		return inspection.Policy{}, inspection.ErrPayloadLimit
	}
	responseLimit := policy.MaxResponseBytes
	if responseLimit <= 0 || responseLimit > 1<<20 {
		responseLimit = 256 << 10
	}
	inputOutputFields := 0
	if policy.Fields[inspection.FieldInput] {
		inputOutputFields++
	}
	if policy.Fields[inspection.FieldOutput] {
		inputOutputFields++
	}
	slots := inputOutputFields + stepCount*inputOutputFields + attemptCount*inputOutputFields
	if policy.Fields[inspection.FieldLogs] {
		slots += 2 * logCount // bounded message and structured attributes
	}
	payloadLimit := min(64<<10, responseLimit)
	if slots > 0 {
		payloadLimit = responseLimit / slots
	}
	if policy.MaxPayloadBytes > 0 && policy.MaxPayloadBytes < payloadLimit {
		payloadLimit = policy.MaxPayloadBytes
	}
	if slots > 0 && payloadLimit < inspection.MinPayloadBytes {
		return inspection.Policy{}, inspection.ErrResponseLimit
	}
	policy.MaxPayloadBytes = payloadLimit
	return policy, nil
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

func sanitizeRaw(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	value, err := decode(raw)
	if err != nil {
		return json.RawMessage(`"[redacted: invalid payload]"`)
	}
	encoded, err := json.Marshal(redact(value))
	if err != nil {
		return json.RawMessage(`"[redacted: unencodable payload]"`)
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
	if len(value) > maxCursorEncodedBytes {
		return 0, fmt.Errorf("inspection: invalid cursor")
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
	event.LogAttrs = cloneRaw(event.LogAttrs)
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
		step.Attempts = append([]inspection.Attempt(nil), step.Attempts...)
		for i := range step.Attempts {
			step.Attempts[i].Input = cloneRaw(step.Attempts[i].Input)
			step.Attempts[i].Output = cloneRaw(step.Attempts[i].Output)
		}
		step.Logs = append([]inspection.Log(nil), step.Logs...)
		for i := range step.Logs {
			step.Logs[i].Attrs = cloneRaw(step.Logs[i].Attrs)
		}
		out[index] = step
	}
	return out
}

func cloneStep(step inspection.Step) inspection.Step { return cloneSteps([]inspection.Step{step})[0] }
