// Package inspection defines the versioned, transport-neutral inspection contract.
package inspection

import (
	"encoding/json"
	"errors"
	"time"
)

const Version = "inspection/v1"

var (
	ErrUnsupportedVersion = errors.New("inspection: unsupported API version")
	ErrInvalidEvent       = errors.New("inspection: invalid event")
)

type Kind string

const (
	RunStarted     Kind = "run.started"
	RunCompleted   Kind = "run.completed"
	RunFailed      Kind = "run.failed"
	RunCanceled    Kind = "run.canceled"
	RunSuspended   Kind = "run.suspended"
	RunUncertain   Kind = "run.uncertain"
	StepProcessing Kind = "step.processing"
	StepCompleted  Kind = "step.completed"
	StepFailed     Kind = "step.failed"
	StepCanceled   Kind = "step.canceled"
	StepUncertain  Kind = "step.uncertain"
)

type Status string

const (
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusCanceled  Status = "canceled"
	StatusSuspended Status = "suspended"
	StatusUncertain Status = "uncertain"
)

// Invocation is trusted execution metadata. Principal must come from the
// application's authenticated execution context, never from workflow input.
type Invocation struct {
	RunID      string
	Principal  string
	ParentRun  string
	ParentStep string
}

// Event is an immutable observation emitted by the engine. Payloads are JSON
// snapshots and are copied by both the emitter and the receiving recorder.
type Event struct {
	Kind       Kind            `json:"kind"`
	RunID      string          `json:"runId"`
	Principal  string          `json:"principal,omitempty"`
	Workflow   string          `json:"workflow,omitempty"`
	ParentRun  string          `json:"parentRun,omitempty"`
	ParentStep string          `json:"parentStep,omitempty"`
	StepID     string          `json:"stepId,omitempty"`
	Attempt    int             `json:"attempt,omitempty"`
	At         time.Time       `json:"at"`
	Input      json.RawMessage `json:"input,omitempty"`
	Output     json.RawMessage `json:"output,omitempty"`
	ErrorCode  string          `json:"errorCode,omitempty"`
	ErrorClass string          `json:"errorClass,omitempty"`
}

type Observer interface{ Observe(Event) }

type Field string

const (
	FieldInput  Field = "input"
	FieldOutput Field = "output"
	FieldError  Field = "error"
)

type Query struct {
	Version string `json:"version"`
	RunID   string `json:"runId"`
	StepID  string `json:"stepId,omitempty"`
	Cursor  string `json:"cursor,omitempty"`
	Limit   int    `json:"limit,omitempty"`
}

type Policy struct {
	Fields           map[Field]bool
	MaxPageSize      int
	MaxPayloadBytes  int
	MaxResponseBytes int
}

type Run struct {
	ID         string          `json:"id"`
	Workflow   string          `json:"workflow,omitempty"`
	ParentRun  string          `json:"parentRun,omitempty"`
	ParentStep string          `json:"parentStep,omitempty"`
	Status     Status          `json:"status"`
	StartedAt  time.Time       `json:"startedAt"`
	FinishedAt time.Time       `json:"finishedAt,omitempty"`
	Input      json.RawMessage `json:"input,omitempty"`
	Output     json.RawMessage `json:"output,omitempty"`
	ErrorCode  string          `json:"errorCode,omitempty"`
	ErrorClass string          `json:"errorClass,omitempty"`
}

type Step struct {
	ID         string          `json:"id"`
	Status     Status          `json:"status"`
	Attempt    int             `json:"attempt"`
	StartedAt  time.Time       `json:"startedAt"`
	FinishedAt time.Time       `json:"finishedAt,omitempty"`
	Input      json.RawMessage `json:"input,omitempty"`
	Output     json.RawMessage `json:"output,omitempty"`
	ErrorCode  string          `json:"errorCode,omitempty"`
	ErrorClass string          `json:"errorClass,omitempty"`
}

type Page struct {
	Version string `json:"version"`
	Run     Run    `json:"run"`
	Steps   []Step `json:"steps"`
	Next    string `json:"next,omitempty"`
}
