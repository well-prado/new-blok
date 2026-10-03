// Package inspection defines the versioned, transport-neutral inspection contract.
package inspection

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

const Version = "inspection/v1"

// Projection envelope limits keep per-step history bounded inside every page.
// At most five recent attempts and five recent logs are projected per step.
const (
	MinPayloadBytes      = 64
	MaxProjectedAttempts = 5
	MaxProjectedLogs     = 4
)

var (
	ErrUnsupportedVersion = errors.New("inspection: unsupported API version")
	ErrInvalidEvent       = errors.New("inspection: invalid event")
	ErrPayloadLimit       = errors.New("inspection: payload limit is below the minimum projection envelope")
	ErrResponseLimit      = errors.New("inspection: response limit cannot fit the minimum projection envelope")
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
	StepLog        Kind = "step.log"
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
	AttemptID  string
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
	AttemptID  string          `json:"attemptId,omitempty"`
	At         time.Time       `json:"at"`
	Input      json.RawMessage `json:"input,omitempty"`
	Output     json.RawMessage `json:"output,omitempty"`
	ErrorCode  string          `json:"errorCode,omitempty"`
	ErrorClass string          `json:"errorClass,omitempty"`
	LogLevel   string          `json:"logLevel,omitempty"`
	LogMessage string          `json:"logMessage,omitempty"`
	LogAttrs   json.RawMessage `json:"logAttrs,omitempty"`
}

type Observer interface{ Observe(Event) }

type Field string

const (
	FieldInput  Field = "input"
	FieldOutput Field = "output"
	FieldError  Field = "error"
	FieldLogs   Field = "logs"
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
	AllowBlobRead    bool
	MaxBlobBytes     int
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
	ID                string          `json:"id"`
	Status            Status          `json:"status"`
	Attempt           int             `json:"attempt"`
	StartedAt         time.Time       `json:"startedAt"`
	FinishedAt        time.Time       `json:"finishedAt,omitempty"`
	Input             json.RawMessage `json:"input,omitempty"`
	Output            json.RawMessage `json:"output,omitempty"`
	ErrorCode         string          `json:"errorCode,omitempty"`
	ErrorClass        string          `json:"errorClass,omitempty"`
	Attempts          []Attempt       `json:"attempts,omitempty"`
	AttemptsTruncated bool            `json:"attemptsTruncated,omitempty"`
	Logs              []Log           `json:"logs,omitempty"`
	LogsTruncated     bool            `json:"logsTruncated,omitempty"`
}

type Attempt struct {
	ID         string          `json:"id"`
	Number     int             `json:"number"`
	Status     Status          `json:"status"`
	StartedAt  time.Time       `json:"startedAt"`
	FinishedAt time.Time       `json:"finishedAt,omitempty"`
	Input      json.RawMessage `json:"input,omitempty"`
	Output     json.RawMessage `json:"output,omitempty"`
	ErrorCode  string          `json:"errorCode,omitempty"`
	ErrorClass string          `json:"errorClass,omitempty"`
}

type Log struct {
	At      time.Time       `json:"at"`
	Level   string          `json:"level"`
	Message string          `json:"message"`
	Attrs   json.RawMessage `json:"attrs,omitempty"`
}

type Page struct {
	Version   string `json:"version"`
	Run       Run    `json:"run"`
	Steps     []Step `json:"steps"`
	Next      string `json:"next,omitempty"`
	Truncated bool   `json:"truncated,omitempty"`
}

// Source is a bounded authorized read port implemented by durable adapters.
// Implementations must authorize principal before reading payloads and apply
// step filters/offset/limit in the backing query before materializing rows.
type Source interface {
	ReadInspection(context.Context, string, string, string, int, int, map[Field]bool, int) (Run, []Step, int, error)
}
