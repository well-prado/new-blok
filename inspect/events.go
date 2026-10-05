package inspect

import (
	"encoding/json"
	"errors"

	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/contract/observe"
	"github.com/well-prado/new-blok/observe/event"
)

// Capture selects which sensitive observation content the live stream keeps.
// The zero value keeps none: a stream composed without an explicit choice
// carries transitions, timing, attempts and classified error labels only.
// Content that is not captured is discarded before it is retained, so no
// reader policy can expose it later.
type Capture struct {
	Inputs  bool
	Outputs bool
	Logs    bool
}

// EventStreamConfig composes a live development event stream. It is
// selected explicitly by the application (app.Config.Inspection, alone or
// with CombineObservers); an application that does not select it has no
// stream and no capture.
type EventStreamConfig struct {
	Hub     event.Config
	Capture Capture
}

// MinEventBytes is the smallest MaxEventBytes an EventStream accepts, so an
// observation without payloads always fits.
const MinEventBytes = 2048

// EventStream is an inspection.Observer that feeds a bounded event.Hub. It is
// telemetry, not state: the journal and the run's terminal outcome port are
// the reliable record, and nothing here can delay or fail them.
type EventStream struct {
	hub     *event.Hub
	capture Capture
	max     int
}

func NewEventStream(config EventStreamConfig) (*EventStream, error) {
	hub, err := event.New(config.Hub)
	if err != nil {
		return nil, err
	}
	if hub.Config().MaxEventBytes < MinEventBytes {
		return nil, errors.New("inspection: event stream MaxEventBytes must be at least 2048")
	}
	return &EventStream{hub: hub, capture: config.Capture, max: hub.Config().MaxEventBytes}, nil
}

// Hub exposes the underlying hub, for Stats and for Close at shutdown.
func (s *EventStream) Hub() *event.Hub { return s.hub }

// Capture reports the stream's capture selection.
func (s *EventStream) Capture() Capture { return s.capture }

var truncatedPayload = json.RawMessage(`{"$truncated":true}`)

// Observe is called synchronously by the engine on the run's goroutine. It
// does bounded local work only: no I/O, no wait on any reader.
func (s *EventStream) Observe(value inspection.Event) {
	if s == nil || s.hub == nil || value.At.IsZero() || len(value.Kind) > 32 {
		return
	}
	for _, identity := range []string{value.RunID, value.Principal, value.Workflow, value.ParentRun, value.ParentStep, value.StepID, value.AttemptID} {
		if len(identity) > event.MaxIdentityBytes {
			s.hub.Drop(value.RunID, value.Principal)
			return
		}
	}
	if value.RunID == "" || value.Principal == "" {
		return
	}
	if value.Kind == inspection.StepLog && !s.capture.Logs {
		return
	}
	owner := value.Principal
	// The owner authorizes readers; it is never put on the wire.
	value.Principal = ""
	value.ErrorCode = safeLabel(value.ErrorCode, 64)
	value.ErrorClass = safeLabel(value.ErrorClass, 64)
	value.LogLevel = safeLabel(value.LogLevel, 16)
	value.LogMessage = redactLogMessage(boundedText(value.LogMessage, 1024))
	value.Input = captured(s.capture.Inputs, value.Input, s.max/2)
	value.Output = captured(s.capture.Outputs, value.Output, s.max/2)
	value.LogAttrs = captured(s.capture.Logs, value.LogAttrs, s.max/2)
	data, err := json.Marshal(value)
	if err == nil && len(data) > s.max {
		value.Input = truncatedIfSet(value.Input)
		value.Output = truncatedIfSet(value.Output)
		value.LogAttrs = truncatedIfSet(value.LogAttrs)
		data, err = json.Marshal(value)
	}
	if err != nil || len(data) > s.max {
		s.hub.Drop(value.RunID, owner)
		return
	}
	item := event.Item{Name: string(value.Kind), Data: data}
	switch value.Kind {
	case inspection.RunStarted:
		item.Start = true
	case inspection.RunCompleted, inspection.RunFailed, inspection.RunCanceled, inspection.RunUncertain:
		item.Terminal = true
	case inspection.StepLog:
		// A worker log is delivered off its call's result path and can land
		// after the run's terminal transition (ADR 0016, #226).
		item.Late = true
	}
	_, _ = s.hub.Publish(value.RunID, owner, item)
}

// captured keeps a redacted copy of raw when the capture is selected and it
// fits half an event; otherwise it is replaced or dropped.
func captured(selected bool, raw json.RawMessage, limit int) json.RawMessage {
	if !selected || len(raw) == 0 {
		return nil
	}
	if len(raw) > limit {
		return truncatedPayload
	}
	return sanitizeRaw(raw)
}

func truncatedIfSet(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	return truncatedPayload
}

// CombineObservers fans one observation out to several observers, each with
// its own copy of the payload bytes.
func CombineObservers(observers ...inspection.Observer) inspection.Observer {
	selected := make(observerGroup, 0, len(observers))
	for _, observer := range observers {
		if observer != nil {
			selected = append(selected, observer)
		}
	}
	switch len(selected) {
	case 0:
		return nil
	case 1:
		return selected[0]
	}
	return selected
}

type observerGroup []inspection.Observer

func (group observerGroup) Observe(value inspection.Event) {
	for _, observer := range group {
		observer.Observe(cloneEvent(value))
	}
}

// ObservesPayloads reports whether any member reads payloads, so the engine
// skips serializing them only when no member would see them (ADR 0020).
func (group observerGroup) ObservesPayloads() bool {
	for _, member := range group {
		declared, ok := member.(observe.PayloadObserver)
		if !ok || declared.ObservesPayloads() {
			return true
		}
	}
	return false
}
