// Package signal defines the authenticated durable-signal boundary.
package signal

import (
	"encoding/json"
	"errors"
)

var ErrUnauthorized = errors.New("signal: unauthorized")

type Envelope struct {
	RunID     string          `json:"runId"`
	SignalID  string          `json:"signalId"`
	Name      string          `json:"name"`
	Payload   json.RawMessage `json:"payload"`
	Principal string          `json:"principal"`
}

type Authorizer interface {
	Authorize(Envelope) bool
}

func (e Envelope) Validate() error {
	if e.RunID == "" || e.SignalID == "" || e.Name == "" || e.Principal == "" || !json.Valid(e.Payload) {
		return errors.New("signal: run, signal ID, name, principal and valid JSON payload are required")
	}
	return nil
}
