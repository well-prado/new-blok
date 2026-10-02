// Package provider defines narrow injected effect providers. Providers own
// external protocol details; workflows own composition and retry policy.
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type ErrorClass string

const (
	Invalid   ErrorClass = "invalid"
	Business  ErrorClass = "business"
	Transient ErrorClass = "transient"
	Uncertain ErrorClass = "uncertain"
)

type Error struct {
	Class          ErrorClass
	Code           string
	Err            error
	IdempotencyKey string
}

func (e *Error) Error() string {
	if e.Err == nil {
		return string(e.Class) + ": " + e.Code
	}
	return string(e.Class) + ": " + e.Code + ": " + e.Err.Error()
}
func (e *Error) Unwrap() error { return e.Err }

type Manifest struct {
	Capabilities    []string
	SecretRefs      []string
	MaxRequestBytes int
}

func (m Manifest) Validate() error {
	if len(m.Capabilities) == 0 {
		return errors.New("provider: capability is required")
	}
	if m.MaxRequestBytes <= 0 {
		return errors.New("provider: request bound is required")
	}
	for _, ref := range m.SecretRefs {
		if ref == "" || strings.Contains(ref, "=") {
			return errors.New("provider: invalid secret ref")
		}
	}
	return nil
}

type Request struct {
	Method         string
	URL            string
	Body           []byte
	IdempotencyKey string
	Headers        http.Header
}
type Response struct {
	Status int
	Body   []byte
}
type Provider interface {
	Execute(context.Context, Request) (Response, error)
}

type HTTP struct {
	Client           *http.Client
	Manifest         Manifest
	MaxResponseBytes int
}

func (h HTTP) Execute(ctx context.Context, req Request) (Response, error) {
	if err := h.Manifest.Validate(); err != nil {
		return Response{}, err
	}
	if len(req.Body) > h.Manifest.MaxRequestBytes {
		return Response{}, &Error{Class: Invalid, Code: "request_too_large", IdempotencyKey: req.IdempotencyKey}
	}
	if req.URL == "" {
		return Response{}, &Error{Class: Invalid, Code: "missing_url", IdempotencyKey: req.IdempotencyKey}
	}
	client := h.Client
	if client == nil {
		client = http.DefaultClient
	}
	httpReq, err := http.NewRequestWithContext(ctx, req.Method, req.URL, bytes.NewReader(req.Body))
	if err != nil {
		return Response{}, &Error{Class: Invalid, Code: "invalid_request", Err: err, IdempotencyKey: req.IdempotencyKey}
	}
	httpReq.Header = req.Headers.Clone()
	if httpReq.Header == nil {
		httpReq.Header = make(http.Header)
	}
	if req.IdempotencyKey != "" {
		httpReq.Header.Set("Idempotency-Key", req.IdempotencyKey)
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		class := Transient
		if ctx.Err() != nil {
			class = Uncertain
		}
		return Response{}, &Error{Class: class, Code: "transport", Err: err, IdempotencyKey: req.IdempotencyKey}
	}
	defer resp.Body.Close()
	max := h.MaxResponseBytes
	if max <= 0 {
		max = 1 << 20
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, int64(max)+1))
	if readErr != nil {
		return Response{}, &Error{Class: Uncertain, Code: "response_read", Err: readErr, IdempotencyKey: req.IdempotencyKey}
	}
	if len(body) > max {
		return Response{}, &Error{Class: Uncertain, Code: "response_too_large", IdempotencyKey: req.IdempotencyKey}
	}
	if resp.StatusCode >= 500 || resp.StatusCode == 408 || resp.StatusCode == 429 {
		return Response{Status: resp.StatusCode, Body: body}, &Error{Class: Transient, Code: http.StatusText(resp.StatusCode), IdempotencyKey: req.IdempotencyKey}
	}
	if resp.StatusCode >= 400 {
		return Response{Status: resp.StatusCode, Body: body}, &Error{Class: Business, Code: http.StatusText(resp.StatusCode), IdempotencyKey: req.IdempotencyKey}
	}
	return Response{Status: resp.StatusCode, Body: body}, nil
}

type Effect struct {
	Provider Provider
	Manifest Manifest
}

func (e Effect) Execute(ctx context.Context, req Request) (Response, error) {
	if e.Provider == nil {
		return Response{}, errors.New("provider: missing provider")
	}
	if err := e.Manifest.Validate(); err != nil {
		return Response{}, err
	}
	return e.Provider.Execute(ctx, req)
}
func JSONBody(value any) ([]byte, error) { return json.Marshal(value) }
func WithTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, d)
}
func IsClass(err error, class ErrorClass) bool {
	var target *Error
	return errors.As(err, &target) && target.Class == class
}
func RedactedError(err error) string {
	var target *Error
	if errors.As(err, &target) {
		return fmt.Sprintf("%s:%s", target.Class, target.Code)
	}
	return "provider:error"
}
