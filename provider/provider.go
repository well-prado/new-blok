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
	"net/url"
	"regexp"
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
	Class ErrorClass
	Code  string
	// Err is retained for source compatibility only; reference adapters never populate it.
	Err            error
	IdempotencyKey string
}

func (e *Error) Error() string {
	return RedactedError(e)
}

// Causes are intentionally not exposed: url.Error and SDK errors often hold credentials.
func (e *Error) IsUncertain() bool { return e.Class == Uncertain }
func (e *Error) IsRetryable() bool { return e.Class == Transient }

type Manifest struct {
	Capabilities     []string
	SecretRefs       []string
	MaxRequestBytes  int
	MaxResponseBytes int
	Timeout          time.Duration
}

func (m Manifest) Validate() error {
	if len(m.Capabilities) > 64 || len(m.SecretRefs) > 64 {
		return errors.New("provider: metadata bound exceeded")
	}
	if len(m.Capabilities) == 0 {
		return errors.New("provider: capability is required")
	}
	if m.MaxRequestBytes <= 0 || m.MaxRequestBytes > 1<<20 {
		return errors.New("provider: request bound is required")
	}
	if m.MaxResponseBytes < 0 || m.MaxResponseBytes > 1<<20 || m.Timeout < 0 || m.Timeout > time.Minute {
		return errors.New("provider: invalid response or deadline bound")
	}
	for _, capability := range m.Capabilities {
		if !opaqueName.MatchString(capability) {
			return errors.New("provider: invalid capability")
		}
	}
	for _, ref := range m.SecretRefs {
		if !opaqueName.MatchString(ref) {
			return errors.New("provider: invalid secret ref")
		}
	}
	return nil
}

var opaqueName = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_:/.-]{0,127}$`)

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
	if ctx.Err() != nil {
		return Response{}, &Error{Class: Invalid, Code: "canceled_before_dispatch"}
	}
	u, parseErr := url.Parse(req.URL)
	if parseErr != nil || u.User != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return Response{}, &Error{Class: Invalid, Code: "invalid_url"}
	}
	if len(req.URL) > 8192 || len(req.IdempotencyKey) > 256 || strings.ContainsAny(req.IdempotencyKey, "\r\n") {
		return Response{}, &Error{Class: Invalid, Code: "request_too_large"}
	}
	headerBytes := 0
	for k, values := range req.Headers {
		headerBytes += len(k)
		for _, v := range values {
			headerBytes += len(v)
		}
	}
	if headerBytes > 8192 {
		return Response{}, &Error{Class: Invalid, Code: "headers_too_large"}
	}
	timeout := h.Manifest.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if req.URL == "" {
		return Response{}, &Error{Class: Invalid, Code: "missing_url", IdempotencyKey: req.IdempotencyKey}
	}
	client := h.Client
	if client == nil {
		client = http.DefaultClient
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	client = &copyClient
	httpReq, err := http.NewRequestWithContext(ctx, req.Method, req.URL, bytes.NewReader(req.Body))
	if err != nil {
		return Response{}, &Error{Class: Invalid, Code: "invalid_request", IdempotencyKey: req.IdempotencyKey}
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
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		return Response{}, &Error{Class: Uncertain, Code: "transport", IdempotencyKey: req.IdempotencyKey}
	}
	defer resp.Body.Close()
	max := h.MaxResponseBytes
	if max <= 0 {
		max = h.Manifest.MaxResponseBytes
		if max == 0 {
			max = 1 << 20
		}
	}
	if max > 1<<20 {
		max = 1 << 20
	}
	if h.Manifest.MaxResponseBytes > 0 && max > h.Manifest.MaxResponseBytes {
		max = h.Manifest.MaxResponseBytes
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, int64(max)+1))
	if readErr != nil {
		return Response{}, &Error{Class: Uncertain, Code: "response_read", IdempotencyKey: req.IdempotencyKey}
	}
	if len(body) > max {
		return Response{}, &Error{Class: Uncertain, Code: "response_too_large", IdempotencyKey: req.IdempotencyKey}
	}
	if resp.StatusCode >= 500 || resp.StatusCode == 408 {
		return Response{}, &Error{Class: Uncertain, Code: "remote_outcome_unknown", IdempotencyKey: req.IdempotencyKey}
	}
	if resp.StatusCode == 429 {
		return Response{}, &Error{Class: Transient, Code: "rate_limited", IdempotencyKey: req.IdempotencyKey}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Response{}, &Error{Class: Business, Code: "remote_rejected", IdempotencyKey: req.IdempotencyKey}
	}
	for name, values := range req.Headers {
		if strings.EqualFold(name, "Content-Type") || strings.EqualFold(name, "Accept") {
			continue
		}
		for _, value := range values {
			if containsCredential(body, value) {
				return Response{}, &Error{Class: Uncertain, Code: "credential_in_response", IdempotencyKey: req.IdempotencyKey}
			}
		}
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
		class := target.Class
		switch class {
		case Invalid, Business, Transient, Uncertain:
		default:
			class = Uncertain
		}
		code := target.Code
		switch code {
		case "request_too_large", "canceled_before_dispatch", "invalid_url", "headers_too_large", "missing_url", "invalid_request", "transport", "response_read", "response_too_large", "remote_outcome_unknown", "rate_limited", "remote_rejected", "credential_in_response", "invalid_input", "invalid_output", "invalid_endpoint", "database_startup", "database_failed", "effect_failed", "deadline_after_dispatch", "provider_panic":
		default:
			code = "effect_failed"
		}
		return fmt.Sprintf("%s:%s", class, code)
	}
	return "provider:error"
}
