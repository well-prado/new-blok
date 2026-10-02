package provider

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
)

// Keys are business identities, stable across retries; callers must not use attempt IDs.
type Keyed interface{ EffectKey() string }
type Port[I Keyed, O any] interface {
	Execute(context.Context, I) (O, error)
}

type HTTPInput struct {
	Key  string `json:"key"`
	Body string `json:"body"`
}

func (i HTTPInput) EffectKey() string { return i.Key }

type HTTPOutput struct {
	Status int    `json:"status"`
	Body   string `json:"body"`
}
type EmailInput struct {
	Key     string `json:"key"`
	To      string `json:"to"`
	Subject string `json:"subject"`
	Text    string `json:"text"`
}

func (i EmailInput) EffectKey() string { return i.Key }

type Receipt struct {
	ID string `json:"id"`
}
type PaymentInput struct {
	Key         string `json:"key"`
	Account     string `json:"account"`
	AmountCents int64  `json:"amountCents"`
	Currency    string `json:"currency"`
}

func (i PaymentInput) EffectKey() string { return i.Key }

type DatabaseInput struct {
	Key      string `json:"key"`
	RecordID string `json:"recordId"`
	Value    string `json:"value"`
}

func (i DatabaseInput) EffectKey() string { return i.Key }

type DatabaseOutput struct {
	RecordID string `json:"recordId"`
	EventID  string `json:"eventId"`
}
type PublishInput struct {
	Key     string `json:"key"`
	Topic   string `json:"topic"`
	Payload string `json:"payload"`
}

func (i PublishInput) EffectKey() string { return i.Key }

type AuditInput struct {
	Key     string `json:"key"`
	Action  string `json:"action"`
	Subject string `json:"subject"`
}

func (i AuditInput) EffectKey() string { return i.Key }

type GenerateInput struct {
	Key    string `json:"key"`
	Prompt string `json:"prompt"`
}

func (i GenerateInput) EffectKey() string { return i.Key }

type GenerateOutput struct {
	Value json.RawMessage `json:"value"`
}

// Endpoint is the synthetic/reference JSON protocol adapter for typed ports.
// Endpoint URL and credential headers belong to composition, never workflow input.
type Endpoint[I Keyed, O any] struct {
	transport HTTP
	url       string
	headers   http.Header
}

func NewEndpoint[I Keyed, O any](endpoint string, headers http.Header, transport HTTP) (*Endpoint[I, O], error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, &Error{Class: Invalid, Code: "invalid_endpoint"}
	}
	if err := transport.Manifest.Validate(); err != nil {
		return nil, err
	}
	return &Endpoint[I, O]{transport: transport, url: endpoint, headers: headers.Clone()}, nil
}
func (p *Endpoint[I, O]) Execute(ctx context.Context, input I) (O, error) {
	var output O
	body, err := json.Marshal(input)
	if err != nil {
		return output, &Error{Class: Invalid, Code: "invalid_input"}
	}
	headers := p.headers.Clone()
	if headers == nil {
		headers = make(http.Header)
	}
	headers.Set("Content-Type", "application/json")
	response, err := p.transport.Execute(ctx, Request{Method: http.MethodPost, URL: p.url, Headers: headers, Body: body, IdempotencyKey: input.EffectKey()})
	if err != nil {
		return output, err
	}
	// A remote response must never echo resolved credential values into output.
	for _, values := range p.headers {
		for _, value := range values {
			if value != "" && containsCredential(response.Body, value) {
				return output, &Error{Class: Uncertain, Code: "credential_in_response"}
			}
		}
	}
	dec := json.NewDecoder(bytes.NewReader(response.Body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&output); err != nil {
		return output, &Error{Class: Uncertain, Code: "invalid_output"}
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return output, &Error{Class: Uncertain, Code: "invalid_output"}
	}
	return output, nil
}
func containsCredential(body []byte, value string) bool {
	// Include the raw bearer/token component, not only the entire header.
	parts := append([]string{value}, strings.Fields(value)...)
	if strings.HasPrefix(value, "Basic ") {
		if decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(value, "Basic ")); err == nil {
			parts = append(parts, strings.SplitN(string(decoded), ":", 2)...)
		}
	}
	var decoded any
	_ = json.Unmarshal(body, &decoded)
	for _, part := range parts {
		if part != "Bearer" && part != "Basic" && len(part) > 0 && (bytes.Contains(body, []byte(part)) || valueContains(decoded, part)) {
			return true
		}
	}
	return false
}
func valueContains(value any, secret string) bool {
	switch v := value.(type) {
	case string:
		return strings.Contains(v, secret)
	case map[string]any:
		for k, value := range v {
			if strings.Contains(k, secret) || valueContains(value, secret) {
				return true
			}
		}
	case []any:
		for _, value := range v {
			if valueContains(value, secret) {
				return true
			}
		}
	}
	return false
}

// RequestEndpoint sends the body verbatim to a fixed HTTP POST endpoint.
type RequestEndpoint struct {
	endpoint *Endpoint[HTTPInput, HTTPOutput]
}

func NewRequestEndpoint(endpoint string, headers http.Header, transport HTTP) (*RequestEndpoint, error) {
	p, err := NewEndpoint[HTTPInput, HTTPOutput](endpoint, headers, transport)
	if err != nil {
		return nil, err
	}
	return &RequestEndpoint{endpoint: p}, nil
}
func (p *RequestEndpoint) Execute(ctx context.Context, in HTTPInput) (HTTPOutput, error) {
	response, err := p.endpoint.transport.Execute(ctx, Request{Method: http.MethodPost, URL: p.endpoint.url, Headers: p.endpoint.headers, Body: []byte(in.Body), IdempotencyKey: in.Key})
	if err != nil {
		return HTTPOutput{}, err
	}
	return HTTPOutput{Status: response.Status, Body: string(response.Body)}, nil
}
func Missing(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Func, reflect.Map, reflect.Slice:
		return v.IsNil()
	}
	return false
}
