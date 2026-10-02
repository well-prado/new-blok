// Package http provides the native HTTP binding and shared admission boundary.
package http

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/internal/engine"
)

type Principal struct {
	ID    string
	Roles []string
}

type Input struct {
	Body      []byte
	Params    map[string]string
	Query     map[string][]string
	Principal Principal
}

type Endpoint struct {
	Method       string
	Path         string
	InputSchema  []byte
	MaxBodyBytes int64
	Timeout      time.Duration
	Authenticate func(*http.Request) (Principal, error)
	Handle       func(context.Context, Input) (any, error)
}

type Server struct {
	application *app.Application
	endpoints   []Endpoint
	requestID   atomic.Uint64
}

func New(application *app.Application, endpoints []Endpoint) (*Server, error) {
	if application == nil {
		return nil, errors.New("nil_application")
	}
	seen := map[string]string{}
	for _, endpoint := range endpoints {
		if endpoint.Method == "" || endpoint.Path == "" || endpoint.Handle == nil {
			return nil, fmt.Errorf("invalid_endpoint: %s %s", endpoint.Method, endpoint.Path)
		}
		key := strings.ToUpper(endpoint.Method) + " " + normalizePath(endpoint.Path)
		if previous, ok := seen[key]; ok {
			return nil, fmt.Errorf("duplicate_route: %s conflicts with %s", key, previous)
		}
		seen[key] = endpoint.Path
	}
	return &Server{application: application, endpoints: append([]Endpoint(nil), endpoints...)}, nil
}

func (s *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	endpoint, params, ok := s.match(request.Method, request.URL.Path)
	if !ok {
		writeJSON(writer, http.StatusNotFound, map[string]any{"error": "not found"})
		return
	}
	lease, err := s.application.Begin()
	if err != nil {
		writeJSON(writer, http.StatusServiceUnavailable, map[string]any{"error": "application unavailable"})
		return
	}
	defer lease.Release()
	principal := Principal{}
	if endpoint.Authenticate != nil {
		principal, err = endpoint.Authenticate(request)
		if err != nil {
			writeJSON(writer, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
			return
		}
	}
	maxBody := endpoint.MaxBodyBytes
	if maxBody <= 0 {
		maxBody = 1 << 20
	}
	reader := http.MaxBytesReader(writer, request.Body, maxBody)
	body, err := io.ReadAll(reader)
	if err != nil {
		writeJSON(writer, http.StatusRequestEntityTooLarge, map[string]any{"error": "request body too large"})
		return
	}
	if len(endpoint.InputSchema) > 0 && len(strings.TrimSpace(string(body))) > 0 {
		parsed, parseErr := schema.Parse(endpoint.InputSchema)
		if parseErr != nil {
			writeJSON(writer, http.StatusInternalServerError, map[string]any{"error": "invalid endpoint schema"})
			return
		}
		if _, validateErr := parsed.Normalize(body); validateErr != nil {
			writeJSON(writer, http.StatusBadRequest, map[string]any{"error": "invalid request"})
			return
		}
	}
	ctx := request.Context()
	var cancel context.CancelFunc
	if endpoint.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, endpoint.Timeout)
		defer cancel()
	}
	input := Input{Body: body, Params: params, Query: request.URL.Query(), Principal: principal}
	result, err := endpoint.Handle(ctx, input)
	if err != nil {
		status := statusFor(err)
		if status >= 500 {
			writeJSON(writer, status, map[string]any{"error": "internal error", "requestId": s.newRequestID()})
			return
		}
		writeJSON(writer, status, map[string]any{"error": "request failed"})
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func (s *Server) match(method, path string) (Endpoint, map[string]string, bool) {
	for _, endpoint := range s.endpoints {
		if strings.EqualFold(endpoint.Method, method) {
			if params, ok := matchPath(endpoint.Path, path); ok {
				return endpoint, params, true
			}
		}
	}
	return Endpoint{}, nil, false
}

func matchPath(pattern, path string) (map[string]string, bool) {
	patternParts := splitPath(pattern)
	pathParts := splitPath(path)
	if len(patternParts) != len(pathParts) {
		return nil, false
	}
	params := map[string]string{}
	for index, part := range patternParts {
		if strings.HasPrefix(part, ":") {
			if len(part) == 1 {
				return nil, false
			}
			params[part[1:]] = pathParts[index]
		} else if part != pathParts[index] {
			return nil, false
		}
	}
	return params, true
}

func splitPath(path string) []string {
	value := strings.Trim(path, "/")
	if value == "" {
		return nil
	}
	return strings.Split(value, "/")
}
func normalizePath(path string) string {
	parts := splitPath(path)
	for index, part := range parts {
		if strings.HasPrefix(part, ":") {
			parts[index] = ":param"
		}
	}
	return "/" + strings.Join(parts, "/")
}

func statusFor(err error) int {
	var engineError *engine.Error
	if errors.As(err, &engineError) {
		switch engineError.Class {
		case "validation":
			return http.StatusBadRequest
		case "cancellation":
			return http.StatusGatewayTimeout
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout
	}
	return http.StatusInternalServerError
}

func (s *Server) newRequestID() string {
	value := s.requestID.Add(1)
	var random [4]byte
	_, _ = rand.Read(random[:])
	return fmt.Sprintf("req-%d-%s", value, hex.EncodeToString(random[:]))
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
