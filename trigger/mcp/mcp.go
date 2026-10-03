// Package mcp exposes reviewed catalog tools to Model Context Protocol
// clients.
//
// An MCP client (an agent host) discovers tools and calls them over the
// Streamable HTTP transport. Every HTTP request is authenticated as a
// principal, and a session stays bound to the principal that opened it. A
// principal sees only the tools the application exposes explicitly and the
// catalog lists for its capabilities; it also sees each tool's descriptor as
// a resource. A call is validated against the tool's input schema, bounded
// in concurrency and time, and dispatched through the catalog: the
// application's admission, which applies capability, budget and approval
// policy. The adapter never dispatches an effect itself. An effect that needs
// a reviewed decision stays blocked until the catalog finds one; the caller
// can only name a decision (in the call's _meta), never grant one. Tool
// failures reach the client as error results that carry a stable code, and
// a client that cancels a call cancels its work.
package mcp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/approval"
	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/contract/tool"
	"github.com/well-prado/new-blok/trigger"
)

// Declaration is the MCP contract: a tool call completes in band, a client
// that cancels a call cancels its work, and every caller is authenticated.
var Declaration = trigger.Declaration{Kind: trigger.MCP, Adapter: "trigger/mcp", Completion: trigger.Memory, Disconnect: trigger.CancelWork, Authentication: trigger.Caller}

const (
	DefaultTimeout         = 30 * time.Second
	MaxTimeout             = 5 * time.Minute
	DefaultMaxConcurrency  = 32
	MaxConcurrencyLimit    = 1024
	DefaultMaxRequestBytes = 256 << 10
	// MaxRequestBytesLimit is the domain value limit: a larger request
	// cannot carry arguments a tool accepts.
	MaxRequestBytesLimit  = schema.MaxPayloadBytes
	DefaultSessionTimeout = 10 * time.Minute
	DefaultMaxPrincipals  = 1024
	MaxPrincipalsLimit    = 1 << 16
	// ApprovalMeta is the _meta key under which a call names the reviewed
	// decision it relies on. Naming a decision grants nothing: the catalog
	// checks that a decision with that id exists for this exact call.
	ApprovalMeta = "newblok.dev/approval"
)

// Tool is one catalog tool as the adapter sees it.
type Tool struct {
	Name, Version, Description string
	InputSchema, OutputSchema  json.RawMessage
	// Effects are the effects the tool may perform; a tool without effects
	// is announced as read-only.
	Effects []string
}

// Call is one tool call the adapter hands to the catalog.
type Call struct {
	Name, Version string
	// Input is the call's arguments, validated against the input schema.
	Input json.RawMessage
	// Approval names the reviewed decision the caller relies on, if any.
	Approval string
	Budget   tool.Budget
}

// Catalog is the application's admission. An application implements it
// over agent.Catalog, or over a policy.CatalogGate when effects need a
// reviewed decision; either applies the principal's capabilities, the
// budget and the policy on every call.
type Catalog interface {
	List(context.Context, tool.Principal) ([]Tool, error)
	Invoke(context.Context, tool.Principal, Call) (json.RawMessage, error)
}

// Authenticator resolves a request's bearer token to a principal: its id,
// the capabilities it was granted and its maximum call depth.
type Authenticator func(ctx context.Context, token string, request *http.Request) (tool.Principal, error)

// Config describes one MCP endpoint.
type Config struct {
	// Name and Version identify the server to clients.
	Name, Version string
	Catalog       Catalog
	Authenticate  Authenticator
	// Expose lists the tools this endpoint may show, as "name@version". A
	// tool the catalog lists but Expose does not name is never visible.
	Expose []string
	// Budget is the budget of every call; its deadline is set per call
	// from Timeout.
	Budget tool.Budget
	// Timeout bounds a call.
	Timeout time.Duration
	// MaxConcurrency bounds the calls in flight; more are refused.
	MaxConcurrency int
	// MaxRequestBytes bounds an HTTP request body.
	MaxRequestBytes int64
	// SessionTimeout closes a session idle for that long.
	SessionTimeout time.Duration
	// MaxPrincipals bounds the per-principal views kept in memory.
	MaxPrincipals int
}

// Server is an MCP endpoint over Streamable HTTP.
type Server struct {
	application *app.Application
	config      Config
	exposed     map[string]bool
	slots       chan struct{}
	transport   http.Handler
	mu          sync.Mutex
	views       map[string]*view
	order       []string
	// retired are evicted views whose sessions may still be open; Shutdown
	// closes them too.
	retired []*view
	calls   sync.WaitGroup
	closing bool
	// inflight holds the calls in flight by session id, so a session its
	// owner ends cancels its calls.
	inflight map[string]*sessionCalls
}

// sessionCalls are one session's calls in flight and the principal that
// owns the session.
type sessionCalls struct {
	owner   string
	cancels map[*context.CancelFunc]struct{}
}

// view is the MCP server one principal sees.
type view struct {
	server    *sdk.Server
	principal tool.Principal
}

var (
	toolRef   = regexp.MustCompile(`^[A-Za-z0-9_./-]{1,100}@[A-Za-z0-9_.+-]{1,27}$`)
	mcpName   = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)
	reference = regexp.MustCompile(`^[\x21-\x7e]{1,256}$`)
)

// ToolName is the MCP name of a catalog tool: its name with "/" as "." and
// its version appended after "_v", within the characters MCP allows.
func ToolName(name, version string) string {
	return strings.ReplaceAll(name, "/", ".") + "_v" + version
}

// New validates the configuration. It opens no listener and starts no
// goroutine.
func New(application *app.Application, config Config) (*Server, error) {
	if application == nil || config.Catalog == nil || config.Authenticate == nil || config.Name == "" || config.Version == "" || len(config.Expose) == 0 {
		return nil, errors.New("mcp: an application, a name, a version, a catalog, an authenticator and exposed tools are required")
	}
	if config.Timeout <= 0 {
		config.Timeout = DefaultTimeout
	}
	if config.MaxConcurrency <= 0 {
		config.MaxConcurrency = DefaultMaxConcurrency
	}
	if config.MaxRequestBytes <= 0 {
		config.MaxRequestBytes = DefaultMaxRequestBytes
	}
	if config.SessionTimeout <= 0 {
		config.SessionTimeout = DefaultSessionTimeout
	}
	if config.MaxPrincipals <= 0 {
		config.MaxPrincipals = DefaultMaxPrincipals
	}
	if config.Timeout > MaxTimeout || config.MaxConcurrency > MaxConcurrencyLimit || config.MaxRequestBytes > MaxRequestBytesLimit || config.MaxPrincipals > MaxPrincipalsLimit {
		return nil, errors.New("mcp: timeout, concurrency, request size or principal count exceeds its limit")
	}
	probe := config.Budget
	probe.Deadline = time.Now().Add(config.Timeout)
	if err := probe.Validate(); err != nil {
		return nil, fmt.Errorf("mcp: budget: %w", err)
	}
	s := &Server{application: application, config: config, exposed: map[string]bool{}, slots: make(chan struct{}, config.MaxConcurrency), views: map[string]*view{}, inflight: map[string]*sessionCalls{}}
	names := map[string]string{}
	for _, ref := range config.Expose {
		if !toolRef.MatchString(ref) || s.exposed[ref] {
			return nil, fmt.Errorf("mcp: exposed tool %q is not a unique name@version", ref)
		}
		name, version, _ := strings.Cut(ref, "@")
		mapped := ToolName(name, version)
		if !mcpName.MatchString(mapped) {
			return nil, fmt.Errorf("mcp: exposed tool %q has no valid MCP name", ref)
		}
		if other, ok := names[mapped]; ok {
			return nil, fmt.Errorf("mcp: exposed tools %q and %q share the MCP name %q", other, ref, mapped)
		}
		names[mapped] = ref
		s.exposed[ref] = true
	}
	streamable := sdk.NewStreamableHTTPHandler(s.serverFor, &sdk.StreamableHTTPOptions{
		SessionTimeout:      config.SessionTimeout,
		MaxRequestBodyBytes: config.MaxRequestBytes,
	})
	// The transport answers a DELETE only after the session's calls return,
	// so a session its owner ends cancels its calls first.
	ending := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodDelete {
			s.endSession(request)
		}
		streamable.ServeHTTP(writer, request)
	})
	s.transport = auth.RequireBearerToken(s.verify, nil)(ending)
	return s, nil
}

// ServeHTTP admits the request, then lets the bearer check and the MCP
// transport handle it. A session's later requests must come from the
// principal that opened it.
func (s *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	lease, err := s.application.Begin()
	if err != nil {
		writer.Header().Set("Retry-After", "1")
		http.Error(writer, "unavailable", http.StatusServiceUnavailable)
		return
	}
	// A GET is the session's standing stream and lasts as long as the
	// session, so it is admitted but does not hold the application open;
	// calls arrive as POSTs and do.
	if request.Method == http.MethodGet {
		lease.Release()
	} else {
		defer lease.Release()
	}
	s.mu.Lock()
	closing := s.closing
	s.mu.Unlock()
	if closing {
		writer.Header().Set("Retry-After", "1")
		http.Error(writer, "unavailable", http.StatusServiceUnavailable)
		return
	}
	s.transport.ServeHTTP(writer, request)
}

// track registers a call's cancel function under its session and owner;
// the returned function unregisters it.
func (s *Server) track(session, owner string, cancel context.CancelFunc) func() {
	if session == "" {
		return func() {}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	calls, ok := s.inflight[session]
	if !ok {
		calls = &sessionCalls{owner: owner, cancels: map[*context.CancelFunc]struct{}{}}
		s.inflight[session] = calls
	}
	calls.cancels[&cancel] = struct{}{}
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(calls.cancels, &cancel)
		if len(calls.cancels) == 0 && s.inflight[session] == calls {
			delete(s.inflight, session)
		}
	}
}

// endSession cancels the calls of the session a DELETE names, if the
// authenticated caller owns it. Anyone else's DELETE cancels nothing, and
// the transport refuses it.
func (s *Server) endSession(request *http.Request) {
	session := request.Header.Get("Mcp-Session-Id")
	info := auth.TokenInfoFromContext(request.Context())
	if session == "" || info == nil {
		return
	}
	s.mu.Lock()
	calls, ok := s.inflight[session]
	if !ok || calls.owner != info.UserID {
		s.mu.Unlock()
		return
	}
	delete(s.inflight, session)
	cancels := make([]*context.CancelFunc, 0, len(calls.cancels))
	for cancel := range calls.cancels {
		cancels = append(cancels, cancel)
	}
	s.mu.Unlock()
	for _, cancel := range cancels {
		(*cancel)()
	}
}

// verify authenticates the bearer token. The token info's user id binds the
// session to the principal: a request for that session as anyone else is
// refused by the transport.
func (s *Server) verify(ctx context.Context, token string, request *http.Request) (*auth.TokenInfo, error) {
	principal, err := s.config.Authenticate(ctx, token, request)
	if err != nil || strings.TrimSpace(principal.ID) == "" {
		return nil, auth.ErrInvalidToken
	}
	principal.Capabilities = append([]string(nil), principal.Capabilities...)
	return &auth.TokenInfo{UserID: principal.ID, Expiration: time.Now().Add(time.Hour), Extra: map[string]any{"principal": principal}}, nil
}

// serverFor returns the MCP server of the request's principal, built once
// per distinct principal and capability set.
func (s *Server) serverFor(request *http.Request) *sdk.Server {
	info := auth.TokenInfoFromContext(request.Context())
	if info == nil {
		return nil
	}
	principal, ok := info.Extra["principal"].(tool.Principal)
	if !ok {
		return nil
	}
	key := viewKey(principal)
	s.mu.Lock()
	if v, ok := s.views[key]; ok {
		s.mu.Unlock()
		return v.server
	}
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(request.Context(), s.config.Timeout)
	defer cancel()
	v, err := s.build(ctx, principal)
	if err != nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.views[key]; ok {
		return existing.server
	}
	if len(s.order) >= s.config.MaxPrincipals {
		// The oldest view goes; its open sessions keep their server until
		// they end or Shutdown closes them.
		live := s.retired[:0]
		for _, old := range s.retired {
			if hasSessions(old.server) {
				live = append(live, old)
			}
		}
		s.retired = append(live, s.views[s.order[0]])
		delete(s.views, s.order[0])
		s.order = s.order[1:]
	}
	s.views[key] = v
	s.order = append(s.order, key)
	return v.server
}

func hasSessions(server *sdk.Server) bool {
	for range server.Sessions() {
		return true
	}
	return false
}

func viewKey(principal tool.Principal) string {
	capabilities := append([]string(nil), principal.Capabilities...)
	sort.Strings(capabilities)
	digest := sha256.Sum256([]byte(fmt.Sprintf("%q\x00%q\x00%d", principal.ID, capabilities, principal.MaxDepth)))
	return hex.EncodeToString(digest[:])
}

// build lists the principal's tools and keeps those exposed whose schemas
// an MCP client can use: an object input schema, and an object output
// schema if any.
func (s *Server) build(ctx context.Context, principal tool.Principal) (*view, error) {
	listed, err := s.config.Catalog.List(ctx, principal)
	if err != nil {
		return nil, err
	}
	server := sdk.NewServer(&sdk.Implementation{Name: s.config.Name, Version: s.config.Version}, &sdk.ServerOptions{
		Capabilities: &sdk.ServerCapabilities{Tools: &sdk.ToolCapabilities{}, Resources: &sdk.ResourceCapabilities{}},
	})
	for _, t := range listed {
		if !s.exposed[t.Name+"@"+t.Version] {
			continue
		}
		input, err := schema.Parse(t.InputSchema)
		if err != nil || input.Type != "object" {
			continue
		}
		var output *schema.Schema
		if len(t.OutputSchema) > 0 {
			parsed, err := schema.Parse(t.OutputSchema)
			if err != nil {
				continue
			}
			output = &parsed
		}
		descriptor := &sdk.Tool{
			Name:        ToolName(t.Name, t.Version),
			Title:       t.Name + "@" + t.Version,
			Description: t.Description,
			InputSchema: json.RawMessage(t.InputSchema),
			Annotations: &sdk.ToolAnnotations{ReadOnlyHint: len(t.Effects) == 0},
		}
		if output != nil && output.Type == "object" {
			descriptor.OutputSchema = json.RawMessage(t.OutputSchema)
		}
		if !addTool(server, descriptor, s.handler(principal, t, input, output)) {
			continue
		}
		resource, err := json.Marshal(map[string]any{"name": t.Name, "version": t.Version, "description": t.Description, "inputSchema": t.InputSchema, "outputSchema": t.OutputSchema, "effects": t.Effects})
		if err != nil {
			continue
		}
		uri := "newblok://tools/" + t.Name + "/" + t.Version
		server.AddResource(&sdk.Resource{URI: uri, Name: ToolName(t.Name, t.Version), MIMEType: "application/json"}, func(context.Context, *sdk.ReadResourceRequest) (*sdk.ReadResourceResult, error) {
			return &sdk.ReadResourceResult{Contents: []*sdk.ResourceContents{{URI: uri, MIMEType: "application/json", Text: string(resource)}}}, nil
		})
	}
	return &view{server: server, principal: principal}, nil
}

// addTool registers a tool, skipping one the SDK refuses: AddTool panics on
// a descriptor it cannot announce.
func addTool(server *sdk.Server, descriptor *sdk.Tool, handler sdk.ToolHandler) (added bool) {
	defer func() {
		if recover() != nil {
			added = false
		}
	}()
	server.AddTool(descriptor, handler)
	return true
}

// failure is a tool error result: the error is the model's to see, and
// carries only a stable code.
func failure(code string) *sdk.CallToolResult {
	text, _ := json.Marshal(map[string]string{"code": code})
	return &sdk.CallToolResult{IsError: true, Content: []sdk.Content{&sdk.TextContent{Text: string(text)}}}
}

// invalidParams is the protocol error for arguments a tool cannot accept.
func invalidParams(code string) error {
	return &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: code}
}

// handler serves one tool of a view. The view is the session's, but each
// call runs as the principal the call's own request authenticated, so
// capabilities revoked since the session opened no longer apply.
func (s *Server) handler(owner tool.Principal, t Tool, input schema.Schema, output *schema.Schema) sdk.ToolHandler {
	return func(ctx context.Context, request *sdk.CallToolRequest) (result *sdk.CallToolResult, err error) {
		defer func() {
			if recover() != nil {
				result, err = failure("internal"), nil
			}
		}()
		var principal tool.Principal
		if request.Extra != nil && request.Extra.TokenInfo != nil {
			principal, _ = request.Extra.TokenInfo.Extra["principal"].(tool.Principal)
		}
		if principal.ID == "" || principal.ID != owner.ID {
			return failure("unauthorized"), nil
		}
		s.mu.Lock()
		if s.closing {
			s.mu.Unlock()
			return failure("unavailable"), nil
		}
		s.calls.Add(1)
		s.mu.Unlock()
		defer s.calls.Done()
		select {
		case s.slots <- struct{}{}:
			defer func() { <-s.slots }()
		default:
			return failure("saturated"), nil
		}
		arguments := request.Params.Arguments
		if len(arguments) == 0 {
			arguments = json.RawMessage("{}")
		}
		normalized, err := input.Normalize(arguments)
		if err != nil {
			return nil, invalidParams("invalid_input")
		}
		var named string
		if value, ok := request.Params.Meta[ApprovalMeta]; ok {
			text, isText := value.(string)
			if !isText || !reference.MatchString(text) {
				return nil, invalidParams("invalid_approval")
			}
			named = text
		}
		ctx, cancel := context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()
		// A session its client ends cancels its calls.
		if request.Session != nil {
			defer s.track(request.Session.ID(), principal.ID, cancel)()
		}
		budget := s.config.Budget
		budget.Deadline, _ = ctx.Deadline()
		out, err := s.config.Catalog.Invoke(ctx, principal, Call{Name: t.Name, Version: t.Version, Input: normalized, Approval: named, Budget: budget})
		if err == nil && ctx.Err() != nil {
			err = ctx.Err()
		}
		if err != nil {
			return failure(codeFor(ctx, err)), nil
		}
		if output != nil {
			if out, err = output.Normalize(out); err != nil {
				return failure("invalid_output"), nil
			}
		}
		result = &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: string(out)}}}
		if output != nil && output.Type == "object" {
			result.StructuredContent = json.RawMessage(out)
		}
		return result, nil
	}
}

// codeFor maps a catalog error to a stable code, never its text.
func codeFor(ctx context.Context, err error) string {
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, trigger.ErrSaturated), errors.Is(err, approval.ErrCapacity):
		return "saturated"
	case errors.Is(err, approval.ErrStale):
		return "approval_stale"
	case errors.Is(err, approval.ErrEvidence):
		return "evidence_required"
	case errors.Is(err, approval.ErrConflict):
		return "conflict"
	case errors.Is(err, approval.ErrDenied):
		return "denied"
	case errors.Is(err, tool.ErrBudget):
		return "budget_exceeded"
	}
	if code, class, ok := trigger.Classify(err); ok && class != "configuration" {
		return code
	}
	return "internal"
}

// Shutdown refuses new calls, waits for those in flight, and closes every
// session.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closing = true
	views := append(make([]*view, 0, len(s.views)+len(s.retired)), s.retired...)
	for _, v := range s.views {
		views = append(views, v)
	}
	s.mu.Unlock()
	done := make(chan struct{})
	go func() { s.calls.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	for _, v := range views {
		for session := range v.server.Sessions() {
			_ = session.Close()
		}
	}
	return nil
}
