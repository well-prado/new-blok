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
// failures reach the client as error results that carry a stable code. A
// client that cancels a call, drops the request that carries it, or ends its
// session cancels its work.
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
	MaxRequestBytesLimit = schema.MaxPayloadBytes
	// DefaultMaxCallsPerPrincipal bounds one principal's calls in flight,
	// so one caller cannot take every slot.
	DefaultMaxCallsPerPrincipal    = 8
	DefaultMaxSessions             = 256
	DefaultMaxSessionsPerPrincipal = 16
	MaxSessionsLimit               = 1 << 16
	DefaultSessionTimeout          = 10 * time.Minute
	DefaultMaxPrincipals           = 1024
	MaxPrincipalsLimit             = 1 << 16
	// ApprovalMeta is the _meta key under which a call names the reviewed
	// decision it relies on. Naming a decision grants nothing: the catalog
	// looks it up and checks it against the call.
	ApprovalMeta = "newblok.dev/approval"

	sessionHeader = "Mcp-Session-Id"
)

// The refusals a Catalog reports. An application maps its catalog's errors
// onto these, onto trigger.ErrSaturated or tool.ErrBudget, or returns a
// trigger.Classified error; anything else reaches the client as "internal".
var (
	ErrDenied           = errors.New("mcp: denied")
	ErrApprovalStale    = errors.New("mcp: approval missing, rejected, expired or for another call")
	ErrEvidenceRequired = errors.New("mcp: trusted evidence required")
	ErrConflict         = errors.New("mcp: conflicting decision")
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
// budget and the policy on every call. Binding a reviewed decision to the
// calling principal is the application's: it chooses the invocation
// identity a decision is recorded for.
type Catalog interface {
	List(context.Context, tool.Principal) ([]Tool, error)
	Invoke(context.Context, tool.Principal, Call) (json.RawMessage, error)
}

// Authenticator resolves a request's bearer token to a principal: its id,
// the capabilities it was granted and its maximum call depth. Any error,
// whatever its cause, refuses the request with 401.
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
	// from Timeout, and its MaxOutputBytes bounds every result.
	Budget tool.Budget
	// Timeout bounds a call.
	Timeout time.Duration
	// MaxConcurrency bounds the calls in flight; more are refused.
	MaxConcurrency int
	// MaxCallsPerPrincipal bounds one principal's calls in flight.
	MaxCallsPerPrincipal int
	// MaxRequestBytes bounds an HTTP request body.
	MaxRequestBytes int64
	// MaxSessions and MaxSessionsPerPrincipal bound the open sessions; a
	// session beyond them is refused with 503.
	MaxSessions, MaxSessionsPerPrincipal int
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
	calls       sync.WaitGroup
	closing     bool
	// running counts each principal's calls in flight.
	running map[string]int
	// opened counts each principal's sessions, including those being
	// opened; live holds every counted session and its principal, so
	// Shutdown closes it even if its view was evicted.
	opened   map[string]int
	sessions int
	live     map[string]liveSession
	watchers sync.WaitGroup
	// snapshotted is set once Shutdown has taken the sessions it closes; a
	// session opened later is closed by its own opening.
	snapshotted bool
	// inflight holds the calls in flight by session id, so a session its
	// owner ends cancels its calls; ended marks such sessions until they
	// close, so a call that starts late is canceled at once.
	inflight map[string]map[*context.CancelFunc]struct{}
	ended    map[string]bool
}

type liveSession struct {
	owner   string
	session *sdk.ServerSession
}

// openingKey carries an opening's state from open to serverFor, so the
// opening finds its session on the very server the transport used.
type openingKey struct{}

type openingState struct {
	mu     sync.Mutex
	server *sdk.Server
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
	if config.MaxCallsPerPrincipal <= 0 {
		config.MaxCallsPerPrincipal = min(DefaultMaxCallsPerPrincipal, config.MaxConcurrency)
	}
	if config.MaxRequestBytes <= 0 {
		config.MaxRequestBytes = DefaultMaxRequestBytes
	}
	if config.MaxSessions <= 0 {
		config.MaxSessions = DefaultMaxSessions
	}
	if config.MaxSessionsPerPrincipal <= 0 {
		config.MaxSessionsPerPrincipal = min(DefaultMaxSessionsPerPrincipal, config.MaxSessions)
	}
	if config.SessionTimeout <= 0 {
		config.SessionTimeout = DefaultSessionTimeout
	}
	if config.MaxPrincipals <= 0 {
		config.MaxPrincipals = DefaultMaxPrincipals
	}
	if config.Timeout > MaxTimeout || config.MaxConcurrency > MaxConcurrencyLimit || config.MaxCallsPerPrincipal > config.MaxConcurrency ||
		config.MaxRequestBytes > MaxRequestBytesLimit || config.MaxSessions > MaxSessionsLimit || config.MaxSessionsPerPrincipal > config.MaxSessions ||
		config.MaxPrincipals > MaxPrincipalsLimit {
		return nil, errors.New("mcp: a timeout, concurrency, request size, session or principal bound exceeds its limit")
	}
	probe := config.Budget
	probe.Deadline = time.Now().Add(config.Timeout)
	if err := probe.Validate(); err != nil {
		return nil, fmt.Errorf("mcp: budget: %w", err)
	}
	s := &Server{application: application, config: config, exposed: map[string]bool{}, slots: make(chan struct{}, config.MaxConcurrency), views: map[string]*view{},
		running: map[string]int{}, opened: map[string]int{}, live: map[string]liveSession{}, inflight: map[string]map[*context.CancelFunc]struct{}{}, ended: map[string]bool{}}
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
	route := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodDelete:
			// The transport answers a DELETE only after the session's calls
			// return, so a session its owner ends cancels its calls first.
			s.endSession(request)
		case request.Method == http.MethodPost && request.Header.Get(sessionHeader) == "":
			s.open(writer, request, streamable)
			return
		}
		streamable.ServeHTTP(writer, request)
	})
	s.transport = auth.RequireBearerToken(s.verify, nil)(route)
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
	// session, so it is admitted but does not hold the application open.
	// Each call holds its own lease while it runs.
	if request.Method == http.MethodGet {
		lease.Release()
	} else {
		defer lease.Release()
		// The request's own work (building the session's view) also
		// stops if the application's drain times out. Only that work is
		// bound: canceling the request itself would drop its response.
		request = request.WithContext(context.WithValue(request.Context(), leaseKey{}, lease))
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

// leaseKey carries a request's application lease to the work it does.
type leaseKey struct{}

// open admits a request that opens a session within the session bounds, and
// keeps the session counted until it closes. The count settles as the
// response starts, before the client can see it and send its next request.
func (s *Server) open(writer http.ResponseWriter, request *http.Request, transport http.Handler) {
	principal := principalOf(auth.TokenInfoFromContext(request.Context()))
	if principal.ID == "" {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	owner := principal.ID
	s.mu.Lock()
	if s.closing || s.sessions >= s.config.MaxSessions || s.opened[owner] >= s.config.MaxSessionsPerPrincipal {
		s.mu.Unlock()
		writer.Header().Set("Retry-After", "1")
		http.Error(writer, "saturated", http.StatusServiceUnavailable)
		return
	}
	s.sessions++
	s.opened[owner]++
	s.mu.Unlock()
	state := &openingState{}
	opening := &openingWriter{ResponseWriter: writer, settle: func(id string) { s.settle(owner, state, id) }}
	defer opening.settled()
	transport.ServeHTTP(opening, request.WithContext(context.WithValue(request.Context(), openingKey{}, state)))
}

// settle keeps a session the transport opened counted until it closes, and
// releases the place of a request that opened none. It runs as the opening's
// response starts, possibly under the transport's stream lock and before
// the initialize request has finished: it must never wait for the session.
func (s *Server) settle(owner string, state *openingState, id string) {
	var session *sdk.ServerSession
	state.mu.Lock()
	server := state.server
	state.mu.Unlock()
	if server != nil && id != "" {
		for candidate := range server.Sessions() {
			if candidate.ID() == id {
				session = candidate
				break
			}
		}
	}
	if session == nil {
		s.closed(owner, "")
		return
	}
	s.mu.Lock()
	if s.snapshotted {
		// Shutdown has already taken the sessions it closes. Closing waits
		// for this very response, so it happens elsewhere.
		s.mu.Unlock()
		go func() {
			_ = session.Close()
			s.closed(owner, "")
		}()
		return
	}
	s.live[id] = liveSession{owner: owner, session: session}
	s.watchers.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.watchers.Done()
		_ = session.Wait()
		s.closed(owner, id)
	}()
}

// openingWriter settles a session opening once, when the response starts or
// the handler returns without one.
type openingWriter struct {
	http.ResponseWriter
	settle func(id string)
	done   bool
}

func (w *openingWriter) settled() {
	if !w.done {
		w.done = true
		w.settle(w.Header().Get(sessionHeader))
	}
}

func (w *openingWriter) WriteHeader(status int) {
	w.settled()
	w.ResponseWriter.WriteHeader(status)
}

func (w *openingWriter) Write(data []byte) (int, error) {
	w.settled()
	return w.ResponseWriter.Write(data)
}

func (w *openingWriter) Flush() {
	w.settled()
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

func (w *openingWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (s *Server) closed(owner, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions--
	if s.opened[owner]--; s.opened[owner] <= 0 {
		delete(s.opened, owner)
	}
	if id != "" {
		delete(s.live, id)
		delete(s.ended, id)
	}
}

// track registers a call's cancel function under its session; the returned
// function unregisters it. A call in a session its owner already ended is
// canceled at once.
func (s *Server) track(session string, cancel context.CancelFunc) func() {
	if session == "" {
		return func() {}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended[session] {
		cancel()
		return func() {}
	}
	calls, ok := s.inflight[session]
	if !ok {
		calls = map[*context.CancelFunc]struct{}{}
		s.inflight[session] = calls
	}
	calls[&cancel] = struct{}{}
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(calls, &cancel)
		if len(calls) == 0 && len(s.inflight[session]) == 0 {
			delete(s.inflight, session)
		}
	}
}

// endSession cancels the calls of the session a DELETE names, if the
// authenticated caller owns it. Anyone else's DELETE cancels nothing, and
// the transport refuses it.
func (s *Server) endSession(request *http.Request) {
	session := request.Header.Get(sessionHeader)
	info := auth.TokenInfoFromContext(request.Context())
	if session == "" || info == nil {
		return
	}
	s.mu.Lock()
	if live, ok := s.live[session]; !ok || live.owner != info.UserID {
		s.mu.Unlock()
		return
	}
	s.ended[session] = true
	calls := s.inflight[session]
	delete(s.inflight, session)
	cancels := make([]*context.CancelFunc, 0, len(calls))
	for cancel := range calls {
		cancels = append(cancels, cancel)
	}
	s.mu.Unlock()
	for _, cancel := range cancels {
		(*cancel)()
	}
}

// verify authenticates the bearer token. The token info's user id binds the
// session to the principal: a request for that session as anyone else is
// refused by the transport. The token info is built per HTTP request, so it
// also carries the request's context: a call ends with the request that
// carries it.
func (s *Server) verify(ctx context.Context, token string, request *http.Request) (*auth.TokenInfo, error) {
	principal, err := s.config.Authenticate(ctx, token, request)
	if err != nil || strings.TrimSpace(principal.ID) == "" {
		return nil, auth.ErrInvalidToken
	}
	principal.Capabilities = append([]string(nil), principal.Capabilities...)
	return &auth.TokenInfo{UserID: principal.ID, Expiration: time.Now().Add(time.Hour), Extra: map[string]any{"principal": principal, "request": request.Context()}}, nil
}

func principalOf(info *auth.TokenInfo) tool.Principal {
	if info == nil {
		return tool.Principal{}
	}
	principal, _ := info.Extra["principal"].(tool.Principal)
	return principal
}

// serverFor returns the MCP server of the request's principal. The
// transport asks on every request; only a request that opens a session
// builds a view, once per distinct principal and capability set.
func (s *Server) serverFor(request *http.Request) (server *sdk.Server) {
	principal := principalOf(auth.TokenInfoFromContext(request.Context()))
	if principal.ID == "" {
		return nil
	}
	if state, ok := request.Context().Value(openingKey{}).(*openingState); ok {
		defer func() {
			state.mu.Lock()
			state.server = server
			state.mu.Unlock()
		}()
	}
	key := viewKey(principal)
	s.mu.Lock()
	v, ok := s.views[key]
	closing := s.closing
	s.mu.Unlock()
	if ok {
		return v.server
	}
	if closing || request.Method != http.MethodPost || request.Header.Get(sessionHeader) != "" {
		return nil
	}
	defer func() {
		if recover() != nil {
			server = nil
		}
	}()
	ctx, cancel := context.WithTimeout(request.Context(), s.config.Timeout)
	defer cancel()
	if lease, ok := request.Context().Value(leaseKey{}).(*app.Lease); ok {
		bound, unbind := lease.Bind(ctx)
		defer unbind()
		ctx = bound
	}
	v, err := s.build(ctx, principal)
	if err != nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return nil
	}
	if existing, ok := s.views[key]; ok {
		return existing.server
	}
	if len(s.order) >= s.config.MaxPrincipals {
		// The oldest view goes; its open sessions keep their server, and the
		// session registry keeps them reachable for Shutdown.
		delete(s.views, s.order[0])
		s.order = s.order[1:]
	}
	s.views[key] = v
	s.order = append(s.order, key)
	return v.server
}

func viewKey(principal tool.Principal) string {
	capabilities := append([]string(nil), principal.Capabilities...)
	sort.Strings(capabilities)
	digest := sha256.Sum256([]byte(fmt.Sprintf("%q\x00%q\x00%d", principal.ID, capabilities, principal.MaxDepth)))
	return hex.EncodeToString(digest[:])
}

// build lists the principal's tools and keeps those exposed whose schemas
// an MCP client can use: an object input schema, and an output schema that
// parses, if any.
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
		var carrier context.Context
		if request.Extra != nil && request.Extra.TokenInfo != nil {
			principal = principalOf(request.Extra.TokenInfo)
			carrier, _ = request.Extra.TokenInfo.Extra["request"].(context.Context)
		}
		if principal.ID == "" || principal.ID != owner.ID {
			return failure("unauthorized"), nil
		}
		ctx, cancel := context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()
		// The work ends when the client cancels the call, drops the request
		// that carries it, or ends its session. Without an event store a
		// dropped response stream cannot be resumed, so its result would be
		// lost anyway.
		if carrier != nil {
			defer context.AfterFunc(carrier, cancel)()
		}
		if request.Session != nil {
			defer s.track(request.Session.ID(), cancel)()
		}
		s.mu.Lock()
		if s.closing {
			s.mu.Unlock()
			return failure("unavailable"), nil
		}
		if s.running[principal.ID] >= s.config.MaxCallsPerPrincipal {
			s.mu.Unlock()
			return failure("saturated"), nil
		}
		s.running[principal.ID]++
		s.calls.Add(1)
		s.mu.Unlock()
		defer func() {
			s.mu.Lock()
			if s.running[principal.ID]--; s.running[principal.ID] <= 0 {
				delete(s.running, principal.ID)
			}
			s.mu.Unlock()
			s.calls.Done()
		}()
		select {
		case s.slots <- struct{}{}:
			defer func() { <-s.slots }()
		default:
			return failure("saturated"), nil
		}
		// The call holds the application open while it runs, whatever
		// happens to the request that carried it.
		lease, err := s.application.Begin()
		if err != nil {
			return failure("unavailable"), nil
		}
		defer lease.Release()
		// The call also stops if the application's drain times out.
		ctx, unbind := lease.Bind(ctx)
		defer unbind()
		arguments := request.Params.Arguments
		if len(arguments) == 0 {
			arguments = json.RawMessage("{}")
		}
		// Arguments the tool cannot accept are the model's to correct: a tool
		// error, not a protocol error.
		normalized, err := input.Normalize(arguments)
		if err != nil {
			return failure("invalid_input"), nil
		}
		var named string
		if value, ok := request.Params.Meta[ApprovalMeta]; ok {
			// _meta is the client's, not the model's: a malformed name is a
			// protocol error.
			text, isText := value.(string)
			if !isText || !reference.MatchString(text) {
				return nil, &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: "invalid_approval"}
			}
			named = text
		}
		// A call already canceled (its session ended or its request is
		// gone) never reaches the catalog.
		if carrier != nil && carrier.Err() != nil {
			cancel()
		}
		if err := ctx.Err(); err != nil {
			return failure(codeFor(ctx, err)), nil
		}
		budget := s.config.Budget
		budget.Deadline, _ = ctx.Deadline()
		out, err := s.config.Catalog.Invoke(ctx, principal, Call{Name: t.Name, Version: t.Version, Input: normalized, Approval: named, Budget: budget})
		if err == nil && ctx.Err() != nil {
			// The catalog returned after the deadline or after the client
			// left: the call has already failed.
			err = ctx.Err()
		}
		if err != nil {
			return failure(codeFor(ctx, err)), nil
		}
		if len(out) > budget.MaxOutputBytes {
			return failure("budget_exceeded"), nil
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
	case app.Aborted(ctx):
		return "unavailable"
	case errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, trigger.ErrSaturated):
		return "saturated"
	case errors.Is(err, ErrApprovalStale):
		return "approval_stale"
	case errors.Is(err, ErrEvidenceRequired):
		return "evidence_required"
	case errors.Is(err, ErrConflict):
		return "conflict"
	case errors.Is(err, ErrDenied):
		return "denied"
	case errors.Is(err, tool.ErrBudget):
		return "budget_exceeded"
	}
	if code, class, ok := trigger.Classify(err); ok && class != "configuration" {
		return code
	}
	return "internal"
}

// Shutdown refuses new sessions and calls, waits for the calls in flight,
// closes every session and waits for its bookkeeping to settle, all within
// ctx.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
	if err := wait(ctx, &s.calls); err != nil {
		return err
	}
	s.mu.Lock()
	s.snapshotted = true
	sessions := map[*sdk.ServerSession]bool{}
	for _, live := range s.live {
		sessions[live.session] = true
	}
	views := make([]*view, 0, len(s.views))
	for _, v := range s.views {
		views = append(views, v)
	}
	s.mu.Unlock()
	for _, v := range views {
		for session := range v.server.Sessions() {
			sessions[session] = true
		}
	}
	var closing sync.WaitGroup
	for session := range sessions {
		closing.Add(1)
		go func() {
			defer closing.Done()
			_ = session.Close()
		}()
	}
	if err := wait(ctx, &closing); err != nil {
		return err
	}
	return wait(ctx, &s.watchers)
}

func wait(ctx context.Context, group *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() { group.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
