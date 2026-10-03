// Package grpc binds unary gRPC methods to workflows.
//
// A binding maps one generated protobuf method to a workflow: the request
// message becomes the workflow's domain input and the workflow's output
// becomes the response message. The mapping is checked at startup against
// the method's descriptors and the workflow's schemas, so a binding that
// could produce a value the workflow rejects, or a workflow output the
// response cannot carry, never starts. Each call is authenticated from its
// metadata and authorized per method on the server, bounded in size, time
// and concurrency, and runs in band: a client that cancels or whose
// deadline passes cancels the workflow. Errors reach the client as gRPC
// status codes carrying only stable codes.
//
// Application services stay apart from the worker runtime transport
// (contract/runtime): they may not use its proto package, and registering
// onto a shared server refuses any service name already there.
package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/trigger"
)

// Declaration is the gRPC contract: a call completes in band with the
// workflow's output, a client that goes away cancels its work, and every
// caller is authenticated.
var Declaration = trigger.Declaration{Kind: trigger.GRPC, Adapter: "trigger/grpc", Completion: trigger.Memory, Disconnect: trigger.CancelWork, Authentication: trigger.Caller}

const (
	DefaultTimeout         = 30 * time.Second
	MaxTimeout             = 5 * time.Minute
	DefaultMaxMessageBytes = 256 << 10
	// MaxMessageBytesLimit is the domain value limit: a larger message
	// cannot fit the JSON value a workflow receives or returns.
	MaxMessageBytesLimit = schema.MaxPayloadBytes
	// DefaultMaxElements bounds the fields and repeated elements a request
	// may carry, counted on the wire before it is decoded.
	DefaultMaxElements = 10_000
	MaxElementsLimit   = 100_000
	// MaxNesting bounds message nesting on the wire.
	MaxNesting            = 32
	DefaultMaxConcurrency = 64
	MaxConcurrencyLimit   = 4096
	DefaultMaxStreams     = 1024
	MaxStreamsLimit       = 1 << 16
	// ReservedPackage is the worker runtime's proto package; application
	// services may not live in it.
	ReservedPackage = "blok.runtime."
	// ErrorDomain is the ErrorInfo domain of every status the adapter
	// returns.
	ErrorDomain = "newblok"
)

// Call is what a binding's workflow receives.
type Call struct {
	// Method is the full gRPC method name, /package.Service/Method.
	Method    string
	Principal trigger.Principal
	// Input is the request, mapped to JSON and validated against the
	// binding's input schema.
	Input json.RawMessage
}

// Binding connects one unary method to a workflow.
type Binding struct {
	// Method is the generated method descriptor, for example
	// orderpb.File_orders_proto.Services().ByName("Orders").Methods().ByName("Place").
	Method protoreflect.MethodDescriptor
	// Workflow names the workflow; WorkflowInput is its input schema.
	Workflow      string
	WorkflowInput []byte
	// InputSchema is the domain value the request maps to; OutputSchema is
	// the value the workflow returns, which becomes the response.
	InputSchema  []byte
	OutputSchema []byte
	Handle       func(context.Context, Call) (json.RawMessage, error)
	// Authorize decides, on the server, whether a principal may call the
	// method. It is required; AllowAuthenticated admits every
	// authenticated principal.
	Authorize func(principal trigger.Principal, method string) error
	// Timeout bounds a call; a client deadline can only shorten it.
	Timeout time.Duration
	// MaxConcurrency bounds the calls in flight; more are refused.
	MaxConcurrency int
	// MaxRequestBytes and MaxResponseBytes bound the encoded messages:
	// the request as received on the wire, the response as encoded.
	MaxRequestBytes  int
	MaxResponseBytes int
	// MaxElements bounds the fields, repeated elements and nested
	// messages of a request, counted on the wire before it is decoded, so
	// a small message cannot decode into a large structure.
	MaxElements int
}

// AllowAuthenticated admits every authenticated principal to a method.
func AllowAuthenticated(trigger.Principal, string) error { return nil }

// Authenticator establishes the caller from a call's context: its metadata,
// or its peer (for example a verified client certificate).
type Authenticator func(context.Context) (trigger.Principal, error)

type binding struct {
	Binding
	method   string
	input    schema.Schema
	output   schema.Schema
	slots    chan struct{}
	request  protoreflect.MessageDescriptor
	response protoreflect.MessageDescriptor
}

// Server holds validated bindings, grouped into services.
type Server struct {
	application  *app.Application
	authenticate Authenticator
	services     map[string][]*binding
	maxMessage   int
}

// MappingError reports a binding that cannot be proven correct at
// startup.
type MappingError struct {
	Method string
	Path   string
	Code   string
	Reason string
}

func (e *MappingError) Error() string {
	return fmt.Sprintf("grpc: %s%s: %s: %s", e.Method, e.Path, e.Code, e.Reason)
}

// New validates every binding. It opens no listener and starts no
// goroutine.
func New(application *app.Application, authenticate Authenticator, bindings []Binding) (*Server, error) {
	if application == nil || authenticate == nil || len(bindings) == 0 {
		return nil, errors.New("grpc: an application, an authenticator and at least one binding are required")
	}
	s := &Server{application: application, authenticate: authenticate, services: map[string][]*binding{}}
	seen := map[string]bool{}
	for _, b := range bindings {
		checked, err := check(b)
		if err != nil {
			return nil, err
		}
		if seen[checked.method] {
			return nil, &MappingError{Method: checked.method, Code: "duplicate_method", Reason: "the method is bound twice"}
		}
		seen[checked.method] = true
		service := string(b.Method.Parent().FullName())
		s.services[service] = append(s.services[service], checked)
		s.maxMessage = max(s.maxMessage, checked.MaxRequestBytes, checked.MaxResponseBytes)
	}
	return s, nil
}

func check(b Binding) (*binding, error) {
	if b.Method == nil || b.Handle == nil || b.Authorize == nil || b.Workflow == "" {
		return nil, errors.New("grpc: a binding needs a method, a workflow, a handler and an authorization")
	}
	method := "/" + string(b.Method.Parent().FullName()) + "/" + string(b.Method.Name())
	fail := func(path, code, reason string) (*binding, error) {
		return nil, &MappingError{Method: method, Path: path, Code: code, Reason: reason}
	}
	if b.Method.IsStreamingClient() || b.Method.IsStreamingServer() {
		return fail("", "streaming_unsupported", "only unary methods are bound; stream progress over SSE or WebSocket")
	}
	if strings.HasPrefix(string(b.Method.Parent().FullName())+".", ReservedPackage) {
		return fail("", "reserved_service", "the worker runtime's proto package is reserved")
	}
	if b.Timeout <= 0 {
		b.Timeout = DefaultTimeout
	}
	if b.MaxConcurrency <= 0 {
		b.MaxConcurrency = DefaultMaxConcurrency
	}
	if b.MaxRequestBytes <= 0 {
		b.MaxRequestBytes = DefaultMaxMessageBytes
	}
	if b.MaxResponseBytes <= 0 {
		b.MaxResponseBytes = DefaultMaxMessageBytes
	}
	if b.MaxElements <= 0 {
		b.MaxElements = DefaultMaxElements
	}
	if b.Timeout > MaxTimeout || b.MaxConcurrency > MaxConcurrencyLimit || b.MaxRequestBytes > MaxMessageBytesLimit || b.MaxResponseBytes > MaxMessageBytesLimit || b.MaxElements > MaxElementsLimit {
		return fail("", "bound_exceeded", "timeout, concurrency, message size or element count exceeds its limit")
	}
	input, err := schema.Parse(b.InputSchema)
	if err != nil {
		return fail("", "invalid_schema", "input schema: "+err.Error())
	}
	output, err := schema.Parse(b.OutputSchema)
	if err != nil {
		return fail("", "invalid_schema", "output schema: "+err.Error())
	}
	if err := requestMaps(b.Method.Input(), input, ""); err != nil {
		var mapping *MappingError
		if errors.As(err, &mapping) {
			mapping.Method = method
		}
		return nil, err
	}
	if err := responseMaps(b.Method.Output(), output, ""); err != nil {
		var mapping *MappingError
		if errors.As(err, &mapping) {
			mapping.Method = method
		}
		return nil, err
	}
	// Every value the request can map to is accepted by the workflow.
	if err := trigger.CheckBindings(b.Workflow, b.WorkflowInput, []trigger.Binding{{ID: method, Kind: trigger.GRPC, Workflow: b.Workflow, InputSchema: b.InputSchema}}); err != nil {
		return fail("", "workflow_mismatch", err.Error())
	}
	return &binding{Binding: b, method: method, input: input, output: output, slots: make(chan struct{}, b.MaxConcurrency), request: b.Method.Input(), response: b.Method.Output()}, nil
}

// ServiceNames lists the services the bindings define.
func (s *Server) ServiceNames() []string {
	names := make([]string, 0, len(s.services))
	for name := range s.services {
		names = append(names, name)
	}
	return names
}

// Registrar is a gRPC server that can report what is registered on it, as
// *grpc.Server does.
type Registrar interface {
	grpc.ServiceRegistrar
	GetServiceInfo() map[string]grpc.ServiceInfo
}

// ErrServiceConflict refuses a service name already registered on a shared
// server, such as the worker runtime's.
var ErrServiceConflict = errors.New("grpc: service is already registered")

// Register adds the bound services to a server, which may be shared with
// other services (a shared listener). It refuses a service name the server
// already has, instead of letting two services collide. The server's own
// options bound message sizes and streams; ServerOptions gives the bounds
// these bindings need. Like any registration, it must happen before the
// server serves: grpc-go exits the process otherwise.
func (s *Server) Register(registrar Registrar) error {
	existing := registrar.GetServiceInfo()
	for name := range s.services {
		if _, ok := existing[name]; ok {
			return fmt.Errorf("%w: %s", ErrServiceConflict, name)
		}
	}
	for name, bindings := range s.services {
		desc := &grpc.ServiceDesc{ServiceName: name, HandlerType: (*any)(nil)}
		for _, b := range bindings {
			desc.Methods = append(desc.Methods, grpc.MethodDesc{MethodName: string(b.Method.Name()), Handler: s.handler(b)})
		}
		registrar.RegisterService(desc, struct{}{})
	}
	return nil
}

// ServerOptions bounds a server that carries these bindings: message sizes
// no larger than the largest binding allows, and concurrent streams.
func (s *Server) ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(s.maxMessage),
		grpc.MaxSendMsgSize(s.maxMessage),
		grpc.MaxConcurrentStreams(DefaultMaxStreams),
	}
}

// NewServer returns a server of its own (an independent listener) bounded
// by ServerOptions, with the bindings registered. extra options, such as
// TLS credentials, are applied after the bounds.
func (s *Server) NewServer(extra ...grpc.ServerOption) (*grpc.Server, error) {
	server := grpc.NewServer(append(s.ServerOptions(), extra...)...)
	if err := s.Register(server); err != nil {
		return nil, err
	}
	return server, nil
}

// refusal builds a status carrying only a stable code, as the message and
// as ErrorInfo.reason.
func refusal(code codes.Code, reason string) error {
	st := status.New(code, reason)
	if detailed, err := st.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: ErrorDomain}); err == nil {
		st = detailed
	}
	return st.Err()
}

func (s *Server) handler(b *binding) func(any, context.Context, func(any) error, grpc.UnaryServerInterceptor) (any, error) {
	return func(_ any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (response any, err error) {
		// A panic in the authenticator, the authorization or the workflow
		// fails this call, not the process and every service sharing it.
		defer func() {
			if recover() != nil {
				response, err = nil, refusal(codes.Internal, "internal")
			}
		}()
		// Cheap refusals first: admission, identity and the method's
		// authorization are decided before the request is decoded.
		lease, err := s.application.Begin()
		if err != nil {
			return nil, refusal(codes.Unavailable, "unavailable")
		}
		defer lease.Release()
		// The call also stops if the application's drain times out.
		ctx, unbind := lease.Bind(ctx)
		defer unbind()
		principal, err := s.authenticate(ctx)
		if err != nil || strings.TrimSpace(principal.ID) == "" {
			return nil, refusal(codes.Unauthenticated, "unauthorized")
		}
		if b.Authorize(principal, b.method) != nil {
			return nil, refusal(codes.PermissionDenied, "forbidden")
		}
		select {
		case b.slots <- struct{}{}:
			defer func() { <-b.slots }()
		default:
			return nil, refusal(codes.ResourceExhausted, "saturated")
		}
		// Only the protobuf encoding is decoded: under another codec the
		// raw-bytes capture below would misread the request.
		if !protobufEncoding(ctx) {
			return nil, refusal(codes.InvalidArgument, "unsupported_encoding")
		}
		// The request is first received as raw wire bytes: an empty message
		// keeps every field unknown, in one copy. Its wire size, element
		// count and nesting are bounded before it is decoded, so a small
		// message cannot decode into a large structure.
		var raw emptypb.Empty
		if err := decode(&raw); err != nil {
			return nil, refusal(codes.InvalidArgument, "invalid_input")
		}
		wireBytes := raw.ProtoReflect().GetUnknown()
		if len(wireBytes) > b.MaxRequestBytes {
			return nil, refusal(codes.ResourceExhausted, "too_large")
		}
		budget := b.MaxElements
		if err := scan(wireBytes, b.request, 0, &budget); err != nil {
			if errors.Is(err, errMalformed) {
				return nil, refusal(codes.InvalidArgument, "invalid_input")
			}
			return nil, refusal(codes.ResourceExhausted, "too_large")
		}
		request := dynamicpb.NewMessage(b.request)
		if err := (proto.UnmarshalOptions{RecursionLimit: MaxNesting + 1}).Unmarshal(wireBytes, request); err != nil {
			return nil, refusal(codes.InvalidArgument, "invalid_input")
		}
		run := func(ctx context.Context, req any) (any, error) {
			message, ok := req.(*dynamicpb.Message)
			if !ok || message.Descriptor() != b.request {
				return nil, refusal(codes.Internal, "internal")
			}
			return s.call(ctx, b, principal, message)
		}
		if interceptor == nil {
			return run(ctx, request)
		}
		return interceptor(ctx, request, &grpc.UnaryServerInfo{Server: struct{}{}, FullMethod: b.method}, run)
	}
}

var (
	toJSON   = protojson.MarshalOptions{UseProtoNames: true, EmitDefaultValues: true}
	fromJSON = protojson.UnmarshalOptions{}
)

// protobufEncoding reports whether a call is encoded as protobuf, the only
// encoding the adapter decodes.
func protobufEncoding(ctx context.Context) bool {
	values := metadata.ValueFromIncomingContext(ctx, "content-type")
	if len(values) != 1 {
		return false
	}
	switch strings.ToLower(values[0]) {
	case "application/grpc", "application/grpc+proto":
		return true
	}
	return false
}

// jsonEstimate is a lower bound of a decoded message's JSON form: the
// names of every field it writes (every implicit-presence field is written
// with its default) and the bytes of its values. It stops past limit, so a
// request whose defaults would expand far beyond the domain value limit is
// refused before its JSON is built.
func jsonEstimate(message protoreflect.Message, limit int) int {
	total := 2
	fields := message.Descriptor().Fields()
	for i := 0; i < fields.Len() && total <= limit; i++ {
		field := fields.Get(i)
		if field.HasPresence() && !message.Has(field) {
			continue
		}
		total += len(field.Name()) + 4
		value := message.Get(field)
		switch {
		case field.IsList():
			list := value.List()
			for j := 0; j < list.Len() && total <= limit; j++ {
				if field.Message() != nil {
					total += jsonEstimate(list.Get(j).Message(), limit-total) + 1
				} else {
					total += scalarEstimate(list.Get(j)) + 1
				}
			}
		case field.Message() != nil:
			total += jsonEstimate(value.Message(), limit-total)
		default:
			total += scalarEstimate(value)
		}
	}
	return total
}

func scalarEstimate(value protoreflect.Value) int {
	switch v := value.Interface().(type) {
	case string:
		return len(v)
	case []byte:
		return len(v) * 4 / 3
	}
	return 1
}

// tooLarge reports a value the schema refused for its size.
func tooLarge(err error) bool {
	var invalid *schema.Error
	return errors.As(err, &invalid) && invalid.Code == "payload_too_large"
}

func (s *Server) call(ctx context.Context, b *binding, principal trigger.Principal, request *dynamicpb.Message) (any, error) {
	if hasUnknown(request.ProtoReflect()) {
		// A field this server's descriptor does not know (a newer client,
		// or a field sent with the wrong wire type) is refused, not
		// silently dropped.
		return nil, refusal(codes.InvalidArgument, "invalid_input")
	}
	if jsonEstimate(request.ProtoReflect(), schema.MaxPayloadBytes) > schema.MaxPayloadBytes {
		return nil, refusal(codes.ResourceExhausted, "too_large")
	}
	encoded, err := toJSON.Marshal(request)
	if err != nil {
		return nil, refusal(codes.InvalidArgument, "invalid_input")
	}
	input, err := b.input.Normalize(encoded)
	if tooLarge(err) {
		return nil, refusal(codes.ResourceExhausted, "too_large")
	}
	if err != nil {
		return nil, refusal(codes.InvalidArgument, "invalid_input")
	}
	ctx, cancel := context.WithTimeout(ctx, b.Timeout)
	defer cancel()
	// When the call's context ends is recorded, so a cancel and a deadline
	// are told apart by when it happened, not by when the workflow returned.
	var fired atomic.Int64
	stop := context.AfterFunc(ctx, func() { fired.Store(time.Now().UnixNano()) })
	defer stop()
	output, err := b.Handle(ctx, Call{Method: b.method, Principal: principal, Input: input})
	if err == nil && ctx.Err() != nil {
		// The workflow returned after its deadline or after the client
		// left: the call has already failed.
		err = ctx.Err()
	}
	if err != nil {
		ended := time.Now()
		if at := fired.Load(); at != 0 {
			ended = time.Unix(0, at)
		}
		return nil, statusFor(ctx, err, ended)
	}
	normalized, err := b.output.Normalize(output)
	if tooLarge(err) {
		return nil, refusal(codes.ResourceExhausted, "response_too_large")
	}
	if err != nil {
		return nil, refusal(codes.Internal, "invalid_output")
	}
	response := dynamicpb.NewMessage(b.response)
	if err := fromJSON.Unmarshal(normalized, response); err != nil {
		return nil, refusal(codes.Internal, "invalid_output")
	}
	if proto.Size(response) > b.MaxResponseBytes {
		return nil, refusal(codes.ResourceExhausted, "response_too_large")
	}
	return response, nil
}

var (
	errMalformed = errors.New("grpc: malformed request")
	errTooLarge  = errors.New("grpc: request exceeds its element or nesting bound")
)

// scan walks a message's wire bytes without decoding them, counting every
// field occurrence, every packed element and every nested message against
// budget, and refusing nesting deeper than MaxNesting.
func scan(data []byte, message protoreflect.MessageDescriptor, depth int, budget *int) error {
	if depth > MaxNesting {
		return errTooLarge
	}
	for len(data) > 0 {
		number, kind, n := protowire.ConsumeTag(data)
		if n < 0 {
			return errMalformed
		}
		data = data[n:]
		if *budget--; *budget < 0 {
			return errTooLarge
		}
		var field protoreflect.FieldDescriptor
		if message != nil {
			field = message.Fields().ByNumber(number)
		}
		if kind != protowire.BytesType || field == nil {
			if n = protowire.ConsumeFieldValue(number, kind, data); n < 0 {
				return errMalformed
			}
			data = data[n:]
			continue
		}
		value, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return errMalformed
		}
		data = data[n:]
		switch {
		case field.Kind() == protoreflect.MessageKind:
			if err := scan(value, field.Message(), depth+1, budget); err != nil {
				return err
			}
		case field.IsList() && field.Kind() != protoreflect.StringKind && field.Kind() != protoreflect.BytesKind:
			// A packed run: count its elements.
			for len(value) > 0 {
				var size int
				switch field.Kind() {
				case protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind, protoreflect.FloatKind:
					size = 4
				case protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind, protoreflect.DoubleKind:
					size = 8
				default:
					_, size = protowire.ConsumeVarint(value)
				}
				if size <= 0 || size > len(value) {
					return errMalformed
				}
				value = value[size:]
				if *budget--; *budget < 0 {
					return errTooLarge
				}
			}
		}
	}
	return nil
}

// hasUnknown reports unknown fields anywhere in a message.
func hasUnknown(message protoreflect.Message) bool {
	if len(message.GetUnknown()) > 0 {
		return true
	}
	found := false
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsList() && field.Message() != nil:
			list := value.List()
			for i := 0; i < list.Len() && !found; i++ {
				found = hasUnknown(list.Get(i).Message())
			}
		case field.Message() != nil && !field.IsMap():
			found = hasUnknown(value.Message())
		}
		return !found
	})
	return found
}

// statusFor maps a workflow error to a status that carries only a stable
// code: never the error's text. A call whose context ended at or after its
// deadline reports DeadlineExceeded even when the client's own cancellation
// arrived first, as it does when the client's deadline is the earlier one;
// one canceled before it reports Canceled.
func statusFor(ctx context.Context, err error, ended time.Time) error {
	deadline, bounded := ctx.Deadline()
	switch {
	case app.Aborted(ctx):
		return refusal(codes.Unavailable, "unavailable")
	case ctx.Err() != nil && bounded && !ended.Before(deadline):
		return refusal(codes.DeadlineExceeded, "deadline_exceeded")
	case errors.Is(ctx.Err(), context.Canceled), errors.Is(ctx.Err(), context.DeadlineExceeded):
		return refusal(codes.Canceled, "canceled")
	case errors.Is(err, trigger.ErrSaturated):
		return refusal(codes.ResourceExhausted, "saturated")
	}
	code, class, ok := trigger.Classify(err)
	if !ok {
		return refusal(codes.Internal, "internal")
	}
	switch class {
	case "validation":
		return refusal(codes.InvalidArgument, code)
	case "admission":
		return refusal(codes.ResourceExhausted, code)
	case "cancellation":
		return refusal(codes.Canceled, code)
	case "configuration":
		return refusal(codes.Internal, "internal")
	default:
		return refusal(codes.FailedPrecondition, code)
	}
}
