package trigger_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/tool"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
	bgrpc "github.com/well-prado/new-blok/trigger/grpc"
	blokhttp "github.com/well-prado/new-blok/trigger/http"
	tmcp "github.com/well-prado/new-blok/trigger/mcp"
	bws "github.com/well-prado/new-blok/trigger/websocket"
)

type note struct {
	Text string `json:"text"`
}

type saved struct {
	Saved bool `json:"saved"`
}

var noteSchema = []byte(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}`)
var savedSchema = []byte(`{"type":"object","properties":{"saved":{"type":"boolean"}},"required":["saved"]}`)

// saveRunner runs a workflow, through the production engine, whose node
// writes the note to the store. With a ledger, an earlier node first
// records the note there, committing its own effect.
type saveRunner struct {
	engine  *engine.Engine
	program contract.InternalProgram
}

func newSaveRunner(t *testing.T, database, ledger store.Database) *saveRunner {
	t.Helper()
	save, err := node.Define("busy/save-note", "1.0.0", func(ctx context.Context, in note) (saved, error) {
		err := database.WithTx(ctx, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `INSERT INTO notes (text) VALUES (?)`, in.Text)
			return err
		})
		return saved{Saved: err == nil}, err
	}, node.Description("Saves a note"), node.Schemas(noteSchema, savedSchema), node.Effects("database:write"))
	if err != nil {
		t.Fatal(err)
	}
	record, err := node.Define("busy/record-note", "1.0.0", func(ctx context.Context, in note) (note, error) {
		return in, ledger.WithTx(ctx, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `INSERT INTO notes (text) VALUES (?)`, in.Text)
			return err
		})
	}, node.Description("Records a note"), node.Schemas(noteSchema, noteSchema), node.Effects("database:write"))
	if err != nil {
		t.Fatal(err)
	}
	workflow, err := flow.Define(flow.Spec{Name: "busy/save", Version: "1.0.0"}, func(b *flow.Builder, in flow.Ref[note]) flow.Ref[saved] {
		if ledger != nil {
			in = flow.Call(b, "record", record, in)
		}
		return flow.Call(b, "save", save, in)
	})
	if err != nil {
		t.Fatal(err)
	}
	program, err := workflow.Lower()
	if err != nil {
		t.Fatal(err)
	}
	return &saveRunner{engine: engine.New(map[string]node.Any{"busy/save-note": save.Any(), "busy/record-note": record.Any()}), program: program}
}

func (r *saveRunner) run(ctx context.Context, input []byte) (json.RawMessage, error) {
	var in note
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}
	result, err := r.engine.Run(ctx, r.program, in)
	if err != nil {
		return nil, err
	}
	return json.Marshal(result.Output)
}

func noteMethod(t *testing.T) protoreflect.MethodDescriptor {
	t.Helper()
	optional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: proto.String("busy/notes.proto"), Package: proto.String("busy"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: proto.String("Note"), Field: []*descriptorpb.FieldDescriptorProto{{Name: proto.String("text"), JsonName: proto.String("text"), Number: proto.Int32(1), Label: optional, Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()}}},
			{Name: proto.String("Saved"), Field: []*descriptorpb.FieldDescriptorProto{{Name: proto.String("saved"), JsonName: proto.String("saved"), Number: proto.Int32(1), Label: optional, Type: descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum()}}},
		},
		Service: []*descriptorpb.ServiceDescriptorProto{{Name: proto.String("Notes"), Method: []*descriptorpb.MethodDescriptorProto{{Name: proto.String("Save"), InputType: proto.String(".busy.Note"), OutputType: proto.String(".busy.Saved")}}}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return file.Services().ByName("Notes").Methods().ByName("Save")
}

type noteCatalog struct{ runner *saveRunner }

func (noteCatalog) List(context.Context, tool.Principal) ([]tmcp.Tool, error) {
	return []tmcp.Tool{{Name: "busy/save", Version: "1.0.0", Description: "Saves a note", InputSchema: noteSchema, OutputSchema: savedSchema, Effects: []string{"database:write"}}}, nil
}

func (c noteCatalog) Invoke(ctx context.Context, _ tool.Principal, call tmcp.Call) (json.RawMessage, error) {
	return c.runner.run(ctx, call.Input)
}

type bearerRT struct{ base http.RoundTripper }

func (b bearerRT) RoundTrip(request *http.Request) (*http.Response, error) {
	request = request.Clone(request.Context())
	request.Header.Set("Authorization", "Bearer alice")
	return b.base.RoundTrip(request)
}

// TestInBandTriggersAnswerSaturatedOnBusyStore: a workflow whose node writes
// to a store that stays write-locked past its busy timeout, run through the
// production engine behind HTTP, gRPC, WebSocket and MCP. Each trigger
// answers with its saturation response, never an internal error, and
// nothing is committed (#190).
func TestInBandTriggersAnswerSaturatedOnBusyStore(t *testing.T) {
	answers, notes, _ := busyAnswers(t, false)
	want := map[string]string{
		"http":      "Service Unavailable saturated 1",
		"grpc":      codes.ResourceExhausted.String() + " saturated",
		"websocket": "saturated",
		"mcp":       `{"code":"saturated"}`,
	}
	for via, expected := range want {
		if answers[via] != expected {
			t.Errorf("%s answered %q; want %q", via, answers[via], expected)
		}
	}
	if notes != 1 {
		t.Errorf("notes=%d; want only the holder's", notes)
	}
}

// TestInBandTriggersDoNotAskForARetryAfterAnEffect: the same busy store, but
// an earlier step has already committed its effect to another store. A
// retry would repeat that effect, so no trigger answers saturated; each
// answers its failure response, without inviting a retry (#190).
func TestInBandTriggersDoNotAskForARetryAfterAnEffect(t *testing.T) {
	answers, notes, recorded := busyAnswers(t, true)
	want := map[string]string{
		"http":      "Internal Server Error internal error",
		"grpc":      codes.FailedPrecondition.String() + " node_error",
		"websocket": "node_error",
		"mcp":       `{"code":"node_error"}`,
	}
	for via, expected := range want {
		if answers[via] != expected {
			t.Errorf("%s answered %q; want %q", via, answers[via], expected)
		}
	}
	if notes != 1 || recorded != 4 {
		t.Errorf("notes=%d recorded=%d; want only the holder's note and one record per trigger", notes, recorded)
	}
}

func openNotes(t *testing.T, name string) store.Database {
	t.Helper()
	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE notes (text TEXT)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return database
}

func countNotes(t *testing.T, database store.Database) int {
	t.Helper()
	count := 0
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRow(`SELECT COUNT(*) FROM notes`).Scan(&count)
	}); err != nil {
		t.Fatal(err)
	}
	return count
}

// busyAnswers calls the workflow once through each trigger while its store
// is write-locked past the busy timeout, and returns each trigger's answer
// and, once the lock is released, the rows the store and the ledger hold.
func busyAnswers(t *testing.T, recordFirst bool) (map[string]string, int, int) {
	ctx := context.Background()
	database := openNotes(t, "notes.db")
	var ledger store.Database
	if recordFirst {
		ledger = openNotes(t, "ledger.db")
	}
	runner := newSaveRunner(t, database, ledger)
	application, err := app.New(app.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(ctx); err != nil {
		t.Fatal(err)
	}
	authenticate := func(request *http.Request) (trigger.Principal, error) {
		if request.Header.Get("Authorization") != "Bearer alice" {
			return trigger.Principal{}, errors.New("unauthenticated")
		}
		return trigger.Principal{ID: "alice"}, nil
	}
	httpServer, err := blokhttp.New(application, []blokhttp.Endpoint{{Method: "POST", Path: "/notes", InputSchema: noteSchema, Authenticate: authenticate, Handle: func(ctx context.Context, in blokhttp.Input) (any, error) {
		return runner.run(ctx, in.Body)
	}}})
	if err != nil {
		t.Fatal(err)
	}
	sockets, err := bws.New(application, bws.Endpoint{Path: "/ws", Authenticate: authenticate, InputSchema: noteSchema, OnMessage: func(ctx context.Context, m bws.Message) (json.RawMessage, error) {
		return runner.run(ctx, m.Input)
	}})
	if err != nil {
		t.Fatal(err)
	}
	budget := tool.Budget{MaxDepth: 4, MaxInputBytes: 1 << 10, MaxOutputBytes: 1 << 10, MaxTokens: 1, MaxCalls: 1, Deadline: time.Now().Add(time.Hour)}
	tools, err := tmcp.New(application, tmcp.Config{Name: "notes", Version: "1.0.0", Catalog: noteCatalog{runner: runner}, Authenticate: func(_ context.Context, token string, _ *http.Request) (tool.Principal, error) {
		if token != "alice" {
			return tool.Principal{}, errors.New("unauthenticated")
		}
		return tool.Principal{ID: "alice", MaxDepth: 4}, nil
	}, Expose: []string{"busy/save@1.0.0"}, Budget: budget, Timeout: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/notes", httpServer)
	mux.Handle("/ws", sockets)
	mux.Handle("/mcp", tools)
	web := httptest.NewServer(mux)
	method := noteMethod(t)
	methods, err := bgrpc.New(application, func(context.Context) (trigger.Principal, error) { return trigger.Principal{ID: "alice"}, nil }, []bgrpc.Binding{{Method: method, Workflow: "busy/save", WorkflowInput: noteSchema, InputSchema: noteSchema, OutputSchema: savedSchema, Authorize: bgrpc.AllowAuthenticated, Timeout: 30 * time.Second, Handle: func(ctx context.Context, call bgrpc.Call) (json.RawMessage, error) {
		return runner.run(ctx, call.Input)
	}}})
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer(methods.ServerOptions()...)
	if err := methods.Register(grpcServer); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = grpcServer.Serve(listener) }()
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	socket, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(web.URL, "http")+"/ws", &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer alice"}}})
	if err != nil {
		t.Fatal(err)
	}
	session, err := sdk.NewClient(&sdk.Implementation{Name: "busy", Version: "1"}, nil).Connect(ctx, &sdk.StreamableClientTransport{Endpoint: web.URL + "/mcp", HTTPClient: &http.Client{Transport: bearerRT{base: http.DefaultTransport}}, MaxRetries: -1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = session.Close()
		socket.CloseNow()
		_ = conn.Close()
		stop, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = tools.Shutdown(stop)
		_ = sockets.Shutdown(stop)
		_ = application.Shutdown(stop)
		grpcServer.Stop()
		web.Close()
	})

	// Hold the store's write lock past its busy timeout.
	holding, release, held := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		held <- database.WithTx(ctx, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, `INSERT INTO notes (text) VALUES ('holder')`); err != nil {
				return err
			}
			close(holding)
			<-release
			return nil
		})
	}()
	<-holding
	var released sync.Once
	t.Cleanup(func() { released.Do(func() { close(release) }) })

	answers := map[string]string{}
	var mu sync.Mutex
	answer := func(via, got string) { mu.Lock(); answers[via] = got; mu.Unlock() }
	var group sync.WaitGroup
	group.Add(4)
	go func() {
		defer group.Done()
		request, _ := http.NewRequest(http.MethodPost, web.URL+"/notes", strings.NewReader(`{"text":"http"}`))
		request.Header.Set("Authorization", "Bearer alice")
		request.Header.Set("Content-Type", "application/json")
		response, err := web.Client().Do(request)
		if err != nil {
			answer("http", err.Error())
			return
		}
		var body map[string]any
		_ = json.NewDecoder(response.Body).Decode(&body)
		response.Body.Close()
		answer("http", strings.TrimSpace(strings.Join([]string{http.StatusText(response.StatusCode), anyString(body["error"]), response.Header.Get("Retry-After")}, " ")))
	}()
	go func() {
		defer group.Done()
		request := dynamicpb.NewMessage(method.Input())
		request.Set(method.Input().Fields().ByName("text"), protoreflect.ValueOfString("grpc"))
		reply := dynamicpb.NewMessage(method.Output())
		err := conn.Invoke(ctx, "/busy.Notes/Save", request, reply)
		answer("grpc", status.Code(err).String()+" "+status.Convert(err).Message())
	}()
	go func() {
		defer group.Done()
		if err := socket.Write(ctx, websocket.MessageText, []byte(`{"id":"1","input":{"text":"ws"}}`)); err != nil {
			answer("websocket", err.Error())
			return
		}
		_, frame, err := socket.Read(ctx)
		if err != nil {
			answer("websocket", err.Error())
			return
		}
		var reply struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(frame, &reply)
		answer("websocket", reply.Error)
	}()
	go func() {
		defer group.Done()
		result, err := session.CallTool(ctx, &sdk.CallToolParams{Name: tmcp.ToolName("busy/save", "1.0.0"), Arguments: json.RawMessage(`{"text":"mcp"}`)})
		if err != nil {
			answer("mcp", err.Error())
			return
		}
		text := ""
		if len(result.Content) > 0 {
			if content, ok := result.Content[0].(*sdk.TextContent); ok {
				text = content.Text
			}
		}
		answer("mcp", text)
	}()
	group.Wait()
	released.Do(func() { close(release) })
	if err := <-held; err != nil {
		t.Fatal(err)
	}
	recorded := 0
	if ledger != nil {
		recorded = countNotes(t, ledger)
	}
	return answers, countNotes(t, database), recorded
}

func anyString(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}
