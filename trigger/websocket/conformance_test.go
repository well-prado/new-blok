package websocket_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/conformance"
	"github.com/well-prado/new-blok/trigger"
	blokws "github.com/well-prado/new-blok/trigger/websocket"
)

// wsDriver opens a real WebSocket connection per delivery; disconnect is a
// client that drops its connection while the message runs.
type wsDriver struct {
	application *app.Application
	handler     *blokws.Server
	server      *http.Server
	mu          sync.Mutex
	address     string
}

func (*wsDriver) Declaration() trigger.Declaration { return blokws.Declaration }

func (d *wsDriver) Open(_ context.Context, env conformance.TriggerEnv) error {
	application, err := app.New(app.Config{})
	if err != nil {
		return err
	}
	handler, err := blokws.New(application, blokws.Endpoint{
		Path: "/ws", InputSchema: env.InputSchema, PingInterval: time.Hour,
		Authenticate: func(r *http.Request) (trigger.Principal, error) {
			return env.Authenticate(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		},
		OnMessage: func(ctx context.Context, m blokws.Message) (json.RawMessage, error) {
			return env.Workflow(ctx, conformance.Call{Input: m.Input, Principal: m.Connection.Principal})
		},
	})
	d.application, d.handler = application, handler
	return err
}

func (d *wsDriver) Start(ctx context.Context) error {
	if err := d.application.Start(ctx); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	d.server = &http.Server{Handler: d.handler, ReadHeaderTimeout: 5 * time.Second}
	d.mu.Lock()
	d.address = listener.Addr().String()
	d.mu.Unlock()
	go func() { _ = d.server.Serve(listener) }()
	return nil
}

func (d *wsDriver) Endpoint() string { d.mu.Lock(); defer d.mu.Unlock(); return d.address }

func (d *wsDriver) Deliver(ctx context.Context, delivery conformance.Delivery) (conformance.Outcome, error) {
	address := d.Endpoint()
	if address == "" {
		return conformance.Outcome{Kind: conformance.Rejected, Code: "unavailable"}, nil
	}
	header := http.Header{}
	if delivery.Credential != "" {
		header.Set("Authorization", "Bearer "+delivery.Credential)
	}
	dialCtx, cancelDial := context.WithTimeout(ctx, 5*time.Second)
	conn, response, err := websocket.Dial(dialCtx, "ws://"+address+"/ws", &websocket.DialOptions{HTTPHeader: header})
	cancelDial()
	if err != nil {
		if response != nil && response.StatusCode == http.StatusUnauthorized {
			return conformance.Outcome{Kind: conformance.Rejected, Code: "unauthorized"}, nil
		}
		return conformance.Outcome{Kind: conformance.Rejected, Code: "unavailable"}, nil
	}
	defer conn.CloseNow()
	frame, err := json.Marshal(map[string]json.RawMessage{"id": json.RawMessage(`"` + delivery.Key + `"`), "input": delivery.Payload})
	if len(delivery.Payload) == 0 {
		frame, err = json.Marshal(map[string]string{"id": delivery.Key})
	}
	if err != nil {
		return conformance.Outcome{}, err
	}
	if err := conn.Write(ctx, websocket.MessageText, frame); err != nil {
		return conformance.Outcome{}, err
	}
	type result struct {
		data []byte
		err  error
	}
	read := make(chan result, 1)
	go func() {
		_, data, err := conn.Read(context.Background())
		read <- result{data, err}
	}()
	select {
	case <-delivery.Disconnect:
		// The client goes away while its message runs.
		conn.CloseNow()
		<-read
		return conformance.Outcome{Kind: conformance.Disconnected}, nil
	case r := <-read:
		if r.err != nil {
			return conformance.Outcome{}, r.err
		}
		var reply struct {
			Output json.RawMessage `json:"output"`
			Error  string          `json:"error"`
		}
		if err := json.Unmarshal(r.data, &reply); err != nil {
			return conformance.Outcome{}, err
		}
		if reply.Error != "" {
			return conformance.Outcome{Kind: conformance.Rejected, Code: reply.Error, Message: string(r.data)}, nil
		}
		return conformance.Outcome{Kind: conformance.Completed, Output: reply.Output}, nil
	}
}

func (d *wsDriver) Stop(ctx context.Context) error {
	shutdownErr := d.handler.Shutdown(ctx)
	closeErr := d.server.Close()
	appErr := d.application.Shutdown(ctx)
	d.mu.Lock()
	d.address = ""
	d.mu.Unlock()
	return errors.Join(shutdownErr, closeErr, appErr)
}

func TestWebSocketAdapterPassesTriggerConformance(t *testing.T) {
	corpus, err := conformance.LoadTriggerCorpus()
	if err != nil {
		t.Fatal(err)
	}
	report, err := conformance.RunTrigger(context.Background(), &wsDriver{}, corpus, conformance.TriggerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ran := 0
	for _, result := range report.Results {
		if result.Applicable {
			ran++
		}
	}
	if ran != 12 {
		t.Fatalf("ran=%d report=%+v", ran, report)
	}
}
