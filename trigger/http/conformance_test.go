package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/conformance"
	"github.com/well-prado/new-blok/trigger"
)

// httpDriver drives the real adapter through a TCP listener and net/http
// client. It never calls the workflow; only the adapter's handler does.
type httpDriver struct {
	application *app.Application
	handler     *Server
	server      *http.Server
	client      *http.Client
	mu          sync.Mutex
	address     string
}

func (*httpDriver) Declaration() trigger.Declaration { return Declaration }

func (d *httpDriver) Open(_ context.Context, env conformance.TriggerEnv) error {
	application, err := app.New(app.Config{})
	if err != nil {
		return err
	}
	handler, err := New(application, []Endpoint{{
		Method:      "POST",
		Path:        "/conformance",
		InputSchema: env.InputSchema,
		Timeout:     10 * time.Second,
		Authenticate: func(request *http.Request) (Principal, error) {
			return env.Authenticate(strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer "))
		},
		Handle: func(ctx context.Context, input Input) (any, error) {
			output, err := env.Workflow(ctx, conformance.Call{Input: input.Body, Principal: input.Principal})
			if err != nil {
				return nil, err
			}
			return output, nil
		},
	}})
	if err != nil {
		return err
	}
	d.application, d.handler = application, handler
	d.client = &http.Client{Transport: &http.Transport{}}
	return nil
}

func (d *httpDriver) Start(ctx context.Context) error {
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

func (d *httpDriver) Endpoint() string { d.mu.Lock(); defer d.mu.Unlock(); return d.address }

func (d *httpDriver) Deliver(ctx context.Context, delivery conformance.Delivery) (conformance.Outcome, error) {
	address := d.Endpoint()
	if address == "" {
		return conformance.Outcome{Kind: conformance.Rejected, Code: "unavailable"}, nil
	}
	requestContext, cancel := context.WithCancel(ctx)
	defer cancel()
	disconnected := make(chan struct{})
	if delivery.Disconnect != nil {
		go func() {
			select {
			case <-delivery.Disconnect:
				close(disconnected)
				cancel()
			case <-requestContext.Done():
			}
		}()
	}
	request, err := http.NewRequestWithContext(requestContext, http.MethodPost, "http://"+address+"/conformance", bytes.NewReader(delivery.Payload))
	if err != nil {
		return conformance.Outcome{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	if delivery.Credential != "" {
		request.Header.Set("Authorization", "Bearer "+delivery.Credential)
	}
	response, err := d.client.Do(request)
	if err != nil {
		select {
		case <-disconnected:
			return conformance.Outcome{Kind: conformance.Disconnected}, nil
		default:
		}
		var opErr *net.OpError
		if errors.As(err, &opErr) {
			return conformance.Outcome{Kind: conformance.Rejected, Code: "unavailable"}, nil
		}
		return conformance.Outcome{}, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return conformance.Outcome{}, err
	}
	if response.StatusCode == http.StatusOK {
		return conformance.Outcome{Kind: conformance.Completed, Output: body}, nil
	}
	var failure struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(body, &failure)
	outcome := conformance.Outcome{Kind: conformance.Rejected, Message: string(body)}
	switch {
	case response.StatusCode == http.StatusUnauthorized:
		outcome.Code = "unauthorized"
	case response.StatusCode == http.StatusBadRequest && failure.Error == "invalid request":
		outcome.Code = "invalid_input"
	case response.StatusCode == http.StatusServiceUnavailable && failure.Error == "application unavailable":
		outcome.Code = "unavailable"
	case response.StatusCode >= 500 && failure.Error == "internal error":
		outcome.Code = "internal"
	default:
		outcome.Code = failure.Error
	}
	return outcome, nil
}

func (d *httpDriver) Stop(ctx context.Context) error {
	shutdownErr := d.application.Shutdown(ctx)
	closeErr := d.server.Close()
	d.client.CloseIdleConnections()
	d.mu.Lock()
	d.address = ""
	d.mu.Unlock()
	return errors.Join(shutdownErr, closeErr)
}

func TestHTTPAdapterPassesTriggerConformanceOverRealListener(t *testing.T) {
	corpus, err := conformance.LoadTriggerCorpus()
	if err != nil {
		t.Fatal(err)
	}
	report, err := conformance.RunTrigger(context.Background(), &httpDriver{}, corpus, conformance.TriggerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ran, skipped := 0, 0
	for _, result := range report.Results {
		if result.Applicable {
			ran++
		} else {
			skipped++
			t.Logf("not applicable: %s (%s)", result.CaseID, result.Reason)
		}
	}
	if ran != 12 || skipped != 7 {
		t.Fatalf("ran=%d skipped=%d report=%+v", ran, skipped, report)
	}
}
