package clusterapp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/internal/cluster"
	"github.com/well-prado/new-blok/store/distributed"
)

// DistributedWorkerDependency wires fenced partition workers into the normal
// application lifecycle. It is a separate dependency from ingress so callers
// may compose both explicitly and shutdown uses the application's drain rules.
func DistributedWorkerDependency(runtime *cluster.Runtime, ownerID string) app.Dependency {
	var mu sync.Mutex
	var cancel context.CancelFunc
	var done chan error
	return app.Dependency{
		Name: "distributed-workers",
		Start: func(ctx context.Context) error {
			if runtime == nil || ownerID == "" {
				return cluster.ErrInvalid
			}
			checkCtx, stop := context.WithTimeout(ctx, time.Second)
			err := runtime.Check(checkCtx)
			stop()
			if err != nil {
				return err
			}
			workerCtx, stopWorker := context.WithCancel(context.Background())
			mu.Lock()
			cancel, done = stopWorker, make(chan error, 1)
			started := done
			mu.Unlock()
			go func() { started <- runtime.Run(workerCtx, ownerID) }()
			return nil
		},
		Close: func(ctx context.Context) error {
			mu.Lock()
			stopWorker, stopped := cancel, done
			mu.Unlock()
			if stopWorker == nil || stopped == nil {
				return nil
			}
			stopWorker()
			select {
			case err := <-stopped:
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
}

// NewDistributedWorkflowHandler composes a real cluster runtime into an HTTP
// ingress route. tenant must return an authenticated, stable tenant identity;
// this handler does not infer tenancy from caller-controlled JSON.
func NewDistributedWorkflowHandler(runtime *cluster.Runtime, workflow string, tenant func(*http.Request) (string, error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if runtime == nil || workflow == "" || tenant == nil {
			http.Error(w, "workflow unavailable", http.StatusServiceUnavailable)
			return
		}
		tenantID, err := tenant(request)
		if err != nil || tenantID == "" {
			http.Error(w, "tenant authorization required", http.StatusUnauthorized)
			return
		}
		requestKey := request.Header.Get("Idempotency-Key")
		if requestKey == "" {
			http.Error(w, "Idempotency-Key is required", http.StatusBadRequest)
			return
		}
		request.Body = http.MaxBytesReader(w, request.Body, cluster.MaxInputBytes)
		input, err := io.ReadAll(request.Body)
		if err != nil || !json.Valid(input) {
			http.Error(w, "request body must be bounded valid JSON", http.StatusBadRequest)
			return
		}
		admission, err := runtime.Admit(request.Context(), cluster.Submission{Tenant: tenantID, RequestKey: requestKey, Workflow: workflow, Input: input})
		if err != nil {
			switch {
			case errors.Is(err, cluster.ErrRequestConflict):
				http.Error(w, "idempotency key conflicts with accepted work", http.StatusConflict)
			case errors.Is(err, distributed.ErrAdmissionFull):
				w.Header().Set("Retry-After", "1")
				http.Error(w, "admission capacity is full", http.StatusTooManyRequests)
			case errors.Is(err, cluster.ErrUnavailable):
				w.Header().Set("Retry-After", "1")
				http.Error(w, "durable admission unavailable", http.StatusServiceUnavailable)
			default:
				http.Error(w, "invalid workflow submission", http.StatusBadRequest)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(admission)
	})
}

type distributedSignalRequest struct {
	WaitID   string          `json:"waitId"`
	SignalID string          `json:"signalId"`
	Payload  json.RawMessage `json:"payload"`
}

// NewDistributedSignalHandler keeps tenant/principal resolution and
// authorization outside durable storage while routing accepted signals through
// the current partition owner fence.
func NewDistributedSignalHandler(
	runtime *cluster.Runtime,
	tenant func(*http.Request) (string, error),
	principal func(*http.Request) (string, error),
	authorize func(*http.Request, string, string) bool,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if runtime == nil || tenant == nil || principal == nil || authorize == nil {
			http.Error(w, "signal delivery unavailable", http.StatusServiceUnavailable)
			return
		}
		tenantID, err := tenant(request)
		if err != nil || tenantID == "" {
			http.Error(w, "tenant authorization required", http.StatusUnauthorized)
			return
		}
		principalID, err := principal(request)
		if err != nil || principalID == "" {
			http.Error(w, "principal authorization required", http.StatusUnauthorized)
			return
		}
		request.Body = http.MaxBytesReader(w, request.Body, cluster.MaxInputBytes)
		var submission distributedSignalRequest
		decoder := json.NewDecoder(request.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&submission); err != nil {
			http.Error(w, "signal must be bounded valid JSON", http.StatusBadRequest)
			return
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			http.Error(w, "signal must contain exactly one JSON value", http.StatusBadRequest)
			return
		}
		result, err := runtime.DeliverSignal(request.Context(), tenantID, submission.WaitID, submission.SignalID, principalID, submission.Payload, authorize(request, tenantID, submission.WaitID))
		if err != nil {
			switch {
			case errors.Is(err, cluster.ErrUnauthorizedSignal):
				http.Error(w, "signal is unauthorized", http.StatusForbidden)
			case errors.Is(err, cluster.ErrUnavailable):
				w.Header().Set("Retry-After", "1")
				http.Error(w, "signal storage unavailable", http.StatusServiceUnavailable)
			case errors.Is(err, cluster.ErrWaitNotFound):
				http.Error(w, "wait not found", http.StatusNotFound)
			case errors.Is(err, cluster.ErrRequestConflict):
				http.Error(w, "signal identity conflicts with prior delivery", http.StatusConflict)
			case errors.Is(err, cluster.ErrInvalid):
				http.Error(w, "invalid signal", http.StatusBadRequest)
			default:
				// Not a client error: the delivery outcome was not established.
				http.Error(w, "signal delivery failed", http.StatusInternalServerError)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(result)
	})
}
