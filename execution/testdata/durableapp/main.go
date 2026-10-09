// Command durableapp is a real Blok application whose workflows are
// durable (flow.Durable), for the process kill tests in
// execution/durable_process_test.go. Every effect appends a line to the
// file EFFECTS names, so effects are counted across processes; the pure
// node hold blocks on the item HOLD_AT names until the file GATE exists.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"
)

type item struct {
	Value int `json:"value"`
}

type batch struct {
	Items []item `json:"items"`
}

var schema = []byte(`{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"]}`)

func effect(name string, run func(item) item) node.Definition[item, item] {
	return node.MustDefine("app/"+name, "1.0.0", func(_ context.Context, in item) (item, error) {
		file, err := os.OpenFile(os.Getenv("EFFECTS"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return item{}, err
		}
		defer file.Close()
		if _, err := fmt.Fprintf(file, "%s-%d\n", name, in.Value); err != nil {
			return item{}, err
		}
		return run(in), file.Sync()
	}, node.Description(name), node.Schemas(schema, schema), node.Effects("app:"+name))
}

func main() {
	holdAt, _ := strconv.Atoi(os.Getenv("HOLD_AT"))
	charge := effect("charge", func(in item) item { return item{Value: in.Value + 1} })
	ship := effect("ship", func(in item) item { return in })
	vip := effect("vip", func(in item) item { return item{Value: in.Value * 10} })
	hold := node.MustDefine("app/hold", "1.0.0", func(ctx context.Context, in item) (item, error) {
		for in.Value == holdAt {
			if _, err := os.Stat(os.Getenv("GATE")); err == nil {
				break
			}
			if os.Getenv("HELD") != "" {
				_ = os.WriteFile(os.Getenv("HELD"), []byte("held"), 0o600)
			}
			select {
			case <-ctx.Done():
				return item{}, ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
		}
		return in, nil
	}, node.Description("hold"), node.Schemas(schema, schema))
	durable := func(name string) flow.Spec { return flow.Spec{Name: name, Version: "1.0.0", Durability: flow.Durable} }
	pay := flow.MustDefine(durable("pay"), func(b *flow.Builder, in flow.Ref[item]) flow.Ref[item] {
		charged := flow.Call(b, "charge", charge, in)
		held := flow.Call(b, "hold", hold, charged)
		return flow.Call(b, "ship", ship, held)
	})
	approve := flow.MustDefine(durable("approve"), func(b *flow.Builder, in flow.Ref[item]) flow.Ref[item] {
		charged := flow.Call(b, "charge", charge, in)
		flow.Wait(b, "approval", "approval", 0)
		return flow.Call(b, "ship", ship, charged)
	})
	loop := flow.MustDefine(durable("batch"), func(b *flow.Builder, in flow.Ref[batch]) flow.Ref[[]item] {
		return flow.Each(b, "lines", flow.Select[batch, []item](in, "items"), 1, func(arm *flow.ArmBuilder, line flow.Ref[item]) flow.Ref[item] {
			charged := flow.Call(arm.Builder(), "charge", charge, line)
			return flow.Call(arm.Builder(), "hold", hold, charged)
		})
	})
	parent := flow.MustDefine(durable("parent"), func(b *flow.Builder, in flow.Ref[item]) flow.Ref[item] {
		charged := flow.Call(b, "charge", charge, in)
		return flow.Child[item, item](b, "kid", "kid", charged)
	})
	kid := flow.MustDefine(durable("kid"), func(b *flow.Builder, in flow.Ref[item]) flow.Ref[item] {
		held := flow.Call(b, "hold", hold, in)
		return flow.Call(b, "vip", vip, held)
	})
	var workflows []execution.DurableWorkflow
	for _, register := range []func() (execution.DurableWorkflow, error){
		func() (execution.DurableWorkflow, error) { return execution.Durable(pay) },
		func() (execution.DurableWorkflow, error) { return execution.Durable(approve) },
		func() (execution.DurableWorkflow, error) { return execution.Durable(loop) },
		func() (execution.DurableWorkflow, error) { return execution.Durable(parent) },
		func() (execution.DurableWorkflow, error) { return execution.Durable(kid) },
	} {
		workflow, err := register()
		if err != nil {
			log.Fatal(err)
		}
		if !strings.Contains(","+os.Getenv("UNREGISTER")+",", ","+workflow.Name()+",") {
			workflows = append(workflows, workflow)
		}
	}
	lease, _ := time.ParseDuration(os.Getenv("LEASE"))
	runtime, err := execution.NewDurable(execution.DurableConfig{
		Path:        os.Getenv("JOURNAL"),
		Nodes:       map[string]node.Any{"app/charge": charge.Any(), "app/ship": ship.Any(), "app/vip": vip.Any(), "app/hold": hold.Any()},
		Workflows:   workflows,
		WakeupLease: lease,
		Interval:    100 * time.Millisecond,
	})
	if err != nil {
		log.Fatal(err)
	}
	application, err := app.New(app.Config{Dependencies: []app.Dependency{runtime.Dependency()}})
	if err != nil {
		log.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /runs/{workflow}", func(w http.ResponseWriter, r *http.Request) {
		var input any
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		result, err := runtime.Run(ctx, r.PathValue("workflow"), input, execution.RunOptions{RequestKey: r.Header.Get("Idempotency-Key")})
		reply(w, result, err)
	})
	mux.HandleFunc("GET /runs/{run}", func(w http.ResponseWriter, r *http.Request) {
		result, err := runtime.Status(r.Context(), r.PathValue("run"))
		reply(w, result, err)
	})
	mux.HandleFunc("POST /runs/{run}/signals/{name}", func(w http.ResponseWriter, r *http.Request) {
		var payload json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&payload)
		result, err := runtime.Signal(r.Context(), r.PathValue("run"), r.PathValue("name"), r.Header.Get("Signal-Id"), "operator", payload)
		reply(w, result, err)
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("ADDR_FILE"), []byte(listener.Addr().String()), 0o600); err != nil {
		log.Fatal(err)
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	<-signals
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
	_ = application.Shutdown(ctx)
}

func reply(w http.ResponseWriter, result any, err error) {
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}
