// Command http selects only the HTTP trigger. It listens on a loopback port,
// reports the address, serves until SIGTERM and then drains.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/well-prado/new-blok/app"
	blokhttp "github.com/well-prado/new-blok/trigger/http"
	"github.com/well-prado/new-blok/trigger/testdata/selection/quote"
)

func main() {
	runner, err := quote.New()
	if err != nil {
		panic(err)
	}
	application, err := app.New(app.Config{Workflows: []app.Workflow{{Name: "selection/quote"}}, Routes: []app.Route{{Method: "POST", Path: "/quotes", Workflow: "selection/quote"}}})
	if err != nil {
		panic(err)
	}
	handler, err := blokhttp.New(application, []blokhttp.Endpoint{{Method: "POST", Path: "/quotes", InputSchema: quote.InputSchema, Handle: func(ctx context.Context, in blokhttp.Input) (any, error) {
		var request quote.Input
		if err := json.Unmarshal(in.Body, &request); err != nil {
			return nil, err
		}
		return runner.Run(ctx, request)
	}}})
	if err != nil {
		panic(err)
	}
	constructed := runtime.NumGoroutine()
	if err := application.Start(context.Background()); err != nil {
		panic(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, os.Interrupt)
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	encoder := json.NewEncoder(os.Stdout)
	_ = encoder.Encode(map[string]any{"listening": listener.Addr().String(), "goroutinesAfterConstruction": constructed})
	<-signals
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		panic(err)
	}
	if err := <-served; !errors.Is(err, http.ErrServerClosed) {
		panic(err)
	}
	if err := application.Shutdown(ctx); err != nil {
		panic(err)
	}
	_ = encoder.Encode(map[string]any{"stopped": true})
}
