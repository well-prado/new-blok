package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/well-prado/new-blok/examples/quote"
)

func main() {
	application, handler, err := quote.NewApplication()
	if err != nil {
		panic(err)
	}
	if err := application.Start(context.Background()); err != nil {
		panic(err)
	}
	server := &http.Server{Addr: ":8080", Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.ListenAndServe() }()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	select {
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			panic(err)
		}
	case <-signals:
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownContext)
		_ = application.Shutdown(shutdownContext)
	}
}
