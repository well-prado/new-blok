// Command shopapp is a consumer in its own Go module. It composes the recipe
// exclusively through exported shop and SQLite APIs.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/well-prado/new-blok/examples/recipes/shop"
	"github.com/well-prado/new-blok/store/sqlite"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	appPath, err := required("SHOP_DB_PATH")
	if err != nil {
		return err
	}
	sinkPath, err := required("SHOP_SINK_DB_PATH")
	if err != nil {
		return err
	}
	address, err := required("SHOP_LISTEN_ADDR")
	if err != nil {
		return err
	}
	alice, err := required("SHOP_TOKEN_ALICE")
	if err != nil {
		return err
	}
	bob, err := required("SHOP_TOKEN_BOB")
	if err != nil {
		return err
	}
	webhookSecret, err := required("SHOP_WEBHOOK_SECRET")
	if err != nil {
		return err
	}
	absoluteApp, err := filepath.Abs(appPath)
	if err != nil {
		return err
	}
	absoluteSink, err := filepath.Abs(sinkPath)
	if err != nil {
		return err
	}
	if absoluteApp == absoluteSink {
		return errors.New("SHOP_SINK_DB_PATH must be separate from SHOP_DB_PATH")
	}
	database, err := (sqlite.Backend{}).Open(ctx, absoluteApp)
	if err != nil {
		return err
	}
	defer database.Close()
	sinkDatabase, err := (sqlite.Backend{}).Open(ctx, absoluteSink)
	if err != nil {
		return err
	}
	defer sinkDatabase.Close()
	sink, err := shop.NewSyntheticSink(ctx, sinkDatabase)
	if err != nil {
		return err
	}
	application, err := shop.New(ctx, shop.Config{
		Database:   database,
		Tokens:     map[string]string{"alice": alice, "bob": bob},
		WebhookKey: []byte(webhookSecret),
		Publisher:  sink,
	})
	if err != nil {
		return err
	}
	if err := application.Start(ctx); err != nil {
		return err
	}
	server := &http.Server{
		Addr:              address,
		Handler:           application.Handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    32 << 10,
	}
	serverError := make(chan error, 1)
	go func() { serverError <- server.ListenAndServe() }()
	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		for workerCtx.Err() == nil {
			processed, processErr := application.ProcessOne(workerCtx)
			drained, drainErr := application.DrainOutbox(workerCtx)
			if processErr != nil || drainErr != nil || (!processed && !drained) {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
	select {
	case <-ctx.Done():
	case err := <-serverError:
		if !errors.Is(err, http.ErrServerClosed) {
			stopWorker()
			<-workerDone
			return err
		}
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stopWorker()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return err
	}
	<-workerDone
	return application.Shutdown(shutdownCtx)
}

func required(name string) (string, error) {
	value := os.Getenv(name)
	if value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}
