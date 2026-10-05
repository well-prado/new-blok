package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/well-prado/new-blok/examples/recipes/shop"
	"github.com/well-prado/new-blok/observe/redact"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: shop {migrate-up|migrate-status|teardown|serve}")
	}
	switch args[0] {
	case "migrate-up", "migrate-status", "teardown", "serve":
	default:
		return fmt.Errorf("unknown command %q; usage: shop {migrate-up|migrate-status|teardown|serve}", args[0])
	}
	path := os.Getenv("SHOP_DB_PATH")
	if path == "" || path == ":memory:" {
		return errors.New("SHOP_DB_PATH must name a persistent SQLite file")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, absolute)
	if err != nil {
		return err
	}
	defer database.Close()
	switch args[0] {
	case "migrate-up":
		return shop.Migrate(ctx, database)
	case "migrate-status":
		var version int
		if err := database.WithTx(ctx, func(tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM shop_schema_migrations`).Scan(&version)
		}); err != nil {
			return err
		}
		fmt.Printf("shop schema version %d\n", version)
		return nil
	case "teardown":
		return shop.Teardown(ctx, database)
	case "serve":
		return serve(ctx, database)
	}
	return errors.New("unreachable command")
}

func serve(parent context.Context, database store.Database) error {
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
	webhookKey, err := required("SHOP_WEBHOOK_SECRET")
	if err != nil {
		return err
	}
	sinkPath, err := required("SHOP_SINK_DB_PATH")
	if err != nil {
		return err
	}
	appPath, err := filepath.Abs(os.Getenv("SHOP_DB_PATH"))
	if err != nil {
		return err
	}
	sinkPath, err = filepath.Abs(sinkPath)
	if err != nil {
		return err
	}
	if appPath == sinkPath {
		return errors.New("SHOP_SINK_DB_PATH must be separate from SHOP_DB_PATH")
	}
	sinkDatabase, err := (sqlite.Backend{}).Open(parent, sinkPath)
	if err != nil {
		return err
	}
	defer sinkDatabase.Close()
	sink, err := shop.NewSyntheticSink(parent, sinkDatabase)
	if err != nil {
		return err
	}
	application, err := shop.New(parent, shop.Config{
		Database:   database,
		Tokens:     map[string]string{"alice": alice, "bob": bob},
		WebhookKey: []byte(webhookKey),
		Publisher:  sink,
	})
	if err != nil {
		return err
	}
	if err := application.Start(parent); err != nil {
		return err
	}
	server := &http.Server{Addr: address, Handler: application.Handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 32 << 10}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.ListenAndServe() }()
	workerCtx, stopWorker := context.WithCancel(parent)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		for workerCtx.Err() == nil {
			processed, processErr := processAvailable(workerCtx, application)
			if processErr != nil && workerCtx.Err() == nil {
				logIterationError(os.Stderr, processErr)
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if !processed {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
	ctx, stopSignals := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	select {
	case <-ctx.Done():
	case err := <-serverErrors:
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

func processAvailable(ctx context.Context, application *shop.Application) (bool, error) {
	processed, processErr := application.ProcessOne(ctx)
	drained, drainErr := application.DrainOutbox(ctx)
	return processed || drained, errors.Join(processErr, drainErr)
}

// logIterationError reports a failed worker or outbox iteration instead of
// dropping it. The message passes the framework's redaction boundary (#80)
// first: an error can quote a header, a payload or a provider response.
func logIterationError(w io.Writer, err error) {
	_, _ = fmt.Fprintln(w, "shop: worker iteration failed: "+redact.Message(err.Error()))
}

func required(name string) (string, error) {
	value := os.Getenv(name)
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}
