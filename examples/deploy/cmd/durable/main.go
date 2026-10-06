package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/well-prado/new-blok/contract/deployment"
	"github.com/well-prado/new-blok/examples/deploy"
)

func run() error {
	c, err := deployment.FromEnv(deployment.Config{ListenerAddress: "127.0.0.1:8080", MaxAdmission: 32, DrainTimeout: 5 * time.Second}, os.LookupEnv)
	if err != nil {
		return err
	}
	d, err := deploy.NewDurable(c, os.Getenv("BLOK_VOLUME"))
	if err != nil {
		return err
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	return d.Run(context.Background(), signals)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		// A journal retaining runs this executable cannot adopt exits with
		// deployment.ExitRetainedIncompatible, which blok dev reports as
		// dev_durable_incompatible instead of restarting it (ADR 0026).
		os.Exit(deployment.ExitCode(err))
	}
}
