// Native deployment selects no store or foreign worker.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	appdeploy "github.com/well-prado/new-blok/app/deploy"
	"github.com/well-prado/new-blok/contract/deployment"
	"github.com/well-prado/new-blok/examples/quote"
)

func run() error {
	c, err := deployment.FromEnv(deployment.Config{ListenerAddress: "127.0.0.1:8080", MaxAdmission: 32, DrainTimeout: 5 * time.Second}, os.LookupEnv)
	if err != nil {
		return err
	}
	a, handler, err := quote.NewApplication()
	if err != nil {
		return err
	}
	// NewApplication lowers and validates the native workflow before returning.
	d, err := appdeploy.NewDeployment(a, c, appdeploy.DeploymentChecks{Artifact: func(context.Context) error { return nil }}, handler)
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
		os.Exit(1)
	}
}
