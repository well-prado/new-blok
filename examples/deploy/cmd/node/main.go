package main

import (
	"context"
	"fmt"
	"github.com/well-prado/new-blok/contract/deployment"
	"github.com/well-prado/new-blok/examples/deploy"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func run() error {
	c, err := deployment.FromEnv(deployment.Config{ListenerAddress: "127.0.0.1:8080", MaxAdmission: 2, DrainTimeout: 5 * time.Second}, os.LookupEnv)
	if err != nil {
		return err
	}
	root, err := os.Getwd()
	if err != nil {
		return err
	}
	d, err := deploy.NewNodeDeployment(c, root)
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
