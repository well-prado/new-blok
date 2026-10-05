// Command shop is blok dev's durable fixture application (E11-T03, ADR
// 0026): examples/deploy's journaled order deployment, whose artifact
// identity is the digest of its own executable and whose readiness refuses
// a journal that retains runs admitted under another identity. Synthetic
// configuration only.
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

// banner changes the executable without changing behaviour.
const banner = "durable fixture v1"

func run() error {
	c, err := deployment.FromEnv(deployment.Config{ListenerAddress: "127.0.0.1:8080", MaxAdmission: 32, DrainTimeout: 5 * time.Second}, os.LookupEnv)
	if err != nil {
		return err
	}
	d, err := deploy.NewDurable(c, os.Getenv("BLOK_VOLUME"))
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, banner)
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
