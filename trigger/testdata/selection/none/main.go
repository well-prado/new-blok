// Command none is an application with no trigger selected. It links no
// trigger package, so it can open no listener and start no adapter goroutine.
package main

import (
	"context"
	"encoding/json"
	"os"
	"runtime"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/trigger/testdata/selection/quote"
)

func main() {
	runner, err := quote.New()
	if err != nil {
		panic(err)
	}
	application, err := app.New(app.Config{Workflows: []app.Workflow{{Name: "selection/quote"}}})
	if err != nil {
		panic(err)
	}
	if err := application.Start(context.Background()); err != nil {
		panic(err)
	}
	output, err := runner.Run(context.Background(), quote.Input{SKU: "coffee", Quantity: 2})
	if err != nil {
		panic(err)
	}
	report := map[string]any{"goroutines": runtime.NumGoroutine(), "output": output}
	if err := application.Shutdown(context.Background()); err != nil {
		panic(err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
		panic(err)
	}
}
