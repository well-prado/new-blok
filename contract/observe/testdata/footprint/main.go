// Command footprint is a Go-only application that selects inspection and
// tracing but no telemetry exporter: the ADR 0020 "off" footprint fixture.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/contract/observe"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/node"
)

type discard struct{}

func (discard) Observe(inspection.Event) {}

type value struct {
	N int `json:"n"`
}

func main() {
	double := node.MustDefine("footprint/double", "1.0.0", func(_ context.Context, in value) (value, error) { return value{N: in.N * 2}, nil },
		node.Description("double"), node.Schemas([]byte(`{"type":"object","properties":{"n":{"type":"integer"}},"required":["n"]}`), []byte(`{"type":"object","properties":{"n":{"type":"integer"}},"required":["n"]}`)), node.Pure())
	application, err := app.New(app.Config{Inspection: discard{}, Trace: observe.TracePolicy{Ratio: 1}})
	if err == nil {
		err = application.Start(context.Background())
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	program := contract.InternalProgram{WorkflowID: "footprint", Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "double", Kind: "call", Node: "footprint/double"},
		{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "double", Path: []string{"n"}}}},
	}}
	result, err := execution.NewRunner(application, map[string]node.Any{"footprint/double": double.Any()}).Run(context.Background(), program, value{N: 21}, inspection.Invocation{RunID: "footprint", Principal: "footprint"})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(result.Output)
}
