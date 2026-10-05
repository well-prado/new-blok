// Command footprint is the same application as contract/observe's footprint
// fixture with the OTLP/HTTP trace, metric and log exporters selected: the
// ADR 0020 "selected" footprint fixture.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/contract/observe"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/observe/otel"
)

type value struct {
	N int `json:"n"`
}

func main() {
	ctx := context.Background()
	endpoint := "http://127.0.0.1:1" // nothing listens; export failures are counted, never fatal
	traces, _ := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint+"/v1/traces"))
	metrics, _ := otlpmetrichttp.New(ctx, otlpmetrichttp.WithEndpointURL(endpoint+"/v1/metrics"))
	logs, _ := otlploghttp.New(ctx, otlploghttp.WithEndpointURL(endpoint+"/v1/logs"))
	exporter, err := otel.New(otel.Config{Traces: traces, Metrics: metrics, Logs: logs, ExportTimeout: 100 * time.Millisecond})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	double := node.MustDefine("footprint/double", "1.0.0", func(_ context.Context, in value) (value, error) { return value{N: in.N * 2}, nil },
		node.Description("double"), node.Schemas([]byte(`{"type":"object","properties":{"n":{"type":"integer"}},"required":["n"]}`), []byte(`{"type":"object","properties":{"n":{"type":"integer"}},"required":["n"]}`)), node.Pure())
	application, err := app.New(app.Config{Inspection: exporter, Trace: observe.TracePolicy{Ratio: 1}})
	if err == nil {
		err = application.Start(ctx)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	program := contract.InternalProgram{WorkflowID: "footprint", Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "double", Kind: "call", Node: "footprint/double"},
		{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "double", Path: []string{"n"}}}},
	}}
	result, err := execution.NewRunner(application, map[string]node.Any{"footprint/double": double.Any()}).Run(ctx, program, value{N: 21}, inspection.Invocation{RunID: "footprint", Principal: "footprint"})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	shutdown, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	_ = exporter.Shutdown(shutdown)
	fmt.Println(result.Output)
}
