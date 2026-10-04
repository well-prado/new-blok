package inspect_test

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract"
	inspectioncontract "github.com/well-prado/new-blok/contract/inspection"
	runtimecontract "github.com/well-prado/new-blok/contract/runtime"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/inspect"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/runtime/worker"
)

type processingObserver struct {
	recorder *inspect.Recorder
	runID    string
	started  chan struct{}
}

func (o *processingObserver) Observe(event inspectioncontract.Event) {
	o.recorder.Observe(event)
	if event.RunID == o.runID && event.Kind == inspectioncontract.StepProcessing {
		select {
		case o.started <- struct{}{}:
		default:
		}
	}
}

func TestActualNodeWorkerRunProducesInspectionProjection(t *testing.T) {
	root := os.Getenv("BLOK_NODE_INTEGRATION_ROOT")
	if root == "" {
		t.Skip("set BLOK_NODE_INTEGRATION_ROOT to run the actual Node worker inspection scenario")
	}
	nodePath, err := exec.LookPath("node")
	if err != nil {
		t.Fatal(err)
	}
	main := filepath.Join(root, "runtime/nodejs/dist/runtime/nodejs/main.js")
	module := filepath.Join(root, "runtime/nodejs/dist/testdata/worker/nodejs/nodes.js")
	if _, err := os.Stat(main); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(nodePath, main, module, "--discover").Output()
	if err != nil {
		t.Fatal(err)
	}
	var discovery struct {
		Nodes         []node.Descriptor `json:"nodes"`
		CatalogDigest string            `json:"catalogDigest"`
	}
	if err := json.Unmarshal(raw, &discovery); err != nil {
		t.Fatal(err)
	}
	digest, err := runtimecontract.CatalogDigest(discovery.Nodes)
	if err != nil || digest != discovery.CatalogDigest {
		t.Fatalf("catalog digest=%q discovered=%q err=%v", digest, discovery.CatalogDigest, err)
	}
	descriptors := map[string]node.Descriptor{}
	for _, candidate := range discovery.Nodes {
		descriptors[candidate.Name] = candidate
	}
	descriptor := descriptors["fixture/quote"]
	if descriptor.Name == "" {
		t.Fatal("Node worker did not discover fixture/quote")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	artifact := runtimecontract.CanonicalDigest([]byte("synthetic-inspection-node-artifact"))
	token := "synthetic-inspection-token-0000000001"
	caps := []runtimecontract.Capability{"http:synthetic"}
	hello := runtimecontract.Hello{Protocol: runtimecontract.ProtocolName, Major: 1, Minor: runtimecontract.ProtocolMinor, ArtifactDigest: artifact, CatalogDigest: digest, Generation: 1, Capabilities: caps, Limits: runtimecontract.DefaultLimits()}
	factory := worker.ProcessFactory{
		Command:        nodePath,
		Args:           []string{main, module},
		Address:        address,
		Token:          token,
		Principal:      "app-1",
		Capabilities:   caps,
		StartupTimeout: 3 * time.Second,
		Env: []string{
			"BLOK_WORKER_ADDRESS=" + address,
			"BLOK_WORKER_TOKEN=" + token,
			"BLOK_WORKER_PRINCIPAL=app-1",
			"BLOK_WORKER_ARTIFACT=" + artifact,
			"BLOK_WORKER_GENERATION=1",
			`BLOK_WORKER_CAPABILITIES=["http:synthetic"]`,
		},
	}
	supervisor, err := worker.New(worker.Config{Hello: hello, Factory: factory, Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := supervisor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := supervisor.Shutdown(ctx); err != nil {
			t.Errorf("Node worker shutdown: %v", err)
		}
	}()
	type input struct {
		SKU      string `json:"sku"`
		Quantity int    `json:"quantity"`
	}
	type output struct {
		Total int64 `json:"totalCents"`
	}
	definition, err := worker.Define[input, output](supervisor, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	program := contract.InternalProgram{WorkflowID: "integration/node-quote", Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "quote", Kind: "call", Node: descriptor.Name},
		{Index: 1, ID: "respond", Kind: "output", References: []contract.Reference{{Step: "quote", Path: []string{"totalCents"}}}},
	}}
	recorder := inspect.NewRecorder()
	providerDesc, slowDesc := descriptors["fixture/provider"], descriptors["fixture/slow"]
	type providerInput struct {
		Kind string `json:"kind"`
	}
	type doneOutput struct {
		Done bool `json:"done"`
	}
	type slowInput struct {
		Milliseconds int `json:"milliseconds"`
	}
	providerNode, err := worker.Define[providerInput, doneOutput](supervisor, providerDesc)
	if err != nil {
		t.Fatal(err)
	}
	slowNode, err := worker.Define[slowInput, doneOutput](supervisor, slowDesc)
	if err != nil {
		t.Fatal(err)
	}
	observer := &processingObserver{recorder: recorder, started: make(chan struct{}, 4)}
	application, err := app.New(app.Config{Workflows: []app.Workflow{{Name: "integration/node-quote"}}, Inspection: observer})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	runner := execution.NewRunner(application, map[string]node.Any{descriptor.Name: definition.Any(), providerDesc.Name: providerNode.Any(), slowDesc.Name: slowNode.Any()})
	observer.runID = "node-run-1"
	result, err := runner.Run(context.Background(), program, input{SKU: "coffee", Quantity: 2}, inspectioncontract.Invocation{RunID: "node-run-1", Principal: "app-1", AttemptID: "node-attempt-1"})
	if err != nil || result.Output != int64(3000) {
		t.Fatalf("Node result=%+v err=%v", result, err)
	}
	page, err := recorder.Inspect("app-1", fullPolicy(), inspectioncontract.Query{Version: inspectioncontract.Version, RunID: "node-run-1"})
	if err != nil || page.Run.Status != inspectioncontract.StatusCompleted || len(page.Steps) != 2 || page.Steps[0].Output == nil {
		t.Fatalf("Node inspection page=%+v err=%v", page, err)
	}
	logPolicy := fullPolicy()
	logPolicy.Fields[inspectioncontract.FieldLogs] = true
	logged, err := recorder.Inspect("app-1", logPolicy, inspectioncontract.Query{Version: inspectioncontract.Version, RunID: "node-run-1"})
	if err != nil || len(logged.Steps[0].Logs) != 1 || logged.Steps[0].Logs[0].Message != "quote calculated" || strings.Contains(string(logged.Steps[0].Logs[0].Attrs), "synthetic-token-value") || !strings.Contains(string(logged.Steps[0].Logs[0].Attrs), "redacted") {
		t.Fatalf("actual Node log inspection=%+v err=%v", logged, err)
	}
	failedProgram := contract.InternalProgram{WorkflowID: "integration/node-quote", Instructions: []contract.InternalInstruction{{Index: 0, ID: "quote", Kind: "call", Node: descriptor.Name}}}
	_, err = runner.Run(context.Background(), failedProgram, input{SKU: "bad-sku", Quantity: 1}, inspectioncontract.Invocation{RunID: "node-run-failed", Principal: "app-1"})
	if err == nil {
		t.Fatal("Node failure scenario unexpectedly succeeded")
	}
	failed, err := recorder.Inspect("app-1", fullPolicy(), inspectioncontract.Query{Version: inspectioncontract.Version, RunID: "node-run-failed"})
	if err != nil || failed.Run.Status != inspectioncontract.StatusFailed || len(failed.Steps) != 1 || failed.Steps[0].Status != inspectioncontract.StatusFailed {
		t.Fatalf("actual Node failure=%+v err=%v", failed, err)
	}
	providerProgram := contract.InternalProgram{WorkflowID: "integration/node-quote", Instructions: []contract.InternalInstruction{{Index: 0, ID: "provider", Kind: "call", Node: providerDesc.Name}}}
	_, err = runner.Run(context.Background(), providerProgram, providerInput{Kind: "uncertain"}, inspectioncontract.Invocation{RunID: "node-run-uncertain", Principal: "app-1"})
	if err == nil {
		t.Fatal("Node uncertain scenario unexpectedly succeeded")
	}
	uncertain, err := recorder.Inspect("app-1", fullPolicy(), inspectioncontract.Query{Version: inspectioncontract.Version, RunID: "node-run-uncertain"})
	if err != nil || uncertain.Run.Status != inspectioncontract.StatusUncertain || uncertain.Steps[0].Status != inspectioncontract.StatusUncertain || strings.Contains(string(uncertain.Steps[0].Output), "synthetic-secret") {
		t.Fatalf("actual Node uncertain=%+v err=%v", uncertain, err)
	}
	slowProgram := contract.InternalProgram{WorkflowID: "integration/node-quote", Instructions: []contract.InternalInstruction{{Index: 0, ID: "slow", Kind: "call", Node: slowDesc.Name}}}
	for {
		select {
		case <-observer.started:
		default:
			goto drained
		}
	}
drained:
	observer.runID = "node-run-canceled"
	ctx, cancel := context.WithCancel(context.Background())
	cancelDone := make(chan error, 1)
	go func() {
		_, runErr := runner.Run(ctx, slowProgram, slowInput{Milliseconds: 5000}, inspectioncontract.Invocation{RunID: "node-run-canceled", Principal: "app-1"})
		cancelDone <- runErr
	}()
	select {
	case <-observer.started:
		cancel()
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("Node slow step did not reach actual worker")
	}
	runErr := <-cancelDone
	if runErr == nil {
		t.Fatal("Node cancellation scenario unexpectedly succeeded")
	}
	canceled, err := recorder.Inspect("app-1", fullPolicy(), inspectioncontract.Query{Version: inspectioncontract.Version, RunID: "node-run-canceled"})
	if err != nil || canceled.Run.Status != inspectioncontract.StatusCanceled {
		t.Fatalf("actual Node cancellation=%+v err=%v runErr=%v", canceled, err, runErr)
	}
}
