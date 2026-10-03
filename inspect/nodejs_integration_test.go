package inspect_test

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	inspectioncontract "github.com/well-prado/new-blok/contract/inspection"
	runtimecontract "github.com/well-prado/new-blok/contract/runtime"
	"github.com/well-prado/new-blok/inspect"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/runtime/worker"
)

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
	var descriptor node.Descriptor
	for _, candidate := range discovery.Nodes {
		if candidate.Name == "fixture/quote" {
			descriptor = candidate
			break
		}
	}
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
	hello := runtimecontract.Hello{Protocol: runtimecontract.ProtocolName, Major: 1, Minor: 0, ArtifactDigest: artifact, CatalogDigest: digest, Generation: 1, Capabilities: caps, Limits: runtimecontract.DefaultLimits()}
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
	result, err := engine.New(map[string]node.Any{descriptor.Name: definition.Any()}).WithObserver(recorder).RunObserved(context.Background(), program, input{SKU: "coffee", Quantity: 2}, inspectioncontract.Invocation{RunID: "node-run-1", Principal: "app-1"})
	if err != nil || result.Output != int64(3000) {
		t.Fatalf("Node result=%+v err=%v", result, err)
	}
	page, err := recorder.Inspect("app-1", fullPolicy(), inspectioncontract.Query{Version: inspectioncontract.Version, RunID: "node-run-1"})
	if err != nil || page.Run.Status != inspectioncontract.StatusCompleted || len(page.Steps) != 2 || page.Steps[0].Output == nil {
		t.Fatalf("Node inspection page=%+v err=%v", page, err)
	}
}
