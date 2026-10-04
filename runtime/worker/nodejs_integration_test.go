package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	contract "github.com/well-prado/new-blok/contract/runtime"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/flowtest"
	"github.com/well-prado/new-blok/node"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestActualNodeWorkerThroughTypedWorkflow(t *testing.T) {
	root := os.Getenv("BLOK_NODE_INTEGRATION_ROOT")
	if root == "" {
		t.Skip("explicit Node conformance gate: set BLOK_NODE_INTEGRATION_ROOT to built Node repository")
	}
	nodePath, err := exec.LookPath("node")
	if err != nil {
		t.Fatal("Node matrix gate requires installed node")
	}
	main := filepath.Join(root, "runtime/nodejs/dist/runtime/nodejs/main.js")
	module := filepath.Join(root, "runtime/nodejs/dist/testdata/worker/nodejs/nodes.js")
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
	digest, err := contract.CatalogDigest(discovery.Nodes)
	if err != nil || digest != discovery.CatalogDigest {
		t.Fatalf("canonical discovery mismatch: %s %s %v", digest, discovery.CatalogDigest, err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	artifact := contract.CanonicalDigest([]byte("synthetic-node-artifact"))
	token := "synthetic-integration-token-0000000001"
	caps := []contract.Capability{"http:synthetic"}
	hello := contract.Hello{Protocol: contract.ProtocolName, Major: 1, Minor: contract.ProtocolMinor, ArtifactDigest: artifact, CatalogDigest: digest, Generation: 1, Capabilities: caps, Limits: contract.DefaultLimits()}
	factory := ProcessFactory{Command: nodePath, Args: []string{main, module}, Address: address, Token: token, Principal: "app-1", Capabilities: caps, StartupTimeout: 3 * time.Second, Env: []string{"BLOK_WORKER_ADDRESS=" + address, "BLOK_WORKER_TOKEN=" + token, "BLOK_WORKER_PRINCIPAL=app-1", "BLOK_WORKER_ARTIFACT=" + artifact, "BLOK_WORKER_GENERATION=1", `BLOK_WORKER_CAPABILITIES=["http:synthetic"]`}}
	supervisor, err := New(Config{Hello: hello, Factory: factory, Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := supervisor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := supervisor.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	}()
	find := func(name string) node.Descriptor {
		for _, d := range discovery.Nodes {
			if d.Name == name {
				return d
			}
		}
		t.Fatalf("missing %s", name)
		return node.Descriptor{}
	}
	type input struct {
		SKU      string `json:"sku"`
		Quantity int    `json:"quantity"`
	}
	type output struct {
		Total int64 `json:"totalCents"`
	}
	quote, err := Define[input, output](supervisor, find("fixture/quote"))
	if err != nil {
		t.Fatal(err)
	}
	wf, err := flow.Define(flow.Spec{Name: "integration/quote", Version: "1.0.0"}, func(w *flow.Builder, in flow.Ref[input]) flow.Ref[output] { return flow.Call(w, "quote", quote, in) })
	if err != nil {
		t.Fatal(err)
	}
	program, err := wf.Lower()
	if err != nil {
		t.Fatal(err)
	}
	run := flowtest.Run(context.Background(), program, map[string]node.Any{"fixture/quote": quote.Any()}, input{"coffee", 2}, flowtest.Options{})
	if !run.OK() {
		t.Fatal(run.Err())
	}
	if run.Response().(output).Total != 3000 {
		t.Fatalf("quote: %+v", run.Response())
	}
	if _, err := quote.Invoke(context.Background(), input{"coffee", 0}); err == nil {
		t.Fatal("invalid quantity dispatched")
	}
	call := func(id, name, body string) contract.Call {
		return contract.Call{CallID: id, AttemptID: "attempt-" + id, Node: name, NodeVersion: "1.0.0", Generation: 1, Deadline: time.Now().Add(time.Second), Input: []byte(body), Capabilities: caps, IdempotencyKey: "logical-operation"}
	}
	for i, key := range []string{string([]byte{0xff}), strings.Repeat("x", 129)} {
		request := call(fmt.Sprintf("invalid-key-%d", i), "fixture/echo", `{"value":"1"}`)
		request.IdempotencyKey = key
		if _, err := supervisor.Call(context.Background(), request); !errors.Is(err, contract.ErrLimitExceeded) {
			t.Fatalf("invalid operation key crossed wire: %v", err)
		}
	}
	business := call("business-key", "fixture/provider", `{"kind":"uncertain"}`)
	business.IdempotencyKey = "order 123/ação"
	businessResult, businessErr := supervisor.Call(context.Background(), business)
	if businessErr != nil || businessResult.Error == nil || businessResult.Error.IdempotencyKey != business.IdempotencyKey || businessResult.Error.Class != contract.ErrorUncertain || businessResult.Error.SafeToRetry() {
		t.Fatalf("business key rejected or lost, or prior rejection closed channel: %+v %v", businessResult, businessErr)
	}
	for i, n := range []string{"9223372036854775807", "-9223372036854775808"} {
		id := []string{"max", "min"}[i]
		r, err := supervisor.Call(context.Background(), call(id, "fixture/echo", `{"value":"`+n+`"}`))
		if err != nil || r.Error != nil || string(r.Output) != `{"value":"`+n+`"}` {
			t.Fatalf("int64: %+v %v", r, err)
		}
	}
	r, err := supervisor.Call(context.Background(), call("overflow", "fixture/echo", `{"value":"9223372036854775808"}`))
	if err != nil || r.Error == nil || r.Error.Class != contract.ErrorInvalidInput {
		t.Fatalf("overflow: %+v %v", r, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, err = supervisor.Call(ctx, call("late", "fixture/late", `{"milliseconds":80}`))
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	for _, kind := range []string{"transient", "uncertain", "domain"} {
		r, err := supervisor.Call(context.Background(), call(kind, "fixture/provider", `{"kind":"`+kind+`"}`))
		if err != nil || r.Error == nil || r.Error.IdempotencyKey != "logical-operation" || strings.Contains(r.Error.Message, "secret") {
			t.Fatalf("provider: %+v %v", r, err)
		}
		if kind == "uncertain" && r.Error.SafeToRetry() {
			t.Fatal("uncertain retry authorized")
		}
	}
	r, err = supervisor.Call(context.Background(), call("panic", "fixture/panic", `{}`))
	if err != nil || r.Error == nil || strings.Contains(r.Error.Message, "secret") {
		t.Fatalf("panic leaked: %+v %v", r, err)
	}
	if _, err := supervisor.Call(context.Background(), call("panic", "fixture/panic", `{}`)); err == nil {
		t.Fatal("duplicate identity accepted")
	}
}
