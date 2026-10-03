package deploy

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/well-prado/new-blok/app"
	appdeploy "github.com/well-prado/new-blok/app/deploy"
	"github.com/well-prado/new-blok/contract/deployment"
	runtime "github.com/well-prado/new-blok/contract/runtime"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/runtime/worker"
	blokhttp "github.com/well-prado/new-blok/trigger/http"
)

//go:embed node-catalog.json
var nodeCatalog []byte

type NodeInput struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
	DelayMS  int    `json:"delayMs"`
}
type NodeOutput struct {
	TotalCents int64 `json:"totalCents"`
}

// nodeArtifact hashes the selected worker implementation, application module,
// catalog, wire schema and dependency lock. Runtime dependencies are installed
// from that lock by the image build. This is not a durable checkpoint inventory.
func nodeArtifact(root string) (string, error) {
	h := sha256.New()
	paths := []string{"examples/deploy/nodes.mjs", "examples/deploy/node-catalog.json", "runtime/nodejs/package-lock.json", "contract/runtime/runtime.proto", "runtime/nodejs/generated/proto.sha256"}
	err := filepath.WalkDir(filepath.Join(root, "runtime/nodejs/dist"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			paths = append(paths, relative)
		}
		return nil
	})
	if err != nil {
		return "", errors.New("deployment: worker artifact unavailable")
	}
	for _, path := range paths {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			return "", errors.New("deployment: worker artifact unavailable")
		}
		h.Write([]byte(path))
		h.Write([]byte{0})
		h.Write(raw)
		h.Write([]byte{0})
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

// NewNodeDeployment selects one persistent child worker on authenticated
// loopback. Neither a broker nor a store is selected by this memory workflow.
func NewNodeDeployment(c deployment.Config, root string) (*appdeploy.Deployment, error) {
	c.WorkerRequired = true
	c.RequiredSecrets = append(c.RequiredSecrets, "BLOK_WORKER_TOKEN")
	if c.MaxAdmission > 32 {
		return nil, deployment.ErrInvalid
	}
	token := os.Getenv("BLOK_WORKER_TOKEN")
	if token == "" {
		return nil, deployment.ErrNotReady
	}
	address := "127.0.0.1:9001"
	if configured, ok := os.LookupEnv("BLOK_WORKER_ADDRESS"); ok {
		address = configured
	}
	host, port, err := net.SplitHostPort(address)
	portNumber, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil || portNumber < 1 || portNumber > 65535 || host != "127.0.0.1" {
		return nil, errors.New("deployment: worker address must be literal loopback 127.0.0.1 with a nonzero port")
	}
	digest, err := nodeArtifact(root)
	if err != nil {
		return nil, err
	}
	var descriptors []node.Descriptor
	if err := json.Unmarshal(nodeCatalog, &descriptors); err != nil {
		return nil, err
	}
	catalog, err := runtime.CatalogDigest(descriptors)
	if err != nil {
		return nil, err
	}
	// The private worker address is explicit and never published by Docker.
	limits := runtime.DefaultLimits()
	limits.MaxConcurrentCalls = 64
	hello := runtime.Hello{Protocol: runtime.ProtocolName, Major: 1, ArtifactDigest: digest, CatalogDigest: catalog, Generation: 1, Limits: limits}
	s, err := worker.New(worker.Config{Hello: hello, Capacity: 64, Factory: worker.ProcessFactory{Command: "node", Dir: root, Args: []string{"runtime/nodejs/dist/runtime/nodejs/main.js", "examples/deploy/nodes.mjs"}, Address: address, Token: token, Principal: "deploy-81", StartupTimeout: 3 * time.Second, Env: []string{"BLOK_WORKER_ADDRESS=" + address, "BLOK_WORKER_TOKEN=" + token, "BLOK_WORKER_PRINCIPAL=deploy-81", "BLOK_WORKER_ARTIFACT=" + digest, "BLOK_WORKER_GENERATION=1", "BLOK_WORKER_CAPABILITIES=[]", "BLOK_WORKER_PROTO=" + filepath.Join(root, "contract/runtime/runtime.proto")}}})
	if err != nil {
		return nil, err
	}
	definition, err := worker.Define[NodeInput, NodeOutput](s, descriptors[0])
	if err != nil {
		return nil, err
	}
	workflow, err := flow.Define(flow.Spec{Name: "deploy/quote", Version: "1.0.0"}, func(w *flow.Builder, input flow.Ref[NodeInput]) flow.Ref[NodeOutput] {
		return flow.Call(w, "quote", definition, input)
	})
	if err != nil {
		return nil, err
	}
	program, err := workflow.Lower()
	if err != nil {
		return nil, err
	}
	a, err := app.New(app.Config{DrainTimeout: c.DrainTimeout, Workflows: []app.Workflow{{Name: "deploy/quote"}}, Routes: []app.Route{{Method: "POST", Path: "/quotes", Workflow: "deploy/quote"}}, Dependencies: []app.Dependency{{Name: "nodejs", Start: func(ctx context.Context) error {
		if err := s.Start(ctx); err != nil {
			return errors.New("deployment: worker unavailable or incompatible")
		}
		return nil
	}, Close: s.Shutdown}}})
	if err != nil {
		return nil, err
	}
	handler, err := blokhttp.New(a, []blokhttp.Endpoint{{Method: "POST", Path: "/quotes", InputSchema: descriptors[0].InputSchema, Handle: func(ctx context.Context, input blokhttp.Input) (any, error) {
		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		var request NodeInput
		if err := json.Unmarshal(input.Body, &request); err != nil {
			return nil, err
		}
		result, err := engine.New(map[string]node.Any{"deploy/quote": definition.Any()}).Run(ctx, program, request)
		if err != nil {
			return nil, errors.New("deployment: worker execution failed")
		}
		return result.Output, nil
	}}})
	if err != nil {
		return nil, err
	}
	return appdeploy.NewDeployment(a, c, appdeploy.DeploymentChecks{Artifact: func(ctx context.Context) error {
		actual, err := nodeArtifact(root)
		if err != nil {
			return err
		}
		if actual != digest {
			return errors.New("deployment: worker artifact changed")
		}
		return ctx.Err()
	}, Worker: func(ctx context.Context) error {
		// Ready() alone is cached negotiation. An actual bounded pure RPC detects a
		// dead process/stream without changing the parent-owned worker API.
		probe, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		defer cancel()
		output, err := definition.Invoke(probe, NodeInput{SKU: "coffee", Quantity: 1})
		if err != nil {
			return err
		}
		if output.TotalCents != 1500 {
			return errors.New("deployment: incompatible worker result")
		}
		return nil
	}}, handler)
}
