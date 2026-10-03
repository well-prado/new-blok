package deploy

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"runtime/debug"

	"github.com/well-prado/new-blok/contract/artifact"
)

// The composition source binds the request mapping and selected order service.
// The executable digest covers that service and all compiled dependencies.
//
//go:embed durable.go
var durableComposition []byte

const durableCheckpointFormat = "deploy-order-dispatch-v1"
const durableInputSchema = `{"type":"object","additionalProperties":false,"properties":{"requestKey":{"type":"string"},"sku":{"type":"string"},"quantity":{"type":"integer","minimum":1,"maximum":100}},"required":["requestKey","sku","quantity"]}`
const durableCheckpointSchema = `{"type":"object","additionalProperties":false,"properties":{"format":{"type":"string"},"request":` + durableInputSchema + `,"queued":{"type":"boolean"}},"required":["format","request","queued"]}`

func deploymentDigest(raw []byte) string {
	h := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(h[:])
}

func durableArtifact() (artifact.Manifest, string, error) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return artifact.Manifest{}, "", errors.New("deployment: build identity unavailable")
	}
	path, err := os.Executable()
	if err != nil {
		return artifact.Manifest{}, "", err
	}
	f, err := os.Open(path)
	if err != nil {
		return artifact.Manifest{}, "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return artifact.Manifest{}, "", err
	}
	lock, err := json.Marshal(info)
	if err != nil {
		return artifact.Manifest{}, "", err
	}
	codec := deploymentDigest([]byte(durableCheckpointFormat + "\x00" + durableCheckpointSchema))
	return artifact.Manifest{
		Name: "deploy/orders", Version: "1.0.0",
		WorkflowDigest:     deploymentDigest(durableComposition),
		NativeBinaryDigest: "sha256:" + hex.EncodeToString(h.Sum(nil)),
		SchemaDigests:      []string{deploymentDigest([]byte(durableInputSchema)), deploymentDigest([]byte(durableCheckpointSchema))},
		LockDigest:         deploymentDigest(lock), CompilerDigest: deploymentDigest([]byte(info.GoVersion)),
		CheckpointFormat: durableCheckpointFormat,
	}, codec, nil
}
