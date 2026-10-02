package conformance

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/artifact"
	"github.com/well-prado/new-blok/contract/schema"
)

type adapter struct{}

func (adapter) Name() string { return "synthetic-narrow-port" }
func (adapter) Run(_ context.Context, c Case) (Result, error) {
	if c.Unsupported {
		return Result{CaseID: c.ID, Unsupported: true, Reason: "matrix not implemented"}, nil
	}
	return Result{CaseID: c.ID, Output: c.ExpectedOutput, Errors: c.ExpectedErrors, Effects: c.ExpectedEffects}, nil
}

func TestRunnerReportsUnsupportedWithoutPassingIt(t *testing.T) {
	report, err := Run(context.Background(), adapter{}, []Case{{ID: "schema-valid", ExpectedOutput: 1}, {ID: "grpc", Unsupported: true}})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Results) != 2 {
		t.Fatalf("report=%+v", report)
	}
	got := map[string]Result{}
	for _, result := range report.Results {
		got[result.CaseID] = result
	}
	if got["schema-valid"].Unsupported || !got["grpc"].Unsupported {
		t.Fatalf("report=%+v", report)
	}
}
func TestRunnerRejectsUnreportedUnsupported(t *testing.T) {
	bad := adapterFunc(func(_ context.Context, c Case) (Result, error) { return Result{CaseID: c.ID}, nil })
	if _, err := Run(context.Background(), bad, []Case{{ID: "missing", Unsupported: true}}); err == nil {
		t.Fatal("unreported unsupported case passed")
	}
}

type adapterFunc func(context.Context, Case) (Result, error)

func (adapterFunc) Name() string                                      { return "bad" }
func (f adapterFunc) Run(ctx context.Context, c Case) (Result, error) { return f(ctx, c) }

type realAdapter struct{}

func (realAdapter) Name() string { return "public-contracts" }
func (realAdapter) Run(_ context.Context, c Case) (Result, error) {
	r := Result{CaseID: c.ID}
	switch c.Kind {
	case "document":
		data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "contracts", "valid.json"))
		if err != nil {
			return r, err
		}
		if _, err := contract.Parse(data); err != nil {
			r.Errors = 1
		} else {
			r.Output = 1
		}
	case "schema":
		if _, err := schema.Parse([]byte(`{"type":"recursive"}`)); err != nil {
			r.Errors = 1
		}
	case "artifact":
		m := artifact.Manifest{Name: "test", Version: "1.0.0", WorkflowDigest: "sha256:0000000000000000000000000000000000000000000000000000000000000000", NativeBinaryDigest: "sha256:1111111111111111111111111111111111111111111111111111111111111111", LockDigest: "sha256:2222222222222222222222222222222222222222222222222222222222222222", CompilerDigest: "sha256:3333333333333333333333333333333333333333333333333333333333333333", CheckpointFormat: "v1"}
		store := artifact.NewStore()
		if _, err := store.Register(m); err != nil {
			return r, err
		}
		m.NativeBinaryDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		if _, err := store.Register(m); err != nil {
			r.Errors = 1
		}
	case "worker":
		r.Unsupported = true
		r.Reason = "worker matrix is not implemented"
	}
	return r, nil
}

func TestVersionedCorpusRunsThroughPublicImplementations(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "conformance", "cases.json"))
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Version int    `json:"version"`
		Cases   []Case `json:"cases"`
	}
	if err := json.Unmarshal(data, &corpus); err != nil {
		t.Fatal(err)
	}
	if corpus.Version != 1 {
		t.Fatalf("version=%d", corpus.Version)
	}
	if _, err := Run(context.Background(), realAdapter{}, corpus.Cases); err != nil {
		t.Fatal(err)
	}
}
