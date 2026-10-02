package artifact

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func fixture() Manifest {
	return Manifest{
		Name: "shop/quote", Version: "1.0.0",
		WorkflowDigest:      "sha256:0000000000000000000000000000000000000000000000000000000000000000",
		NativeBinaryDigest:  "sha256:1111111111111111111111111111111111111111111111111111111111111111",
		WorkerCatalogDigest: "sha256:2222222222222222222222222222222222222222222222222222222222222222",
		SchemaDigests:       []string{"sha256:4444444444444444444444444444444444444444444444444444444444444444", "sha256:3333333333333333333333333333333333333333333333333333333333333333"},
		LockDigest:          "sha256:5555555555555555555555555555555555555555555555555555555555555555",
		CompilerDigest:      "sha256:6666666666666666666666666666666666666666666666666666666666666666",
		CheckpointFormat:    "checkpoint-v1",
	}
}

func TestDigestIsStableForSchemaOrder(t *testing.T) {
	a := fixture()
	b := fixture()
	b.SchemaDigests = []string{a.SchemaDigests[1], a.SchemaDigests[0]}
	da, err := a.Digest()
	if err != nil {
		t.Fatal(err)
	}
	db, err := b.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if da != db {
		t.Fatalf("schema order changed digest: %s != %s", da, db)
	}
}

func TestExecutionInputsChangeDigest(t *testing.T) {
	a := fixture()
	da, _ := a.Digest()
	for name, mutate := range map[string]func(*Manifest){
		"binary": func(m *Manifest) {
			m.NativeBinaryDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		},
		"schema": func(m *Manifest) {
			m.SchemaDigests[0] = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		},
		"lock": func(m *Manifest) {
			m.LockDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		},
	} {
		t.Run(name, func(t *testing.T) {
			b := a
			mutate(&b)
			db, err := b.Digest()
			if err != nil {
				t.Fatal(err)
			}
			if da == db {
				t.Fatal("execution input did not change digest")
			}
		})
	}
}

func TestStoreRejectsVersionReplacementAndLocatesByDigest(t *testing.T) {
	s := NewStore()
	a := fixture()
	d, err := s.Register(a)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Locate(d); !ok {
		t.Fatal("registered artifact not found by digest")
	}
	b := a
	b.NativeBinaryDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if _, err := s.Register(b); err == nil || err.(*Error).Code != "artifact_version_conflict" {
		t.Fatalf("got %v, want version conflict", err)
	}
}

func TestCheckpointIdentity(t *testing.T) {
	m := fixture()
	d, err := m.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyCheckpoint(m, Checkpoint{ArtifactDigest: d, CheckpointFormat: m.CheckpointFormat}); err != nil {
		t.Fatal(err)
	}
	if err := VerifyCheckpoint(m, Checkpoint{ArtifactDigest: d, CheckpointFormat: "checkpoint-v0"}); err == nil || err.(*Error).Code != "incompatible_checkpoint" {
		t.Fatalf("got %v, want incompatible checkpoint", err)
	}
}

func TestManifestRejectsLocalPaths(t *testing.T) {
	m := fixture()
	m.CheckpointFormat = "/tmp/checkpoint"
	if err := m.Validate(); err == nil {
		t.Fatal("local path accepted")
	}
}

func TestGoldenManifest(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "artifacts", "golden-manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Digest(); err != nil {
		t.Fatal(err)
	}
}

func TestCanonicalJSONIgnoresFormatting(t *testing.T) {
	a, err := CanonicalJSON([]byte(`{"b":2,"a":{"z":true,"y":1}}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := CanonicalJSON([]byte(" { \"a\": {\"y\":1,\"z\":true}, \"b\": 2 } "))
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != string(b) {
		t.Fatalf("canonical JSON differs: %s != %s", a, b)
	}
}
