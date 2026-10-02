package runtime

import (
	"bytes"
	contract "github.com/well-prado/new-blok/contract/runtime"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
)

func TestActualNodeAuthorizedLargeBlob(t *testing.T) {
	root := os.Getenv("BLOK_NODE_INTEGRATION_ROOT")
	if root == "" {
		t.Skip("explicit Node blob conformance gate")
	}
	token := "synthetic-blob-token-0000000000001"
	other := "synthetic-other-token-00000000001"
	caps := []contract.Capability{"blob:read", "blob:write"}
	auth, err := NewTokenAuthenticator([]Credential{{Token: token, Principal: "blob-app", Capabilities: caps}, {Token: other, Principal: "other-app", Capabilities: caps}})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewBlobStore(contract.MaxBlobBytes)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("synthetic"), 150000)
	ref := contract.BlobRef{Digest: contract.CanonicalDigest(data), Size: len(data)}
	session, err := auth.NewSession(token, "blob-app", caps)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutAuthorized(session, ref, data); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(BlobHandler(auth, store))
	defer server.Close()
	for _, fixture := range []struct {
		name, credential, principal string
		size                        int
		ok                          bool
	}{{"authorized", token, "blob-app", len(data), true}, {"wrong-token", "wrong-token-000000000000000000000", "blob-app", len(data), false}, {"spoof", token, "other-app", len(data), false}, {"cross-owner", other, "other-app", len(data), false}, {"wrong-size", token, "blob-app", len(data) - 1, false}, {"over-bound", token, "blob-app", contract.MaxBlobBytes + 1, false}, {"revoked", token, "blob-app", len(data), false}} {
		if fixture.name == "revoked" {
			auth.Revoke(token)
		}
		command := exec.Command("node", filepath.Join(root, "testdata/worker/blob-read.mjs"))
		command.Env = append(os.Environ(), "BLOK_BLOB_SDK="+filepath.Join(root, "runtime/nodejs/dist/sdk/nodejs/index.js"), "BLOK_BLOB_ENDPOINT="+server.URL, "BLOK_BLOB_TOKEN="+fixture.credential, "BLOK_BLOB_PRINCIPAL="+fixture.principal, "BLOK_BLOB_DIGEST="+ref.Digest, "BLOK_BLOB_SIZE="+strconv.Itoa(fixture.size))
		out, err := command.CombinedOutput()
		if fixture.ok {
			if err != nil || string(out) != "1350000\n" {
				t.Fatalf("%s: %s %v", fixture.name, out, err)
			}
		} else if err == nil {
			t.Fatalf("%s authorized", fixture.name)
		}
	}
}
