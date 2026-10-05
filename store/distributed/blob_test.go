package distributed

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/well-prado/new-blok/internal/clustertest"
)

// sharedBlobBucket is the one bucket every distributed blob test uses. A
// SeaweedFS bucket owns a collection, and every collection claims volume
// slots from a small fixed budget (five on a small disk). A bucket per test
// leaks slots on a long-lived dev cluster until every upload fails with "No
// writable volumes" (#292), so the bucket is created once per cluster and
// never removed; tests isolate themselves with payloads no other test shares.
const sharedBlobBucket = "blok-distributed-tests"

// sharedBlobStore returns the cluster's shared test bucket, creating it on
// first use.
func sharedBlobStore(t *testing.T, endpoint string) *S3BlobStore {
	t.Helper()
	blobs, err := NewS3BlobStore(endpoint, sharedBlobBucket, "spike-access", "spike-secret-only-local", false)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := blobs.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	return blobs
}

// uniqueBlobPayload returns a payload no other test or run shares. Blob keys
// are the content digest, so a unique payload is a unique key: that is the
// per-test isolation inside the shared bucket.
func uniqueBlobPayload(label string) []byte {
	return []byte(fmt.Sprintf("%s %d-%d", label, os.Getpid(), time.Now().UnixNano()))
}

// removeBlobPayloads deletes the objects the given payloads were stored
// under, so the shared bucket does not accumulate data across runs.
func removeBlobPayloads(t *testing.T, blobs *S3BlobStore, payloads ...[]byte) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, payload := range payloads {
			digest := sha256.Sum256(payload)
			if err := blobs.client.RemoveObject(ctx, blobs.bucket, hex.EncodeToString(digest[:]), minio.RemoveObjectOptions{}); err != nil {
				t.Errorf("remove synthetic blob object: %v", err)
			}
		}
	})
}

func TestS3BlobOutageBlocksReferenceAndResume(t *testing.T) {
	endpoint := os.Getenv("BLOK_DISTRIBUTED_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set BLOK_DISTRIBUTED_S3_ENDPOINT for the real S3-compatible object-store test")
	}
	// Wait for the cluster lock before this test's deadlines start.
	clustertest.Endpoints(t)
	blobs := sharedBlobStore(t, endpoint)
	payload := uniqueBlobPayload("synthetic durable artifact for distributed-store spike")
	removeBlobPayloads(t, blobs, payload)
	partition := fmt.Sprintf("blob-%d", time.Now().UnixNano())

	journal, _ := integrationStore(t, "BLOK_DISTRIBUTED_ENDPOINTS")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	owner, err := journal.Acquire(ctx, partition, "blob-owner", 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("docker", composeArgs("pause", "s3")...).CombinedOutput(); err != nil {
		t.Fatalf("pause object service before reference commit: %v: %s", err, output)
	}
	paused := true
	defer func() {
		if paused {
			_, _ = exec.Command("docker", composeArgs("unpause", "s3")...).CombinedOutput()
		}
	}()
	blockedCtx, blockedCancel := context.WithTimeout(context.Background(), 2*time.Second)
	beforeErrors := make(map[string]struct{})
	if err := journal.CommitBlob(blockedCtx, owner, "unavailable-before-reference", blobs, payload); err != nil {
		beforeErrors[errorLabel(err)] = struct{}{}
	}
	blockedCancel()
	if output, err := exec.Command("docker", composeArgs("unpause", "s3")...).CombinedOutput(); err != nil {
		t.Fatalf("restore object service: %v: %s", err, output)
	}
	paused = false
	// Measured after the object service is back: a reference committed or
	// an output readable through it would both be visible now.
	outputPublished := 0
	if output, err := journal.ReadBlob(ctx, partition, "unavailable-before-reference", blobs); err == nil && output != nil {
		outputPublished++
	}
	assertScenarioFixture(t, "blob-unavailable-before-reference", map[string]any{
		"referenceCommitted": committedEvents(t, ctx, journal, partition, "unavailable-before-reference"), "outputPublished": outputPublished, "errors": sortedErrorLabels(beforeErrors),
	})

	if err := journal.CommitBlob(ctx, owner, "blob-backed-output", blobs, payload); err != nil {
		t.Fatalf("commit verified blob reference: %v", err)
	}
	read, err := journal.ReadBlob(ctx, partition, "blob-backed-output", blobs)
	if err != nil || string(read) != string(payload) {
		t.Fatalf("read verified blob = %q, err=%v", read, err)
	}

	if output, err := exec.Command("docker", composeArgs("pause", "s3")...).CombinedOutput(); err != nil {
		t.Fatalf("pause object service after reference commit: %v: %s", err, output)
	}
	paused = true
	readCtx, readCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer readCancel()
	afterErrors := make(map[string]struct{})
	resumeAllowed := true
	if _, err := journal.ReadBlob(readCtx, partition, "blob-backed-output", blobs); err != nil {
		resumeAllowed = false
		afterErrors[errorLabel(err)] = struct{}{}
	}
	assertScenarioFixture(t, "blob-unavailable-after-reference", map[string]any{
		"referenceStillDurable": committedEvents(t, ctx, journal, partition, "blob-backed-output") == 1, "resumeAllowed": resumeAllowed, "errors": sortedErrorLabels(afterErrors),
	})
}

func TestPartitionTakeoverKeepsTimerSignalAndBlobReferences(t *testing.T) {
	endpoint := os.Getenv("BLOK_DISTRIBUTED_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set BLOK_DISTRIBUTED_S3_ENDPOINT for the real S3-compatible object-store test")
	}
	// Wait for the cluster lock before this test's deadlines start.
	clustertest.Endpoints(t)
	blobs := sharedBlobStore(t, endpoint)
	artifactPayload := uniqueBlobPayload("synthetic retained artifact")
	removeBlobPayloads(t, blobs, artifactPayload)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	journal, _ := integrationStore(t, "BLOK_DISTRIBUTED_ENDPOINTS")
	partition := fmt.Sprintf("migration-%d", time.Now().UnixNano())
	oldOwner, err := journal.Acquire(ctx, partition, "before-migration", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Commit(ctx, oldOwner, "timer-record", "timer", []byte(`{"due":"synthetic"}`)); err != nil {
		t.Fatal(err)
	}
	if err := journal.Commit(ctx, oldOwner, "signal-record", "signal", []byte(`{"name":"synthetic"}`)); err != nil {
		t.Fatal(err)
	}
	if err := journal.CommitBlob(ctx, oldOwner, "artifact-record", blobs, artifactPayload); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2400 * time.Millisecond)
	// etcd revokes an expired lease on its own schedule (it checks on a
	// 500ms tick), so the old lease can outlive its 2s TTL by a moment.
	// Retry while the old owner still holds the partition; the takeover
	// semantics under test are unchanged. With a warm bucket this race is
	// no longer hidden behind a slow first upload (#292).
	var newOwner Owner
	takeoverDeadline := time.Now().Add(8 * time.Second)
	for {
		newOwner, err = journal.Acquire(ctx, partition, "after-migration", 10*time.Second)
		if !errors.Is(err, ErrOwnershipLost) || time.Now().After(takeoverDeadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("take over stable partition: %v", err)
	}
	takeoverErrors := make(map[string]struct{})
	for _, delivery := range []struct {
		id      string
		kind    string
		payload string
	}{
		{id: "timer-claim", kind: "timer-claim", payload: `{"timerID":"timer-record"}`},
		{id: "signal-delivery", kind: "signal-delivery", payload: `{"signalID":"signal-record"}`},
	} {
		if err := journal.Commit(ctx, oldOwner, delivery.id, delivery.kind, []byte(delivery.payload)); err != nil {
			takeoverErrors[errorLabel(err)] = struct{}{}
		}
		if err := journal.Commit(ctx, newOwner, delivery.id, delivery.kind, []byte(delivery.payload)); err != nil && !errors.Is(err, ErrAlreadyWritten) {
			t.Fatalf("new owner %s commit: %v", delivery.kind, err)
		}
		encoded, err := journal.Read(ctx, partition, delivery.id)
		var record event
		if err != nil || json.Unmarshal(encoded, &record) != nil || record.Kind != delivery.kind || record.Fence != newOwner.Token || record.Incarnation != newOwner.Incarnation {
			t.Fatalf("fenced %s record = %+v, err=%v", delivery.kind, record, err)
		}
	}
	retained := map[string]bool{}
	for id, kind := range map[string]string{"timer-record": "timer", "signal-record": "signal"} {
		encoded, err := journal.Read(ctx, partition, id)
		var record event
		retained[kind] = err == nil && encoded != nil && json.Unmarshal(encoded, &record) == nil && record.Kind == kind && record.Partition == partition && record.Fence == oldOwner.Token
	}
	artifact, err := journal.ReadBlob(ctx, partition, "artifact-record", blobs)
	blobVerified := err == nil && string(artifact) == string(artifactPayload)
	if err := journal.Commit(ctx, oldOwner, "stale-after-migration", "state", []byte(`{"stale":true}`)); err != nil {
		takeoverErrors[errorLabel(err)] = struct{}{}
	}
	if err := journal.Commit(ctx, newOwner, "new-owner-state", "state", []byte(`{"current":true}`)); err != nil {
		takeoverErrors[errorLabel(err)] = struct{}{}
	}
	oldCommitted := 0
	for _, id := range []string{"timer-claim", "signal-delivery", "stale-after-migration"} {
		staleCount, _ := committedByFence(t, ctx, journal, partition, id, oldOwner, newOwner)
		oldCommitted += staleCount
	}
	_, newCommitted := committedByFence(t, ctx, journal, partition, "new-owner-state", oldOwner, newOwner)
	assertScenarioFixture(t, "partition-takeover-timer-signal-blob", map[string]any{
		"timerRecordRetained": retained["timer"], "signalRecordRetained": retained["signal"],
		"blobDigestVerified": blobVerified, "oldOwnerCommitted": oldCommitted,
		"newOwnerCommitted": newCommitted, "errors": sortedErrorLabels(takeoverErrors),
	})
}
