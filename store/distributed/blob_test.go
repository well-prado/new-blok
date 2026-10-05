package distributed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/well-prado/new-blok/internal/clustertest"
)

func TestS3BlobOutageBlocksReferenceAndResume(t *testing.T) {
	endpoint := os.Getenv("BLOK_DISTRIBUTED_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set BLOK_DISTRIBUTED_S3_ENDPOINT for the real S3-compatible object-store test")
	}
	// Wait for the cluster lock before this test's deadlines start.
	clustertest.Endpoints(t)
	bucket := fmt.Sprintf("blok-%d", time.Now().UnixNano())
	blobs, err := NewS3BlobStore(endpoint, bucket, "spike-access", "spike-secret-only-local", false)
	if err != nil {
		t.Fatal(err)
	}
	setupCtx, setupCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := blobs.EnsureBucket(setupCtx); err != nil {
		setupCancel()
		t.Fatal(err)
	}
	setupCancel()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cleanupCancel()
		if err := blobs.RemoveBucket(cleanupCtx); err != nil {
			t.Errorf("remove synthetic blob bucket: %v", err)
		}
	})

	journal, _ := integrationStore(t, "BLOK_DISTRIBUTED_ENDPOINTS")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	owner, err := journal.Acquire(ctx, bucket, "blob-owner", 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("synthetic durable artifact for distributed-store spike")

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
	if output, err := journal.ReadBlob(ctx, bucket, "unavailable-before-reference", blobs); err == nil && output != nil {
		outputPublished++
	}
	assertScenarioFixture(t, "blob-unavailable-before-reference", map[string]any{
		"referenceCommitted": committedEvents(t, ctx, journal, bucket, "unavailable-before-reference"), "outputPublished": outputPublished, "errors": sortedErrorLabels(beforeErrors),
	})

	if err := journal.CommitBlob(ctx, owner, "blob-backed-output", blobs, payload); err != nil {
		t.Fatalf("commit verified blob reference: %v", err)
	}
	read, err := journal.ReadBlob(ctx, bucket, "blob-backed-output", blobs)
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
	if _, err := journal.ReadBlob(readCtx, bucket, "blob-backed-output", blobs); err != nil {
		resumeAllowed = false
		afterErrors[errorLabel(err)] = struct{}{}
	}
	assertScenarioFixture(t, "blob-unavailable-after-reference", map[string]any{
		"referenceStillDurable": committedEvents(t, ctx, journal, bucket, "blob-backed-output") == 1, "resumeAllowed": resumeAllowed, "errors": sortedErrorLabels(afterErrors),
	})
}

func TestPartitionTakeoverKeepsTimerSignalAndBlobReferences(t *testing.T) {
	endpoint := os.Getenv("BLOK_DISTRIBUTED_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set BLOK_DISTRIBUTED_S3_ENDPOINT for the real S3-compatible object-store test")
	}
	// Wait for the cluster lock before this test's deadlines start.
	clustertest.Endpoints(t)
	bucket := fmt.Sprintf("blok-%d", time.Now().UnixNano())
	blobs, err := NewS3BlobStore(endpoint, bucket, "spike-access", "spike-secret-only-local", false)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := blobs.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cleanupCancel()
		if err := blobs.RemoveBucket(cleanupCtx); err != nil {
			t.Errorf("remove synthetic migration bucket: %v", err)
		}
	})
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
	if err := journal.CommitBlob(ctx, oldOwner, "artifact-record", blobs, []byte("synthetic retained artifact")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2400 * time.Millisecond)
	newOwner, err := journal.Acquire(ctx, partition, "after-migration", 10*time.Second)
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
	blobVerified := err == nil && string(artifact) == "synthetic retained artifact"
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
