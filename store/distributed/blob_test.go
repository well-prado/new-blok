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
)

func TestS3BlobOutageBlocksReferenceAndResume(t *testing.T) {
	endpoint := os.Getenv("BLOK_DISTRIBUTED_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set BLOK_DISTRIBUTED_S3_ENDPOINT for the real S3-compatible object-store test")
	}
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
	err = journal.CommitBlob(blockedCtx, owner, "unavailable-before-reference", blobs, payload)
	blockedCancel()
	if !errors.Is(err, ErrBlobUnavailable) {
		t.Fatalf("commit while object service unavailable = %v, want ErrBlobUnavailable", err)
	}
	value, err := journal.Read(ctx, bucket, "unavailable-before-reference")
	if err != nil || value != nil {
		t.Fatalf("outage created a durable reference: value=%s err=%v", value, err)
	}
	if output, err := exec.Command("docker", composeArgs("unpause", "s3")...).CombinedOutput(); err != nil {
		t.Fatalf("restore object service: %v: %s", err, output)
	}
	paused = false

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
	if _, err := journal.ReadBlob(readCtx, bucket, "blob-backed-output", blobs); !errors.Is(err, ErrBlobUnavailable) {
		t.Fatalf("resume read during object outage = %v, want ErrBlobUnavailable", err)
	}
}

func TestPartitionTakeoverKeepsTimerSignalAndBlobReferences(t *testing.T) {
	endpoint := os.Getenv("BLOK_DISTRIBUTED_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set BLOK_DISTRIBUTED_S3_ENDPOINT for the real S3-compatible object-store test")
	}
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
	for id, kind := range map[string]string{"timer-record": "timer", "signal-record": "signal"} {
		encoded, err := journal.Read(ctx, partition, id)
		if err != nil {
			t.Fatalf("read %s after takeover: %v", kind, err)
		}
		var record event
		if err := json.Unmarshal(encoded, &record); err != nil || record.Kind != kind || record.Partition != partition {
			t.Fatalf("%s record after takeover = %+v, err=%v", kind, record, err)
		}
	}
	artifact, err := journal.ReadBlob(ctx, partition, "artifact-record", blobs)
	if err != nil || string(artifact) != "synthetic retained artifact" {
		t.Fatalf("blob reference after takeover = %q, err=%v", artifact, err)
	}
	if err := journal.Commit(ctx, oldOwner, "stale-after-migration", "state", []byte(`{"stale":true}`)); !errors.Is(err, ErrOwnershipLost) {
		t.Fatalf("old owner commit after partition takeover = %v, want ErrOwnershipLost", err)
	}
	if err := journal.Commit(ctx, newOwner, "new-owner-state", "state", []byte(`{"current":true}`)); err != nil {
		t.Fatalf("new owner commit after partition takeover: %v", err)
	}
}
