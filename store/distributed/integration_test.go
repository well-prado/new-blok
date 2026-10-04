package distributed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestDistributedCompetingOwnersAndExpiredOwnerAreFenced(t *testing.T) {
	store, client := integrationStore(t, "BLOK_DISTRIBUTED_ENDPOINTS")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	partition := fmt.Sprintf("fence-%d", time.Now().UnixNano())
	owner, err := store.Acquire(ctx, partition, "owner-a", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Commit(ctx, owner, "before-expiry", "state", []byte(`{"value":1}`)); err != nil {
		t.Fatal(err)
	}

	var acquired int
	for i := 0; i < 12; i++ {
		if _, err := store.Acquire(ctx, partition, fmt.Sprintf("competitor-%02d", i), 2*time.Second); err == nil {
			acquired++
		}
	}
	if acquired != 0 {
		t.Fatalf("live owner allowed %d competing owners", acquired)
	}

	// The old owner is intentionally idle past its lease deadline, matching a
	// paused process that cannot renew. Its resumed write must be rejected.
	time.Sleep(2400 * time.Millisecond)
	newOwner, err := store.Acquire(ctx, partition, "owner-b", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if newOwner.Token <= owner.Token {
		t.Fatalf("fence did not increase: old=%d new=%d", owner.Token, newOwner.Token)
	}
	if err := store.Commit(ctx, owner, "after-takeover", "state", []byte(`{"value":2}`)); !errors.Is(err, ErrOwnershipLost) {
		t.Fatalf("stale owner commit error = %v, want ErrOwnershipLost", err)
	}
	if err := store.Commit(ctx, newOwner, "after-takeover", "state", []byte(`{"value":3}`)); err != nil {
		t.Fatal(err)
	}
	value, err := store.Read(ctx, partition, "after-takeover")
	if err != nil || !strings.Contains(string(value), `"value":3`) {
		t.Fatalf("committed value = %s, err=%v", value, err)
	}
	_ = client
}

func TestReplicaPauseCatchupAndQuorumLoss(t *testing.T) {
	store, client := integrationStore(t, "BLOK_DISTRIBUTED_ENDPOINTS")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	partition := fmt.Sprintf("replica-%d", time.Now().UnixNano())
	owner, err := store.Acquire(ctx, partition, "owner", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	compose := composeArgs("pause", "etcd3")
	if output, err := exec.Command("docker", compose...).CombinedOutput(); err != nil {
		t.Fatalf("pause replica: %v: %s", err, output)
	}
	paused := true
	defer func() {
		if paused {
			_, _ = exec.Command("docker", composeArgs("unpause", "etcd3")...).CombinedOutput()
		}
	}()
	if err := store.Commit(ctx, owner, "catchup", "state", []byte(`{"n":7}`)); err != nil {
		t.Fatalf("majority commit with one paused replica: %v", err)
	}
	if output, err := exec.Command("docker", composeArgs("unpause", "etcd3")...).CombinedOutput(); err != nil {
		t.Fatalf("resume replica: %v: %s", err, output)
	}
	paused = false
	if err := waitRead(ctx, client, "localhost:32379", eventKey(partition, "catchup")); err != nil {
		t.Fatalf("recovered replica did not catch up: %v", err)
	}

	output, err := exec.Command("docker", composeArgs("pause", "etcd2", "etcd3")...).CombinedOutput()
	if err != nil {
		t.Fatalf("remove quorum: %v: %s", err, output)
	}
	quorumPaused := true
	defer func() {
		if quorumPaused {
			_, _ = exec.Command("docker", composeArgs("unpause", "etcd2", "etcd3")...).CombinedOutput()
		}
	}()
	quorumCtx, quorumCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer quorumCancel()
	const operationID = "no-quorum-stable-operation"
	const operationPayload = `{"n":8}`
	if err := store.Commit(quorumCtx, owner, operationID, "state", []byte(operationPayload)); err == nil {
		t.Fatal("commit succeeded after two of three voting members were paused")
	}
	if output, err := exec.Command("docker", composeArgs("unpause", "etcd2", "etcd3")...).CombinedOutput(); err != nil {
		t.Fatalf("restore quorum: %v: %s", err, output)
	}
	quorumPaused = false
	// A deadline means the acknowledgment is unknown, not that the Raft
	// proposal definitely failed. Reconcile the stable operation ID first;
	// retry only when absent, and accept only the same committed payload.
	for {
		value, readErr := store.Read(ctx, partition, operationID)
		if readErr == nil {
			if value != nil {
				assertEventPayload(t, value, partition, operationID, operationPayload)
				break
			}
			commitErr := store.Commit(ctx, owner, operationID, "state", []byte(operationPayload))
			if commitErr != nil && !errors.Is(commitErr, ErrAlreadyWritten) {
				t.Fatalf("retry same operation ID after absent reconciliation: %v", commitErr)
			}
			continue
		}
		select {
		case <-ctx.Done():
			t.Fatalf("cluster did not recover for reconciliation: %v", readErr)
		case <-time.After(100 * time.Millisecond):
		}
	}
	value, err := store.Read(ctx, partition, operationID)
	if err != nil || value == nil {
		t.Fatalf("stable operation missing after reconciliation: value=%s err=%v", value, err)
	}
	assertEventPayload(t, value, partition, operationID, operationPayload)
}

func assertEventPayload(t *testing.T, encoded []byte, partition, id, expected string) {
	t.Helper()
	var committed event
	if err := json.Unmarshal(encoded, &committed); err != nil {
		t.Fatalf("decode reconciled event: %v", err)
	}
	if committed.Partition != partition || committed.ID != id || string(committed.Payload) != expected {
		t.Fatalf("reconciled event = %+v, want partition=%q id=%q payload=%s", committed, partition, id, expected)
	}
}

func TestNetworkPartitionedVoterAllowsMajorityCommitAndCatchesUp(t *testing.T) {
	store, client := integrationStore(t, "BLOK_DISTRIBUTED_ENDPOINTS")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	partition := fmt.Sprintf("network-partition-%d", time.Now().UnixNano())
	owner, err := store.Acquire(ctx, partition, "majority-owner", 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	const network = "blok-distributed-spike"
	const voter = "blok-distributed-spike-etcd3-1"
	if output, err := exec.Command("docker", "network", "disconnect", network, voter).CombinedOutput(); err != nil {
		t.Fatalf("isolate one voter from its peer/client network: %v: %s", err, output)
	}
	connected := false
	restoreNetwork := func() error {
		if connected {
			return nil
		}
		output, err := exec.Command("docker", "network", "connect", "--alias", "etcd3", network, voter).CombinedOutput()
		if err != nil {
			return fmt.Errorf("reconnect isolated voter: %w: %s", err, output)
		}
		connected = true
		return nil
	}
	defer func() {
		if err := restoreNetwork(); err != nil {
			t.Errorf("restore voter network after test: %v", err)
		}
	}()
	if err := store.Commit(ctx, owner, "majority-during-partition", "state", []byte(`{"majority":true}`)); err != nil {
		t.Fatalf("two connected voters failed to commit: %v", err)
	}
	if err := restoreNetwork(); err != nil {
		t.Fatal(err)
	}
	if err := waitRead(ctx, client, "localhost:32379", eventKey(partition, "majority-during-partition")); err != nil {
		t.Fatalf("isolated voter did not recover the majority commit: %v", err)
	}
}

func TestPausedOwnerProcessCannotCommitAfterTakeover(t *testing.T) {
	store, _ := integrationStore(t, "BLOK_DISTRIBUTED_ENDPOINTS")
	partition := fmt.Sprintf("process-pause-%d", time.Now().UnixNano())
	ready := filepath.Join(t.TempDir(), "owner-ready")
	resume := filepath.Join(t.TempDir(), "owner-resume")
	command := exec.Command(os.Args[0], "-test.run=^TestPausedOwnerHelper$")
	command.Env = append(os.Environ(),
		"BLOK_DISTRIBUTED_OWNER_HELPER=1",
		"BLOK_DISTRIBUTED_PARTITION="+partition,
		"BLOK_DISTRIBUTED_READY="+ready,
		"BLOK_DISTRIBUTED_RESUME="+resume,
	)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	finished := false
	defer func() {
		if !finished {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("owner subprocess did not acquire its lease")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := suspendProcess(command.Process); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2500 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	newOwner, err := store.Acquire(ctx, partition, "takeover-owner", 8*time.Second)
	if err != nil {
		t.Fatalf("take over expired paused process lease: %v", err)
	}
	if err := store.Commit(ctx, newOwner, "paused-race", "state", []byte(`{"winner":"new"}`)); err != nil {
		t.Fatalf("new owner commit: %v", err)
	}
	if err := os.WriteFile(resume, []byte("resume"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := resumeProcess(command.Process); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("resumed stale owner did not observe fencing: %v", err)
	}
	finished = true
	value, err := store.Read(ctx, partition, "paused-race")
	if err != nil || !strings.Contains(string(value), `"winner":"new"`) {
		t.Fatalf("stale process replaced new owner's commit: %s, err=%v", value, err)
	}
}

func TestPausedOwnerHelper(t *testing.T) {
	if os.Getenv("BLOK_DISTRIBUTED_OWNER_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	store, _ := integrationStore(t, "BLOK_DISTRIBUTED_ENDPOINTS")
	partition := os.Getenv("BLOK_DISTRIBUTED_PARTITION")
	owner, err := store.Acquire(context.Background(), partition, "paused-owner", 2*time.Second)
	if err != nil {
		t.Fatalf("acquire helper lease: %v", err)
	}
	if err := os.WriteFile(os.Getenv("BLOK_DISTRIBUTED_READY"), []byte(fmt.Sprint(owner.Token)), 0o600); err != nil {
		t.Fatal(err)
	}
	stopRenewal := make(chan struct{})
	defer close(stopRenewal)
	go func() {
		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopRenewal:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_ = store.Renew(ctx, owner)
				cancel()
			}
		}
	}()
	for {
		if _, err := os.Stat(os.Getenv("BLOK_DISTRIBUTED_RESUME")); err == nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := store.Commit(ctx, owner, "paused-race", "state", []byte(`{"winner":"old"}`)); !errors.Is(err, ErrOwnershipLost) {
		t.Fatalf("resumed owner commit error = %v, want ErrOwnershipLost", err)
	}
}

func TestSnapshotRestoreRotatesIncarnationAndPreservesPartitionData(t *testing.T) {
	store, client := integrationStore(t, "BLOK_DISTRIBUTED_ENDPOINTS")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	partition := fmt.Sprintf("restore-%d", time.Now().UnixNano())
	oldOwner, err := store.Acquire(ctx, partition, "pre-restore-owner", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Commit(ctx, oldOwner, "snapshot-event", "state", []byte(`{"retained":true}`)); err != nil {
		t.Fatal(err)
	}

	directory := t.TempDir()
	snapshotPath := filepath.Join(directory, "snapshot.db")
	snapshot, err := client.Snapshot(ctx)
	if err != nil {
		t.Fatalf("create etcd snapshot: %v", err)
	}
	file, err := os.Create(snapshotPath)
	if err != nil {
		_ = snapshot.Close()
		t.Fatal(err)
	}
	_, copyErr := io.Copy(file, snapshot)
	closeErr := snapshot.Close()
	syncErr := file.Sync()
	fileErr := file.Close()
	if copyErr != nil || closeErr != nil || syncErr != nil || fileErr != nil {
		t.Fatalf("write etcd snapshot: copy=%v stream-close=%v sync=%v file-close=%v", copyErr, closeErr, syncErr, fileErr)
	}

	command := exec.Command("docker", "run", "--rm", "--entrypoint=/usr/local/bin/etcdutl", "-v", directory+":/restore", "quay.io/coreos/etcd:v3.6.5", "snapshot", "restore", "/restore/snapshot.db", "--name=restore", "--data-dir=/restore/restored", "--initial-cluster=restore=http://restore:2380", "--initial-advertise-peer-urls=http://restore:2380")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("restore snapshot with etcdutl v3.6.5: %v: %s", err, output)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	name := fmt.Sprintf("blok-distributed-restore-%d", time.Now().UnixNano())
	args := []string{"run", "-d", "--name", name, "--network", "blok-distributed-spike", "-p", fmt.Sprintf("127.0.0.1:%d:2379", port), "-v", directory + ":/restore", "quay.io/coreos/etcd:v3.6.5", "etcd", "--name=restore", "--data-dir=/restore/restored", "--listen-peer-urls=http://0.0.0.0:2380", "--initial-advertise-peer-urls=http://restore:2380", "--listen-client-urls=http://0.0.0.0:2379", "--advertise-client-urls=http://restore:2379", "--initial-cluster=restore=http://restore:2380", "--initial-cluster-state=new"}
	if output, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
		t.Fatalf("start isolated restored member: %v: %s", err, output)
	}
	defer func() { _, _ = exec.Command("docker", "rm", "-f", name).CombinedOutput() }()
	restoredClient, err := clientv3.New(clientv3.Config{Endpoints: []string{fmt.Sprintf("http://127.0.0.1:%d", port)}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer restoredClient.Close()
	if err := waitEndpoint(ctx, restoredClient); err != nil {
		t.Fatalf("restored cluster did not become linearizably available: %v", err)
	}

	preRotate, err := New(ctx, restoredClient, oldOwner.Incarnation)
	if err != nil {
		t.Fatal(err)
	}
	if value, err := preRotate.Read(ctx, partition, "snapshot-event"); err != nil || !strings.Contains(string(value), `"retained":true`) {
		t.Fatalf("restored partition state missing before epoch rotation: %s, err=%v", value, err)
	}
	oldKey, err := restoredClient.Get(ctx, ownerKey(oldOwner.Incarnation, partition))
	if err != nil || len(oldKey.Kvs) != 1 || oldKey.Kvs[0].CreateRevision != oldOwner.Token || string(oldKey.Kvs[0].Value) != oldOwner.ID {
		t.Fatalf("restored snapshot did not preserve the pre-restore owner token: response=%v err=%v", oldKey, err)
	}
	newIncarnation := fmt.Sprintf("restored-%d", time.Now().UnixNano())
	if err := preRotate.RotateIncarnation(ctx, oldOwner.Incarnation, newIncarnation); err != nil {
		t.Fatalf("rotate incarnation before exposing restored cluster: %v", err)
	}
	staleStore, err := New(ctx, restoredClient, oldOwner.Incarnation)
	if err != nil {
		t.Fatalf("construct stale pre-restore client: %v", err)
	}
	if err := staleStore.Commit(ctx, oldOwner, "stale-after-restore", "state", []byte(`{"stale":true}`)); !errors.Is(err, ErrOwnershipLost) {
		t.Fatalf("pre-restore owner commit = %v, want ErrOwnershipLost", err)
	}
	if _, err := staleStore.Acquire(ctx, partition, "stale-store-after-restore", 3*time.Second); !errors.Is(err, ErrIncarnation) {
		t.Fatalf("stale store acquire after incarnation rotation = %v, want ErrIncarnation", err)
	}
	postRotate, err := New(ctx, restoredClient, newIncarnation)
	if err != nil {
		t.Fatal(err)
	}
	if value, err := postRotate.Read(ctx, partition, "snapshot-event"); err != nil || !strings.Contains(string(value), `"retained":true`) {
		t.Fatalf("incarnation rotation hid restored state: %s, err=%v", value, err)
	}
	newOwner, err := postRotate.Acquire(ctx, partition, "post-restore-owner", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := postRotate.Commit(ctx, newOwner, "post-restore-event", "state", []byte(`{"restored":true}`)); err != nil {
		t.Fatalf("new-incarnation owner commit: %v", err)
	}
}

func waitEndpoint(ctx context.Context, client *clientv3.Client) error {
	for {
		if _, err := client.Get(ctx, incarnationKey()); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func integrationStore(t *testing.T, env string) (*Store, *clientv3.Client) {
	t.Helper()
	endpoints := strings.Split(os.Getenv(env), ",")
	if endpoints[0] == "" {
		t.Skipf("set %s to run against the real three-member etcd cluster", env)
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	incarnation := "spike-test-incarnation-v1"
	current, err := client.Get(context.Background(), incarnationKey())
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Kvs) > 0 {
		incarnation = string(current.Kvs[0].Value)
	}
	store, err := New(context.Background(), client, incarnation)
	if err != nil {
		t.Fatal(err)
	}
	return store, client
}

func waitRead(ctx context.Context, client *clientv3.Client, endpoint, key string) error {
	reader, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		return err
	}
	defer reader.Close()
	for {
		response, err := reader.Get(ctx, key)
		if err == nil && len(response.Kvs) == 1 {
			return nil
		}
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func composeArgs(args ...string) []string {
	_, file, _, _ := runtime.Caller(0)
	composePath := filepath.Join(filepath.Dir(file), "..", "..", "benchmarks", "distributed", "compose.yaml")
	return append([]string{"compose", "-p", "blok-distributed-spike", "-f", composePath}, args...)
}
