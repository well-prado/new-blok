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
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestDistributedCompetingOwnersAndExpiredOwnerAreFenced(t *testing.T) {
	store, client := integrationStore(t, "BLOK_DISTRIBUTED_ENDPOINTS")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	partition := fmt.Sprintf("fence-%d", time.Now().UnixNano())
	owner, err := store.Acquire(ctx, partition, "owner-a", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	singleErrors := make(map[string]struct{})
	if err := store.Commit(ctx, owner, "before-expiry", "state", []byte(`{"value":1}`)); err != nil {
		singleErrors[errorLabel(err)] = struct{}{}
	}
	currentOwners := 0
	if current, err := store.CurrentOwner(ctx, partition); err == nil && current.Token == owner.Token && current.ID == owner.ID {
		currentOwners++
	}
	assertScenarioFixture(t, "single-current-owner", map[string]any{
		"acquired": currentOwners, "committed": committedEvents(t, ctx, store, partition, "before-expiry"), "errors": sortedErrorLabels(singleErrors),
	})

	var acquired int
	var rejected int
	var committed int
	observedErrors := make(map[string]struct{})
	for i := 0; i < 12; i++ {
		competitor, err := store.Acquire(ctx, partition, fmt.Sprintf("competitor-%02d", i), 2*time.Second)
		if err == nil {
			acquired++
			if err := store.Commit(ctx, competitor, fmt.Sprintf("competitor-event-%02d", i), "state", []byte(`{"competitor":true}`)); err != nil {
				t.Fatalf("acquired competitor could not commit: %v", err)
			}
			committed++
		} else if errors.Is(err, ErrOwnershipLost) {
			rejected++
			observedErrors[errorLabel(err)] = struct{}{}
		} else {
			t.Fatalf("competing acquisition returned non-fencing error: %v", err)
		}
	}
	if acquired != 0 || rejected != 12 {
		t.Fatalf("live owner acquisitions: extra owners=%d fenced rejections=%d, want 0 and 12", acquired, rejected)
	}
	assertScenarioFixture(t, "competing-owner-before-expiry", map[string]any{
		"additionalOwners": acquired, "committed": committed, "errors": sortedErrorLabels(observedErrors),
	})

	// The old owner is intentionally idle past its lease deadline, matching a
	// paused process that cannot renew. Its resumed write must be rejected.
	time.Sleep(5400 * time.Millisecond)
	newOwner, err := store.Acquire(ctx, partition, "owner-b", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if newOwner.Token <= owner.Token {
		t.Fatalf("fence did not increase: old=%d new=%d", owner.Token, newOwner.Token)
	}
	staleErrors := make(map[string]struct{})
	if err := store.Commit(ctx, owner, "after-takeover", "state", []byte(`{"value":2}`)); err != nil {
		staleErrors[errorLabel(err)] = struct{}{}
	}
	if err := store.Commit(ctx, newOwner, "after-takeover", "state", []byte(`{"value":3}`)); err != nil && !errors.Is(err, ErrAlreadyWritten) {
		t.Fatal(err)
	}
	oldCommitted, newCommitted := committedByFence(t, ctx, store, partition, "after-takeover", owner, newOwner)
	assertScenarioFixture(t, "paused-owner-after-expiry-and-takeover", map[string]any{
		"oldOwnerCommitted": oldCommitted, "newOwnerCommitted": newCommitted, "errors": sortedErrorLabels(staleErrors),
	})
	_ = client
}

func TestConcurrentAcquisitionHasExactlyOneFencedWinner(t *testing.T) {
	store, _ := integrationStore(t, "BLOK_DISTRIBUTED_ENDPOINTS")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	partition := fmt.Sprintf("concurrent-fence-%d", time.Now().UnixNano())
	const contenders = 12
	type result struct {
		owner Owner
		err   error
	}
	start := make(chan struct{})
	results := make(chan result, contenders)
	var ready sync.WaitGroup
	ready.Add(contenders)
	for i := 0; i < contenders; i++ {
		go func(index int) {
			defer ready.Done()
			<-start
			owner, err := store.Acquire(ctx, partition, fmt.Sprintf("simultaneous-%02d", index), 20*time.Second)
			results <- result{owner: owner, err: err}
		}(i)
	}
	close(start)
	ready.Wait()
	close(results)

	var winner Owner
	var winners, rejected int
	observedErrors := make(map[string]struct{})
	for outcome := range results {
		if outcome.err == nil {
			winner = outcome.owner
			winners++
			continue
		}
		if !errors.Is(outcome.err, ErrOwnershipLost) {
			t.Fatalf("simultaneous acquisition returned non-fencing error: %v", outcome.err)
		}
		rejected++
		observedErrors[errorLabel(outcome.err)] = struct{}{}
	}
	if winners != 1 || rejected != contenders-1 {
		t.Fatalf("simultaneous acquisitions: winners=%d fenced rejections=%d, want 1 and %d", winners, rejected, contenders-1)
	}
	defer store.cleanupLease(winner.LeaseID)
	committed := 0
	if err := store.Commit(ctx, winner, "race-winner", "state", []byte(`{"winner":true}`)); err != nil {
		t.Fatalf("only acquired owner commit: %v", err)
	}
	committed++
	if value, err := store.Read(ctx, partition, "race-winner"); err != nil || value == nil {
		t.Fatalf("winner record missing: value=%s err=%v", value, err)
	}
	assertScenarioFixture(t, "concurrent-competing-owners", map[string]any{
		"successfulOwners": winners, "rejectedOwners": rejected,
		"committed": committed, "errors": sortedErrorLabels(observedErrors),
	})
}

func errorLabel(err error) string {
	switch {
	case errors.Is(err, ErrOwnershipLost):
		return "ownership_lost"
	case errors.Is(err, ErrIncarnation):
		return "incarnation_mismatch"
	case errors.Is(err, ErrBlobUnavailable):
		return "blob_unavailable"
	case errors.Is(err, context.DeadlineExceeded):
		return "acknowledgment_unknown"
	default:
		return "unexpected: " + err.Error()
	}
}

// committedEvents counts the committed events with id in partition.
func committedEvents(t *testing.T, ctx context.Context, store *Store, partition, id string) int {
	t.Helper()
	events, err := store.ListEvents(ctx, partition)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event.ID == id {
			count++
		}
	}
	return count
}

// committedByFence reports how many committed events with id carry each
// owner's fence, read back from etcd rather than inferred from return values.
func committedByFence(t *testing.T, ctx context.Context, store *Store, partition, id string, old, current Owner) (int, int) {
	t.Helper()
	events, err := store.ListEvents(ctx, partition)
	if err != nil {
		t.Fatal(err)
	}
	oldCount, currentCount := 0, 0
	for _, event := range events {
		if event.ID != id {
			continue
		}
		if event.Fence == old.Token && event.Incarnation == old.Incarnation {
			oldCount++
		}
		if event.Fence == current.Token && event.Incarnation == current.Incarnation {
			currentCount++
		}
	}
	return oldCount, currentCount
}

func sortedErrorLabels(observed map[string]struct{}) []string {
	labels := make([]string, 0, len(observed))
	for label := range observed {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	return labels
}

func TestReplicaPauseCatchupAndQuorumLoss(t *testing.T) {
	store, client := integrationStore(t, "BLOK_DISTRIBUTED_ENDPOINTS")
	endpoints := strings.Split(os.Getenv("BLOK_DISTRIBUTED_ENDPOINTS"), ",")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	partition := fmt.Sprintf("replica-%d", time.Now().UnixNano())
	owner, err := store.Acquire(ctx, partition, "owner", time.Minute)
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
	if err := waitEndpointAt(ctx, endpoints[0]); err != nil {
		t.Fatalf("remaining voters did not establish a linearizable quorum: %v", err)
	}
	majorityClient, err := clientv3.New(clientv3.Config{Endpoints: endpoints[:2], DialTimeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("connect through remaining voters: %v", err)
	}
	defer majorityClient.Close()
	majorityStore, err := New(ctx, majorityClient, owner.Incarnation)
	if err != nil {
		t.Fatalf("initialize store through remaining voters: %v", err)
	}
	voterErrors := make(map[string]struct{})
	if err := majorityStore.Commit(ctx, owner, "catchup", "state", []byte(`{"n":7}`)); err != nil {
		voterErrors[errorLabel(err)] = struct{}{}
	}
	majorityCommits := committedEvents(t, ctx, majorityStore, partition, "catchup")
	if output, err := exec.Command("docker", composeArgs("unpause", "etcd3")...).CombinedOutput(); err != nil {
		t.Fatalf("resume replica: %v: %s", err, output)
	}
	paused = false
	catchupCtx, catchupCancel := context.WithTimeout(ctx, 20*time.Second)
	recovered := waitRead(catchupCtx, client, endpoints[2], eventKey(partition, "catchup")) == nil
	catchupCancel()
	assertScenarioFixture(t, "one-voter-paused", map[string]any{
		"majorityCommit": majorityCommits, "recoveredReplicaReadsCommit": recovered, "errors": sortedErrorLabels(voterErrors),
	})

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
	quorumErrors := make(map[string]struct{})
	acknowledgment := "acknowledged"
	if err := store.Commit(quorumCtx, owner, operationID, "state", []byte(operationPayload)); err != nil {
		quorumErrors[errorLabel(err)] = struct{}{}
		acknowledgment = "rejected"
		if errorLabel(err) == "acknowledgment_unknown" {
			acknowledgment = "unknown"
		}
	}
	if output, err := exec.Command("docker", composeArgs("unpause", "etcd2", "etcd3")...).CombinedOutput(); err != nil {
		t.Fatalf("restore quorum: %v: %s", err, output)
	}
	quorumPaused = false
	// A deadline means the acknowledgment is unknown, not that the Raft
	// proposal definitely failed. Reconcile the stable operation ID first;
	// retry only when absent, and accept only the same committed payload.
	reconciled := false
	for {
		value, readErr := store.Read(ctx, partition, operationID)
		if readErr == nil {
			if value != nil {
				assertEventPayload(t, value, owner, operationID, "state", operationPayload)
				reconciled = true
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
	assertEventPayload(t, value, owner, operationID, "state", operationPayload)
	assertScenarioFixture(t, "two-voters-paused", map[string]any{
		"acknowledgment": acknowledgment, "reconcileByStableID": reconciled,
		"committedRecordsAfterRecovery": committedEvents(t, ctx, store, partition, operationID),
		"errors":                        sortedErrorLabels(quorumErrors),
	})
}

func assertEventPayload(t *testing.T, encoded []byte, owner Owner, id, kind, expected string) {
	t.Helper()
	var committed event
	if err := json.Unmarshal(encoded, &committed); err != nil {
		t.Fatalf("decode reconciled event: %v", err)
	}
	if committed.Partition != owner.Partition || committed.ID != id || committed.Kind != kind || committed.Fence != owner.Token || committed.Incarnation != owner.Incarnation || string(committed.Payload) != expected {
		t.Fatalf("reconciled event = %+v, want partition=%q id=%q kind=%q fence=%d incarnation=%q payload=%s", committed, owner.Partition, id, kind, owner.Token, owner.Incarnation, expected)
	}
}

func TestNetworkPartitionedVoterAllowsMajorityCommitAndCatchesUp(t *testing.T) {
	store, client := integrationStore(t, "BLOK_DISTRIBUTED_ENDPOINTS")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	partition := fmt.Sprintf("network-partition-%d", time.Now().UnixNano())
	owner, err := store.Acquire(ctx, partition, "majority-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	network := os.Getenv("BLOK_DISTRIBUTED_ETCD_NETWORK")
	if network == "" {
		network = "blok-distributed-spike"
	}
	voters := integrationEtcdVoters()
	voter := voters[2]
	endpoints := strings.Split(os.Getenv("BLOK_DISTRIBUTED_ENDPOINTS"), ",")
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
			return
		}
		recoveryCtx, recoveryCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer recoveryCancel()
		if err := waitEndpointAt(recoveryCtx, endpoints[2]); err != nil {
			t.Errorf("wait for isolated voter quorum recovery: %v", err)
		}
	}()
	if err := waitEndpointAt(ctx, endpoints[0]); err != nil {
		t.Fatalf("remaining voters did not establish a linearizable quorum: %v", err)
	}
	majorityClient, err := clientv3.New(clientv3.Config{Endpoints: endpoints[:2], DialTimeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("connect through non-partitioned voters: %v", err)
	}
	defer majorityClient.Close()
	majorityStore, err := New(ctx, majorityClient, owner.Incarnation)
	if err != nil {
		t.Fatalf("initialize store through non-partitioned voters: %v", err)
	}
	partitionErrors := make(map[string]struct{})
	if err := majorityStore.Commit(ctx, owner, "majority-during-partition", "state", []byte(`{"majority":true}`)); err != nil {
		partitionErrors[errorLabel(err)] = struct{}{}
	}
	majorityCommits := committedEvents(t, ctx, majorityStore, partition, "majority-during-partition")
	if err := restoreNetwork(); err != nil {
		t.Fatal(err)
	}
	if err := waitEndpointAt(ctx, endpoints[2]); err != nil {
		t.Fatalf("isolated voter did not regain a linearizable quorum: %v", err)
	}
	catchupCtx, catchupCancel := context.WithTimeout(ctx, 20*time.Second)
	caughtUp := waitRead(catchupCtx, client, endpoints[2], eventKey(partition, "majority-during-partition")) == nil
	catchupCancel()
	assertScenarioFixture(t, "one-voter-network-partition", map[string]any{
		"majorityCommit": majorityCommits, "partitionedReplicaCatchesUp": caughtUp, "errors": sortedErrorLabels(partitionErrors),
	})
}

func TestPausedOwnerProcessCannotCommitAfterTakeover(t *testing.T) {
	store, _ := integrationStore(t, "BLOK_DISTRIBUTED_ENDPOINTS")
	partition := fmt.Sprintf("process-pause-%d", time.Now().UnixNano())
	ready := filepath.Join(t.TempDir(), "owner-ready")
	resume := filepath.Join(t.TempDir(), "owner-resume")
	result := filepath.Join(t.TempDir(), "owner-result")
	command := exec.Command(os.Args[0], "-test.run=^$")
	command.Env = append(os.Environ(),
		"BLOK_DISTRIBUTED_OWNER_HELPER=1",
		"BLOK_DISTRIBUTED_PARTITION="+partition,
		"BLOK_DISTRIBUTED_READY="+ready,
		"BLOK_DISTRIBUTED_RESUME="+resume,
		"BLOK_DISTRIBUTED_RESULT="+result,
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
		t.Fatalf("resumed stale owner helper failed: %v", err)
	}
	finished = true
	label, err := os.ReadFile(result)
	if err != nil {
		t.Fatalf("stale owner did not report its resumed commit: %v", err)
	}
	staleOwner := Owner{Partition: partition, Incarnation: newOwner.Incarnation}
	if tokenData, err := os.ReadFile(ready); err == nil {
		_, _ = fmt.Sscan(string(tokenData), &staleOwner.Token)
	}
	_, newCommitted := committedByFence(t, ctx, store, partition, "paused-race", staleOwner, newOwner)
	oldCommitted, _ := committedByFence(t, ctx, store, partition, "paused-stale-write", staleOwner, newOwner)
	processErrors := map[string]struct{}{}
	if string(label) != "committed" {
		processErrors[string(label)] = struct{}{}
	}
	assertScenarioFixture(t, "paused-owner-after-expiry-and-takeover", map[string]any{
		"oldOwnerCommitted": oldCommitted, "newOwnerCommitted": newCommitted, "errors": sortedErrorLabels(processErrors),
	})
}

// runPausedOwnerHelper is the stale owner process for
// TestPausedOwnerProcessCannotCommitAfterTakeover, dispatched by TestMain.
// It reports the label of its resumed commit attempt; the test process reads
// the committed records back from etcd.
func runPausedOwnerHelper() error {
	endpoints := strings.Split(os.Getenv("BLOK_DISTRIBUTED_ENDPOINTS"), ",")
	client, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 2 * time.Second})
	if err != nil {
		return err
	}
	defer client.Close()
	current, err := client.Get(context.Background(), incarnationKey())
	if err != nil || len(current.Kvs) == 0 {
		return fmt.Errorf("read cluster incarnation: %v", err)
	}
	store, err := New(context.Background(), client, string(current.Kvs[0].Value))
	if err != nil {
		return err
	}
	partition := os.Getenv("BLOK_DISTRIBUTED_PARTITION")
	owner, err := store.Acquire(context.Background(), partition, "paused-owner", 2*time.Second)
	if err != nil {
		return fmt.Errorf("acquire helper lease: %w", err)
	}
	if err := os.WriteFile(os.Getenv("BLOK_DISTRIBUTED_READY"), []byte(fmt.Sprint(owner.Token)), 0o600); err != nil {
		return err
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
	label := "committed"
	// A distinct event identity: only the owner fence can reject this write.
	if err := store.Commit(ctx, owner, "paused-stale-write", "state", []byte(`{"winner":"old"}`)); err != nil {
		label = errorLabel(err)
	}
	return os.WriteFile(os.Getenv("BLOK_DISTRIBUTED_RESULT"), []byte(label), 0o600)
}

func TestSnapshotRestoreRotatesIncarnationAndPreservesPartitionData(t *testing.T) {
	store, _ := integrationStore(t, "BLOK_DISTRIBUTED_ENDPOINTS")
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
	// The maintenance snapshot is served by one member and is not
	// linearizable: a follower can still be applying the event above. A
	// linearizable read through that same member first waits until it has
	// applied the event, so the snapshot is guaranteed to contain it.
	snapshotMember, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(os.Getenv("BLOK_DISTRIBUTED_ENDPOINTS"), ",")[:1], DialTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer snapshotMember.Close()
	if applied, err := snapshotMember.Get(ctx, eventKey(partition, "snapshot-event")); err != nil || len(applied.Kvs) != 1 {
		t.Fatalf("snapshot member has not applied the pre-snapshot event: response=%v err=%v", applied, err)
	}
	snapshot, err := snapshotMember.Snapshot(ctx)
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
	for i := 0; i < 32; i++ {
		if err := store.Commit(ctx, oldOwner, fmt.Sprintf("post-snapshot-noise-%02d", i), "synthetic-revision", []byte(`{"synthetic":true}`)); err != nil {
			t.Fatalf("advance source-cluster revision after snapshot: %v", err)
		}
	}
	futureOwner, err := store.Acquire(ctx, partition+"-future", "post-snapshot-owner", 30*time.Second)
	if err != nil {
		t.Fatalf("advance source-cluster revision beyond snapshot: %v", err)
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
	network := os.Getenv("BLOK_DISTRIBUTED_ETCD_NETWORK")
	if network == "" {
		network = "blok-distributed-spike"
	}
	args := []string{"run", "-d", "--name", name, "--network", network, "-p", fmt.Sprintf("127.0.0.1:%d:2379", port), "-v", directory + ":/restore", "quay.io/coreos/etcd:v3.6.5", "etcd", "--name=restore", "--data-dir=/restore/restored", "--listen-peer-urls=http://0.0.0.0:2380", "--initial-advertise-peer-urls=http://restore:2380", "--listen-client-urls=http://0.0.0.0:2379", "--advertise-client-urls=http://restore:2379", "--initial-cluster=restore=http://restore:2380", "--initial-cluster-state=new"}
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
	staleStore, err := New(ctx, restoredClient, oldOwner.Incarnation)
	if err != nil {
		t.Fatalf("construct pre-restore client handle: %v", err)
	}
	newIncarnation := fmt.Sprintf("restored-%d", time.Now().UnixNano())
	if err := preRotate.RotateIncarnation(ctx, oldOwner.Incarnation, newIncarnation); err != nil {
		t.Fatalf("rotate incarnation before exposing restored cluster: %v", err)
	}
	restoreErrors := make(map[string]struct{})
	if _, err := New(ctx, restoredClient, oldOwner.Incarnation); err != nil {
		restoreErrors[errorLabel(err)] = struct{}{}
	}
	oldIncarnationCommit, oldIncarnationRenew := 0, 0
	if err := staleStore.Commit(ctx, oldOwner, "stale-after-restore", "state", []byte(`{"stale":true}`)); err != nil {
		restoreErrors[errorLabel(err)] = struct{}{}
	} else {
		oldIncarnationCommit++
	}
	if err := staleStore.Renew(ctx, oldOwner); err != nil {
		restoreErrors[errorLabel(err)] = struct{}{}
	} else {
		oldIncarnationRenew++
	}
	if staleAcquired, err := staleStore.Acquire(ctx, partition, "stale-store-after-restore", 3*time.Second); err != nil {
		restoreErrors[errorLabel(err)] = struct{}{}
	} else {
		_ = staleStore.Release(ctx, staleAcquired)
		restoreErrors["stale_store_acquired"] = struct{}{}
	}
	postRotate, err := New(ctx, restoredClient, newIncarnation)
	if err != nil {
		t.Fatal(err)
	}
	if value, err := postRotate.Read(ctx, partition, "snapshot-event"); err != nil || !strings.Contains(string(value), `"retained":true`) {
		t.Fatalf("incarnation rotation hid restored state: %s, err=%v", value, err)
	}
	newOwner, err := postRotate.Acquire(ctx, partition+"-fresh", "post-restore-owner", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := postRotate.Commit(ctx, newOwner, "post-restore-event", "state", []byte(`{"restored":true}`)); err != nil {
		restoreErrors[errorLabel(err)] = struct{}{}
	}
	assertScenarioFixture(t, "snapshot-restore-revision-rewind", map[string]any{
		"numericRevisionRewound": newOwner.Token < futureOwner.Token, "oldIncarnationCommit": oldIncarnationCommit + committedEvents(t, ctx, postRotate, partition, "stale-after-restore"),
		"oldIncarnationRenew": oldIncarnationRenew, "freshIncarnationCommit": committedEvents(t, ctx, postRotate, partition+"-fresh", "post-restore-event"),
		"errors": sortedErrorLabels(restoreErrors),
	})
}

type fixtureDocument struct {
	FixtureVersion int  `json:"fixtureVersion"`
	Synthetic      bool `json:"synthetic"`
	Cases          []struct {
		Name     string         `json:"name"`
		Expected map[string]any `json:"expected"`
	} `json:"cases"`
}

func assertScenarioFixture(t *testing.T, name string, actual map[string]any) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "distributed", "fixtures.json"))
	if err != nil {
		t.Fatalf("read synthetic acceptance fixture: %v", err)
	}
	var fixtures fixtureDocument
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatalf("decode synthetic acceptance fixture: %v", err)
	}
	if fixtures.FixtureVersion != 1 || !fixtures.Synthetic {
		t.Fatalf("fixture metadata must remain version 1 and synthetic: %+v", fixtures)
	}
	var expected map[string]any
	for _, fixture := range fixtures.Cases {
		if fixture.Name == name {
			expected = fixture.Expected
			break
		}
	}
	if expected == nil {
		t.Fatalf("fixture %q not found", name)
	}
	if !reflect.DeepEqual(expected, normalizeFixtureValue(t, actual)) {
		t.Fatalf("scenario %q actual=%v, fixture expected=%v", name, actual, expected)
	}
}

func normalizeFixtureValue(t *testing.T, value any) any {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode scenario result: %v", err)
	}
	var normalized any
	if err := json.Unmarshal(data, &normalized); err != nil {
		t.Fatalf("normalize scenario result: %v", err)
	}
	return normalized
}

func waitEndpoint(ctx context.Context, client *clientv3.Client) error {
	for {
		probeCtx, cancel := context.WithTimeout(ctx, time.Second)
		_, err := client.Get(probeCtx, incarnationKey())
		cancel()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func waitEndpointAt(ctx context.Context, endpoint string) error {
	reader, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		return err
	}
	defer reader.Close()
	return waitEndpoint(ctx, reader)
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

func integrationEtcdVoters() []string {
	voters := strings.Split(os.Getenv("BLOK_DISTRIBUTED_ETCD_VOTERS"), ",")
	if len(voters) == 3 && voters[0] != "" && voters[1] != "" && voters[2] != "" {
		return voters
	}
	return []string{"blok-distributed-spike-etcd1-1", "blok-distributed-spike-etcd2-1", "blok-distributed-spike-etcd3-1"}
}

func waitRead(ctx context.Context, client *clientv3.Client, endpoint, key string) error {
	reader, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		return err
	}
	defer reader.Close()
	for {
		probeCtx, probeCancel := context.WithTimeout(ctx, time.Second)
		response, err := reader.Get(probeCtx, key)
		probeCancel()
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

// composeArgs maps a fault on a compose service to the docker command that
// injects it. An isolated environment names its containers through
// BLOK_DISTRIBUTED_ETCD_VOTERS and BLOK_DISTRIBUTED_S3_CONTAINER, so a fault
// reaches the store the test actually uses; otherwise the shared
// benchmarks/distributed compose project is targeted.
func composeArgs(args ...string) []string {
	if len(args) > 1 && (os.Getenv("BLOK_DISTRIBUTED_ETCD_VOTERS") != "" || os.Getenv("BLOK_DISTRIBUTED_S3_CONTAINER") != "") {
		services := map[string]string{}
		if os.Getenv("BLOK_DISTRIBUTED_ETCD_VOTERS") != "" {
			voters := integrationEtcdVoters()
			services["etcd1"], services["etcd2"], services["etcd3"] = voters[0], voters[1], voters[2]
		}
		if container := os.Getenv("BLOK_DISTRIBUTED_S3_CONTAINER"); container != "" {
			services["s3"] = container
		}
		mapped := []string{args[0]}
		for _, service := range args[1:] {
			voter, ok := services[service]
			if !ok {
				mapped = nil
				break
			}
			mapped = append(mapped, voter)
		}
		if mapped != nil {
			return mapped
		}
	}
	_, file, _, _ := runtime.Caller(0)
	composePath := filepath.Join(filepath.Dir(file), "..", "..", "benchmarks", "distributed", "compose.yaml")
	return append([]string{"compose", "-p", "blok-distributed-spike", "-f", composePath}, args...)
}
