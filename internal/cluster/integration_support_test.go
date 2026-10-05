package cluster

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/internal/clustertest"
	"github.com/well-prado/new-blok/store/distributed"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
)

// integrationIncarnation is the cluster incarnation established inside each
// test's private etcd key namespace. Every test therefore starts from an empty
// partition space and its own immutable runtime settings while still using
// the real three-voter cluster for every transaction.
const integrationIncarnation = "integration-incarnation-v1"

var integrationNamespaces sync.Map

// integrationEndpoints returns the real cluster's endpoints and holds the
// shared cluster lock for the rest of t (see internal/clustertest), so no
// other package's disruptive test pauses voters while t uses them.
func integrationEndpoints(t *testing.T) []string {
	t.Helper()
	return clustertest.Endpoints(t)
}

// integrationNamespace returns the private etcd key prefix for t. Every store
// created for the same test shares it, so separate clients act as separate
// ingress nodes or owners over one isolated keyspace.
func integrationNamespace(t *testing.T) string {
	t.Helper()
	if value, ok := integrationNamespaces.Load(t); ok {
		return value.(string)
	}
	prefix := fmt.Sprintf("/blok-it/%d-%d/", os.Getpid(), time.Now().UnixNano())
	integrationNamespaces.Store(t, prefix)
	t.Cleanup(func() { integrationNamespaces.Delete(t) })
	return prefix
}

func namespacedEtcdClient(endpoints []string, prefix string) (*clientv3.Client, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:            endpoints,
		DialTimeout:          2 * time.Second,
		DialKeepAliveTime:    time.Second,
		DialKeepAliveTimeout: time.Second,
	})
	if err != nil {
		return nil, err
	}
	client.KV = namespace.NewKV(client.KV, prefix)
	client.Lease = namespace.NewLease(client.Lease, prefix)
	return client, nil
}

// integrationClient opens a separate real etcd client inside t's namespace.
func integrationClient(t *testing.T) *clientv3.Client {
	t.Helper()
	client, err := namespacedEtcdClient(integrationEndpoints(t), integrationNamespace(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func integrationDistributedStore(t *testing.T) *distributed.Store {
	t.Helper()
	return integrationStoreFor(t, integrationClient(t))
}

func integrationStoreFor(t *testing.T, client distributed.Client) *distributed.Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := distributed.New(ctx, client, integrationIncarnation)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// hookedClient intercepts real etcd transactions by the keys they write. A
// hook runs once, on the matching transaction selected by skip, either before
// the transaction is sent or after it committed successfully. It never
// fabricates a response: the transaction itself always reaches etcd.
type hookedClient struct {
	*clientv3.Client
	hook  atomic.Pointer[txnHook]
	grant atomic.Int64

	acquiredMu sync.Mutex
	acquired   []time.Time
}

// acquisitions returns, in commit order, when this client's partition-owner
// acquisitions committed. Only a transaction that actually wrote the owner key
// counts; a rejected acquisition (partition held elsewhere) does not.
func (c *hookedClient) acquisitions() []time.Time {
	c.acquiredMu.Lock()
	defer c.acquiredMu.Unlock()
	return append([]time.Time(nil), c.acquired...)
}

func isOwnerKey(key string) bool {
	return strings.Contains(key, "/incarnations/") && strings.HasSuffix(key, "/owner")
}

type txnHook struct {
	pattern string
	skip    int32
	after   bool
	run     func()
	runKeys func([]string)
	seen    atomic.Int32
	fired   atomic.Bool
}

func (c *hookedClient) arm(pattern string, skip int, after bool, run func()) *txnHook {
	hook := &txnHook{pattern: pattern, skip: int32(skip), after: after, run: run}
	c.hook.Store(hook)
	return hook
}

// armKeys is arm with a hook that receives the keys the selected
// transaction writes, e.g. to learn which admission slots it chose.
func (c *hookedClient) armKeys(pattern string, skip int, after bool, run func([]string)) *txnHook {
	hook := &txnHook{pattern: pattern, skip: int32(skip), after: after, runKeys: run}
	c.hook.Store(hook)
	return hook
}

func (h *txnHook) fire(keys []string) {
	if h.runKeys != nil {
		h.runKeys(append([]string(nil), keys...))
		return
	}
	h.run()
}

func (c *hookedClient) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	c.grant.Add(1)
	return c.Client.Grant(ctx, ttl)
}

func (c *hookedClient) Txn(ctx context.Context) clientv3.Txn {
	return &hookedTxn{Txn: c.Client.Txn(ctx), client: c}
}

type hookedTxn struct {
	clientv3.Txn
	client *hookedClient
	puts   []string
}

func (t *hookedTxn) If(compares ...clientv3.Cmp) clientv3.Txn {
	t.Txn = t.Txn.If(compares...)
	return t
}

func (t *hookedTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	for _, operation := range ops {
		if operation.IsPut() {
			t.puts = append(t.puts, string(operation.KeyBytes()))
		}
	}
	t.Txn = t.Txn.Then(ops...)
	return t
}

func (t *hookedTxn) Else(ops ...clientv3.Op) clientv3.Txn {
	t.Txn = t.Txn.Else(ops...)
	return t
}

func (t *hookedTxn) Commit() (*clientv3.TxnResponse, error) {
	hook := t.client.hook.Load()
	selected := false
	if hook != nil && !hook.fired.Load() {
		for _, key := range t.puts {
			if strings.Contains(key, hook.pattern) {
				selected = hook.seen.Add(1) == hook.skip+1
				break
			}
		}
	}
	if selected && !hook.after && hook.fired.CompareAndSwap(false, true) {
		hook.fire(t.puts)
	}
	response, err := t.Txn.Commit()
	if err == nil && response.Succeeded {
		for _, key := range t.puts {
			if isOwnerKey(key) {
				t.client.acquiredMu.Lock()
				t.client.acquired = append(t.client.acquired, time.Now())
				t.client.acquiredMu.Unlock()
				break
			}
		}
	}
	if selected && hook.after && err == nil && response.Succeeded && hook.fired.CompareAndSwap(false, true) {
		hook.fire(t.puts)
	}
	return response, err
}

// effectLedger is an append-only file of synthetic external effects. It is
// shared by the test process and owner/worker subprocesses, so effect counts
// survive SIGKILL and are measured rather than declared.
type effectLedger string

func (l effectLedger) append(kind, key string) error {
	file, err := os.OpenFile(string(l), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(file, "%s %s %d\n", kind, key, os.Getpid()); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

// counts returns kind -> key -> occurrences.
func (l effectLedger) counts() (map[string]map[string]int, error) {
	result := map[string]map[string]int{}
	file, err := os.Open(string(l))
	if errors.Is(err, os.ErrNotExist) {
		return result, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 3 {
			return nil, fmt.Errorf("malformed ledger line %q", scanner.Text())
		}
		if result[fields[0]] == nil {
			result[fields[0]] = map[string]int{}
		}
		result[fields[0]][fields[1]]++
	}
	return result, scanner.Err()
}

func (l effectLedger) total(kind string) int {
	counts, err := l.counts()
	if err != nil {
		panic(err)
	}
	total := 0
	for _, count := range counts[kind] {
		total += count
	}
	return total
}

// acquireWhenFree retries until the previous owner's lease has expired or was
// released. It never steals: Acquire only succeeds when no owner key exists.
func acquireWhenFree(t *testing.T, ctx context.Context, store *distributed.Store, partition, ownerID string, ttl time.Duration) distributed.Owner {
	t.Helper()
	for {
		owner, err := store.Acquire(ctx, partition, ownerID, ttl)
		if err == nil {
			releaseOwnerOnCleanup(t, store, owner)
			return owner
		}
		if !errors.Is(err, distributed.ErrOwnershipLost) {
			t.Fatalf("acquire %s: %v", partition, err)
		}
		if waitErr := waitContext(ctx, 100*time.Millisecond); waitErr != nil {
			t.Fatalf("partition %s was not released: %v", partition, waitErr)
		}
	}
}

// tenantsInPartition returns count distinct tenants routed to partition.
func tenantsInPartition(runtime *Runtime, partition, prefix string, count int) []string {
	tenants := make([]string, 0, count)
	for candidate := 0; len(tenants) < count; candidate++ {
		tenant := fmt.Sprintf("%s-%03d", prefix, candidate)
		if runtime.Partition(tenant) == partition {
			tenants = append(tenants, tenant)
		}
	}
	return tenants
}

func waitForFile(ctx context.Context, path string) error {
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		if err := waitContext(ctx, 20*time.Millisecond); err != nil {
			return fmt.Errorf("wait for %s: %w", path, err)
		}
	}
}
