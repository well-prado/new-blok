package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Lease renewal tolerance (#405). Every test here runs the real worker loop
// (Runtime.Run) against the real three-voter cluster. Faults are injected only
// on the worker's own lease calls: keepalives are delayed or refused before
// they reach etcd, and one partition release can be refused so its lease
// genuinely expires on the server. Durable state is never fabricated.

var (
	errInjectedKeepAlive = errors.New("injected keepalive failure")
	errInjectedRelease   = errors.New("injected release failure")
)

// renewalOwnerKey is partition p-0000's owner key inside a test namespace.
const renewalOwnerKey = "/blok/v1/incarnations/" + integrationIncarnation + "/partitions/p-0000/owner"

type renewalFaultClient struct {
	*clientv3.Client
	// keepAliveDelay holds every keepalive back before it is sent.
	keepAliveDelay time.Duration
	// failFirstLease refuses every keepalive for the first granted lease and
	// the first owner-key release, so that lease expires on the server.
	failFirstLease bool

	grants         atomic.Int64
	failedLease    atomic.Int64
	keepAlives     atomic.Int64
	slowestRenewal atomic.Int64
	acquisitions   atomic.Int64
	releaseBlocked atomic.Pointer[time.Time]

	// events records every lease call and every failed transaction, so a
	// failing run says why the worker gave its partition up.
	eventsMu sync.Mutex
	created  time.Time
	events   []string
}

func (c *renewalFaultClient) record(format string, args ...any) {
	c.eventsMu.Lock()
	defer c.eventsMu.Unlock()
	if c.created.IsZero() {
		c.created = time.Now()
	}
	c.events = append(c.events, fmt.Sprintf("+%s ", time.Since(c.created).Round(time.Millisecond))+fmt.Sprintf(format, args...))
}

func (c *renewalFaultClient) timeline() string {
	c.eventsMu.Lock()
	defer c.eventsMu.Unlock()
	return strings.Join(c.events, "\n")
}

func (c *renewalFaultClient) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	n := c.grants.Add(1)
	response, err := c.Client.Grant(ctx, ttl)
	if err == nil {
		c.record("grant #%d lease=%x ttl=%ds", n, int64(response.ID), response.TTL)
	} else {
		c.record("grant #%d error: %v", n, err)
	}
	if err == nil && c.failFirstLease && n == 1 {
		c.failedLease.Store(int64(response.ID))
	}
	return response, err
}

func (c *renewalFaultClient) KeepAliveOnce(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseKeepAliveResponse, error) {
	c.keepAlives.Add(1)
	if c.failFirstLease && int64(id) == c.failedLease.Load() {
		c.record("keepalive lease=%x refused (injected)", int64(id))
		return nil, errInjectedKeepAlive
	}
	started := time.Now()
	var keepAliveErr error
	defer func() {
		c.record("keepalive lease=%x took %s err=%v", int64(id), time.Since(started).Round(time.Millisecond), keepAliveErr)
		elapsed := int64(time.Since(started))
		for {
			current := c.slowestRenewal.Load()
			if elapsed <= current || c.slowestRenewal.CompareAndSwap(current, elapsed) {
				return
			}
		}
	}()
	if c.keepAliveDelay > 0 {
		timer := time.NewTimer(c.keepAliveDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			keepAliveErr = ctx.Err()
			return nil, keepAliveErr
		case <-timer.C:
		}
	}
	response, err := c.Client.KeepAliveOnce(ctx, id)
	keepAliveErr = err
	return response, err
}

func (c *renewalFaultClient) Txn(ctx context.Context) clientv3.Txn {
	return &renewalFaultTxn{Txn: c.Client.Txn(ctx), client: c}
}

type renewalFaultTxn struct {
	clientv3.Txn
	client        *renewalFaultClient
	putsOwner     bool
	deletesOwners bool
}

func (t *renewalFaultTxn) If(compares ...clientv3.Cmp) clientv3.Txn {
	t.Txn = t.Txn.If(compares...)
	return t
}

func (t *renewalFaultTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	for _, operation := range ops {
		key := string(operation.KeyBytes())
		if isOwnerKey(key) && operation.IsPut() {
			t.putsOwner = true
		}
		if isOwnerKey(key) && operation.IsDelete() {
			t.deletesOwners = true
		}
	}
	t.Txn = t.Txn.Then(ops...)
	return t
}

func (t *renewalFaultTxn) Else(ops ...clientv3.Op) clientv3.Txn {
	t.Txn = t.Txn.Else(ops...)
	return t
}

func (t *renewalFaultTxn) Commit() (*clientv3.TxnResponse, error) {
	if t.deletesOwners && t.client.failFirstLease {
		now := time.Now()
		if t.client.releaseBlocked.CompareAndSwap(nil, &now) {
			t.client.record("release refused (injected)")
			return nil, errInjectedRelease
		}
	}
	response, err := t.Txn.Commit()
	switch {
	case err != nil:
		t.client.record("txn error (owner put=%v delete=%v): %v", t.putsOwner, t.deletesOwners, err)
	case t.deletesOwners:
		t.client.record("release committed=%v", response.Succeeded)
	case t.putsOwner:
		t.client.record("acquire committed=%v", response.Succeeded)
	case !response.Succeeded:
		t.client.record("txn compare failed")
	}
	if err == nil && response.Succeeded && t.putsOwner {
		t.client.acquisitions.Add(1)
	}
	return response, err
}

type renewalInput struct {
	Key    string `json:"key"`
	Stream bool   `json:"stream"`
}

type renewalOutput struct {
	Key string `json:"key"`
}

const renewalInputSchema = `{"type":"object","properties":{"key":{"type":"string"},"stream":{"type":"boolean"}},"required":["key","stream"],"additionalProperties":false}`
const renewalOutputSchema = `{"type":"object","properties":{"key":{"type":"string"}},"required":["key"],"additionalProperties":false}`

// renewalInvocations counts effect invocations per run key, so a duplicate or
// a lost effect is visible per run rather than only in a total.
type renewalInvocations struct {
	mu     sync.Mutex
	counts map[string]int
}

func (r *renewalInvocations) add(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.counts == nil {
		r.counts = map[string]int{}
	}
	r.counts[key]++
}

func (r *renewalInvocations) get(key string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[key]
}

// renewalHarness runs one partition (p-0000) under the real worker loop.
type renewalHarness struct {
	t       *testing.T
	ctx     context.Context
	runtime *Runtime
	runs    map[string]struct{ tenant, runID string }
}

func newRenewalHarness(t *testing.T, client *renewalFaultClient, ownerTTL time.Duration, effect node.Any) *renewalHarness {
	t.Helper()
	store := integrationStoreFor(t, client)
	program := contract.InternalProgram{WorkflowID: "renewal-fixture", Digest: "sha256:" + strings.Repeat("e", 64), Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "effect", Kind: "call", Node: "fixture/renewal-effect"},
		{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "effect"}}},
	}}
	workflow := Workflow{Program: program, DecodeInput: func(raw json.RawMessage) (any, error) {
		var input renewalInput
		if err := json.Unmarshal(raw, &input); err != nil {
			return nil, err
		}
		return input, nil
	}}
	limits := Limits{Partitions: 1, PartitionAdmissions: 16, TenantAdmissions: 4, OwnerTTL: ownerTTL}
	runtime, err := New(store, engine.New(map[string]node.Any{"fixture/renewal-effect": effect}), map[string]Workflow{"renewal-fixture": workflow}, limits)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	if err := runtime.Check(ctx); err != nil {
		t.Fatal(err)
	}
	return &renewalHarness{t: t, ctx: ctx, runtime: runtime, runs: map[string]struct{ tenant, runID string }{}}
}

func (h *renewalHarness) admit(tenant string, input renewalInput) {
	h.t.Helper()
	raw, _ := json.Marshal(input)
	admission, err := h.runtime.Admit(h.ctx, Submission{Tenant: tenant, RequestKey: input.Key, Workflow: "renewal-fixture", Input: raw})
	if err != nil || !admission.Accepted {
		h.t.Fatalf("admit %s: admission=%+v err=%v", input.Key, admission, err)
	}
	h.runs[input.Key] = struct{ tenant, runID string }{tenant, admission.RunID}
}

// startWorker runs Runtime.Run until the returned stop is called.
func (h *renewalHarness) startWorker(ownerID string) func() {
	workerCtx, cancel := context.WithCancel(h.ctx)
	done := make(chan error, 1)
	go func() { done <- h.runtime.Run(workerCtx, ownerID) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
	h.t.Cleanup(stop)
	return stop
}

// awaitTerminal returns every admitted run once all are terminal.
func (h *renewalHarness) awaitTerminal(within time.Duration) map[string]RunRecord {
	h.t.Helper()
	deadline := time.Now().Add(within)
	for {
		records := map[string]RunRecord{}
		for key, run := range h.runs {
			record, err := h.runtime.GetRun(h.ctx, run.tenant, run.runID)
			if err == nil && isTerminal(record.State) {
				records[key] = record
			}
		}
		if len(records) == len(h.runs) {
			return records
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("only %d of %d runs terminal after %s: %v", len(records), len(h.runs), within, records)
		}
		if err := waitContext(h.ctx, 50*time.Millisecond); err != nil {
			h.t.Fatal(err)
		}
	}
}

// TestSlowLeaseRenewalKeepsPartition: renewals slower than OwnerTTL/4 but
// well inside the lease must not make the worker give up its partition, and
// the run in flight must complete with exactly one effect (#405 (a), (c)).
func TestSlowLeaseRenewalKeepsPartition(t *testing.T) {
	const ownerTTL = 8 * time.Second
	// Above the old per-renewal timeout (OwnerTTL/4 = 2s), and leaving more
	// than a second of the window a renewal has inside the proven lease
	// (about 5/12 of OwnerTTL = 3.33s) for the real keepalive and the
	// ownership check under load.
	const delay = 2200 * time.Millisecond
	client := &renewalFaultClient{Client: integrationClient(t), keepAliveDelay: delay}
	var invocations renewalInvocations
	effect := node.MustDefine("fixture/renewal-effect", "1.0.0", func(ctx context.Context, input renewalInput) (renewalOutput, error) {
		invocations.add(input.Key)
		// The effect spans two renewals, due OwnerTTL/3 = 2.67s apart.
		timer := time.NewTimer(7 * time.Second)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return renewalOutput{}, ctx.Err()
		case <-timer.C:
		}
		return renewalOutput{Key: input.Key}, nil
	}, node.Description("long effect spanning slow lease renewals"), node.Schemas([]byte(renewalInputSchema), []byte(renewalOutputSchema)), node.Effects("fixture:renewal-slow")).Any()
	h := newRenewalHarness(t, client, ownerTTL, effect)
	h.admit("slow-renewal-tenant", renewalInput{Key: "slow-renewal-run"})
	stop := h.startWorker("slow-renewal-owner")
	records := h.awaitTerminal(30 * time.Second)
	stop()
	run := records["slow-renewal-run"]
	t.Logf("slow renewals: keepalives=%d slowest=%s acquisitions=%d run=%s errorCode=%q effects=%d", client.keepAlives.Load(), time.Duration(client.slowestRenewal.Load()).Round(time.Millisecond), client.acquisitions.Load(), run.State, run.ErrorCode, invocations.get("slow-renewal-run"))
	if acquisitions := client.acquisitions.Load(); acquisitions != 1 {
		t.Fatalf("worker acquired the partition %d times, want 1: a slow renewal dropped it; lease timeline:\n%s", acquisitions, client.timeline())
	}
	if run.State != "completed" || string(run.Output) != `{"key":"slow-renewal-run"}` {
		t.Fatalf("run=%+v, want completed with its output", run)
	}
	if count := invocations.get("slow-renewal-run"); count != 1 {
		t.Fatalf("effect invoked %d times, want exactly 1", count)
	}
	if client.keepAlives.Load() < 2 || time.Duration(client.slowestRenewal.Load()) < delay {
		t.Fatalf("the run did not span slow renewals: keepalives=%d slowest=%s", client.keepAlives.Load(), time.Duration(client.slowestRenewal.Load()))
	}
}

// TestFailingLeaseRenewalStopsWorkBeforeExpiry: renewals that keep failing
// until the lease really expires must make the worker give the partition up,
// and no effect may still be running once the lease is gone; every run ends
// with exactly one effect (#405 (b), (c)).
func TestFailingLeaseRenewalStopsWorkBeforeExpiry(t *testing.T) {
	const ownerTTL = 4 * time.Second
	client := &renewalFaultClient{Client: integrationClient(t), failFirstLease: true}
	observer := integrationClient(t)
	var invocations renewalInvocations
	var emissions, violations atomic.Int64
	var lastEmission atomic.Pointer[time.Time]
	effect := node.MustDefine("fixture/renewal-effect", "1.0.0", func(ctx context.Context, input renewalInput) (renewalOutput, error) {
		invocations.add(input.Key)
		if !input.Stream {
			return renewalOutput{Key: input.Key}, nil
		}
		// A continuing external effect: it keeps acting for as long as the
		// worker lets it. After each action it asks etcd (through a separate
		// client) whether the partition is still owned. A missing owner key
		// means this action happened after the lease could have expired.
		for {
			emissions.Add(1)
			now := time.Now()
			lastEmission.Store(&now)
			checkCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			owner, err := observer.Get(checkCtx, renewalOwnerKey)
			cancel()
			if err == nil && len(owner.Kvs) == 0 {
				violations.Add(1)
			}
			timer := time.NewTimer(10 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return renewalOutput{}, ctx.Err()
			case <-timer.C:
			}
		}
	}, node.Description("continuing effect that checks partition ownership after every action"), node.Schemas([]byte(renewalInputSchema), []byte(renewalOutputSchema)), node.Effects("fixture:renewal-stream")).Any()
	h := newRenewalHarness(t, client, ownerTTL, effect)
	// Fair selection starts with the smallest tenant, so the streaming run is
	// the one in flight when renewals start failing.
	h.admit("a-stream-tenant", renewalInput{Key: "stream-run", Stream: true})
	for i := 0; i < 4; i++ {
		h.admit(fmt.Sprintf("b-tenant-%d", i), renewalInput{Key: fmt.Sprintf("plain-run-%d", i)})
	}
	// Watch the failed lease from outside the worker: the first instant etcd
	// reports it gone.
	var expiredSeen atomic.Pointer[time.Time]
	watchCtx, stopWatch := context.WithCancel(h.ctx)
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		for watchCtx.Err() == nil {
			if lease := client.failedLease.Load(); lease != 0 {
				ttl, err := observer.TimeToLive(watchCtx, clientv3.LeaseID(lease))
				if err == nil && ttl.TTL == -1 {
					now := time.Now()
					expiredSeen.Store(&now)
					return
				}
			}
			_ = waitContext(watchCtx, 20*time.Millisecond)
		}
	}()
	started := time.Now()
	stop := h.startWorker("failing-renewal-owner")
	records := h.awaitTerminal(45 * time.Second)
	stop()
	stopWatch()
	<-watchDone
	blocked, expired, last := client.releaseBlocked.Load(), expiredSeen.Load(), lastEmission.Load()
	if blocked == nil || expired == nil || last == nil {
		t.Fatalf("missing evidence: gave up=%v lease expiry seen=%v last emission=%v", blocked, expired, last)
	}
	t.Logf("failing renewals: gave up after %s, last action %s, lease gone %s (margin before expiry %s); emissions=%d violations=%d acquisitions=%d",
		blocked.Sub(started).Round(time.Millisecond), last.Sub(started).Round(time.Millisecond), expired.Sub(started).Round(time.Millisecond), expired.Sub(*last).Round(time.Millisecond), emissions.Load(), violations.Load(), client.acquisitions.Load())
	if violations.Load() != 0 {
		t.Fatalf("%d effect actions ran after the partition's lease had expired", violations.Load())
	}
	if !last.Before(*expired) {
		t.Fatalf("last effect action at %s is not before the lease was seen expired at %s", last.Sub(started), expired.Sub(started))
	}
	if !blocked.Before(*expired) {
		t.Fatalf("worker gave the partition up at %s, after its lease was seen expired at %s", blocked.Sub(started), expired.Sub(started))
	}
	if client.acquisitions.Load() < 2 {
		t.Fatalf("acquisitions=%d: the worker never gave up and re-acquired the partition", client.acquisitions.Load())
	}
	for key, record := range records {
		if count := invocations.get(key); count != 1 {
			t.Fatalf("run %s effect invoked %d times, want exactly 1 (state %s)", key, count, record.State)
		}
		if key == "stream-run" {
			// Its effect was dispatched without a committed result: the
			// successor must not run it again (ADR 0019).
			if record.State != "uncertain" {
				t.Fatalf("interrupted streaming run state=%s code=%q, want uncertain", record.State, record.ErrorCode)
			}
			continue
		}
		if record.State != "completed" || string(record.Output) != fmt.Sprintf(`{"key":%q}`, key) {
			t.Fatalf("run %s=%+v, want completed with its output", key, record)
		}
	}
}
