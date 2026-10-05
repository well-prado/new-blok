// Package clustertest coordinates the tests that share one real etcd cluster
// through the BLOK_DISTRIBUTED_* environment.
//
// `go test ./...` runs one test binary per package, several at a time. The
// distributed packages (store/distributed, internal/cluster, app/cluster, and
// any later package) all talk to the same three voters, and some of their
// tests pause or partition those voters. Without coordination one package's
// quorum loss lands in the middle of another package's test.
//
// Every cluster test therefore holds a cross-process lock on a file keyed by
// the cluster's endpoints: a shared lock for ordinary tests ([Endpoints]) and
// an exclusive lock for tests that disrupt the voters ([Disrupt],
// [PauseQuorum]). Ordinary tests still run concurrently with each other; a
// disruptive test waits until they drain and runs alone. A waiting disruptive
// test blocks new shared holders, so it is not starved. Before an exclusive
// lock is released the cluster must be healthy again (one agreed leader and
// Raft term, unchanged for longer than an election timeout, and every voter
// answering a linearizable read), so the next test never inherits a cluster
// that is still recovering. When a disruptive test acquires the lock, voters
// that an earlier, killed test binary left paused are unpaused, and a cluster
// that is still unhealthy fails the test with the reason instead of being
// disrupted further.
//
// Locks are released by t.Cleanup on success, failure, skip or panic, and by
// the kernel when a test binary exits or is killed (for example on -timeout).
// A different cluster has a different key, so two clusters never block each
// other. In one process, tests run sequentially; parallel cluster tests in one
// package are refused rather than silently sharing an exclusive lock.
//
// The lock files live under os.TempDir() (blok-clustertest/<key>.lock), so the
// guard covers only processes that resolve the same temporary directory. `go
// test` passes TMPDIR through unchanged, so one `go test ./...` is covered. Two
// runs with different TMPDIR values (different users, sandboxes, containers,
// or an explicit TMPDIR=...) against the same cluster do not see each other's
// locks and can still interfere.
package clustertest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	// EndpointsEnv lists the client endpoints of the shared cluster.
	EndpointsEnv = "BLOK_DISTRIBUTED_ENDPOINTS"
	// VotersEnv names the three voter containers that disruptive tests pause.
	VotersEnv = "BLOK_DISTRIBUTED_ETCD_VOTERS"
	// LockTimeoutEnv overrides how long a test waits for the cluster lock
	// (a Go duration; default 8m, never past the test binary's deadline).
	LockTimeoutEnv = "BLOK_DISTRIBUTED_LOCK_TIMEOUT"
)

// Mode is the kind of cluster lock a test holds.
type Mode int

const (
	// Shared is held by tests that use the cluster without disrupting it.
	Shared Mode = 1
	// Exclusive is held by tests that pause, kill or partition voters.
	Exclusive Mode = 2
)

func (m Mode) String() string {
	switch m {
	case Shared:
		return "shared"
	case Exclusive:
		return "exclusive"
	}
	return "none"
}

var (
	defaultWait = 8 * time.Minute
	// slowWait is how long a wait runs before it is logged with the holders.
	slowWait = 5 * time.Second
	// healthBound bounds the post-disruption recovery wait.
	healthBound = 90 * time.Second
	// healthCheck, voterPaused and unpauseVoter are replaced only by this
	// package's own lock tests, whose fake endpoints and voters have no
	// cluster behind them.
	healthCheck  = WaitHealthy
	voterPaused  = dockerPaused
	unpauseVoter = dockerUnpause
)

func dockerPaused(voter string) (bool, error) {
	output, err := exec.Command("docker", "inspect", "--format", "{{.State.Paused}}", voter).CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("docker inspect %s: %w: %s", voter, err, strings.TrimSpace(string(output)))
	}
	return strings.TrimSpace(string(output)) == "true", nil
}

func dockerUnpause(voter string) error {
	if output, err := exec.Command("docker", "unpause", voter).CombinedOutput(); err != nil {
		return fmt.Errorf("docker unpause %s: %w: %s", voter, err, strings.TrimSpace(string(output)))
	}
	return nil
}

// Endpoints returns the shared cluster's endpoints and holds a shared lock on
// it for the rest of t. It skips t when the cluster environment is unset. The
// first hold in a test may wait for another package's disruptive test, so
// take it before the test starts its own deadlines or leases.
func Endpoints(t testing.TB) []string {
	t.Helper()
	endpoints := envEndpoints(t)
	Hold(t, endpoints, Shared)
	return endpoints
}

// Disrupt holds the exclusive lock on the shared cluster for the rest of t and
// returns its endpoints. Call it first in any test that pauses, kills or
// partitions a voter, before the test creates leases or deadlines that a wait
// for the lock could outlast. On acquisition it recovers voters left paused
// by an earlier, killed test binary and fails t if the cluster is not healthy
// (see [RecoverVoters]). When t ends the cluster must be healthy again before
// the lock is released.
func Disrupt(t testing.TB) []string {
	t.Helper()
	endpoints := envEndpoints(t)
	if lockFor(endpoints).holdsExclusive(t) {
		// A parent test already acquired, and checked, the cluster.
		return endpoints
	}
	Hold(t, endpoints, Exclusive)
	RecoverVoters(t, endpoints)
	return endpoints
}

// RecoverVoters runs under the exclusive lock. It unpauses any voter that is
// paused (only a disruptive test pauses voters, and none holds the lock, so a
// paused voter was left by a binary killed mid-disruption), then fails t
// unless the cluster is healthy. A voter that cannot be inspected is reported
// and left to the health check.
func RecoverVoters(t testing.TB, endpoints []string) {
	t.Helper()
	for _, voter := range Voters() {
		paused, err := voterPaused(voter)
		if err != nil {
			t.Logf("cluster %s: cannot inspect voter %s before disrupting: %v", Key(endpoints), voter, err)
			continue
		}
		if !paused {
			continue
		}
		t.Logf("cluster %s: voter %s was left paused, probably by a test binary killed mid-disruption; unpausing it", Key(endpoints), voter)
		if err := unpauseVoter(voter); err != nil {
			t.Errorf("cluster %s: unpause leftover paused voter: %v", Key(endpoints), err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), healthBound)
	defer cancel()
	if err := healthCheck(ctx, endpoints); err != nil {
		t.Fatalf("cluster %s is unhealthy before the disruptive test %s; recover it (unpause voters, reconnect networks) first: %v", Key(endpoints), t.Name(), err)
	}
}

func envEndpoints(t testing.TB) []string {
	t.Helper()
	endpoints := normalize(strings.Split(os.Getenv(EndpointsEnv), ","))
	if len(endpoints) == 0 {
		t.Skipf("set %s to run against the real three-member etcd cluster", EndpointsEnv)
	}
	return endpoints
}

// Voters returns the three voter container names, defaulting to the
// benchmarks/distributed compose project.
func Voters() []string {
	voters := strings.Split(os.Getenv(VotersEnv), ",")
	if len(voters) == 3 && voters[0] != "" && voters[1] != "" && voters[2] != "" {
		return voters
	}
	return []string{"blok-distributed-spike-etcd1-1", "blok-distributed-spike-etcd2-1", "blok-distributed-spike-etcd3-1"}
}

// PauseQuorum pauses two of the three voters and returns an idempotent
// restore (also registered as cleanup). Restore unpauses them and waits until
// the cluster is healthy again. t, or a test it runs under, must already hold
// the exclusive lock through [Disrupt]: taking it here could wait for other
// packages mid-test, while the caller's leases and deadlines run.
func PauseQuorum(t testing.TB) func() {
	t.Helper()
	endpoints := envEndpoints(t)
	if !lockFor(endpoints).holdsExclusive(t) {
		t.Fatalf("clustertest.PauseQuorum in %s: call clustertest.Disrupt(t) at the start of the disruptive test first", t.Name())
	}
	voters := Voters()
	paused := make([]string, 0, 2)
	restore := func() {
		if len(paused) == 0 {
			return
		}
		for index := len(paused) - 1; index >= 0; index-- {
			if output, err := exec.Command("docker", "unpause", paused[index]).CombinedOutput(); err != nil {
				t.Errorf("restore voter %s: %v: %s", paused[index], err, output)
			}
		}
		paused = paused[:0]
		ctx, cancel := context.WithTimeout(context.Background(), healthBound)
		defer cancel()
		if err := healthCheck(ctx, endpoints); err != nil {
			t.Errorf("cluster did not recover after restoring voters: %v", err)
		}
	}
	t.Cleanup(restore)
	for _, voter := range voters[1:] {
		if output, err := exec.Command("docker", "pause", voter).CombinedOutput(); err != nil {
			restore()
			t.Fatalf("pause voter %s: %v: %s", voter, err, output)
		}
		paused = append(paused, voter)
	}
	return restore
}

// Key identifies a cluster by its endpoint set, independent of order.
func Key(endpoints []string) string {
	sum := sha256.Sum256([]byte(strings.Join(normalize(endpoints), "\n")))
	return hex.EncodeToString(sum[:8])
}

func normalize(endpoints []string) []string {
	result := make([]string, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/"); endpoint != "" {
			result = append(result, endpoint)
		}
	}
	sort.Strings(result)
	return result
}

// Hold holds mode on the cluster named by endpoints until t ends. A test may
// call it repeatedly; a later Exclusive upgrades an earlier Shared hold.
func Hold(t testing.TB, endpoints []string, mode Mode) {
	t.Helper()
	endpoints = normalize(endpoints)
	state := lockFor(endpoints)
	if err := state.hold(t, mode); err != nil {
		t.Fatal(err)
	}
}

// holdsExclusive reports whether t or a test it runs under holds the
// exclusive lock.
func (l *clusterLock) holdsExclusive(t testing.TB) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, existing := range l.claims {
		if existing.mode == Exclusive && (existing.t == t || existing.t.Name() == t.Name() || strings.HasPrefix(t.Name(), existing.t.Name()+"/")) {
			return true
		}
	}
	return false
}

// claim is one test's hold on one cluster.
type claim struct {
	t    testing.TB
	mode Mode
}

// clusterLock is this process's view of one cluster lock. The OS lock is held
// in the strongest mode any live claim needs; claims are per test.
type clusterLock struct {
	mu        sync.Mutex
	key       string
	endpoints []string
	dir       string
	held      Mode
	release   func() error
	claims    []*claim
}

var (
	locksMu sync.Mutex
	locks   = map[string]*clusterLock{}
)

func lockFor(endpoints []string) *clusterLock {
	key := Key(endpoints)
	locksMu.Lock()
	defer locksMu.Unlock()
	if state := locks[key]; state != nil {
		return state
	}
	state := &clusterLock{key: key, endpoints: endpoints, dir: filepath.Join(os.TempDir(), "blok-clustertest")}
	locks[key] = state
	return state
}

func related(a, b string) bool {
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

func (l *clusterLock) hold(t testing.TB, mode Mode) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, existing := range l.claims {
		if existing.t == t && existing.mode >= mode {
			return nil
		}
		if (existing.mode == Exclusive || mode == Exclusive) && !related(existing.t.Name(), t.Name()) {
			return fmt.Errorf("cluster %s: %s needs a %s lock while %s holds %s in this process; parallel cluster tests cannot share a disruption", l.key, t.Name(), mode, existing.t.Name(), existing.mode)
		}
	}
	if mode > l.held {
		if err := l.transition(t, mode); err != nil {
			return err
		}
	}
	c := &claim{t: t, mode: mode}
	l.claims = append(l.claims, c)
	t.Cleanup(func() { l.drop(t, c) })
	return nil
}

// transition moves the OS lock to mode. It always passes through unlocked: a
// shared-to-exclusive upgrade drops the shared lock first, so two upgraders
// can never wait on each other.
func (l *clusterLock) transition(t testing.TB, mode Mode) error {
	if l.release != nil {
		if err := l.release(); err != nil {
			return fmt.Errorf("cluster %s: release %s lock: %w", l.key, l.held, err)
		}
		l.release, l.held = nil, 0
		removeHolder(l.dir, l.key)
	}
	if mode == 0 {
		return nil
	}
	deadline := time.Now().Add(waitBound(t))
	release, err := acquire(l.dir, l.key, mode, deadline, holderInfo{Mode: mode, Test: t.Name()}, t.Logf)
	if err != nil {
		return err
	}
	l.release, l.held = release, mode
	return nil
}

func (l *clusterLock) drop(t testing.TB, c *claim) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if c.mode == Exclusive {
		// The disruption is over only when the cluster has recovered;
		// releasing earlier hands the next test a cluster mid-election.
		ctx, cancel := context.WithTimeout(context.Background(), healthBound)
		err := healthCheck(ctx, l.endpoints)
		cancel()
		if err != nil {
			t.Errorf("cluster %s still unhealthy %s after the disruptive test %s; releasing the exclusive lock anyway: %v", l.key, healthBound, t.Name(), err)
		}
	}
	for index, existing := range l.claims {
		if existing == c {
			l.claims = append(l.claims[:index], l.claims[index+1:]...)
			break
		}
	}
	var need Mode
	var needer testing.TB = t
	for _, existing := range l.claims {
		if existing.mode > need {
			need, needer = existing.mode, existing.t
		}
	}
	if need == l.held {
		return
	}
	if err := l.transition(needer, need); err != nil {
		t.Errorf("%v", err)
	}
}

func waitBound(t testing.TB) time.Duration {
	bound := defaultWait
	if value := os.Getenv(LockTimeoutEnv); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil && parsed > 0 {
			bound = parsed
		}
	}
	if deadlined, ok := t.(interface{ Deadline() (time.Time, bool) }); ok {
		if deadline, ok := deadlined.Deadline(); ok {
			// Fail with the holders named rather than as a -timeout panic.
			if remaining := time.Until(deadline) - 15*time.Second; remaining < bound {
				bound = max(remaining, time.Second)
			}
		}
	}
	return bound
}

// holderInfo describes one process's hold, for diagnosing a long wait.
type holderInfo struct {
	PID     int
	Mode    Mode
	Package string
	Test    string
	Since   time.Time
}

func (h holderInfo) String() string {
	return fmt.Sprintf("pid %d %s %s %s since %s", h.PID, h.Mode, h.Package, h.Test, h.Since.Format(time.RFC3339))
}

func holderDir(dir, key string) string { return filepath.Join(dir, key+".holders") }

func writeHolder(dir, key string, info holderInfo) {
	info.PID = os.Getpid()
	info.Since = time.Now()
	if wd, err := os.Getwd(); err == nil {
		info.Package = wd
	}
	path := holderDir(dir, key)
	if err := os.MkdirAll(path, 0o700); err != nil {
		return
	}
	text := fmt.Sprintf("%d\n%d\n%s\n%s\n%d\n", info.PID, info.Mode, info.Package, info.Test, info.Since.UnixNano())
	temporary := filepath.Join(path, fmt.Sprintf(".%d.tmp", info.PID))
	if os.WriteFile(temporary, []byte(text), 0o600) == nil {
		_ = os.Rename(temporary, filepath.Join(path, strconv.Itoa(info.PID)))
	}
}

func removeHolder(dir, key string) {
	_ = os.Remove(filepath.Join(holderDir(dir, key), strconv.Itoa(os.Getpid())))
}

// holders lists the live processes recorded as holding the lock. A record of
// a process that has exited is stale and removed.
func holders(dir, key string) []holderInfo {
	entries, err := os.ReadDir(holderDir(dir, key))
	if err != nil {
		return nil
	}
	var result []holderInfo
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == os.Getpid() {
			continue
		}
		path := filepath.Join(holderDir(dir, key), entry.Name())
		if !processAlive(pid) {
			_ = os.Remove(path)
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		fields := strings.Split(string(data), "\n")
		if len(fields) < 5 {
			continue
		}
		mode, _ := strconv.Atoi(fields[1])
		since, _ := strconv.ParseInt(fields[4], 10, 64)
		result = append(result, holderInfo{PID: pid, Mode: Mode(mode), Package: fields[2], Test: fields[3], Since: time.Unix(0, since)})
	}
	return result
}

func describeHolders(dir, key string) string {
	list := holders(dir, key)
	if len(list) == 0 {
		return "no live holder recorded"
	}
	parts := make([]string, len(list))
	for index, holder := range list {
		parts[index] = holder.String()
	}
	return strings.Join(parts, "; ")
}

// errWouldBlock reports that a non-blocking lock attempt found it held.
var errWouldBlock = errors.New("lock held")

// acquire takes the OS lock in mode, polling until deadline. A waiting
// exclusive locker holds the gate file, which every new locker must pass
// first, so a stream of shared holders cannot starve it.
func acquire(dir, key string, mode Mode, deadline time.Time, info holderInfo, logf func(string, ...any)) (func() error, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	main, err := os.OpenFile(filepath.Join(dir, key+".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	gate, err := os.OpenFile(filepath.Join(dir, key+".gate"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		_ = main.Close()
		return nil, err
	}
	fail := func(err error) (func() error, error) {
		_ = unlockFile(gate)
		_ = gate.Close()
		_ = main.Close()
		return nil, err
	}
	start := time.Now()
	nextLog := start.Add(slowWait)
	// wait is one poll interval; it fails once the deadline passes and logs
	// the current holders when the wait has been slow.
	wait := func(what string) error {
		now := time.Now()
		if now.After(deadline) {
			return fmt.Errorf("cluster %s: gave up after %s waiting for the %s lock (%s); holders: %s", key, now.Sub(start).Round(time.Millisecond), mode, what, describeHolders(dir, key))
		}
		if now.After(nextLog) {
			logf("cluster %s: waited %s for the %s lock (%s); holders: %s", key, now.Sub(start).Round(time.Second), mode, what, describeHolders(dir, key))
			nextLog = now.Add(30 * time.Second)
		}
		time.Sleep(25 * time.Millisecond)
		return nil
	}
	for {
		err := tryLock(gate, Exclusive)
		if err == nil {
			break
		}
		if !errors.Is(err, errWouldBlock) {
			return fail(err)
		}
		if err := wait("gate"); err != nil {
			return fail(err)
		}
	}
	if mode == Exclusive {
		info.Test += " (waiting)"
		writeHolder(dir, key, info)
		info.Test = strings.TrimSuffix(info.Test, " (waiting)")
	}
	for {
		err := tryLock(main, mode)
		if err == nil {
			break
		}
		if !errors.Is(err, errWouldBlock) {
			removeHolder(dir, key)
			return fail(err)
		}
		if err := wait("cluster"); err != nil {
			removeHolder(dir, key)
			return fail(err)
		}
	}
	_ = unlockFile(gate)
	_ = gate.Close()
	writeHolder(dir, key, info)
	if waited := time.Since(start); waited > slowWait {
		logf("cluster %s: acquired the %s lock after %s", key, mode, waited.Round(time.Second))
	}
	return func() error {
		err := unlockFile(main)
		return errors.Join(err, main.Close())
	}, nil
}
