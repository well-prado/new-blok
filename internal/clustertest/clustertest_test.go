package clustertest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// These tests exercise the lock across real processes: a helper is this test
// binary re-executed to run only TestHelperProcess, which holds a lock until
// told to release it. They need no cluster; the endpoints are fake, unique
// per test, so their keys never collide with a real cluster's.

const (
	helperRoleEnv      = "BLOK_CLUSTERTEST_HELPER"
	helperEndpointsEnv = "BLOK_CLUSTERTEST_HELPER_ENDPOINTS"
	helperDirEnv       = "BLOK_CLUSTERTEST_HELPER_DIR"
)

func TestMain(m *testing.M) {
	// Fake endpoints have no cluster behind them. The health check itself is
	// exercised against the real cluster by TestReleaseWaitsForRecovery.
	healthCheck = func(context.Context, []string) error { return nil }
	os.Exit(m.Run())
}

func requireLocks(t *testing.T) {
	t.Helper()
	if !lockSupported {
		t.Skip("the cross-process cluster lock is not enforced on this platform")
	}
}

func fakeEndpoints(t *testing.T) []string {
	t.Helper()
	name := strings.NewReplacer("/", "-", " ", "-").Replace(t.Name())
	endpoints := []string{
		fmt.Sprintf("http://fake-%d-%d-%s-a:2379", os.Getpid(), time.Now().UnixNano(), name),
		fmt.Sprintf("http://fake-%d-%d-%s-b:2379", os.Getpid(), time.Now().UnixNano(), name),
	}
	key := Key(endpoints)
	dir := filepath.Join(os.TempDir(), "blok-clustertest")
	t.Cleanup(func() {
		_ = os.Remove(filepath.Join(dir, key+".lock"))
		_ = os.Remove(filepath.Join(dir, key+".gate"))
		_ = os.RemoveAll(holderDir(dir, key))
	})
	return endpoints
}

// available reports whether mode could be taken on endpoints right now, from
// a fresh file descriptor (flock locks on separate opens conflict even within
// one process).
func available(t *testing.T, endpoints []string, mode Mode) bool {
	t.Helper()
	path := filepath.Join(os.TempDir(), "blok-clustertest", Key(endpoints)+".lock")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	err = tryLock(file, mode)
	if errors.Is(err, errWouldBlock) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	_ = unlockFile(file)
	return true
}

type helper struct {
	cmd    *exec.Cmd
	dir    string
	output *bytes.Buffer
	done   chan error
}

// startHelper runs role in a separate process and waits until it reports
// that it holds its lock.
func startHelper(t *testing.T, role string, endpoints []string) *helper {
	t.Helper()
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$", "-test.v", "-test.count=1")
	cmd.Env = append(os.Environ(), helperRoleEnv+"="+role, helperEndpointsEnv+"="+strings.Join(endpoints, ","), helperDirEnv+"="+dir)
	output := &bytes.Buffer{}
	cmd.Stdout, cmd.Stderr = output, output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	h := &helper{cmd: cmd, dir: dir, output: output, done: make(chan error, 1)}
	go func() { h.done <- cmd.Wait() }()
	t.Cleanup(func() {
		h.release()
		select {
		case <-h.done:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
		}
	})
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(dir, "ready")); err == nil {
			return h
		}
		select {
		case err := <-h.done:
			h.done <- err
			if _, statErr := os.Stat(filepath.Join(dir, "ready")); statErr == nil {
				return h
			}
			t.Fatalf("helper %s exited before holding its lock: %v\n%s", role, err, output)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("helper %s did not take its lock\n%s", role, output)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (h *helper) release() { _ = os.WriteFile(filepath.Join(h.dir, "release"), nil, 0o600) }

func (h *helper) wait(t *testing.T) error {
	t.Helper()
	select {
	case err := <-h.done:
		h.done <- err
		return err
	case <-time.After(30 * time.Second):
		t.Fatalf("helper did not exit\n%s", h.output)
		return nil
	}
}

// TestHelperProcess is the body of a helper process; it is skipped in a
// normal run.
func TestHelperProcess(t *testing.T) {
	role := os.Getenv(helperRoleEnv)
	if role == "" {
		t.Skip("helper process only")
	}
	endpoints := strings.Split(os.Getenv(helperEndpointsEnv), ",")
	dir := os.Getenv(helperDirEnv)
	ready := func() {
		if err := os.WriteFile(filepath.Join(dir, "ready"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	awaitRelease := func() {
		deadline := time.Now().Add(60 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(filepath.Join(dir, "release")); err == nil {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("never released")
	}
	switch role {
	case "shared":
		Hold(t, endpoints, Shared)
	case "exclusive":
		Hold(t, endpoints, Exclusive)
	case "disrupt":
		t.Setenv(EndpointsEnv, strings.Join(endpoints, ","))
		Disrupt(t)
	case "fatal":
		// A disruptive subtest fails while holding the exclusive lock. Its
		// parent then checks, from a fresh descriptor in this same process,
		// that the failure released the lock.
		t.Run("fails-holding-exclusive", func(t *testing.T) {
			Hold(t, endpoints, Exclusive)
			t.Fatal("synthetic failure while holding the exclusive cluster lock")
		})
		result := "released"
		if !available(t, endpoints, Exclusive) {
			result = "still-held"
		}
		if err := os.WriteFile(filepath.Join(dir, "result"), []byte(result), 0o600); err != nil {
			t.Fatal(err)
		}
		ready()
		return
	default:
		t.Fatalf("unknown helper role %q", role)
	}
	ready()
	awaitRelease()
}

func TestKeyIsPerClusterAndOrderInsensitive(t *testing.T) {
	a := []string{"http://127.0.0.1:1", "http://127.0.0.1:2", "http://127.0.0.1:3"}
	if Key(a) != Key([]string{" http://127.0.0.1:3/", "http://127.0.0.1:1", "http://127.0.0.1:2", ""}) {
		t.Fatal("the same endpoint set in another order or spelling produced another key")
	}
	if Key(a) == Key([]string{"http://127.0.0.1:4", "http://127.0.0.1:5", "http://127.0.0.1:6"}) {
		t.Fatal("two different clusters share a key")
	}
}

func TestExclusiveHolderBlocksSharedAcrossProcesses(t *testing.T) {
	requireLocks(t)
	endpoints := fakeEndpoints(t)
	h := startHelper(t, "exclusive", endpoints)
	if available(t, endpoints, Shared) {
		t.Fatal("a shared lock was available while another process held the exclusive lock")
	}
	deadline := time.Now().Add(300 * time.Millisecond)
	_, err := acquire(filepath.Join(os.TempDir(), "blok-clustertest"), Key(endpoints), Shared, deadline, holderInfo{Mode: Shared, Test: t.Name()}, t.Logf)
	if err == nil {
		t.Fatal("acquired a shared lock while another process held the exclusive lock")
	}
	// The bounded wait names who holds the cluster.
	if want := fmt.Sprintf("pid %d exclusive", h.cmd.Process.Pid); !strings.Contains(err.Error(), want) || !strings.Contains(err.Error(), "TestHelperProcess") {
		t.Fatalf("wait error %q does not name the holder (%q, TestHelperProcess)", err, want)
	}
	h.release()
	if err := h.wait(t); err != nil {
		t.Fatalf("helper: %v\n%s", err, h.output)
	}
	if !available(t, endpoints, Exclusive) {
		t.Fatal("the lock was still held after the holder exited")
	}
}

func TestSharedHoldersCoexistAndExcludeDisruption(t *testing.T) {
	requireLocks(t)
	endpoints := fakeEndpoints(t)
	startHelper(t, "shared", endpoints)
	startHelper(t, "shared", endpoints)
	if !available(t, endpoints, Shared) {
		t.Fatal("shared holders excluded another shared holder")
	}
	if available(t, endpoints, Exclusive) {
		t.Fatal("an exclusive lock was available while two processes held shared locks")
	}
}

// TestDisruptHoldsTheExclusiveLock is the property the issue rests on: a
// disruptive test excludes every other cluster test, even a shared one.
func TestDisruptHoldsTheExclusiveLock(t *testing.T) {
	requireLocks(t)
	endpoints := fakeEndpoints(t)
	startHelper(t, "disrupt", endpoints)
	if available(t, endpoints, Shared) {
		t.Fatal("Disrupt let a shared holder in: a pause could land mid-test in another package")
	}
}

func TestWaitingDisruptionIsNotStarvedByNewSharedHolders(t *testing.T) {
	requireLocks(t)
	endpoints := fakeEndpoints(t)
	reader := startHelper(t, "shared", endpoints)
	dir, key := filepath.Join(os.TempDir(), "blok-clustertest"), Key(endpoints)
	acquired := make(chan error, 1)
	go func() {
		release, err := acquire(dir, key, Exclusive, time.Now().Add(20*time.Second), holderInfo{Mode: Exclusive, Test: t.Name()}, t.Logf)
		if err == nil {
			err = release()
		}
		acquired <- err
	}()
	// Give the exclusive waiter time to take the gate.
	time.Sleep(200 * time.Millisecond)
	_, err := acquire(dir, key, Shared, time.Now().Add(300*time.Millisecond), holderInfo{Mode: Shared, Test: t.Name()}, t.Logf)
	if err == nil {
		t.Fatal("a new shared holder overtook a waiting exclusive one")
	}
	reader.release()
	if err := <-acquired; err != nil {
		t.Fatalf("the waiting exclusive locker did not get the lock once the shared holder left: %v", err)
	}
}

func TestDifferentClustersDoNotBlockEachOther(t *testing.T) {
	requireLocks(t)
	first, second := fakeEndpoints(t), fakeEndpoints(t)
	startHelper(t, "exclusive", first)
	if !available(t, second, Exclusive) {
		t.Fatal("a disruption on one cluster blocked a different cluster")
	}
	if available(t, first, Shared) {
		t.Fatal("the first cluster's exclusive lock is not held; the comparison proves nothing")
	}
}

func TestLockIsReleasedWhenTheHoldingTestFails(t *testing.T) {
	requireLocks(t)
	endpoints := fakeEndpoints(t)
	h := startHelper(t, "fatal", endpoints)
	if err := h.wait(t); err == nil {
		t.Fatalf("the helper's synthetic failure did not fail it\n%s", h.output)
	}
	result, err := os.ReadFile(filepath.Join(h.dir, "result"))
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "released" {
		t.Fatalf("after the holding test failed, the lock was %s\n%s", result, h.output)
	}
}

func TestUpgradeWithinOneTestThenDowngrade(t *testing.T) {
	requireLocks(t)
	endpoints := fakeEndpoints(t)
	t.Run("upgrade", func(t *testing.T) {
		Hold(t, endpoints, Shared)
		if available(t, endpoints, Exclusive) || !available(t, endpoints, Shared) {
			t.Fatal("shared hold is not shared")
		}
		t.Run("disruptive-subtest", func(t *testing.T) {
			Hold(t, endpoints, Exclusive)
			if available(t, endpoints, Shared) {
				t.Fatal("a subtest's exclusive hold under a shared parent is not exclusive")
			}
		})
		if available(t, endpoints, Exclusive) || !available(t, endpoints, Shared) {
			t.Fatal("the parent did not return to a shared hold after its disruptive subtest")
		}
	})
	if !available(t, endpoints, Exclusive) {
		t.Fatal("the lock outlived the test that held it")
	}
}

// recorder is a testing.TB whose failures and cleanups are captured, so a
// test can watch another hold fail without failing itself.
type recorder struct {
	testing.TB
	cleanups []func()
	errors   []string
}

func (r *recorder) Name() string     { return r.TB.Name() + "/recorded" }
func (r *recorder) Helper()          {}
func (r *recorder) Cleanup(f func()) { r.cleanups = append(r.cleanups, f) }
func (r *recorder) Errorf(format string, a ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, a...))
}
func (r *recorder) Fatalf(format string, a ...any) { r.Errorf(format, a...) }

func (r *recorder) finish() {
	for index := len(r.cleanups) - 1; index >= 0; index-- {
		r.cleanups[index]()
	}
}

// TestReleaseWaitsForRecovery runs against the real cluster: a disruptive
// hold that ends with a voter still paused must fail rather than hand the
// next test an unrecovered cluster, and WaitHealthy names the unreachable
// voter until it is back.
func TestReleaseWaitsForRecovery(t *testing.T) {
	requireLocks(t)
	previousCheck, previousBound := healthCheck, healthBound
	t.Cleanup(func() { healthCheck, healthBound = previousCheck, previousBound })
	healthCheck = WaitHealthy
	endpoints := Disrupt(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := WaitHealthy(ctx, endpoints); err != nil {
		t.Fatalf("cluster unhealthy before the test: %v", err)
	}
	voter, endpoint := Voters()[2], strings.Split(os.Getenv(EndpointsEnv), ",")[2]
	t.Cleanup(func() { _ = exec.Command("docker", "unpause", voter).Run() })

	healthBound = 3 * time.Second
	disruptive := &recorder{TB: t}
	Hold(disruptive, endpoints, Exclusive)
	if output, err := exec.Command("docker", "pause", voter).CombinedOutput(); err != nil {
		t.Fatalf("pause %s: %v: %s", voter, err, output)
	}
	disruptive.finish()
	if len(disruptive.errors) != 1 || !strings.Contains(disruptive.errors[0], "still unhealthy") || !strings.Contains(disruptive.errors[0], endpoint) {
		t.Fatalf("a disruptive hold released with %s paused reported %q, want one unhealthy error naming %s", voter, disruptive.errors, endpoint)
	}
	t.Logf("release check caught the unrecovered cluster: %s", disruptive.errors[0])

	if output, err := exec.Command("docker", "unpause", voter).CombinedOutput(); err != nil {
		t.Fatalf("unpause %s: %v: %s", voter, err, output)
	}
	healthBound = previousBound
	recoverCtx, recoverCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer recoverCancel()
	if err := WaitHealthy(recoverCtx, endpoints); err != nil {
		t.Fatalf("cluster did not recover after unpausing %s: %v", voter, err)
	}
}
