package order

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

const (
	crashPhaseEnv  = "NEWBLOK_ORDER_CRASH_PHASE"
	crashPathEnv   = "NEWBLOK_ORDER_CRASH_PATH"
	crashMarkerEnv = "NEWBLOK_ORDER_CRASH_MARKER"
	crashLogEnv    = "NEWBLOK_ORDER_CRASH_LOG"
)

var crashRequest = Request{RequestKey: "crash", SKU: "coffee", Quantity: 2}

// TestOrderCrashMatrix kills a child process with SIGKILL at each handoff of
// an order, reopens the store and recovers it as a restarted process would:
// the producer's client retries its submission, then the worker and the
// dispatcher run, first at the time of the crash and then past every lease.
// After each stage the request key's rows and publishes must equal the
// fixture's crash case.
func TestOrderCrashMatrix(t *testing.T) {
	if phase := os.Getenv(crashPhaseEnv); phase != "" {
		runOrderCrashChild(phase)
		return
	}
	for _, crash := range loadOrderFixtures(t).Crashes {
		t.Run(crash.Phase, func(t *testing.T) {
			directory := t.TempDir()
			path, marker, log := filepath.Join(directory, "orders.db"), filepath.Join(directory, "marker"), filepath.Join(directory, "publishes")
			offset := new(atomic.Int64)
			run := openRun(t, path, log, crashRequest.RequestKey, "ok", offset)
			switch crash.Phase {
			case "producer-before-commit", "producer-after-commit":
			case "handler-between-order-and-outbox":
				run.enqueue(crashRequest)
			case "dispatcher-after-publish":
				run.enqueue(crashRequest)
				if processed, err := run.service.ProcessOnce(context.Background()); err != nil || !processed {
					t.Fatalf("processed=%v err=%v", processed, err)
				}
			default:
				t.Fatalf("unknown crash phase %q", crash.Phase)
			}
			run.close()

			child := exec.Command(os.Args[0], "-test.run=^TestOrderCrashMatrix$")
			child.Env = append(os.Environ(), crashPhaseEnv+"="+crash.Phase, crashPathEnv+"="+path, crashMarkerEnv+"="+marker, crashLogEnv+"="+log)
			if err := child.Start(); err != nil {
				t.Fatal(err)
			}
			waitForCrashMarker(t, marker)
			if err := child.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			// Killing a child that already exited is not an error, so check
			// that it died of the kill, still parked.
			_ = child.Wait()
			if status, ok := child.ProcessState.Sys().(syscall.WaitStatus); !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatalf("crash child was not killed at its crash point: %v", child.ProcessState)
			}

			run = openRun(t, path, log, crashRequest.RequestKey, "ok", offset)
			defer run.close()
			run.expect("after the crash", crash.AfterCrash)
			// The producer's client never saw an acknowledgment and retries;
			// for a later phase the retry is a duplicate delivery.
			run.enqueue(crashRequest)
			// This stage assumes less than the 30s default leases passed on
			// the wall clock between the child's claim and this drain.
			run.drain(1, false)
			run.expect("recovered at the time of the crash", crash.AtCrashTime)
			run.drain(3, true)
			run.expect("recovered after every lease", crash.AfterLeases)
		})
	}
}

// runOrderCrashChild parks the order at phase until the parent kills it.
func runOrderCrashChild(phase string) {
	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, os.Getenv(crashPathEnv))
	if err != nil {
		panic(err)
	}
	parking := &parkingDatabase{Database: database}
	service, err := New(ctx, parking, map[string]int64{"coffee": 1500}, nil)
	if err != nil {
		panic(err)
	}
	switch phase {
	case "producer-before-commit":
		parking.park = parkBeforeCommit
		_, err = service.Enqueue(ctx, crashRequest)
	case "producer-after-commit":
		parking.park = parkAfterCommit
		_, err = service.Enqueue(ctx, crashRequest)
	case "handler-between-order-and-outbox":
		service.afterOrderInsert = parkForKill
		_, err = service.ProcessOnce(ctx)
	case "dispatcher-after-publish":
		_, err = service.DispatchOne(ctx, func(_ context.Context, event Event) error {
			if err := appendPublish(os.Getenv(crashLogEnv), event.ID); err != nil {
				return err
			}
			parkForKill()
			return nil
		})
	}
	panic("order crash child was not killed at " + phase + ": " + errString(err))
}

func errString(err error) string {
	if err == nil {
		return "no error"
	}
	return err.Error()
}

// parkForKill tells the parent the child reached its crash point and waits
// to be killed there.
func parkForKill() {
	if err := os.WriteFile(os.Getenv(crashMarkerEnv), []byte("ready"), 0o600); err != nil {
		panic(err)
	}
	time.Sleep(10 * time.Second)
}

func waitForCrashMarker(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("crash child did not reach its crash point: %s", path)
}

type parkPoint int

const (
	parkNever parkPoint = iota
	// parkBeforeCommit parks inside the transaction once its statements ran.
	parkBeforeCommit
	// parkAfterCommit parks once the transaction committed.
	parkAfterCommit
)

// parkingDatabase parks the child at its producer's commit. It forwards the
// write domain and busy timeout of the database it wraps.
type parkingDatabase struct {
	store.Database
	park parkPoint
}

func (d *parkingDatabase) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	switch d.park {
	case parkBeforeCommit:
		return d.Database.WithTx(ctx, func(tx *sql.Tx) error {
			if err := fn(tx); err != nil {
				return err
			}
			parkForKill()
			return nil
		})
	case parkAfterCommit:
		if err := d.Database.WithTx(ctx, fn); err != nil {
			return err
		}
		parkForKill()
		return nil
	}
	return d.Database.WithTx(ctx, fn)
}

func (d *parkingDatabase) WriteDomain() *store.WriteDomain {
	domain, _ := store.WriteDomainOf(d.Database)
	return domain
}

func (d *parkingDatabase) BusyTimeout() time.Duration {
	timeout, _ := store.BusyTimeoutOf(d.Database)
	return timeout
}
