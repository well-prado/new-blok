package worker

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
)

// timedDB takes the write lock first in every transaction and records how
// long the writer waited for it and how long it then held it.
type timedDB struct {
	store.Database
	mu    sync.Mutex
	waits []time.Duration // begin -> first statement done (lock acquisition incl. busy waits)
	holds []time.Duration // first statement done -> commit returned (lock held)
}

func (d *timedDB) WriteDomain() *store.WriteDomain { w, _ := store.WriteDomainOf(d.Database); return w }

func (d *timedDB) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	start := time.Now()
	var acquired time.Time
	err := d.Database.WithTx(ctx, func(tx *sql.Tx) error {
		// Take the write lock first so waiting and holding can be separated.
		if _, err := tx.ExecContext(ctx, `UPDATE zz_lock SET x = x WHERE 0`); err != nil {
			return err
		}
		acquired = time.Now()
		return fn(tx)
	})
	end := time.Now()
	if !acquired.IsZero() {
		d.mu.Lock()
		d.waits = append(d.waits, acquired.Sub(start))
		d.holds = append(d.holds, end.Sub(acquired))
		d.mu.Unlock()
	}
	return err
}

func pct(xs []time.Duration, p float64) time.Duration {
	if len(xs) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), xs...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[int(float64(len(s)-1)*p)]
}

// TestMeasureWriterWaits reports the write-lock wait and hold distributions
// under TestConcurrentWorkersNeverFailBusy's load (#214): 200 submissions, 16
// in flight, 4 workers. With writers queued in arrival order the longest wait
// stays near contenders × hold time; under SQLite's busy handler alone it
// reached the 5 s busy timeout. ADR 0003 records the numbers.
//
//	NEWBLOK_MEASURE_WRITER_WAITS=1 go test -run TestMeasureWriterWaits -count=8 -v ./trigger/worker/
func TestMeasureWriterWaits(t *testing.T) {
	if os.Getenv("NEWBLOK_MEASURE_WRITER_WAITS") == "" {
		t.Skip("set NEWBLOK_MEASURE_WRITER_WAITS=1 to measure writer waits")
	}
	ctx := context.Background()
	raw, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "busy.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if err := raw.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE zz_lock (x INTEGER)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	db := &timedDB{Database: raw}
	queue, err := New(ctx, db, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.RegisterKind("busy", []byte(`{"type":"object"}`)); err != nil {
		t.Fatal(err)
	}
	const jobs, workers, inFlight = 200, 4, 16
	var submitters sync.WaitGroup
	slots := make(chan struct{}, inFlight)
	var failed int
	var fmu sync.Mutex
	for i := range jobs {
		submitters.Add(1)
		go func() {
			defer submitters.Done()
			slots <- struct{}{}
			defer func() { <-slots }()
			if _, err := queue.Submit(ctx, trigger.Submission{Key: fmt.Sprintf("busy-%d", i), Kind: "busy", Payload: []byte(`{}`)}); err != nil {
				fmu.Lock()
				failed++
				fmu.Unlock()
			}
		}()
	}
	var group sync.WaitGroup
	deadline := time.Now().Add(30 * time.Second)
	done := map[string]bool{}
	var dmu sync.Mutex
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for time.Now().Before(deadline) {
				dmu.Lock()
				n := len(done)
				dmu.Unlock()
				if n >= jobs-failed {
					return
				}
				if _, err := queue.ProcessOnce(ctx, func(_ context.Context, _ Tx, job Job) error {
					dmu.Lock()
					done[job.RequestKey] = true
					dmu.Unlock()
					return nil
				}); err != nil {
					fmu.Lock()
					failed++
					fmu.Unlock()
				}
			}
		}()
	}
	submitters.Wait()
	group.Wait()
	db.mu.Lock()
	defer db.mu.Unlock()
	var sumHold time.Duration
	for _, h := range db.holds {
		sumHold += h
	}
	over1s := 0
	for _, w := range db.waits {
		if w > time.Second {
			over1s++
		}
	}
	t.Logf("tx=%d failed=%d | hold p50=%v p99=%v max=%v total=%v | wait p50=%v p90=%v p99=%v max=%v >1s=%d",
		len(db.holds), failed, pct(db.holds, .5), pct(db.holds, .99), pct(db.holds, 1), sumHold,
		pct(db.waits, .5), pct(db.waits, .9), pct(db.waits, .99), pct(db.waits, 1), over1s)
}
