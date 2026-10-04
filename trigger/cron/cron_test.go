package cron_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
	"github.com/well-prado/new-blok/trigger/cron"
	"github.com/well-prado/new-blok/trigger/worker"
)

// fakeClock drives the scheduler deterministically; After fires once the
// clock is advanced past its deadline.
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []waiter
}

type waiter struct {
	at time.Time
	ch chan time.Time
}

func newClock(at time.Time) *fakeClock { return &fakeClock{now: at} }

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- c.now
		return ch
	}
	c.waiters = append(c.waiters, waiter{at: c.now.Add(d), ch: ch})
	return ch
}

func (c *fakeClock) Set(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = at
	kept := c.waiters[:0]
	for _, w := range c.waiters {
		if !w.at.After(at) {
			w.ch <- at
		} else {
			kept = append(kept, w)
		}
	}
	c.waiters = kept
}

var principal = trigger.Principal{ID: "schedule:reports"}

func openQueue(t *testing.T) (store.Database, *worker.Queue) {
	t.Helper()
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	queue, err := worker.New(context.Background(), database, nil)
	if err != nil {
		t.Fatal(err)
	}
	return database, queue
}

func jobs(t *testing.T, database store.Database) int {
	t.Helper()
	var n int
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM worker_jobs`).Scan(&n)
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

func schedule(name, spec string) cron.Schedule {
	return cron.Schedule{Name: name, Spec: spec, TimeZone: "UTC", Kind: "report.build", Payload: json.RawMessage(`{"report":"daily"}`), InputSchema: []byte(`{"type":"object","properties":{"report":{"type":"string"}},"required":["report"]}`), Principal: principal}
}

func times(values []time.Time) []string {
	var out []string
	for _, v := range values {
		out = append(out, v.UTC().Format("15:04"))
	}
	return out
}

func tick(t *testing.T, s *cron.Scheduler) cron.Result {
	t.Helper()
	results, err := s.Tick(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		return cron.Result{}
	}
	return results[0]
}

func at(hour, minute int) time.Time { return time.Date(2026, 10, 3, hour, minute, 0, 0, time.UTC) }

func TestDuplicateTicksAndWallClockShifts(t *testing.T) {
	database, queue := openQueue(t)
	clock := newClock(at(10, 30))
	s, err := cron.New(context.Background(), database, queue, queue, clock)
	if err != nil {
		t.Fatal(err)
	}
	sched := schedule("hourly", "0 * * * *")
	sched.MaxCatchUp = 2
	if added, err := s.Add(context.Background(), sched); err != nil || !added {
		t.Fatalf("added=%v err=%v", added, err)
	}
	steps := []struct {
		name      string
		at        time.Time
		submitted []string
		missed    int
		totalJobs int
	}{
		{"not yet due", at(10, 59), nil, 0, 0},
		{"due", at(11, 0), []string{"11:00"}, 0, 1},
		{"duplicate tick at the same instant", at(11, 0), nil, 0, 1},
		{"clock jumps backwards", at(10, 5), nil, 0, 1},
		{"clock returns to the fired instant", at(11, 0), nil, 0, 1},
		{"clock jumps forward past four firings", at(15, 10), []string{"14:00", "15:00"}, 2, 3},
		{"tick again after the jump", at(15, 10), nil, 0, 3},
	}
	for _, step := range steps {
		clock.Set(step.at)
		result := tick(t, s)
		behind := step.name == "clock jumps backwards"
		if (result.Behind > 0) != behind {
			t.Fatalf("%s: behind=%v", step.name, result.Behind)
		}
		if fmt.Sprint(times(result.Submitted)) != fmt.Sprint(step.submitted) || result.Missed != step.missed || len(result.Duplicates) != 0 || jobs(t, database) != step.totalJobs {
			t.Fatalf("%s: submitted=%v duplicates=%v missed=%d jobs=%d, want %v missed=%d jobs=%d", step.name, times(result.Submitted), times(result.Duplicates), result.Missed, jobs(t, database), step.submitted, step.missed, step.totalJobs)
		}
	}
	if next, ok := s.Next("hourly"); !ok || !next.Equal(at(16, 0)) {
		t.Fatalf("next=%v", next)
	}
}

func TestRestartResumesFromCursorAndRefusesRedefinition(t *testing.T) {
	database, queue := openQueue(t)
	clock := newClock(at(10, 30))
	first, err := cron.New(context.Background(), database, queue, queue, clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Add(context.Background(), schedule("hourly", "0 * * * *")); err != nil {
		t.Fatal(err)
	}
	clock.Set(at(11, 0))
	tick(t, first)
	if added, err := first.Add(context.Background(), schedule("hourly", "0 * * * *")); err != nil || added {
		t.Fatalf("re-adding the same definition: added=%v err=%v", added, err)
	}
	if _, err := first.Add(context.Background(), schedule("hourly", "30 * * * *")); !errors.Is(err, cron.ErrConflict) {
		t.Fatalf("redefinition in memory: %v", err)
	}
	// A new process over the same store.
	second, err := cron.New(context.Background(), database, queue, queue, clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Add(context.Background(), schedule("hourly", "30 * * * *")); !errors.Is(err, cron.ErrConflict) {
		t.Fatalf("redefinition across restart: %v", err)
	}
	if added, err := second.Add(context.Background(), schedule("hourly", "0 * * * *")); err != nil || added {
		t.Fatalf("resume: added=%v err=%v", added, err)
	}
	if result := tick(t, second); len(result.Submitted) != 0 || jobs(t, database) != 1 {
		t.Fatalf("restart re-fired an occurrence: %+v jobs=%d", result, jobs(t, database))
	}
	clock.Set(at(12, 0))
	if result := tick(t, second); fmt.Sprint(times(result.Submitted)) != "[12:00]" {
		t.Fatalf("after restart: %+v", result)
	}
}

func TestOverlapPolicies(t *testing.T) {
	for _, tc := range []struct {
		policy    cron.OverlapPolicy
		submitted []string
		skipped   []string
		heldAfter bool
	}{
		{cron.OverlapAllow, []string{"11:00", "12:00", "13:00"}, nil, false},
		{cron.OverlapSkip, []string{"11:00"}, []string{"12:00", "13:00"}, false},
		{cron.OverlapCoalesce, []string{"11:00"}, nil, true},
	} {
		t.Run(string(tc.policy), func(t *testing.T) {
			database, queue := openQueue(t)
			clock := newClock(at(10, 30))
			s, err := cron.New(context.Background(), database, queue, queue, clock)
			if err != nil {
				t.Fatal(err)
			}
			sched := schedule("hourly", "0 * * * *")
			sched.Overlap = tc.policy
			if _, err := s.Add(context.Background(), sched); err != nil {
				t.Fatal(err)
			}
			var submitted, skipped []string
			var held *time.Time
			for hour := 11; hour <= 13; hour++ {
				clock.Set(at(hour, 0))
				result := tick(t, s)
				submitted = append(submitted, times(result.Submitted)...)
				skipped = append(skipped, times(result.Skipped)...)
				held = result.Held
			}
			// The 11:00 work never ran, so it is still unsettled.
			if fmt.Sprint(submitted) != fmt.Sprint(tc.submitted) || fmt.Sprint(skipped) != fmt.Sprint(tc.skipped) || (held != nil) != tc.heldAfter {
				t.Fatalf("submitted=%v skipped=%v held=%v", submitted, skipped, held)
			}
			if tc.policy != cron.OverlapCoalesce {
				return
			}
			if !held.Equal(at(13, 0)) {
				t.Fatalf("coalesce held %v, want the newest occurrence 13:00", held)
			}
			// Once the 11:00 work settles, the newest held occurrence is
			// submitted, and only it.
			if _, err := queue.ProcessOnce(context.Background(), func(context.Context, worker.Tx, worker.Job) error { return nil }); err != nil {
				t.Fatal(err)
			}
			clock.Set(at(13, 1))
			result := tick(t, s)
			if fmt.Sprint(times(result.Submitted)) != "[13:00]" || result.Held != nil || jobs(t, database) != 2 {
				t.Fatalf("after settling: %+v jobs=%d", result, jobs(t, database))
			}
		})
	}
}

func TestScheduleValidation(t *testing.T) {
	database, queue := openQueue(t)
	s, err := cron.New(context.Background(), database, queue, nil, newClock(at(10, 0)))
	if err != nil {
		t.Fatal(err)
	}
	valid := schedule("ok", "0 * * * *")
	for name, mutate := range map[string]func(*cron.Schedule){
		"invalid payload":       func(c *cron.Schedule) { c.Payload = json.RawMessage(`{"report":7}`) },
		"missing payload":       func(c *cron.Schedule) { c.Payload = nil },
		"implicit local zone":   func(c *cron.Schedule) { c.TimeZone = "Local" },
		"unknown zone":          func(c *cron.Schedule) { c.TimeZone = "Mars/Olympus" },
		"empty zone":            func(c *cron.Schedule) { c.TimeZone = "" },
		"never fires":           func(c *cron.Schedule) { c.Spec = "0 0 31 2 *" },
		"bad gap policy":        func(c *cron.Schedule) { c.Gap = "maybe" },
		"unbounded catch-up":    func(c *cron.Schedule) { c.MaxCatchUp = cron.MaxCatchUpLimit + 1 },
		"negative catch-up":     func(c *cron.Schedule) { c.MaxCatchUp = cron.NoCatchUp - 1 },
		"no principal":          func(c *cron.Schedule) { c.Principal = trigger.Principal{} },
		"bad name":              func(c *cron.Schedule) { c.Name = "Daily Report" },
		"overlap needs tracker": func(c *cron.Schedule) { c.Overlap = cron.OverlapSkip },
	} {
		sched := valid
		mutate(&sched)
		if _, err := s.Add(context.Background(), sched); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	bad := valid
	bad.Payload = json.RawMessage(`{"report":7}`)
	if _, err := s.Add(context.Background(), bad); !errors.Is(err, trigger.ErrInvalidInput) {
		t.Fatalf("invalid payload error %v does not wrap ErrInvalidInput", err)
	}
	if _, err := s.Add(context.Background(), valid); err != nil {
		t.Fatal(err)
	}
}

// memorySubmitter counts submissions without a store, to profile the
// scheduler alone.
type memorySubmitter struct {
	mu   sync.Mutex
	keys map[string]bool
}

func (m *memorySubmitter) Submit(_ context.Context, s trigger.Submission) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.keys[s.Key] {
		return false, nil
	}
	m.keys[s.Key] = true
	return true, nil
}

// TestManySchedulesOneGoroutine registers 10 000 schedules across zones and
// runs them with the single scheduling goroutine: one goroutine total, the
// memory per schedule and the cost of a tick in which every schedule fires.
func TestManySchedulesOneGoroutine(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "cron.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	clock := newClock(at(10, 30))
	submitter := &memorySubmitter{keys: map[string]bool{}}
	s, err := cron.New(context.Background(), database, submitter, nil, clock)
	if err != nil {
		t.Fatal(err)
	}
	zones := []string{"UTC", "America/New_York", "Europe/London", "Asia/Kolkata", "Australia/Lord_Howe"}
	const count = 10000
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	began := time.Now()
	all := make([]cron.Schedule, count)
	for i := range all {
		all[i] = schedule(fmt.Sprintf("s%05d", i), fmt.Sprintf("%d * * * *", i%60))
		all[i].TimeZone = zones[i%len(zones)]
	}
	if _, err := s.AddAll(context.Background(), all); err != nil {
		t.Fatal(err)
	}
	addTime := time.Since(began)
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	baseline := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, nil) }()
	time.Sleep(50 * time.Millisecond)
	if extra := runtime.NumGoroutine() - baseline; extra != 1 {
		t.Fatalf("Run started %d goroutines for %d schedules, want 1", extra, count)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v", err)
	}
	// Every schedule is due once within the next hour.
	clock.Set(at(11, 30))
	began = time.Now()
	results, err := s.Tick(context.Background())
	tickTime := time.Since(began)
	if err != nil {
		t.Fatal(err)
	}
	fired := 0
	for _, r := range results {
		fired += len(r.Submitted)
	}
	perSchedule := float64(int64(after.HeapInuse)-int64(before.HeapInuse)) / count
	t.Logf("%d schedules: add %v (%.1fµs each), %.0f heap bytes each, tick firing all %d took %v", count, addTime, float64(addTime.Microseconds())/count, perSchedule, fired, tickTime)
	if fired != count || perSchedule > 16<<10 {
		t.Fatalf("fired=%d heap per schedule=%.0f", fired, perSchedule)
	}
}

// TestDenseSchedulesProfile measures minutely schedules, the densest a
// five-field expression allows: a steady-state tick of 1000 of them in a DST
// zone, and the first tick after three days down.
func TestDenseSchedulesProfile(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "cron.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	clock := newClock(at(10, 30))
	submitter := &memorySubmitter{keys: map[string]bool{}}
	s, err := cron.New(context.Background(), database, submitter, nil, clock)
	if err != nil {
		t.Fatal(err)
	}
	const count = 1000
	all := make([]cron.Schedule, count)
	for i := range all {
		spec := "* * * * *"
		if i%2 == 1 {
			spec = "*/5 * * * *"
		}
		all[i] = schedule(fmt.Sprintf("d%04d", i), spec)
		all[i].TimeZone = "America/New_York"
	}
	if _, err := s.AddAll(context.Background(), all); err != nil {
		t.Fatal(err)
	}
	clock.Set(at(10, 35))
	began := time.Now()
	results, err := s.Tick(context.Background())
	steady := time.Since(began)
	if err != nil || len(results) != count {
		t.Fatalf("steady tick: %d results, err=%v", len(results), err)
	}
	clock.Set(at(10, 35).AddDate(0, 0, 3))
	began = time.Now()
	results, err = s.Tick(context.Background())
	catchUp := time.Since(began)
	if err != nil || len(results) != count {
		t.Fatalf("catch-up tick: %d results, err=%v", len(results), err)
	}
	for _, r := range results {
		// The on-time occurrences (10:34, exactly LateAfter old, and 10:35
		// for minutely; 10:35 for */5) and the latest late one; the rest of
		// three days (4320 or 864 firings) are missed.
		minutely := r.Schedule[len(r.Schedule)-1]%2 == 0
		if (minutely && (len(r.Submitted) != 3 || r.Missed != 4317)) || (!minutely && (len(r.Submitted) != 2 || r.Missed != 862)) {
			t.Fatalf("catch-up %s: submitted=%d missed=%d", r.Schedule, len(r.Submitted), r.Missed)
		}
	}
	t.Logf("%d dense schedules (minutely and */5, America/New_York): steady tick %v, tick after 3 days down %v", count, steady, catchUp)
	if steady > denseTickBound || catchUp > denseTickBound {
		t.Fatalf("steady %v, catch-up %v, bound %v", steady, catchUp, denseTickBound)
	}
}

// TestRunFiresOnTheClock drives Run with the fake clock: firings happen when
// the clock reaches them, without polling.
func TestRunFiresOnTheClock(t *testing.T) {
	database, queue := openQueue(t)
	clock := newClock(at(10, 59))
	s, err := cron.New(context.Background(), database, queue, queue, clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Add(context.Background(), schedule("hourly", "0 * * * *")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Run(ctx, nil) }()
	time.Sleep(20 * time.Millisecond)
	if jobs(t, database) != 0 {
		t.Fatal("fired early")
	}
	clock.Set(at(11, 0))
	deadline := time.Now().Add(5 * time.Second)
	for jobs(t, database) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("Run did not fire when the clock reached the occurrence")
		}
		time.Sleep(5 * time.Millisecond)
	}
	clock.Set(at(12, 0))
	for jobs(t, database) != 2 {
		if time.Now().After(deadline) {
			t.Fatal("Run did not fire the following occurrence")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestCatchUpCountsOnlyLateOccurrences: an occurrence handled within
// LateAfter of its instant is on time and always fires; MaxCatchUp bounds
// only the late ones. NoCatchUp is reachable and submits none of them.
func TestCatchUpCountsOnlyLateOccurrences(t *testing.T) {
	for _, tc := range []struct {
		name      string
		catchUp   int
		resume    time.Time
		submitted []string
		missed    int
	}{
		{"default catches up the latest late one", 0, at(15, 10), []string{"15:00"}, 3},
		{"no catch-up submits no late one", cron.NoCatchUp, at(15, 10), nil, 4},
		{"on time plus the latest late one", 0, at(15, 0).Add(30 * time.Second), []string{"14:00", "15:00"}, 2},
		{"no catch-up still fires the on-time one", cron.NoCatchUp, at(15, 0).Add(30 * time.Second), []string{"15:00"}, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database, queue := openQueue(t)
			clock := newClock(at(10, 30))
			s, err := cron.New(context.Background(), database, queue, queue, clock)
			if err != nil {
				t.Fatal(err)
			}
			sched := schedule("hourly", "0 * * * *")
			sched.MaxCatchUp = tc.catchUp
			if _, err := s.Add(context.Background(), sched); err != nil {
				t.Fatal(err)
			}
			clock.Set(at(11, 0))
			if result := tick(t, s); fmt.Sprint(times(result.Submitted)) != "[11:00]" {
				t.Fatalf("on time: %+v", result)
			}
			clock.Set(tc.resume)
			result := tick(t, s)
			if fmt.Sprint(times(result.Submitted)) != fmt.Sprint(tc.submitted) || result.Missed != tc.missed {
				t.Fatalf("after downtime: submitted=%v missed=%d, want %v missed=%d", times(result.Submitted), result.Missed, tc.submitted, tc.missed)
			}
			clock.Set(at(16, 0))
			if result := tick(t, s); fmt.Sprint(times(result.Submitted)) != "[16:00]" || result.Missed != 0 {
				t.Fatalf("next on-time occurrence: %+v", result)
			}
		})
	}
}

// TestConcurrentTicksDoNotDuplicate calls Tick from several goroutines at
// once, while Run also ticks and readers run alongside: every occurrence is
// submitted exactly once.
func TestConcurrentTicksDoNotDuplicate(t *testing.T) {
	database, queue := openQueue(t)
	clock := newClock(at(10, 30))
	s, err := cron.New(context.Background(), database, queue, queue, clock)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		sched := schedule(fmt.Sprintf("minutely-%d", i), "* * * * *")
		sched.MaxCatchUp = 5
		if _, err := s.Add(context.Background(), sched); err != nil {
			t.Fatal(err)
		}
	}
	var submitted, duplicates atomic.Int64
	count := func(results []cron.Result) {
		for _, r := range results {
			submitted.Add(int64(len(r.Submitted)))
			duplicates.Add(int64(len(r.Duplicates)))
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, func(results []cron.Result, _ error) { count(results) }) }()
	for minute := 31; minute <= 40; minute++ {
		clock.Set(at(10, minute))
		var wg sync.WaitGroup
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results, err := s.Tick(context.Background())
				if err != nil {
					t.Error(err)
				}
				count(results)
				s.Next("minutely-0")
			}()
		}
		wg.Wait()
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v", err)
	}
	if submitted.Load() != 30 || duplicates.Load() != 0 || jobs(t, database) != 30 {
		t.Fatalf("submitted=%d duplicates=%d jobs=%d, want 30, 0 and 30", submitted.Load(), duplicates.Load(), jobs(t, database))
	}
}

// TestClockCorrectedAfterForwardStepIsReported pins the documented limit: a
// clock stepped a day forward fires the occurrences due then (the on-time
// one and the latest late one); once corrected,
// the schedule is silent until the clock passes its cursor, and every tick
// reports how far behind the clock is.
func TestClockCorrectedAfterForwardStepIsReported(t *testing.T) {
	database, queue := openQueue(t)
	clock := newClock(at(10, 30))
	s, err := cron.New(context.Background(), database, queue, queue, clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Add(context.Background(), schedule("hourly", "0 * * * *")); err != nil {
		t.Fatal(err)
	}
	tomorrow := at(11, 0).AddDate(0, 0, 1)
	clock.Set(tomorrow)
	if result := tick(t, s); fmt.Sprint(times(result.Submitted)) != "[10:00 11:00]" || result.Behind != 0 {
		t.Fatalf("stepped forward: %+v", result)
	}
	for _, corrected := range []time.Time{at(10, 59), at(11, 0), at(12, 0)} {
		clock.Set(corrected)
		result := tick(t, s)
		if len(result.Submitted) != 0 || result.Behind != tomorrow.Sub(corrected) {
			t.Fatalf("corrected to %s: %+v, want nothing fired and behind %v", corrected.Format("15:04"), result, tomorrow.Sub(corrected))
		}
	}
	clock.Set(tomorrow.Add(time.Hour))
	if result := tick(t, s); fmt.Sprint(times(result.Submitted)) != "[12:00]" || result.Behind != 0 {
		t.Fatalf("after the clock passed the cursor: %+v", result)
	}
}

// failing refuses submissions of the named schedules while fail is set.
type failing struct {
	inner trigger.Submitter
	fail  atomic.Bool
	names []string
}

func (f *failing) Submit(ctx context.Context, s trigger.Submission) (bool, error) {
	for _, name := range f.names {
		if f.fail.Load() && strings.HasPrefix(s.Key, "cron:"+name+":") {
			return false, errors.New("store unavailable")
		}
	}
	return f.inner.Submit(ctx, s)
}

// TestFailingScheduleBacksOffWithoutStoppingRun: one schedule's failing
// submissions are retried with backoff while Run keeps firing the others.
func TestFailingScheduleBacksOffWithoutStoppingRun(t *testing.T) {
	database, queue := openQueue(t)
	clock := newClock(at(10, 30))
	submit := &failing{inner: queue, names: []string{"bad"}}
	submit.fail.Store(true)
	s, err := cron.New(context.Background(), database, submit, queue, clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddAll(context.Background(), []cron.Schedule{schedule("bad", "0 * * * *"), schedule("good", "0 * * * *")}); err != nil {
		t.Fatal(err)
	}
	// A direct tick: bad fails, good fires, and an immediate retry waits
	// for the backoff.
	clock.Set(at(11, 0))
	results, err := s.Tick(context.Background())
	if err == nil || len(results) != 2 || results[0].Err == nil || results[1].Err != nil || len(results[1].Submitted) != 1 {
		t.Fatalf("first tick: %+v err=%v", results, err)
	}
	if again, _ := s.Tick(context.Background()); len(again) != 0 {
		t.Fatalf("retried inside the backoff: %+v", again)
	}
	var mu sync.Mutex
	var reported []cron.Result
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- s.Run(ctx, func(results []cron.Result, _ error) {
			mu.Lock()
			defer mu.Unlock()
			reported = append(reported, results...)
		})
	}()
	waitFor := func(what string, ok func() bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !ok() {
			select {
			case err := <-done:
				t.Fatalf("Run returned %v while waiting for %s", err, what)
			default:
			}
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	// Still failing: the retry after the backoff fails again and Run goes on.
	clock.Set(at(11, 0).Add(2 * time.Second))
	waitFor("a reported failure", func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, r := range reported {
			if r.Schedule == "bad" && r.Err != nil {
				return true
			}
		}
		return false
	})
	submit.fail.Store(false)
	clock.Set(at(11, 0).Add(30 * time.Second))
	waitFor("the recovered schedule to fire", func() bool { return jobs(t, database) == 2 })
	clock.Set(at(12, 0))
	waitFor("both schedules to fire again", func() bool { return jobs(t, database) == 4 })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v", err)
	}
}

// flaky fails every store transaction while fail is set.
type flaky struct {
	store.Database
	fail atomic.Bool
}

func (f *flaky) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	if f.fail.Load() {
		return errors.New("disk full")
	}
	return f.Database.WithTx(ctx, fn)
}

func storedCursor(t *testing.T, database store.Database, name string) time.Time {
	t.Helper()
	var nanos int64
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `SELECT last_occurrence FROM cron_cursors WHERE name = ?`, name).Scan(&nanos)
	}); err != nil {
		t.Fatal(err)
	}
	return time.Unix(0, nanos).UTC()
}

// TestFailedCursorWriteIsRetried: a tick whose cursor transaction fails
// reports it, and the next tick writes the cursor even with nothing due.
func TestFailedCursorWriteIsRetried(t *testing.T) {
	database, queue := openQueue(t)
	store := &flaky{Database: database}
	clock := newClock(at(10, 30))
	s, err := cron.New(context.Background(), store, queue, queue, clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Add(context.Background(), schedule("hourly", "0 * * * *")); err != nil {
		t.Fatal(err)
	}
	clock.Set(at(11, 0))
	store.fail.Store(true)
	results, err := s.Tick(context.Background())
	if err == nil || len(results) != 1 || len(results[0].Submitted) != 1 {
		t.Fatalf("failing write: %+v err=%v", results, err)
	}
	store.fail.Store(false)
	if got := storedCursor(t, database, "hourly"); !got.Equal(at(10, 30)) {
		t.Fatalf("cursor written despite the failure: %v", got)
	}
	clock.Set(at(11, 5))
	if results, err := s.Tick(context.Background()); err != nil || len(results) != 0 {
		t.Fatalf("retry tick: %+v err=%v", results, err)
	}
	if got := storedCursor(t, database, "hourly"); !got.Equal(at(11, 0)) {
		t.Fatalf("cursor after the retry: %v, want 11:00", got)
	}
}

// TestDefinitionIgnoresFormatting: whitespace, key order and macros do not
// change a definition, the normalized payload is what is submitted, and a
// changed schema is a different definition.
func TestDefinitionIgnoresFormatting(t *testing.T) {
	database, queue := openQueue(t)
	clock := newClock(at(10, 30))
	s, err := cron.New(context.Background(), database, queue, queue, clock)
	if err != nil {
		t.Fatal(err)
	}
	first := schedule("hourly", "0 * * * *")
	first.InputSchema = []byte(`{"type":"object","properties":{"report":{"type":"string"},"copies":{"type":"integer"}},"required":["report"]}`)
	first.Payload = json.RawMessage(`{ "report" : "daily", "copies" : 2 }`)
	if _, err := s.Add(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	same := schedule("hourly", "@hourly")
	same.InputSchema = []byte(`{"required":["report"],"properties":{"copies":{"type":"integer"},"report":{"type":"string"}},"type":"object"}`)
	same.Payload = json.RawMessage(`{"copies":2,"report":"daily"}`)
	if added, err := s.Add(context.Background(), same); err != nil || added {
		t.Fatalf("reformatted definition: added=%v err=%v", added, err)
	}
	changed := same
	changed.InputSchema = []byte(`{"type":"object","properties":{"report":{"type":"string"},"copies":{"type":"integer"}},"required":["report"],"additionalProperties":false}`)
	if _, err := s.Add(context.Background(), changed); !errors.Is(err, cron.ErrConflict) {
		t.Fatalf("changed schema: %v", err)
	}
	clock.Set(at(11, 0))
	tick(t, s)
	job, err := queue.Get(context.Background(), cron.SubmissionKey("hourly", at(11, 0)))
	if err != nil {
		t.Fatal(err)
	}
	if string(job.Payload) != `{"copies":2,"report":"daily"}` {
		t.Fatalf("submitted payload %s, want the normalized form", job.Payload)
	}
}

// TestFailedOnTimeOccurrenceStaysOnTime: a store outage longer than
// LateAfter, retried with backoff, must not turn the occurrence that was on
// time when it first failed into a dropped late one.
func TestFailedOnTimeOccurrenceStaysOnTime(t *testing.T) {
	database, queue := openQueue(t)
	clock := newClock(at(10, 30))
	submit := &failing{inner: queue, names: []string{"daily"}}
	submit.fail.Store(true)
	s, err := cron.New(context.Background(), database, submit, queue, clock)
	if err != nil {
		t.Fatal(err)
	}
	sched := schedule("daily", "0 11 * * *")
	sched.MaxCatchUp = cron.NoCatchUp
	if _, err := s.Add(context.Background(), sched); err != nil {
		t.Fatal(err)
	}
	// The ticks Run makes while backing off: 1, 2, 4, 8, 16 and 32 s.
	for _, offset := range []time.Duration{0, 1, 3, 7, 15, 31, 63} {
		clock.Set(at(11, 0).Add(offset * time.Second))
		results, err := s.Tick(context.Background())
		if err == nil || len(results) != 1 || results[0].Err == nil || results[0].Missed != 0 {
			t.Fatalf("store down, %ds after the occurrence: %+v err=%v", offset, results, err)
		}
	}
	submit.fail.Store(false)
	clock.Set(at(11, 2).Add(7 * time.Second))
	result := tick(t, s)
	if fmt.Sprint(times(result.Submitted)) != "[11:00]" || result.Missed != 0 || jobs(t, database) != 1 {
		t.Fatalf("after the store recovered: %+v jobs=%d", result, jobs(t, database))
	}
	clock.Set(at(11, 3))
	if result := tick(t, s); len(result.Submitted)+len(result.Duplicates) != 0 {
		t.Fatalf("handled again: %+v", result)
	}
}

// TestLateAfterIsInclusive: an occurrence exactly LateAfter old is on time;
// one a nanosecond older is late.
func TestLateAfterIsInclusive(t *testing.T) {
	database, queue := openQueue(t)
	clock := newClock(at(10, 30))
	s, err := cron.New(context.Background(), database, queue, queue, clock)
	if err != nil {
		t.Fatal(err)
	}
	sched := schedule("hourly", "0 * * * *")
	sched.MaxCatchUp = cron.NoCatchUp
	if _, err := s.Add(context.Background(), sched); err != nil {
		t.Fatal(err)
	}
	clock.Set(at(11, 0).Add(cron.LateAfter))
	if result := tick(t, s); fmt.Sprint(times(result.Submitted)) != "[11:00]" || result.Missed != 0 {
		t.Fatalf("exactly LateAfter old: %+v", result)
	}
	clock.Set(at(12, 0).Add(cron.LateAfter + time.Nanosecond))
	if result := tick(t, s); len(result.Submitted) != 0 || result.Missed != 1 {
		t.Fatalf("a nanosecond later: %+v", result)
	}
}

// TestAddWaitsForACursorWrite: a registration made while a tick holds the
// store's write lock (its cursor write) waits for it and succeeds. Reading
// before writing would fail: the read takes a snapshot that the tick's
// commit makes stale, and SQLite refuses to upgrade it (SQLITE_BUSY).
func TestAddWaitsForACursorWrite(t *testing.T) {
	database, queue := openQueue(t)
	clock := newClock(at(10, 30))
	s, err := cron.New(context.Background(), database, queue, queue, clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Add(context.Background(), schedule("base", "* * * * *")); err != nil {
		t.Fatal(err)
	}
	holding := make(chan struct{})
	held := make(chan error, 1)
	const hold = 300 * time.Millisecond
	go func() {
		held <- database.WithTx(context.Background(), func(tx *sql.Tx) error {
			// A cursor write, as a tick's flush makes it.
			if _, err := tx.ExecContext(context.Background(), `UPDATE cron_cursors SET updated_at = updated_at + 1`); err != nil {
				return err
			}
			close(holding)
			time.Sleep(hold)
			return nil
		})
	}()
	<-holding
	began := time.Now()
	_, err = s.Add(context.Background(), schedule("late", "0 * * * *"))
	waited := time.Since(began)
	if err := <-held; err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatalf("a registration during a cursor write failed after %v: %v", waited, err)
	}
	if waited < hold/2 {
		t.Fatalf("the registration did not wait for the cursor write (%v)", waited)
	}
}

// TestRunDoesNotWakeItself: Run's own ticks must not wake it (only host
// ticks and registrations do), or it would spin.
func TestRunDoesNotWakeItself(t *testing.T) {
	database, queue := openQueue(t)
	clock := newClock(at(10, 30))
	s, err := cron.New(context.Background(), database, queue, queue, clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Add(context.Background(), schedule("hourly", "0 * * * *")); err != nil {
		t.Fatal(err)
	}
	// The clock reads behind the cursor, so every tick reports a result.
	clock.Set(at(10, 0))
	var reports atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, func([]cron.Result, error) { reports.Add(1) }) }()
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done
	// The first tick, and one for the wake the registration left.
	if n := reports.Load(); n > 2 {
		t.Fatalf("Run ticked %d times in 100ms with nothing due", n)
	}
}
