package worker

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/observe"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/trigger"
)

// Retention of finished jobs (#290). Every marker below is synthetic.

// retentionClock is a settable clock shared by a queue and its test.
type retentionClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *retentionClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *retentionClock) Set(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = at
}

var retentionStart = time.Unix(1_000_000, 0).UTC()

// markedJob is a job whose every application-supplied field carries its own
// synthetic marker, so a byte search can tell what survived.
type markedJob struct {
	name                                          string
	key, payload, principal, traceID, state, fail string
}

func marked(name string) markedJob {
	upper := strings.ToUpper(name)
	return markedJob{
		name:      name,
		key:       "SYNTHETIC-290-KEY-" + upper,
		payload:   "SYNTHETIC-290-PAYLOAD-" + upper,
		principal: "SYNTHETIC-290-PRINCIPAL-" + upper,
		// The trace id is hex, so its marker is a hex run of its own.
		traceID: digest([]byte("synthetic-290-trace-" + name))[:32],
		state:   "s290=synthetictrace" + strings.ToLower(strings.ReplaceAll(name, "-", "")),
		fail:    "SYNTHETIC-290-ERROR-" + upper,
	}
}

// markers lists what the job's record holds: payload, principal, request
// key, trace context and, for a failed job, its error text.
func (m markedJob) markers() []string {
	return []string{m.payload, m.principal, m.key, m.traceID, m.state, m.fail}
}

func (m markedJob) request(t *testing.T) EnqueueRequest {
	t.Helper()
	// The payload repeats its marker past a page, so part of it lives on
	// overflow pages too.
	payload, err := json.Marshal(map[string]string{"note": m.payload, "long": strings.Repeat(m.payload+" ", 300)})
	if err != nil {
		t.Fatal(err)
	}
	return EnqueueRequest{
		RequestKey: m.key, Kind: "retention.test", Payload: payload, MaxAttempts: 1,
		Principal: trigger.Principal{ID: m.principal, Roles: []string{"synthetic"}},
		Trace:     parent(t, "00-"+m.traceID+"-00f067aa0ba902b7-01", m.state),
	}
}

type retentionRig struct {
	path     string
	database store.Database
	clock    *retentionClock
	queue    *Queue
}

func newRetentionRig(t *testing.T, opts ...Option) *retentionRig {
	t.Helper()
	path := filepath.Join(t.TempDir(), "worker.db")
	database := openSQLite(t, path)
	clock := &retentionClock{now: retentionStart}
	queue, err := New(context.Background(), database, clock.Now, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return &retentionRig{path: path, database: database, clock: clock, queue: queue}
}

// finish enqueues the job and processes it to completion, or to dead with
// its error marker, at the rig's current time.
func (r *retentionRig) finish(t *testing.T, job markedJob, state string) {
	t.Helper()
	if result, err := r.queue.Enqueue(context.Background(), job.request(t)); err != nil || !result.Accepted {
		t.Fatalf("enqueue %s: %+v %v", job.name, result, err)
	}
	processed, err := r.queue.ProcessOnce(context.Background(), func(_ context.Context, _ Tx, claimed Job) error {
		if claimed.RequestKey != job.key {
			return fmt.Errorf("claimed %s, want %s", claimed.RequestKey, job.key)
		}
		if state == StateDead {
			return &HandlerError{Message: job.fail}
		}
		return nil
	})
	if err != nil || !processed {
		t.Fatalf("process %s: processed=%v err=%v", job.name, processed, err)
	}
	if got, err := r.queue.Get(context.Background(), job.key); err != nil || got.State != state {
		t.Fatalf("%s is %+v (%v); want %s", job.name, got, err, state)
	}
}

// pending enqueues the job and leaves it pending.
func (r *retentionRig) pending(t *testing.T, job markedJob) {
	t.Helper()
	if result, err := r.queue.Enqueue(context.Background(), job.request(t)); err != nil || !result.Accepted {
		t.Fatalf("enqueue %s: %+v %v", job.name, result, err)
	}
}

// exec runs one statement in a write transaction of its own.
func (r *retentionRig) exec(t *testing.T, query string, args ...any) {
	t.Helper()
	if err := r.database.WithTx(store.Writer(context.Background()), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(context.Background(), query, args...)
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

// processing leaves the job claimed with its lease until leaseUntil, as a
// worker that started the attempt and then died leaves it (#245).
func (r *retentionRig) processing(t *testing.T, job markedJob, leaseUntil time.Time) {
	t.Helper()
	r.pending(t, job)
	r.exec(t, `UPDATE worker_jobs SET state = ?, attempt = 1, lease_until = ? WHERE request_key = ?`, StateProcessing, leaseUntil.UnixNano(), job.key)
}

// checkpoint writes the log back into the database file, so the markers are
// in the main file before compaction and not only in the log.
func (r *retentionRig) checkpoint(t *testing.T) {
	t.Helper()
	purger, ok := store.PurgerOf(r.database)
	if !ok {
		t.Fatal("the SQLite store must purge")
	}
	if err := purger.PurgeLog(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// filesHolding reports which of the database's files hold marker.
func filesHolding(t *testing.T, path, marker string) []string {
	t.Helper()
	var found []string
	for _, file := range []string{path, path + "-wal", path + "-shm"} {
		data, err := os.ReadFile(file)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte(marker)) {
			found = append(found, filepath.Base(file))
		}
	}
	return found
}

// rowDump reads every column of the job's row, or "" without one.
func rowDump(t *testing.T, database store.Database, requestKey string) string {
	t.Helper()
	var dump string
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		err := tx.QueryRowContext(context.Background(), `SELECT job_id || '|' || request_key || '|' || kind || '|' || hex(payload_json) || '|' || payload_digest || '|' || attempt || '|' || deferrals || '|' ||
			principal_json || '|' || max_attempts || '|' || state || '|' || available_at || '|' || COALESCE(lease_until, 'null') || '|' || error_text || '|' || created_at || '|' || updated_at || '|' || enqueue_seq || '|' || traceparent || '|' || tracestate
			FROM worker_jobs WHERE request_key = ?`, requestKey).Scan(&dump)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return dump
}

func tableCount(t *testing.T, database store.Database, table string) int {
	t.Helper()
	var count int
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM `+table).Scan(&count)
	}); err != nil {
		t.Fatal(err)
	}
	return count
}

// TestCompactionErasesExpiredJobsFromTheFiles: after Compact, no byte of an
// expired completed or dead job's payload, principal, request key, trace
// context or error text is left in the database, its log or its
// shared-memory file; the pending and processing jobs beside them keep
// every byte.
func TestCompactionErasesExpiredJobsFromTheFiles(t *testing.T) {
	r := newRetentionRig(t)
	completed, dead := marked("completed"), marked("dead")
	keptPending, keptProcessing := marked("kept-pending"), marked("kept-processing")
	r.finish(t, completed, StateCompleted)
	r.finish(t, dead, StateDead)
	r.pending(t, keptPending)
	r.processing(t, keptProcessing, retentionStart.Add(time.Hour))
	r.clock.Set(retentionStart.Add(48 * time.Hour))
	r.checkpoint(t)
	for _, job := range []markedJob{completed, dead, keptPending, keptProcessing} {
		markers := job.markers()
		if job != dead {
			markers = markers[:len(markers)-1] // only the dead job failed
		}
		for _, marker := range markers {
			if found := filesHolding(t, r.path, marker); !slices.Contains(found, "worker.db") {
				t.Fatalf("fixture: before compaction %s's marker %q must be in the database file, found in %v", job.name, marker, found)
			}
		}
	}
	cutoff := r.clock.Now().Add(-24 * time.Hour)
	report, err := r.queue.Compact(context.Background(), Retention{Completed: cutoff, Dead: cutoff})
	if err != nil {
		t.Fatal(err)
	}
	if report.Compacted != 2 || report.Held != 0 || !report.LogPurged || report.PurgePending {
		t.Errorf("report %+v; want both expired jobs compacted and the log purged", report)
	}
	for _, job := range []markedJob{completed, dead} {
		for _, marker := range job.markers() {
			if found := filesHolding(t, r.path, marker); len(found) > 0 {
				t.Errorf("expired %s job's marker %q survives in %v", job.name, marker, found)
			}
		}
		if dump := rowDump(t, r.database, job.key); dump != "" {
			t.Errorf("expired %s job still has its row: %s", job.name, dump)
		}
	}
	for _, job := range []markedJob{keptPending, keptProcessing} {
		for _, marker := range job.markers()[:5] {
			if found := filesHolding(t, r.path, marker); len(found) == 0 {
				t.Errorf("%s job lost its marker %q", job.name, marker)
			}
		}
	}
}

// TestCompactionNeverTouchesUnfinishedJobs: pending, waiting, processing and
// stalled jobs keep every column, however old, under any cutoff, and are
// then delivered with their content.
func TestCompactionNeverTouchesUnfinishedJobs(t *testing.T) {
	r := newRetentionRig(t)
	pending, waiting := marked("pending"), marked("waiting")
	active, stalled := marked("active"), marked("stalled")
	r.pending(t, pending)
	r.pending(t, waiting)
	r.exec(t, `UPDATE worker_jobs SET attempt = 1, available_at = ?, error_text = 'retry later' WHERE request_key = ?`, retentionStart.Add(1000*time.Hour).UnixNano(), waiting.key)
	r.processing(t, active, retentionStart.Add(1000*time.Hour))
	r.processing(t, stalled, retentionStart.Add(30*time.Second))
	unfinished := []markedJob{pending, waiting, active, stalled}
	before := map[string]string{}
	for _, job := range unfinished {
		before[job.key] = rowDump(t, r.database, job.key)
	}
	r.clock.Set(retentionStart.Add(500 * time.Hour))
	future := retentionStart.Add(100_000 * time.Hour)
	report, err := r.queue.Compact(context.Background(), Retention{Completed: future, Dead: future, Tombstones: future})
	if err != nil {
		t.Fatal(err)
	}
	if report.Compacted != 0 || report.Held != 0 || report.ExpiredTombstones != 0 {
		t.Fatalf("report %+v; want nothing erased", report)
	}
	for _, job := range unfinished {
		if after := rowDump(t, r.database, job.key); after != before[job.key] {
			t.Errorf("%s job changed:\nbefore %s\nafter  %s", job.name, before[job.key], after)
		}
	}
	want := pending.request(t)
	processed, err := r.queue.ProcessOnce(context.Background(), func(_ context.Context, _ Tx, job Job) error {
		if job.RequestKey != stalled.key && job.RequestKey != pending.key {
			return fmt.Errorf("claimed %s", job.RequestKey)
		}
		if job.RequestKey == pending.key && (!bytes.Equal(job.Payload, want.Payload) || job.Principal.ID != pending.principal || job.Trace != want.Trace) {
			return fmt.Errorf("pending job lost content: %+v", job)
		}
		return nil
	})
	if err != nil || !processed {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
}

// TestDuplicateOfACompactedJobIsStillDeduplicated: the tombstone keeps ADR
// 0006's contract after the job's content is gone. Within the dedupe window
// (until Retention.Tombstones passes the job's finish), the same key, kind,
// payload and principal is a duplicate (accepted=false, nothing runs), with
// any trace; other content under the key conflicts. Once the window ends the
// key is new again.
func TestDuplicateOfACompactedJobIsStillDeduplicated(t *testing.T) {
	r := newRetentionRig(t)
	done, failed, later := marked("done"), marked("failed"), marked("later")
	r.finish(t, done, StateCompleted)
	r.finish(t, failed, StateDead)
	r.clock.Set(retentionStart.Add(10 * time.Hour))
	r.finish(t, later, StateCompleted)
	r.clock.Set(retentionStart.Add(48 * time.Hour))
	now := r.clock.Now()
	if report, err := r.queue.Compact(context.Background(), Retention{Completed: now, Dead: now}); err != nil || report.Compacted != 3 {
		t.Fatalf("report %+v err=%v", report, err)
	}
	for _, job := range []struct {
		marked markedJob
		state  string
	}{{done, StateCompleted}, {failed, StateDead}, {later, StateCompleted}} {
		request := job.marked.request(t)
		result, err := r.queue.Enqueue(context.Background(), request)
		if err != nil || result.Accepted || !result.Job.Compacted || result.Job.State != job.state || result.Job.ID != "job:"+digest([]byte(request.RequestKey))[:32] ||
			result.Job.Payload != nil || result.Job.Principal.ID != "" || result.Job.Trace != (observe.TraceContext{}) || result.Job.Error != "" {
			t.Fatalf("duplicate of compacted %s: %+v err=%v", job.marked.name, result, err)
		}
		accepted, err := r.queue.Submit(context.Background(), trigger.Submission{Key: request.RequestKey, Kind: request.Kind, Payload: request.Payload, Principal: request.Principal})
		if err != nil || accepted {
			t.Fatalf("submitted duplicate of %s: accepted=%v err=%v", job.marked.name, accepted, err)
		}
		retraced := request
		retraced.Trace = parent(t, traceB, "")
		if result, err := r.queue.Enqueue(context.Background(), retraced); err != nil || result.Accepted {
			t.Fatalf("duplicate with another trace: %+v err=%v", result, err)
		}
		got, err := r.queue.Get(context.Background(), request.RequestKey)
		if err != nil || !got.Compacted || got.State != job.state {
			t.Fatalf("get %s: %+v err=%v", job.marked.name, got, err)
		}
		if settled, err := r.queue.Settled(context.Background(), request.RequestKey); err != nil || !settled {
			t.Fatalf("settled %s=%v err=%v", job.marked.name, settled, err)
		}
		for name, change := range map[string]func(*EnqueueRequest){
			"payload":   func(e *EnqueueRequest) { e.Payload = []byte(`{"note":"other"}`) },
			"principal": func(e *EnqueueRequest) { e.Principal = trigger.Principal{ID: "someone-else"} },
			"no principal": func(e *EnqueueRequest) {
				e.Principal = trigger.Principal{}
			},
			"kind": func(e *EnqueueRequest) { e.Kind = "other.kind" },
		} {
			reused := request
			change(&reused)
			if _, err := r.queue.Enqueue(context.Background(), reused); !errors.Is(err, ErrRequestConflict) || !errors.Is(err, trigger.ErrConflict) {
				t.Fatalf("%s reused with another %s: err=%v; want a conflict", job.marked.name, name, err)
			}
		}
	}
	if rows := tableCount(t, r.database, "worker_jobs"); rows != 0 {
		t.Fatalf("duplicates of compacted jobs created %d rows", rows)
	}
	if processed, err := r.queue.ProcessOnce(context.Background(), func(context.Context, Tx, Job) error { return errors.New("must not run") }); err != nil || processed {
		t.Fatalf("a duplicate of a compacted job ran: processed=%v err=%v", processed, err)
	}
	// The window ends for jobs that finished before the tombstone cutoff:
	// done and failed finished at the start, later 10 hours on.
	report, err := r.queue.Compact(context.Background(), Retention{Tombstones: retentionStart.Add(time.Hour)})
	if err != nil || report.ExpiredTombstones != 2 {
		t.Fatalf("report %+v err=%v; want the two older tombstones expired", report, err)
	}
	if result, err := r.queue.Enqueue(context.Background(), later.request(t)); err != nil || result.Accepted || !result.Job.Compacted {
		t.Fatalf("within its window later is still a duplicate: %+v err=%v", result, err)
	}
	if _, err := r.queue.Get(context.Background(), done.key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get after the window: %v", err)
	}
	result, err := r.queue.Enqueue(context.Background(), done.request(t))
	if err != nil || !result.Accepted || result.Job.Compacted || result.Job.State != StatePending {
		t.Fatalf("after its window done is a new job: %+v err=%v", result, err)
	}
}

// TestLegalHoldAndMinimumRetentionKeepJobs: the queue's legal hold keeps a
// job with all its content, a hold that panics keeps every job (fails
// closed), and no cutoff erases a job younger than the minimum retention.
func TestLegalHoldAndMinimumRetentionKeepJobs(t *testing.T) {
	t.Run("hold", func(t *testing.T) {
		var seen []RetainedJob
		r := newRetentionRig(t, WithRetentionHold(func(job RetainedJob) bool {
			seen = append(seen, job)
			return strings.Contains(job.Principal.ID, "HELD")
		}))
		held, free, heldDead := marked("held"), marked("free"), marked("held-dead")
		r.finish(t, held, StateCompleted)
		r.finish(t, free, StateCompleted)
		r.finish(t, heldDead, StateDead)
		before := rowDump(t, r.database, held.key)
		r.clock.Set(retentionStart.Add(48 * time.Hour))
		now := r.clock.Now()
		// A batch of one: held jobs must not stop the cursor.
		report, err := r.queue.Compact(context.Background(), Retention{Completed: now, Dead: now, Batch: 1})
		if err != nil || report.Compacted != 1 || report.Held != 2 {
			t.Fatalf("report %+v err=%v; want free compacted and both held jobs kept", report, err)
		}
		if after := rowDump(t, r.database, held.key); after != before {
			t.Fatalf("held job changed:\nbefore %s\nafter  %s", before, after)
		}
		if dump := rowDump(t, r.database, heldDead.key); !strings.Contains(dump, heldDead.fail) {
			t.Fatalf("held dead job lost its content: %s", dump)
		}
		if dump := rowDump(t, r.database, free.key); dump != "" {
			t.Fatalf("free job kept: %s", dump)
		}
		for _, marker := range held.markers()[:5] {
			if found := filesHolding(t, r.path, marker); len(found) == 0 {
				t.Fatalf("held job's marker %q left the files", marker)
			}
		}
		if len(seen) != 3 || seen[0].RequestKey != held.key || seen[0].State != StateCompleted || seen[0].Kind != "retention.test" ||
			!seen[0].FinishedAt.Equal(retentionStart) || seen[2].State != StateDead || seen[2].Principal.ID != heldDead.principal {
			t.Fatalf("hold saw %+v", seen)
		}
	})
	t.Run("panicking hold fails closed", func(t *testing.T) {
		r := newRetentionRig(t, WithRetentionHold(func(RetainedJob) bool { panic("synthetic hold failure") }))
		first, second := marked("first"), marked("second")
		r.finish(t, first, StateCompleted)
		r.finish(t, second, StateDead)
		r.clock.Set(retentionStart.Add(48 * time.Hour))
		now := r.clock.Now()
		report, err := r.queue.Compact(context.Background(), Retention{Completed: now, Dead: now})
		if err != nil || report.Compacted != 0 || report.Held != 2 {
			t.Fatalf("report %+v err=%v; want both jobs kept", report, err)
		}
		for _, job := range []markedJob{first, second} {
			if dump := rowDump(t, r.database, job.key); !strings.Contains(dump, job.principal) {
				t.Fatalf("%s lost its content: %q", job.name, dump)
			}
		}
	})
	t.Run("minimum retention", func(t *testing.T) {
		r := newRetentionRig(t, WithMinRetention(72*time.Hour))
		old, young := marked("old"), marked("young")
		r.clock.Set(retentionStart.Add(-52 * time.Hour))
		r.finish(t, old, StateCompleted)
		r.clock.Set(retentionStart)
		r.finish(t, young, StateDead)
		r.clock.Set(retentionStart.Add(48 * time.Hour))
		// Old finished 100 hours ago, young 48: the cutoff now asks for
		// both, and the legal minimum keeps young.
		now := r.clock.Now()
		report, err := r.queue.Compact(context.Background(), Retention{Completed: now, Dead: now})
		if err != nil || report.Compacted != 1 {
			t.Fatalf("report %+v err=%v", report, err)
		}
		if rowDump(t, r.database, old.key) != "" || !strings.Contains(rowDump(t, r.database, young.key), young.principal) {
			t.Fatal("want old erased and young kept")
		}
	})
	t.Run("negative minimum refused", func(t *testing.T) {
		if _, err := New(context.Background(), openSQLite(t, filepath.Join(t.TempDir(), "worker.db")), nil, WithMinRetention(-time.Second)); err == nil {
			t.Fatal("want a negative minimum retention refused")
		}
	})
}

// TestCensusIsCorrectAfterCompaction: compaction leaves the census's live
// counts and backlog lag exactly as they were (#105, ADR 0022), and dead
// letters count those still retained for an operator: a compacted dead job
// is no longer one, a held one still is.
func TestCensusIsCorrectAfterCompaction(t *testing.T) {
	r := newRetentionRig(t, WithRetentionHold(func(job RetainedJob) bool { return strings.Contains(job.Principal.ID, "HELD") }))
	r.finish(t, marked("done-1"), StateCompleted)
	r.finish(t, marked("done-2"), StateCompleted)
	r.finish(t, marked("dead-1"), StateDead)
	r.finish(t, marked("dead-2"), StateDead)
	r.finish(t, marked("held-dead"), StateDead)
	r.pending(t, marked("pending"))
	r.pending(t, marked("waiting"))
	r.exec(t, `UPDATE worker_jobs SET available_at = ? WHERE request_key = ?`, retentionStart.Add(1000*time.Hour).UnixNano(), marked("waiting").key)
	r.processing(t, marked("active"), retentionStart.Add(1000*time.Hour))
	r.processing(t, marked("stalled"), retentionStart.Add(30*time.Second))
	r.clock.Set(retentionStart.Add(48 * time.Hour))
	before, beforeText := censusText(t, r.queue)
	if before.Pending != 1 || before.Waiting != 1 || before.Active != 1 || before.Stalled != 1 || before.DeadLetters != 3 || before.OldestPending != 48*time.Hour {
		t.Fatalf("fixture census %+v", before)
	}
	now := r.clock.Now()
	// Dead letters have their own cutoff: one for completed jobs keeps them.
	report, err := r.queue.Compact(context.Background(), Retention{Completed: now})
	if err != nil || report.Compacted != 2 || report.Held != 0 {
		t.Fatalf("report %+v err=%v", report, err)
	}
	if work, _ := censusText(t, r.queue); work != before {
		t.Fatalf("census after compacting completed jobs %+v; want %+v", work, before)
	}
	report, err = r.queue.Compact(context.Background(), Retention{Completed: now, Dead: now})
	if err != nil || report.Compacted != 2 || report.Held != 1 {
		t.Fatalf("report %+v err=%v", report, err)
	}
	after, afterText := censusText(t, r.queue)
	want := before
	want.DeadLetters = 1
	if after != want {
		t.Fatalf("census after compaction %+v; want %+v", after, want)
	}
	if !strings.Contains(beforeText, `blok_work_dead_letters{blok_source="orders"} 3`) || !strings.Contains(afterText, `blok_work_dead_letters{blok_source="orders"} 1`) {
		t.Fatalf("dead-letter metric:\nbefore\n%s\nafter\n%s", beforeText, afterText)
	}
}

// batchDatabase records, for each marked write transaction, how many job
// rows and tombstones it removed or added.
type batchDatabase struct {
	store.Database
	purger      store.Purger
	mu          sync.Mutex
	jobs, tombs int
	deltas      [][2]int
}

func (d *batchDatabase) PurgeLog(ctx context.Context) error  { return d.purger.PurgeLog(ctx) }
func (d *batchDatabase) PurgeFree(ctx context.Context) error { return d.purger.PurgeFree(ctx) }

func (d *batchDatabase) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	if !store.IsWriter(ctx) {
		return d.Database.WithTx(ctx, fn)
	}
	return d.Database.WithTx(ctx, func(tx *sql.Tx) error {
		if err := fn(tx); err != nil {
			return err
		}
		var jobs, tombs int
		if err := tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM worker_jobs), (SELECT COUNT(*) FROM worker_compacted)`).Scan(&jobs, &tombs); err != nil {
			return err
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		d.deltas = append(d.deltas, [2]int{d.jobs - jobs, d.tombs - tombs})
		d.jobs, d.tombs = jobs, tombs
		return nil
	})
}

// TestCompactionIsBatched: no write transaction erases more than
// Retention.Batch jobs or tombstones, so a large backlog is erased in
// bounded steps with the write lock released between them; and the
// candidates are read through the finished-job index, so a batch does not
// scan the history before it.
func TestCompactionIsBatched(t *testing.T) {
	r := newRetentionRig(t)
	purger, _ := store.PurgerOf(r.database)
	recorder := &batchDatabase{Database: r.database, purger: purger}
	queue, err := New(context.Background(), recorder, r.clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	const jobs = 10
	for i := range jobs {
		r.finish(t, marked(fmt.Sprintf("batch-%02d", i)), StateCompleted)
	}
	r.clock.Set(retentionStart.Add(48 * time.Hour))
	recorder.jobs, recorder.deltas = jobs, nil
	now := r.clock.Now()
	report, err := queue.Compact(context.Background(), Retention{Completed: now, Batch: 3})
	if err != nil || report.Compacted != jobs || report.Batches != 4 {
		t.Fatalf("report %+v err=%v; want %d jobs in 4 batches", report, err, jobs)
	}
	compacted := 0
	for _, delta := range recorder.deltas {
		if delta[0] > 3 {
			t.Fatalf("one transaction erased %d jobs; batches %v", delta[0], recorder.deltas)
		}
		compacted += delta[0]
	}
	if compacted != jobs {
		t.Fatalf("batches %v erased %d jobs", recorder.deltas, compacted)
	}
	recorder.deltas = nil
	report, err = queue.Compact(context.Background(), Retention{Tombstones: now, Batch: 3})
	if err != nil || report.ExpiredTombstones != jobs || report.Batches != 4 {
		t.Fatalf("report %+v err=%v; want %d tombstones in 4 batches", report, err, jobs)
	}
	for _, delta := range recorder.deltas {
		if delta[1] < -3 || delta[1] > 3 {
			t.Fatalf("one transaction expired %d tombstones; batches %v", delta[1], recorder.deltas)
		}
	}
	for _, batch := range []int{-1, MaxCompactBatch + 1} {
		if _, err := queue.Compact(context.Background(), Retention{Completed: now, Batch: batch}); err == nil {
			t.Fatalf("batch %d accepted", batch)
		}
	}
	var plan strings.Builder
	if err := r.database.WithTx(context.Background(), func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(context.Background(), `EXPLAIN QUERY PLAN `+compactionCandidates, StateCompleted, 1, 0, 0, "", 1)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				return err
			}
			plan.WriteString(detail + "\n")
		}
		return rows.Err()
	}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan.String(), "worker_jobs_finished") || strings.Contains(plan.String(), "SCAN worker_jobs\n") || strings.Contains(plan.String(), "TEMP B-TREE") {
		t.Fatalf("candidate plan does not read through the finished-job index:\n%s", plan.String())
	}
}

// TestCompactionWritesFirst: a compaction batch takes the write lock with
// its first statement, so it waits for a writer on another handle instead
// of failing busy on a stale snapshot (#176).
func TestCompactionWritesFirst(t *testing.T) {
	r := newRetentionRig(t)
	r.finish(t, marked("contended"), StateCompleted)
	r.clock.Set(retentionStart.Add(48 * time.Hour))
	other := openSQLite(t, r.path)
	holding, held := make(chan struct{}), make(chan error, 1)
	go func() {
		held <- other.WithTx(context.Background(), func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(context.Background(), `UPDATE worker_jobs SET enqueue_seq = enqueue_seq`); err != nil {
				return err
			}
			close(holding)
			time.Sleep(200 * time.Millisecond)
			return nil
		})
	}()
	<-holding
	report, err := r.queue.Compact(context.Background(), Retention{Completed: r.clock.Now()})
	if heldErr := <-held; heldErr != nil {
		t.Fatal(heldErr)
	}
	if err != nil || report.Compacted != 1 {
		t.Fatalf("report %+v err=%v; want the compaction to wait for the other writer", report, err)
	}
}

// TestBlockedLogPurgeIsRetried: a reader that keeps the log in use blocks
// the purge without failing the compaction; the debt is persisted and the
// next Compact, in a reopened queue and erasing nothing, purges the log
// (the #281 pending-purge pattern).
func TestBlockedLogPurgeIsRetried(t *testing.T) {
	r := newRetentionRig(t)
	gone := marked("gone")
	r.finish(t, gone, StateCompleted)
	r.clock.Set(retentionStart.Add(48 * time.Hour))
	reader := openSQLite(t, r.path)
	reading, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- reader.WithTx(context.Background(), func(tx *sql.Tx) error {
			var count int
			if err := tx.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM worker_jobs`).Scan(&count); err != nil {
				return err
			}
			close(reading)
			<-release
			return nil
		})
	}()
	<-reading
	report, err := r.queue.Compact(context.Background(), Retention{Completed: r.clock.Now()})
	close(release)
	if readErr := <-done; readErr != nil {
		t.Fatal(readErr)
	}
	if err != nil || report.Compacted != 1 || report.LogPurged || !report.PurgePending {
		t.Fatalf("report %+v err=%v; want the purge left pending", report, err)
	}
	if found := filesHolding(t, r.path, gone.payload); !slices.Contains(found, "worker.db-wal") {
		t.Fatalf("fixture: the blocked purge must leave the content in the log, found in %v", found)
	}
	queue, err := New(context.Background(), r.database, r.clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	report, err = queue.Compact(context.Background(), Retention{})
	if err != nil || report.Compacted != 0 || !report.LogPurged || report.PurgePending {
		t.Fatalf("report %+v err=%v; want the owed purge done", report, err)
	}
	for _, marker := range gone.markers()[:5] {
		if found := filesHolding(t, r.path, marker); len(found) > 0 {
			t.Fatalf("marker %q survives in %v", marker, found)
		}
	}
	if report, err = queue.Compact(context.Background(), Retention{}); err != nil || report.LogPurged || report.PurgePending {
		t.Fatalf("report %+v err=%v; want no purge once the debt is paid", report, err)
	}
}

// TestRetentionMigratesAQueueFromBeforeIt: a queue written before #290 gains
// the tombstone table, counters and index in place, idempotently, and its
// finished jobs can then be compacted.
func TestRetentionMigratesAQueueFromBeforeIt(t *testing.T) {
	database := openSQLite(t, filepath.Join(t.TempDir(), "worker.db"))
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		if err := createJobs(context.Background(), tx); err != nil {
			return err
		}
		_, err := tx.ExecContext(context.Background(), `INSERT INTO worker_jobs (job_id, request_key, kind, payload_json, payload_digest, max_attempts, state, available_at, created_at, updated_at)
			VALUES ('job:legacy', 'legacy', 'test', '{"note":"SYNTHETIC-290-LEGACY"}', 'digest', 3, 'completed', 0, 0, 0)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	clock := &retentionClock{now: retentionStart}
	for range 2 {
		if _, err := New(context.Background(), database, clock.Now); err != nil {
			t.Fatal(err)
		}
	}
	queue, err := New(context.Background(), database, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	if report, err := queue.Compact(context.Background(), Retention{Completed: clock.Now()}); err != nil || report.Compacted != 1 {
		t.Fatalf("report %+v err=%v", report, err)
	}
	if job, err := queue.Get(context.Background(), "legacy"); err != nil || !job.Compacted || job.ID != "job:legacy" || job.State != StateCompleted {
		t.Fatalf("legacy job %+v err=%v", job, err)
	}
}
