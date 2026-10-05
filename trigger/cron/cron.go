// Package cron submits durable work at scheduled wall-clock times.
//
// Each schedule is a cron expression evaluated in an IANA time zone with
// explicit daylight-saving policies. An occurrence is identified by the UTC
// instant it fires at and submitted through trigger.Submitter under
// "cron:<schedule>:<instant>", so a repeated tick, a restart or a clock that
// moves backwards never duplicates accepted work. A persisted cursor records
// the last handled occurrence; it is written after the submission commits, so
// a crash between the two can only repeat a submission, which deduplicates.
// One goroutine drives every schedule; nothing sleeps per schedule.
package cron

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/trigger"
)

// Declaration is the cron contract: occurrences are submitted durably, an
// occurrence that was not committed is submitted again after a restart, and
// application code produces every occurrence.
var Declaration = trigger.Declaration{Kind: trigger.Cron, Adapter: "trigger/cron", Completion: trigger.Durable, Disconnect: trigger.Redeliver, Authentication: trigger.TrustedProducer}

// OverlapPolicy decides what happens when an occurrence is due while the
// previous occurrence's work has not settled.
type OverlapPolicy string

const (
	// OverlapAllow submits regardless.
	OverlapAllow OverlapPolicy = "allow"
	// OverlapSkip drops the occurrence.
	OverlapSkip OverlapPolicy = "skip"
	// OverlapCoalesce holds the newest occurrence and submits it once the
	// previous work settles; older held occurrences are replaced.
	OverlapCoalesce OverlapPolicy = "coalesce"
)

const (
	// DefaultMaxCatchUp is the catch-up of a schedule that leaves
	// MaxCatchUp zero: the most recent late occurrence is submitted.
	DefaultMaxCatchUp = 1
	// NoCatchUp submits no late occurrence; only on-time ones fire.
	NoCatchUp       = -1
	MaxCatchUpLimit = 100
	// LateAfter is how long after its instant an occurrence is still on
	// time (inclusive). A later one is late: it was missed, typically during
	// downtime, and is subject to MaxCatchUp. An on-time occurrence whose
	// tick fails stays on time while it is retried.
	LateAfter = time.Minute
	// maxRetained bounds the on-time occurrences kept for retry while a
	// schedule keeps failing; older ones fall back to the late rule.
	maxRetained = 100
	// recheckInterval is how often held occurrences re-check whether the
	// previous work settled.
	recheckInterval = time.Second
	// missedCountCap bounds Result.Missed; a larger count is clamped to it.
	missedCountCap = 10000
	// maxBackoff bounds how long a failing schedule, or a failing cursor
	// write, waits before it is retried.
	maxBackoff = 5 * time.Minute
)

// ErrConflict refuses a schedule whose name is already bound, in this
// scheduler or in the store, to a different definition.
var ErrConflict = errors.New("cron: schedule name is bound to a different definition")

// Schedule is one cron binding.
type Schedule struct {
	Name     string
	Spec     string
	TimeZone string
	Kind     string
	// Payload is the input every occurrence submits; it is validated
	// against InputSchema when the schedule is added.
	Payload     json.RawMessage
	InputSchema []byte
	Principal   trigger.Principal
	Gap         GapPolicy
	Fold        FoldPolicy
	Overlap     OverlapPolicy
	// MaxCatchUp is how many of the most recent late occurrences (see
	// LateAfter) are submitted, for example after downtime; older ones are
	// counted and skipped. Zero selects DefaultMaxCatchUp; NoCatchUp
	// submits none. On-time occurrences are always handled.
	MaxCatchUp int
}

// Tracker reports whether the work submitted under a key has settled
// (finished or dead-lettered). Overlap policies other than allow need it.
type Tracker interface {
	Settled(ctx context.Context, key string) (bool, error)
}

// Clock is the scheduler's time source.
type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time                         { return time.Now() }
func (systemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Result reports what a tick did for one schedule.
type Result struct {
	Schedule   string
	Submitted  []time.Time
	Duplicates []time.Time
	Skipped    []time.Time
	Held       *time.Time
	// Missed counts late occurrences left out by MaxCatchUp. It is exact
	// up to 10 000 and clamped there.
	Missed int
	// Behind is set when the clock reads earlier than the schedule's
	// cursor, the last occurrence it handled: the clock was stepped
	// forward and corrected, or is now wrong. Nothing fires until the
	// clock passes the cursor again.
	Behind time.Duration
	// Err is the schedule's failure in this tick; it is retried with
	// backoff while the other schedules continue.
	Err error
}

// entry is one registered schedule. Its fields change only inside Tick,
// which tickMu serializes; next, hasNext, pending and retryAt are also read
// outside Tick, so they are written under mu.
type entry struct {
	schedule    Schedule
	occurrences Occurrences
	payload     json.RawMessage
	catchUp     int
	digest      string
	cursor      time.Time
	lastKey     string
	pending     *time.Time
	next        time.Time
	hasNext     bool
	failures    int
	retryAt     time.Time
	// retry holds on-time occurrences a failed tick could not handle; they
	// stay on time while retried, so backoff cannot make them late.
	retry []time.Time
}

type Scheduler struct {
	database store.Database
	submit   trigger.Submitter
	tracker  Tracker
	clock    Clock
	// tickMu serializes ticks; addMu serializes registrations. mu guards
	// the entry map, the fields of entry read outside Tick and the unwritten
	// cursor updates. No store transaction runs under mu.
	tickMu       sync.Mutex
	addMu        sync.Mutex
	mu           sync.Mutex
	entries      map[string]*entry
	dirty        map[*entry]cursorUpdate
	flushFails   int
	flushRetryAt time.Time
	wake         chan struct{}
}

var scheduleName = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,63}$`)

// New prepares the cursor table. tracker may be nil when no schedule uses an
// overlap policy other than allow.
func New(ctx context.Context, database store.Database, submit trigger.Submitter, tracker Tracker, clock Clock) (*Scheduler, error) {
	if database == nil || submit == nil {
		return nil, errors.New("cron: database and submitter are required")
	}
	if clock == nil {
		clock = systemClock{}
	}
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS cron_cursors (
			name TEXT PRIMARY KEY,
			digest TEXT NOT NULL,
			last_occurrence INTEGER NOT NULL,
			last_key TEXT NOT NULL DEFAULT '',
			pending INTEGER,
			updated_at INTEGER NOT NULL
		)`)
		return err
	}); err != nil {
		return nil, fmt.Errorf("cron: schema: %w", err)
	}
	return &Scheduler{database: database, submit: submit, tracker: tracker, clock: clock, entries: map[string]*entry{}, wake: make(chan struct{}, 1)}, nil
}

// SubmissionKey is the durable identity of an occurrence.
func SubmissionKey(name string, instant time.Time) string {
	return "cron:" + name + ":" + instant.UTC().Format(time.RFC3339)
}

// validate checks a schedule whose defaults are applied and returns its
// occurrences, its normalized payload and its definition digest. The digest
// covers the parsed expression, the normalized payload and the parsed
// schema, so formatting and key order do not change a definition.
func (s Schedule) validate() (*entry, error) {
	if !scheduleName.MatchString(s.Name) || s.Kind == "" || strings.TrimSpace(s.Principal.ID) == "" {
		return nil, fmt.Errorf("cron: schedule %q needs a name, kind and principal", s.Name)
	}
	spec, err := Parse(s.Spec)
	if err != nil {
		return nil, err
	}
	location, err := time.LoadLocation(s.TimeZone)
	if err != nil || s.TimeZone == "" || s.TimeZone == "Local" {
		return nil, fmt.Errorf("cron: schedule %s needs an explicit IANA time zone", s.Name)
	}
	if s.Gap != GapSkip && s.Gap != GapShift {
		return nil, fmt.Errorf("cron: schedule %s gap policy must be skip or shift", s.Name)
	}
	if s.Fold != FoldOnce && s.Fold != FoldTwice {
		return nil, fmt.Errorf("cron: schedule %s fold policy must be once or twice", s.Name)
	}
	if s.Overlap != OverlapAllow && s.Overlap != OverlapSkip && s.Overlap != OverlapCoalesce {
		return nil, fmt.Errorf("cron: schedule %s overlap policy must be allow, skip or coalesce", s.Name)
	}
	if s.MaxCatchUp < NoCatchUp || s.MaxCatchUp > MaxCatchUpLimit {
		return nil, fmt.Errorf("cron: schedule %s catch-up must be NoCatchUp or 0-%d", s.Name, MaxCatchUpLimit)
	}
	input, err := schema.Parse(s.InputSchema)
	if err != nil {
		return nil, fmt.Errorf("cron: schedule %s schema: %w", s.Name, err)
	}
	payload, err := input.Normalize(s.Payload)
	if err != nil {
		return nil, fmt.Errorf("%w: schedule %s payload: %v", trigger.ErrInvalidInput, s.Name, err)
	}
	definition, err := json.Marshal([]any{spec.String(), s.TimeZone, s.Kind, json.RawMessage(payload), input, s.Principal, s.Gap, s.Fold, s.Overlap, s.MaxCatchUp})
	if err != nil {
		return nil, fmt.Errorf("cron: schedule %s definition: %w", s.Name, err)
	}
	sum := sha256.Sum256(definition)
	catchUp := s.MaxCatchUp
	if catchUp == NoCatchUp {
		catchUp = 0
	}
	return &entry{schedule: s, occurrences: Occurrences{Spec: spec, Location: location, Gap: s.Gap, Fold: s.Fold}, payload: payload, catchUp: catchUp, digest: hex.EncodeToString(sum[:])}, nil
}

// Add validates and registers one schedule; see AddAll.
func (s *Scheduler) Add(ctx context.Context, schedule Schedule) (bool, error) {
	added, err := s.AddAll(ctx, []Schedule{schedule})
	if err != nil {
		return false, err
	}
	return added[0], nil
}

// AddAll validates and registers schedules in one store transaction. A
// schedule seen for the first time starts from now and does not backfill; a
// schedule already in the store resumes from its cursor. Adding the same
// definition again reports false; a different definition under a used name
// is refused with ErrConflict. Nothing is registered if any schedule fails.
func (s *Scheduler) AddAll(ctx context.Context, schedules []Schedule) ([]bool, error) {
	entries := make([]*entry, len(schedules))
	added := make([]bool, len(schedules))
	seen := map[string]string{}
	for i, schedule := range schedules {
		if schedule.MaxCatchUp == 0 {
			schedule.MaxCatchUp = DefaultMaxCatchUp
		}
		if schedule.Gap == "" {
			schedule.Gap = GapSkip
		}
		if schedule.Fold == "" {
			schedule.Fold = FoldOnce
		}
		if schedule.Overlap == "" {
			schedule.Overlap = OverlapAllow
		}
		e, err := schedule.validate()
		if err != nil {
			return nil, err
		}
		if schedule.Overlap != OverlapAllow && s.tracker == nil {
			return nil, fmt.Errorf("cron: schedule %s overlap policy %s needs a tracker", schedule.Name, schedule.Overlap)
		}
		if previous, ok := seen[schedule.Name]; ok && previous != e.digest {
			return nil, ErrConflict
		}
		seen[schedule.Name] = e.digest
		entries[i] = e
	}
	// Registrations are serialized, so the names checked here cannot be
	// registered by anyone else before this one commits; ticks and readers
	// are not blocked by the store transaction.
	s.addMu.Lock()
	defer s.addMu.Unlock()
	s.mu.Lock()
	for i, e := range entries {
		if existing, ok := s.entries[e.schedule.Name]; ok {
			if existing.digest != e.digest {
				s.mu.Unlock()
				return nil, ErrConflict
			}
			entries[i] = nil
		}
	}
	s.mu.Unlock()
	var fresh []*entry
	now := s.clock.Now().UTC()
	err := s.database.WithTx(store.Writer(ctx), func(tx *sql.Tx) error {
		fresh = fresh[:0]
		for i, e := range entries {
			if e == nil {
				continue
			}
			// Write first: a transaction that reads before writing cannot
			// wait for a concurrent cursor write and fails with SQLITE_BUSY;
			// one that writes first waits under the busy timeout.
			inserted, err := tx.ExecContext(ctx, `INSERT INTO cron_cursors (name, digest, last_occurrence, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT(name) DO NOTHING`, e.schedule.Name, e.digest, now.UnixNano(), now.UnixNano())
			if err != nil {
				return err
			}
			if rows, err := inserted.RowsAffected(); err != nil {
				return err
			} else if rows == 1 {
				e.cursor, e.lastKey, e.pending = now, "", nil
				added[i] = true
				fresh = append(fresh, e)
				continue
			}
			var storedDigest, lastKey string
			var last int64
			var pending sql.NullInt64
			err = tx.QueryRowContext(ctx, `SELECT digest, last_occurrence, last_key, pending FROM cron_cursors WHERE name = ?`, e.schedule.Name).Scan(&storedDigest, &last, &lastKey, &pending)
			switch {
			case err != nil:
				return err
			case storedDigest != e.digest:
				return ErrConflict
			default:
				e.cursor, e.lastKey, e.pending = time.Unix(0, last).UTC(), lastKey, nil
				if pending.Valid {
					held := time.Unix(0, pending.Int64).UTC()
					e.pending = &held
				}
				added[i] = false
			}
			fresh = append(fresh, e)
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrConflict) {
			return nil, ErrConflict
		}
		return nil, fmt.Errorf("cron: cursor: %w", err)
	}
	for _, e := range fresh {
		e.next, e.hasNext = s.nextAfter(e, e.cursor)
	}
	s.mu.Lock()
	for _, e := range fresh {
		s.entries[e.schedule.Name] = e
	}
	s.mu.Unlock()
	s.signal()
	return added, nil
}

func (s *Scheduler) nextAfter(e *entry, t time.Time) (time.Time, bool) {
	occurrence, ok := e.occurrences.Next(t)
	return occurrence.Instant, ok
}

func (s *Scheduler) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Next reports when a schedule fires next.
func (s *Scheduler) Next(name string) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[name]
	if !ok || !e.hasNext {
		return time.Time{}, false
	}
	return e.next, true
}

// Tick handles every schedule that is due at the clock's current time,
// every held occurrence whose previous work has settled, and every schedule
// whose cursor is ahead of the clock (reported as Behind). Ticks are
// serialized. A schedule that fails is retried with backoff; the others are
// unaffected. The returned error joins every failure of the tick. Run is
// woken afterwards, since the tick may have changed what it waits for.
func (s *Scheduler) Tick(ctx context.Context) ([]Result, error) {
	results, err := s.tick(ctx)
	s.signal()
	return results, err
}

func (s *Scheduler) tick(ctx context.Context) ([]Result, error) {
	s.tickMu.Lock()
	defer s.tickMu.Unlock()
	now := s.clock.Now().UTC()
	s.mu.Lock()
	due := make([]*entry, 0)
	for _, e := range s.entries {
		if e.retryAt.After(now) {
			continue
		}
		if (e.hasNext && !e.next.After(now)) || e.pending != nil || e.cursor.After(now) {
			due = append(due, e)
		}
	}
	// Cursor updates a failed write left behind are written with this
	// tick's; a newer update for the same schedule replaces them.
	updates := s.dirty
	if updates == nil {
		updates = map[*entry]cursorUpdate{}
	}
	s.mu.Unlock()
	sort.Slice(due, func(i, j int) bool { return due[i].schedule.Name < due[j].schedule.Name })
	var results []Result
	var failures []error
	for _, e := range due {
		result, err := s.fire(ctx, e, now, updates)
		result.Err = err
		s.mu.Lock()
		if err != nil {
			e.failures++
			e.retryAt = now.Add(backoff(e.failures))
			failures = append(failures, err)
		} else {
			e.failures, e.retryAt = 0, time.Time{}
		}
		s.mu.Unlock()
		results = append(results, result)
	}
	// Cursors are written after every submission of this tick, in one
	// transaction. A crash before it only repeats submissions, which
	// deduplicate; a failed write is retried by the next tick.
	err := s.flush(ctx, updates)
	s.mu.Lock()
	if err != nil {
		s.dirty = updates
		s.flushFails++
		s.flushRetryAt = now.Add(backoff(s.flushFails))
		failures = append(failures, err)
	} else {
		s.dirty, s.flushFails, s.flushRetryAt = nil, 0, time.Time{}
	}
	s.mu.Unlock()
	return results, errors.Join(failures...)
}

// backoff is 1 s after the first failure, doubling up to maxBackoff.
func backoff(failures int) time.Duration {
	if failures > 9 {
		return maxBackoff
	}
	return min(time.Second<<(failures-1), maxBackoff)
}

type cursorUpdate struct {
	cursor  time.Time
	lastKey string
	pending *time.Time
}

func (s *Scheduler) flush(ctx context.Context, updates map[*entry]cursorUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	now := s.clock.Now().UnixNano()
	return s.database.WithTx(store.Writer(ctx), func(tx *sql.Tx) error {
		for e, u := range updates {
			var held any
			if u.pending != nil {
				held = u.pending.UnixNano()
			}
			if _, err := tx.ExecContext(ctx, `UPDATE cron_cursors SET last_occurrence = ?, last_key = ?, pending = ?, updated_at = ? WHERE name = ?`, u.cursor.UnixNano(), u.lastKey, held, now, e.schedule.Name); err != nil {
				return fmt.Errorf("cron: %s: cursor: %w", e.schedule.Name, err)
			}
		}
		return nil
	})
}

// fire handles one schedule within Tick. When it fails, the occurrences
// that were on time are kept for retry.
func (s *Scheduler) fire(ctx context.Context, e *entry, now time.Time, updates map[*entry]cursorUpdate) (Result, error) {
	result, err := s.attempt(ctx, e, now, updates)
	if err != nil {
		e.retry = s.retained(e, now)
	} else {
		e.retry = nil
	}
	return result, err
}

// retained lists the on-time occurrences of this tick and those already
// kept for retry that are still unhandled, at most maxRetained, newest kept.
func (s *Scheduler) retained(e *entry, now time.Time) []time.Time {
	var kept []time.Time
	for _, instant := range e.retry {
		if instant.After(e.cursor) {
			kept = append(kept, instant)
		}
	}
	from := now.Add(-LateAfter - time.Nanosecond)
	if from.Before(e.cursor) {
		from = e.cursor
	}
	onTime, _ := e.occurrences.Between(from, now, math.MaxInt, missedCountCap)
	for _, occurrence := range onTime {
		if !slices.ContainsFunc(kept, occurrence.Instant.Equal) {
			kept = append(kept, occurrence.Instant)
		}
	}
	if len(kept) > maxRetained {
		kept = kept[len(kept)-maxRetained:]
	}
	return kept
}

func (s *Scheduler) attempt(ctx context.Context, e *entry, now time.Time, updates map[*entry]cursorUpdate) (Result, error) {
	result := Result{Schedule: e.schedule.Name}
	if e.cursor.After(now) {
		result.Behind = e.cursor.Sub(now)
	}
	if e.pending != nil {
		submitted, err := s.releaseHeld(ctx, e, &result, updates)
		if err != nil {
			return result, err
		}
		if !submitted {
			held := *e.pending
			result.Held = &held
		}
	}
	err := s.catchUp(ctx, e, now, &result, updates)
	return result, err
}

// catchUp handles the occurrences due since the cursor: every on-time one,
// every one kept on time for retry and, of the late ones, the most recent
// catch-up many. Late ones left out are counted as missed and passed.
func (s *Scheduler) catchUp(ctx context.Context, e *entry, now time.Time, result *Result, updates map[*entry]cursorUpdate) error {
	// Late means more than LateAfter old: at or before late.
	from, late := e.cursor, now.Add(-LateAfter-time.Nanosecond)
	var due []time.Time
	passAll := false
	if from.Before(late) {
		window, missed := e.occurrences.Between(from, late, e.catchUp, missedCountCap)
		result.Missed = missed
		passAll = missed > 0 && len(window) == 0
		for _, occurrence := range window {
			due = append(due, occurrence.Instant)
		}
		from = late
	}
	onTime, _ := e.occurrences.Between(from, now, math.MaxInt, missedCountCap)
	for _, occurrence := range onTime {
		due = append(due, occurrence.Instant)
	}
	for _, instant := range e.retry {
		if !instant.After(e.cursor) || slices.ContainsFunc(due, instant.Equal) {
			continue
		}
		due = append(due, instant)
		if !instant.After(late) && result.Missed > 0 {
			result.Missed--
		}
	}
	slices.SortFunc(due, time.Time.Compare)
	for _, instant := range due {
		if err := s.handle(ctx, e, instant, result, updates); err != nil {
			return err
		}
	}
	if passAll {
		s.save(e, late, e.lastKey, e.pending, updates)
	}
	next, hasNext := s.nextAfter(e, e.cursor)
	s.mu.Lock()
	e.next, e.hasNext = next, hasNext
	s.mu.Unlock()
	return nil
}

// handle applies the overlap policy to one due occurrence, submits it if
// due, and only then advances the cursor.
func (s *Scheduler) handle(ctx context.Context, e *entry, instant time.Time, result *Result, updates map[*entry]cursorUpdate) error {
	if e.schedule.Overlap != OverlapAllow && e.lastKey != "" {
		settled, err := s.tracker.Settled(ctx, e.lastKey)
		if err != nil {
			return fmt.Errorf("cron: %s: tracker: %w", e.schedule.Name, err)
		}
		if !settled {
			if e.schedule.Overlap == OverlapSkip {
				result.Skipped = append(result.Skipped, instant)
				s.save(e, instant, e.lastKey, nil, updates)
				return nil
			}
			held := instant
			result.Held = &held
			s.save(e, instant, e.lastKey, &held, updates)
			return nil
		}
	}
	key := SubmissionKey(e.schedule.Name, instant)
	accepted, err := s.submit.Submit(ctx, trigger.Submission{Key: key, Kind: e.schedule.Kind, Payload: e.payload, Principal: e.schedule.Principal})
	if err != nil {
		return fmt.Errorf("cron: %s: submit %s: %w", e.schedule.Name, instant.Format(time.RFC3339), err)
	}
	if accepted {
		result.Submitted = append(result.Submitted, instant)
	} else {
		result.Duplicates = append(result.Duplicates, instant)
	}
	s.save(e, instant, key, nil, updates)
	return nil
}

// releaseHeld submits a held occurrence once the previous work settled.
func (s *Scheduler) releaseHeld(ctx context.Context, e *entry, result *Result, updates map[*entry]cursorUpdate) (bool, error) {
	settled, err := s.tracker.Settled(ctx, e.lastKey)
	if err != nil {
		return false, fmt.Errorf("cron: %s: tracker: %w", e.schedule.Name, err)
	}
	if !settled {
		return false, nil
	}
	held := *e.pending
	key := SubmissionKey(e.schedule.Name, held)
	accepted, err := s.submit.Submit(ctx, trigger.Submission{Key: key, Kind: e.schedule.Kind, Payload: e.payload, Principal: e.schedule.Principal})
	if err != nil {
		return false, fmt.Errorf("cron: %s: submit held %s: %w", e.schedule.Name, held.Format(time.RFC3339), err)
	}
	if accepted {
		result.Submitted = append(result.Submitted, held)
	} else {
		result.Duplicates = append(result.Duplicates, held)
	}
	s.save(e, e.cursor, key, nil, updates)
	return true, nil
}

// save records a cursor change in memory and queues it for the tick's single
// cursor transaction. The cursor never moves backwards.
func (s *Scheduler) save(e *entry, cursor time.Time, lastKey string, pending *time.Time, updates map[*entry]cursorUpdate) {
	if cursor.Before(e.cursor) {
		cursor = e.cursor
	}
	s.mu.Lock()
	e.cursor, e.lastKey, e.pending = cursor, lastKey, pending
	s.mu.Unlock()
	updates[e] = cursorUpdate{cursor: cursor, lastKey: lastKey, pending: pending}
}

// wait returns how long until something is due: the earliest next firing,
// the recheck interval while any occurrence is held, and no earlier than a
// failing schedule's or cursor write's retry.
func (s *Scheduler) wait() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	wait := time.Hour
	for _, e := range s.entries {
		var at time.Time
		has := false
		if e.hasNext {
			at, has = e.next, true
		}
		if e.pending != nil {
			if recheck := now.Add(recheckInterval); !has || recheck.Before(at) {
				at, has = recheck, true
			}
		}
		if !has {
			continue
		}
		if e.retryAt.After(at) {
			at = e.retryAt
		}
		wait = min(wait, at.Sub(now))
	}
	if s.dirty != nil {
		wait = min(wait, s.flushRetryAt.Sub(now))
	}
	return max(wait, 0)
}

// Run is the single scheduling goroutine: it ticks, then waits until the
// next firing, a retry or a newly added schedule, until ctx ends. A failing
// schedule backs off (1 s, doubling to 5 min) while the others keep running;
// the cursor guarantees a later tick resumes without losing or duplicating
// occurrences. report, when not nil, receives each tick's results and error.
func (s *Scheduler) Run(ctx context.Context, report func([]Result, error)) error {
	for {
		results, err := s.tick(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if report != nil && (len(results) > 0 || err != nil) {
			report(results, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.wake:
		case <-s.clock.After(s.wait()):
		}
	}
}
