// Package slo is the operational SLO metric contract (ADR 0022): the state
// an application's components report about readiness, admission, unfinished
// work, timers, workers and storage, the liveness classification that alerts
// are built on, and a standard-library Prometheus text exposition of it.
//
// It imports only the standard library and contract/observe, so an
// application that does not select a monitoring stack pays for nothing but
// the counters it already keeps. Components report a Snapshot through a
// Source; a Sampler merges sources with a bounded wait per source. The
// optional observe/otel module exports the same snapshot over OTLP, and
// app/deploy renders it on /metrics. Both use the names in Catalogue.
//
// Liveness is the alerting contract. Work parked on a signal, a timer or a
// retry backoff is Waiting and never pages. Work whose owner is gone (an
// expired lease, a partition without a live owner) is Stalled and pages.
// Work parked on an effect whose outcome is unknown is Uncertain: it needs
// reconciliation, never an automatic retry, and is neither an error nor a
// stall.
package slo

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"sync/atomic"
	"time"

	"github.com/well-prado/new-blok/contract/observe"
)

// Liveness classifies one unfinished unit of work (a queued job, an admitted
// run) for alerting.
type Liveness string

const (
	// Pending work is admitted and waits for an owner that exists. Its age
	// is backlog lag (Work.OldestPending), not a stall.
	Pending Liveness = "pending"
	// Active work is executing under a live owner or lease.
	Active Liveness = "active"
	// Waiting work is suspended on a signal or timer, or delayed by a retry
	// backoff. It makes progress without an owner and never pages; an
	// overdue timer shows up as timer lag instead.
	Waiting Liveness = "waiting"
	// Uncertain work dispatched an effect whose outcome was never committed.
	// It awaits reconciliation, is never retried automatically, and is
	// reported apart from both errors and stalls.
	Uncertain Liveness = "uncertain"
	// Stalled work is not waiting and not uncertain, and the owner it needs
	// is gone: its claim's lease expired, or its partition has no live owner.
	Stalled Liveness = "stalled"
)

// Livenesses is the closed liveness vocabulary, in exposition order.
var Livenesses = []Liveness{Pending, Active, Waiting, Uncertain, Stalled}

// Observation is what a census knows about one unfinished unit of work.
type Observation struct {
	// Waiting: parked on a signal, a timer or a retry backoff.
	Waiting bool
	// Uncertain: a dispatched effect has no committed result.
	Uncertain bool
	// Claimed: an owner started the work.
	Claimed bool
	// OwnerLive: for claimed work, the claimant's lease is live; for
	// unclaimed work, an owner able to claim it exists. It is false only when
	// the census knows that owner is absent.
	OwnerLive bool
}

// Classify maps an observation to its liveness. The order is the contract:
// uncertain work is never reported as stalled (it must not be re-dispatched
// by an operator who reads "stalled" as "restart it"), and waiting work never
// needs an owner, so a missing owner cannot make it stalled.
func Classify(o Observation) Liveness {
	switch {
	case o.Uncertain:
		return Uncertain
	case o.Waiting:
		return Waiting
	case !o.OwnerLive:
		return Stalled
	case o.Claimed:
		return Active
	default:
		return Pending
	}
}

// RejectReason is why admission refused a request.
type RejectReason string

const (
	RejectCapacity    RejectReason = "capacity"    // bounded admission is full
	RejectDraining    RejectReason = "draining"    // the process is shutting down
	RejectNotReady    RejectReason = "not_ready"   // a required dependency is not ready
	RejectUnavailable RejectReason = "unavailable" // the durable store could not commit admission
	RejectInvalid     RejectReason = "invalid"     // the request can never be admitted
	RejectConflict    RejectReason = "conflict"    // the request key is bound to other input
)

// RejectReasons is the closed rejection vocabulary.
var RejectReasons = []RejectReason{RejectCapacity, RejectDraining, RejectNotReady, RejectUnavailable, RejectInvalid, RejectConflict}

// Bounds of a snapshot. They are part of the cardinality budget (ADR 0022).
const (
	// MaxNamed bounds the entries of each named list in a snapshot (work
	// sources, timer sources, workers, stores, dependencies).
	MaxNamed = 16
	// MaxNameBytes bounds a source, worker, store or dependency name.
	MaxNameBytes = 64
	// DefaultSampleTimeout bounds one source's sample.
	DefaultSampleTimeout = time.Second
	// MaxSampleTimeout is the largest accepted per-source sample timeout.
	MaxSampleTimeout = 30 * time.Second
	// MaxSources bounds the sources one Sampler merges.
	MaxSources = 32
)

// Snapshot is the operational state at one instant. Every part is optional;
// a nil pointer or empty list is not exported.
type Snapshot struct {
	Readiness  *Readiness
	Admission  *Admission
	Work       []Work
	Timers     []Timers
	Workers    []Worker
	Partitions *Partitions
	Storage    []Storage
	// Sources is filled by a Sampler only: every source's freshness.
	Sources []SourceStatus
	// SampleFailures counts sources that failed, timed out or reported an
	// invalid or conflicting snapshot, cumulatively per Sampler.
	SampleFailures uint64
}

// Readiness mirrors the deployment readiness decision.
type Readiness struct {
	Ready, Draining bool
	Dependencies    []Dependency
}

// Dependency is one readiness input (artifact, store, worker, secrets).
type Dependency struct {
	Name  string
	Ready bool
}

// Admission is the bounded edge admission state and its cumulative outcomes.
type Admission struct {
	Active, Capacity int
	Accepted         uint64
	Rejected         map[RejectReason]uint64
}

// Work is a census of one source of unfinished work (a queue, the cluster
// runtime). Counts are point-in-time; Truncated means the census hit its read
// bound and the counts are lower bounds.
type Work struct {
	Source                                       string
	Pending, Active, Waiting, Uncertain, Stalled int
	// DeadLetters are terminal failures retained for an operator.
	DeadLetters int
	// OldestPending is the age of the oldest pending item: backlog lag.
	OldestPending time.Duration
	// Takeover is how long a normal takeover can leave work reported as
	// stalled before a successor claims it (for the cluster, the owner TTL
	// plus the acquire interval). The stall alert's window must be at least
	// this long (blok.census.takeover). Zero when the census already waits
	// out ownership changes itself, as the queue census does.
	Takeover  time.Duration
	Truncated bool
}

// Add counts one unit of work of liveness l.
func (w *Work) Add(l Liveness) { w.AddN(l, 1) }

// AddN counts n units of work of liveness l.
func (w *Work) AddN(l Liveness, n int) {
	switch l {
	case Pending:
		w.Pending += n
	case Active:
		w.Active += n
	case Waiting:
		w.Waiting += n
	case Uncertain:
		w.Uncertain += n
	case Stalled:
		w.Stalled += n
	}
}

// Count returns the number of items of liveness l.
func (w Work) Count(l Liveness) int {
	switch l {
	case Pending:
		return w.Pending
	case Active:
		return w.Active
	case Waiting:
		return w.Waiting
	case Uncertain:
		return w.Uncertain
	case Stalled:
		return w.Stalled
	}
	return 0
}

// Timers is a census of due timers of one source. Lag is how far past its
// due time the oldest unfired timer is; zero when none is overdue.
type Timers struct {
	Source    string
	Overdue   int
	Lag       time.Duration
	Truncated bool
}

// Worker is the availability of one external worker runtime.
type Worker struct {
	Name               string
	Ready              bool
	InFlight, Capacity int
}

// Partitions is the ownership of a partitioned durable runtime. A partition
// without a live owner can neither claim work nor fire its timers.
type Partitions struct {
	Total, Owned int
}

// Storage is the size of one durable store. Budget is the operator's declared
// limit; zero means none was declared.
type Storage struct {
	Name         string
	Used, Budget int64
}

var ErrInvalid = errors.New("slo: invalid snapshot")

func validName(kind, name string) error {
	if !observe.ValidLabel(name) || len(name) > MaxNameBytes {
		return fmt.Errorf("%w: %s name %q", ErrInvalid, kind, name)
	}
	return nil
}

func checkNames[T any](kind string, items []T, name func(T) string) error {
	if len(items) > MaxNamed {
		return fmt.Errorf("%w: %d %s entries, at most %d", ErrInvalid, len(items), kind, MaxNamed)
	}
	seen := map[string]bool{}
	for _, item := range items {
		n := name(item)
		if err := validName(kind, n); err != nil {
			return err
		}
		if seen[n] {
			return fmt.Errorf("%w: duplicate %s %q", ErrInvalid, kind, n)
		}
		seen[n] = true
	}
	return nil
}

// Validate checks every bound: names are bounded labels, lists are at most
// MaxNamed long without duplicates, counts are not negative and rejection
// reasons are from RejectReasons. An exporter refuses an invalid snapshot
// rather than export an unbounded label.
func (s Snapshot) Validate() error {
	if s.Readiness != nil {
		if err := checkNames("dependency", s.Readiness.Dependencies, func(d Dependency) string { return d.Name }); err != nil {
			return err
		}
	}
	if a := s.Admission; a != nil {
		if a.Active < 0 || a.Capacity < 0 {
			return fmt.Errorf("%w: negative admission", ErrInvalid)
		}
		for reason := range a.Rejected {
			if !slices.Contains(RejectReasons, reason) {
				return fmt.Errorf("%w: rejection reason %q", ErrInvalid, reason)
			}
		}
	}
	if err := checkNames("work source", s.Work, func(w Work) string { return w.Source }); err != nil {
		return err
	}
	for _, w := range s.Work {
		if w.Pending < 0 || w.Active < 0 || w.Waiting < 0 || w.Uncertain < 0 || w.Stalled < 0 || w.DeadLetters < 0 || w.OldestPending < 0 || w.Takeover < 0 {
			return fmt.Errorf("%w: negative work count for %q", ErrInvalid, w.Source)
		}
	}
	if err := checkNames("timer source", s.Timers, func(t Timers) string { return t.Source }); err != nil {
		return err
	}
	for _, t := range s.Timers {
		if t.Overdue < 0 || t.Lag < 0 {
			return fmt.Errorf("%w: negative timer census for %q", ErrInvalid, t.Source)
		}
	}
	if err := checkNames("worker", s.Workers, func(w Worker) string { return w.Name }); err != nil {
		return err
	}
	for _, w := range s.Workers {
		if w.InFlight < 0 || w.Capacity < 0 {
			return fmt.Errorf("%w: negative worker load for %q", ErrInvalid, w.Name)
		}
	}
	if p := s.Partitions; p != nil && (p.Total < 0 || p.Owned < 0 || p.Owned > p.Total) {
		return fmt.Errorf("%w: partitions %d owned of %d", ErrInvalid, p.Owned, p.Total)
	}
	if err := checkNames("store", s.Storage, func(st Storage) string { return st.Name }); err != nil {
		return err
	}
	if len(s.Sources) > MaxSources {
		return fmt.Errorf("%w: %d source statuses, at most %d", ErrInvalid, len(s.Sources), MaxSources)
	}
	seenSources := map[string]bool{}
	for _, st := range s.Sources {
		if err := validName("source", st.Name); err != nil {
			return err
		}
		if seenSources[st.Name] || st.Age < 0 {
			return fmt.Errorf("%w: source status %q duplicate or negative age", ErrInvalid, st.Name)
		}
		seenSources[st.Name] = true
	}
	for _, st := range s.Storage {
		if st.Used < 0 || st.Budget < 0 {
			return fmt.Errorf("%w: negative storage for %q", ErrInvalid, st.Name)
		}
	}
	return nil
}

// merge adds part to s. A singleton part already present, a duplicate name
// or a list over its bound refuses the whole part, leaving s unchanged.
func (s *Snapshot) merge(part Snapshot) error {
	next := *s
	if part.Readiness != nil {
		if next.Readiness != nil {
			return fmt.Errorf("%w: two sources report readiness", ErrInvalid)
		}
		next.Readiness = part.Readiness
	}
	if part.Admission != nil {
		if next.Admission != nil {
			return fmt.Errorf("%w: two sources report admission", ErrInvalid)
		}
		next.Admission = part.Admission
	}
	if part.Partitions != nil {
		if next.Partitions != nil {
			return fmt.Errorf("%w: two sources report partitions", ErrInvalid)
		}
		next.Partitions = part.Partitions
	}
	next.Work = append(slices.Clip(next.Work), part.Work...)
	next.Timers = append(slices.Clip(next.Timers), part.Timers...)
	next.Workers = append(slices.Clip(next.Workers), part.Workers...)
	next.Storage = append(slices.Clip(next.Storage), part.Storage...)
	if err := next.Validate(); err != nil {
		return err
	}
	*s = next
	return nil
}

// Source is one component's part of the snapshot.
type Source struct {
	// Name identifies the source on blok.source.up and blok.source.age: a
	// bounded label, unique within a Sampler. A census source reports its
	// Work and Timers under the same name, so a rule can tell "zero stalled"
	// from "the census that would report stalled is not answering".
	Name string
	// Read reports the part. It must honour ctx; the Sampler abandons it at
	// its timeout either way.
	Read func(context.Context) (Snapshot, error)
	// Timeout bounds Read; zero means the Sampler's timeout. At most
	// MaxSampleTimeout.
	Timeout time.Duration
	// Informational marks a source whose staleness must not page (a store
	// size, say). Every other source feeds a paging alert, which would go
	// silent with it, so its staleness pages (ADR 0022).
	Informational bool
}

// Func is a source named name that reads with read.
func Func(name string, read func(context.Context) (Snapshot, error)) Source {
	return Source{Name: name, Read: read}
}

// SourceStatus is the Sampler's account of one source at a sample: Up when
// this sample succeeded, Age since its last successful sample (or since the
// Sampler was created, if it never succeeded). The Sampler always reports
// every source, so a failing or stuck source is visible as stale instead of
// vanishing with the series it would have reported.
type SourceStatus struct {
	Name  string
	Up    bool
	Age   time.Duration
	Pages bool
}

// Sampler merges sources into one snapshot. Each source runs on its own
// goroutine bounded by its timeout; a source still running from an earlier
// sample is skipped (counted) rather than started again, so a stuck source
// costs at most one goroutine. A failed, timed-out, invalid or conflicting
// source contributes nothing, is counted in SampleFailures and is reported
// down, with its age growing, in Snapshot.Sources. Sampling never fails as
// a whole.
type Sampler struct {
	sources  []Source
	busy     []atomic.Bool
	last     []atomic.Int64 // unix nanos of the last successful sample
	timeout  time.Duration
	clock    func() time.Time
	failures atomic.Uint64
}

// NewSampler bounds each source's sample by its own timeout, or by timeout
// (zero means DefaultSampleTimeout).
func NewSampler(timeout time.Duration, sources ...Source) (*Sampler, error) {
	return newSampler(time.Now, timeout, sources)
}

// NewSamplerWithClock is NewSampler with the clock that ages sources.
func NewSamplerWithClock(clock func() time.Time, timeout time.Duration, sources ...Source) (*Sampler, error) {
	return newSampler(clock, timeout, sources)
}

func newSampler(clock func() time.Time, timeout time.Duration, sources []Source) (*Sampler, error) {
	if timeout == 0 {
		timeout = DefaultSampleTimeout
	}
	if clock == nil {
		return nil, fmt.Errorf("%w: nil clock", ErrInvalid)
	}
	if timeout < time.Millisecond || timeout > MaxSampleTimeout {
		return nil, fmt.Errorf("%w: sample timeout %v outside 1ms..%v", ErrInvalid, timeout, MaxSampleTimeout)
	}
	if len(sources) > MaxSources {
		return nil, fmt.Errorf("%w: %d sources, at most %d", ErrInvalid, len(sources), MaxSources)
	}
	names := map[string]bool{}
	for _, source := range sources {
		if source.Read == nil {
			return nil, fmt.Errorf("%w: source %q has no Read", ErrInvalid, source.Name)
		}
		if err := validName("source", source.Name); err != nil {
			return nil, err
		}
		if names[source.Name] {
			return nil, fmt.Errorf("%w: duplicate source %q", ErrInvalid, source.Name)
		}
		names[source.Name] = true
		if source.Timeout < 0 || source.Timeout > MaxSampleTimeout {
			return nil, fmt.Errorf("%w: source %q timeout %v", ErrInvalid, source.Name, source.Timeout)
		}
	}
	s := &Sampler{sources: slices.Clone(sources), busy: make([]atomic.Bool, len(sources)), last: make([]atomic.Int64, len(sources)), timeout: timeout, clock: clock}
	now := clock().UnixNano()
	for i := range s.last {
		s.last[i].Store(now)
	}
	return s, nil
}

type sampled struct {
	part Snapshot
	err  error
}

// Sample queries every source concurrently and merges the parts in source
// order, so the result does not depend on which source answered first.
func (s *Sampler) Sample(ctx context.Context) Snapshot {
	if s == nil {
		return Snapshot{}
	}
	type pending struct {
		result chan sampled
		ctx    context.Context
		cancel context.CancelFunc
	}
	waits := make([]*pending, len(s.sources))
	for i, source := range s.sources {
		if !s.busy[i].CompareAndSwap(false, true) {
			continue
		}
		timeout := source.Timeout
		if timeout == 0 {
			timeout = s.timeout
		}
		sourceCtx, cancel := context.WithTimeout(ctx, timeout)
		w := &pending{result: make(chan sampled, 1), ctx: sourceCtx, cancel: cancel}
		waits[i] = w
		go func() {
			defer s.busy[i].Store(false)
			defer func() {
				if recover() != nil {
					w.result <- sampled{err: errors.New("slo: source panicked")}
				}
			}()
			part, err := source.Read(sourceCtx)
			w.result <- sampled{part: part, err: err}
		}()
	}
	var snapshot Snapshot
	for i, w := range waits {
		up := false
		if w != nil {
			r, ok := receive(w.ctx, w.result)
			w.cancel()
			up = ok && r.err == nil && len(r.part.Sources) == 0 && r.part.Validate() == nil && snapshot.merge(r.part) == nil
		}
		now := s.clock()
		if up {
			s.last[i].Store(now.UnixNano())
		} else {
			s.failures.Add(1)
		}
		age := time.Duration(0)
		if !up {
			age = max(now.Sub(time.Unix(0, s.last[i].Load())), 0)
		}
		snapshot.Sources = append(snapshot.Sources, SourceStatus{Name: s.sources[i].Name, Up: up, Age: age, Pages: !s.sources[i].Informational})
	}
	snapshot.SampleFailures = s.failures.Load()
	return snapshot
}

// receive prefers a result that is already there over an expired context.
func receive(ctx context.Context, result <-chan sampled) (sampled, bool) {
	select {
	case r := <-result:
		return r, true
	default:
	}
	select {
	case r := <-result:
		return r, true
	case <-ctx.Done():
		return sampled{}, false
	}
}

// Failures is the cumulative number of failed source samples.
func (s *Sampler) Failures() uint64 {
	if s == nil {
		return 0
	}
	return s.failures.Load()
}

// FileStorage reports the combined size of paths as store name (for example a
// SQLite database with its -wal and -shm files). A missing file counts as
// zero bytes; any other stat error fails the sample. budget is the
// operator's declared limit in bytes (zero for none).
func FileStorage(name string, budget int64, paths ...string) Source {
	return Source{Name: name, Informational: true, Read: func(ctx context.Context) (Snapshot, error) {
		var used int64
		for _, path := range paths {
			if err := ctx.Err(); err != nil {
				return Snapshot{}, err
			}
			info, err := os.Stat(path)
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return Snapshot{}, err
			}
			used += info.Size()
		}
		return Snapshot{Storage: []Storage{{Name: name, Used: used, Budget: budget}}}, nil
	}}
}
