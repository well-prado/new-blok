// Package resumer executes durable runs on a single host through the SQLite
// journal (ADR 0027): it starts admitted runs, and on start and every
// interval it resumes the runs whose wait fired (a signal, a due timer) or
// whose execution was interrupted. A suspended run holds no goroutine: its
// execution returns at the wait, its lease is released, and only the
// journal remembers it.
package resumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/internal/journal"
)

// Outcome is how one execution of a run ended.
type Outcome string

const (
	Completed   Outcome = "completed"
	Failed      Outcome = "failed"
	Uncertain   Outcome = "uncertain"
	Suspended   Outcome = "suspended"
	Interrupted Outcome = "interrupted" // lease lost, stopped, unknown workflow or a transient fault: not settled
)

// ErrClosed: the resumer is closing and starts nothing.
var ErrClosed = errors.New("resumer: closed")

// Workflow is a registered workflow: its program, and how to decode a
// run's admitted JSON input into the value its nodes take.
type Workflow struct {
	Program     contract.InternalProgram
	DecodeInput func(json.RawMessage) (any, error)
}

type Config struct {
	Journal *journal.Journal
	Engine  *engine.Engine
	// Workflows by name. A run executes its workflow's program, whose
	// digest must be the artifact the run was admitted with.
	Workflows map[string]Workflow
	// Interval between sweeps; zero means one second.
	Interval time.Duration
	// Batch is how many runs a sweep takes of each kind, at most as many
	// as there are free workers; zero means 100.
	Batch int
	// Workers bounds concurrent executions; zero means 4. A run is leased
	// only once a worker is free for it.
	Workers int
	// Clock measures leases; it must be the clock the journal's lease
	// calls are given everywhere else. Nil means time.Now.
	Clock func() time.Time
	// RenewEvery is how often, in real time, an execution renews its run
	// lease; zero means a third of the journal's wakeup lease.
	RenewEvery time.Duration
	// MaxRetries bounds how many times in a row a run whose execution hit
	// a transient fault is retried before it is failed; zero means 5.
	MaxRetries int
	// OnError, if set, receives the errors of sweeps and settlements.
	OnError func(error)
	// Settled, if set, observes every execution's outcome. It runs on the
	// execution's worker, before the worker is freed.
	Settled func(runID string, outcome Outcome, err error)
}

// Resumer runs executions; see the package comment.
type Resumer struct {
	config    Config
	lease     time.Duration
	workflows []string
	slots     chan struct{}
	stop      chan struct{}
	wake      chan struct{}

	mu        sync.Mutex
	scanned   time.Time                    // the last interrupted-run scan
	running   map[int64]context.CancelFunc // by lease token
	retries   map[string]int               // consecutive transient faults here, by live run
	closing   bool
	executing sync.WaitGroup
}

func New(config Config) (*Resumer, error) {
	if config.Journal == nil || config.Engine == nil || len(config.Workflows) == 0 {
		return nil, errors.New("resumer: journal, engine and workflows are required")
	}
	var names []string
	for name, workflow := range config.Workflows {
		if name == "" || workflow.Program.Digest == "" || workflow.DecodeInput == nil {
			return nil, fmt.Errorf("resumer: workflow %q needs a program digest and an input decoder", name)
		}
		names = append(names, name)
	}
	slices.Sort(names)
	if config.Interval <= 0 {
		config.Interval = time.Second
	}
	if config.Batch <= 0 {
		config.Batch = 100
	}
	if config.Workers <= 0 {
		config.Workers = 4
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	lease := config.Journal.WakeupLease()
	if config.RenewEvery <= 0 {
		config.RenewEvery = lease / 3
	}
	if config.RenewEvery >= lease {
		return nil, fmt.Errorf("resumer: renewing every %v cannot keep a %v lease", config.RenewEvery, lease)
	}
	if config.MaxRetries <= 0 {
		config.MaxRetries = 5
	}
	return &Resumer{config: config, lease: lease, workflows: names, slots: make(chan struct{}, config.Workers), stop: make(chan struct{}), wake: make(chan struct{}, 1), running: map[int64]context.CancelFunc{}, retries: map[string]int{}}, nil
}

// Run sweeps at once and then every interval until ctx ends or Close is
// called.
func (r *Resumer) Run(ctx context.Context) {
	ticker := time.NewTicker(r.config.Interval)
	defer ticker.Stop()
	for {
		_ = r.Sweep(ctx)
		select {
		case <-ctx.Done():
			return
		case <-r.stop:
			return
		case <-ticker.C:
		case <-r.wake:
		}
	}
}

// Wake asks Run to sweep now, for example after a signal.
func (r *Resumer) Wake() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Sweep takes, for the workers that are free, woken runs, runs with a due
// timer and interrupted runs, and starts executing them. It returns once
// they are started, with the errors of the listings (also given to
// OnError).
func (r *Resumer) Sweep(ctx context.Context) error {
	if r.isClosing() {
		return ErrClosed
	}
	now := r.config.Clock()
	j := r.config.Journal
	var errs []error
	list := func(take func(limit int) ([]journal.RunLease, error)) {
		reserved := r.reserve(r.config.Batch)
		if reserved == 0 {
			return
		}
		leases, err := take(reserved)
		r.unreserve(reserved - len(leases))
		if err != nil {
			errs = append(errs, err)
			r.report(err)
			return
		}
		r.startReserved(ctx, leases)
	}
	list(func(limit int) ([]journal.RunLease, error) {
		woken, err := j.PendingResumptions(ctx, now, limit)
		return leasesOf(woken), err
	})
	list(func(limit int) ([]journal.RunLease, error) {
		due, err := j.ClaimDueWaits(ctx, now, limit)
		return leasesOf(due), err
	})
	// A run is interrupted only once its lease lapses (or a lease after
	// its admission), so looking a third of a lease apart finds every one
	// within a lease and a third of becoming one; the scan reads every
	// live run, which mostly wait, so it is not done every sweep. A sweep
	// with no free worker does not scan, and does not count as a scan:
	// the next sweep with a free worker does.
	r.mu.Lock()
	due := r.scanned.IsZero() || now.Sub(r.scanned) >= r.lease/3 || now.Before(r.scanned)
	r.mu.Unlock()
	if due {
		list(func(limit int) ([]journal.RunLease, error) {
			r.mu.Lock()
			r.scanned = now
			r.mu.Unlock()
			if err := r.prune(ctx); err != nil {
				errs = append(errs, err)
				r.report(err)
			}
			return j.InterruptedRuns(ctx, now, limit, r.workflows)
		})
	}
	return errors.Join(errs...)
}

// prune forgets the transient-fault counts of runs that have ended or no
// longer exist: another holder executed them to the end, or they were
// canceled. A run this resumer settles is forgotten as it settles.
func (r *Resumer) prune(ctx context.Context) error {
	r.mu.Lock()
	runs := make([]string, 0, len(r.retries))
	for runID := range r.retries {
		runs = append(runs, runID)
	}
	r.mu.Unlock()
	for _, runID := range runs {
		run, err := r.config.Journal.Run(ctx, runID)
		switch {
		case errors.Is(err, journal.ErrNotFound):
		case err != nil:
			return err
		case run.State == "accepted":
			continue
		}
		r.forget(runID)
	}
	return nil
}

func leasesOf(waits []journal.WaitRecord) []journal.RunLease {
	seen := map[string]bool{}
	var leases []journal.RunLease
	for _, wait := range waits {
		if !seen[wait.RunID] {
			seen[wait.RunID] = true
			leases = append(leases, journal.RunLease{RunID: wait.RunID, Token: wait.LeaseToken})
		}
	}
	return leases
}

// reserve takes up to n free workers without waiting.
func (r *Resumer) reserve(n int) int {
	for taken := 0; taken < n; taken++ {
		select {
		case r.slots <- struct{}{}:
		default:
			return taken
		}
	}
	return n
}

func (r *Resumer) unreserve(n int) {
	for range n {
		<-r.slots
	}
}

// Start waits for a free worker, leases an admitted run and starts
// executing it. A run another holder has is journal.ErrLeaseLost; a
// closing resumer refuses with ErrClosed.
func (r *Resumer) Start(ctx context.Context, runID string) error {
	if r.isClosing() {
		return ErrClosed
	}
	select {
	case r.slots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-r.stop:
		return ErrClosed
	}
	token, err := r.config.Journal.TakeRunLease(ctx, runID, r.config.Clock())
	if err != nil {
		r.unreserve(1)
		return err
	}
	if !r.startReserved(ctx, []journal.RunLease{{RunID: runID, Token: token}}) {
		return ErrClosed
	}
	return nil
}

func (r *Resumer) isClosing() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closing
}

// startReserved starts one execution per lease, each on a worker already
// reserved for it. It reports false when the resumer was closing and the
// runs were handed back unexecuted.
func (r *Resumer) startReserved(ctx context.Context, leases []journal.RunLease) bool {
	started := true
	for _, lease := range leases {
		r.mu.Lock()
		if r.closing {
			r.mu.Unlock()
			_ = r.config.Journal.ReleaseRunLease(context.WithoutCancel(ctx), lease.RunID, lease.Token)
			r.unreserve(1)
			started = false
			continue
		}
		runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		r.running[lease.Token] = cancel
		r.executing.Add(1)
		r.mu.Unlock()
		go func() {
			defer r.executing.Done()
			defer r.unreserve(1)
			defer cancel()
			outcome, err := r.execute(runCtx, lease)
			r.mu.Lock()
			delete(r.running, lease.Token)
			r.mu.Unlock()
			r.settled(lease.RunID, outcome, err)
		}()
	}
	return started
}

func (r *Resumer) settled(runID string, outcome Outcome, err error) {
	if r.config.Settled == nil {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			r.report(fmt.Errorf("resumer: Settled panicked for run %s: %v", runID, p))
		}
	}()
	r.config.Settled(runID, outcome, err)
}

func (r *Resumer) report(err error) {
	if err != nil && r.config.OnError != nil {
		r.config.OnError(err)
	}
}

// execute runs one leased run until it suspends or ends, renewing its lease
// meanwhile, settles the outcome under that lease, and releases it.
func (r *Resumer) execute(ctx context.Context, lease journal.RunLease) (outcome Outcome, err error) {
	j := r.config.Journal
	runJournal := j.ForRun(lease.RunID, lease.Token)
	settle := context.WithoutCancel(ctx)
	defer func() { _ = j.ReleaseRunLease(settle, lease.RunID, lease.Token) }()
	fail := func(code, class string, cause error) (Outcome, error) {
		err := runJournal.FailRun(settle, code, class)
		if errors.Is(err, journal.ErrUncertain) {
			// The run holds an effect of unknown outcome (a sibling
			// fail-fast canceled, say): it cannot fail, only end
			// uncertain. Retrying would report the same failure forever.
			if err = runJournal.MarkRunUncertain(settle, code, class); err == nil {
				r.forget(lease.RunID)
				return Uncertain, cause
			}
		}
		if err != nil {
			r.report(err)
			return Interrupted, errors.Join(cause, err)
		}
		r.forget(lease.RunID)
		return Failed, cause
	}
	if ctx.Err() != nil {
		return Interrupted, ctx.Err()
	}
	run, err := j.Run(ctx, lease.RunID)
	if err != nil {
		return Interrupted, err
	}
	workflow, ok := r.config.Workflows[run.Workflow]
	if !ok || workflow.Program.Digest != run.ArtifactDigest {
		// Another build may know it: leave the run, do not fail it.
		return Interrupted, fmt.Errorf("resumer: run %s needs workflow %q at %s, not registered here", run.RunID, run.Workflow, run.ArtifactDigest)
	}
	input, err := decode(workflow, run.Input)
	if err != nil {
		return fail("input_decode", "validation", err)
	}
	execCtx, cancel := context.WithCancel(ctx)
	renewed := make(chan struct{})
	var lost bool // set before renewed closes
	go func() {
		defer close(renewed)
		ticker := time.NewTicker(r.config.RenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-execCtx.Done():
				return
			case <-ticker.C:
				err := j.RenewRunLease(execCtx, lease.RunID, lease.Token, r.config.Clock())
				if errors.Is(err, journal.ErrLeaseLost) {
					lost = true
					cancel()
					return
				}
				if err != nil && execCtx.Err() == nil {
					// The lease may still lapse: report it, keep executing
					// and renewing; the lease token fences a lapsed one.
					r.report(fmt.Errorf("resumer: renewing the lease of run %s: %w", lease.RunID, err))
				}
			}
		}
	}()
	result, runErr := r.config.Engine.RunJournaled(execCtx, workflow.Program, input, lease.RunID, runJournal)
	cancel()
	<-renewed
	var engineErr *engine.Error
	switch {
	case runErr == nil:
		output, err := json.Marshal(result.Output)
		if err == nil {
			err = runJournal.CompleteRun(settle, output)
		}
		// Defensive: the engine commits the output step under the same
		// byte bound (MaxStepResultBytes is MaxRunOutputBytes) and the
		// same fence before it returns, so today an oversize or
		// conflicting output fails in RunJournaled (the Permanent case
		// below) and never reaches this. Should the bounds part, it fails
		// the run rather than retrying it.
		if errors.Is(err, journal.ErrRunOutputLimit) || journal.Permanent(err) {
			return fail("output_rejected", "validation", err)
		}
		if errors.Is(err, journal.ErrLeaseLost) {
			// Another holder has the run: not its fault, not a retry.
			r.forget(lease.RunID)
			return Interrupted, err
		}
		if err != nil {
			return r.transient(lease.RunID, runJournal, err)
		}
		r.forget(lease.RunID)
		return Completed, nil
	case errors.As(runErr, &engineErr) && engineErr.Suspended:
		r.forget(lease.RunID)
		return Suspended, nil
	case lost, errors.Is(runErr, journal.ErrLeaseLost), ctx.Err() != nil:
		// Not ours to settle: another holder has the run, or we are
		// stopping. It is taken again once the lease is free; its fault
		// count starts again.
		r.forget(lease.RunID)
		return Interrupted, runErr
	case errors.As(runErr, &engineErr) && engineErr.Uncertain:
		if err := runJournal.MarkRunUncertain(settle, engineErr.Code, engineErr.Class); err != nil {
			r.report(err)
			return Interrupted, errors.Join(runErr, err)
		}
		r.forget(lease.RunID)
		return Uncertain, runErr
	case journal.Permanent(runErr):
		// A conflict with what the run recorded (another input, a changed
		// wait plan, a canceled wait, an oversize result): no retry can fix
		// it.
		code := "journal_conflict"
		if errors.As(runErr, &engineErr) {
			code = engineErr.Code
		}
		return fail(code, "conflict", runErr)
	case errors.As(runErr, &engineErr) && engineErr.Class != "persistence":
		return fail(engineErr.Code, engineErr.Class, runErr)
	default:
		return r.transient(lease.RunID, runJournal, runErr)
	}
}

// transient leaves a run that hit a transient fault to be retried, at the
// interrupted-run scan's pace, until MaxRetries in a row; then fails it.
func (r *Resumer) transient(runID string, runJournal *journal.RunJournal, cause error) (Outcome, error) {
	r.mu.Lock()
	r.retries[runID]++
	exhausted := r.retries[runID] > r.config.MaxRetries
	r.mu.Unlock()
	r.report(cause)
	if !exhausted {
		return Interrupted, cause
	}
	if err := runJournal.FailRun(context.Background(), "retries_exhausted", "persistence"); err != nil {
		r.report(err)
		return Interrupted, errors.Join(cause, err)
	}
	r.forget(runID)
	return Failed, cause
}

func (r *Resumer) forget(runID string) {
	r.mu.Lock()
	delete(r.retries, runID)
	r.mu.Unlock()
}

// decode runs a workflow's input decoder, turning a panic into an error.
func decode(workflow Workflow, raw json.RawMessage) (input any, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("resumer: input decoder panicked: %v", p)
		}
	}()
	return workflow.DecodeInput(raw)
}

// Close stops sweeping and waits until ctx ends for running executions to
// finish; then it cancels the rest and returns ctx.Err() at once. Every
// execution releases its run lease as it returns, so the next holder may
// take its run; one stuck in a node that ignores cancellation releases it
// when that node returns.
func (r *Resumer) Close(ctx context.Context) error {
	r.mu.Lock()
	if !r.closing {
		r.closing = true
		close(r.stop)
	}
	r.mu.Unlock()
	drained := make(chan struct{})
	go func() {
		r.executing.Wait()
		close(drained)
	}()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		r.mu.Lock()
		for _, cancel := range r.running {
			cancel()
		}
		r.mu.Unlock()
		return ctx.Err()
	}
}
