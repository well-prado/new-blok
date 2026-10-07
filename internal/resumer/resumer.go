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
	Interrupted Outcome = "interrupted" // lease lost, stopped, or a journal error: not settled
)

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
	// Batch is how many runs a sweep takes of each kind; zero means 100.
	Batch int
	// Workers bounds concurrent executions; zero means 4.
	Workers int
	// Lease is the journal's wakeup lease (journal.Config.WakeupLease);
	// executions renew at a third of it. Zero means 30 seconds.
	Lease time.Duration
	// Clock measures leases; it must be the clock the journal's lease
	// calls are given everywhere else. Nil means time.Now.
	Clock func() time.Time
	// Settled, if set, observes every execution's outcome.
	Settled func(runID string, outcome Outcome, err error)
}

// Resumer runs executions; see the package comment.
type Resumer struct {
	config Config
	slots  chan struct{}
	stop   chan struct{}
	wake   chan struct{}

	mu        sync.Mutex
	scanned   time.Time // the last interrupted-run scan
	running   map[string]context.CancelFunc
	closing   bool
	executing sync.WaitGroup
}

func New(config Config) (*Resumer, error) {
	if config.Journal == nil || config.Engine == nil || len(config.Workflows) == 0 {
		return nil, errors.New("resumer: journal, engine and workflows are required")
	}
	for name, workflow := range config.Workflows {
		if name == "" || workflow.Program.Digest == "" || workflow.DecodeInput == nil {
			return nil, fmt.Errorf("resumer: workflow %q needs a program digest and an input decoder", name)
		}
	}
	if config.Interval <= 0 {
		config.Interval = time.Second
	}
	if config.Batch <= 0 {
		config.Batch = 100
	}
	if config.Workers <= 0 {
		config.Workers = 4
	}
	if config.Lease <= 0 {
		config.Lease = 30 * time.Second
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	return &Resumer{config: config, slots: make(chan struct{}, config.Workers), stop: make(chan struct{}), wake: make(chan struct{}, 1), running: map[string]context.CancelFunc{}}, nil
}

// Run sweeps at once and then every interval until ctx ends or Close is
// called. It returns when it has stopped sweeping; executions it started
// may still be running until Close drains them.
func (r *Resumer) Run(ctx context.Context) {
	ticker := time.NewTicker(r.config.Interval)
	defer ticker.Stop()
	for {
		r.Sweep(ctx)
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

// Sweep takes one batch of each kind of resumable run and starts executing
// them: woken runs, runs with a due timer, interrupted runs. It returns
// once they are started, not finished.
func (r *Resumer) Sweep(ctx context.Context) {
	now := r.config.Clock()
	if woken, err := r.config.Journal.PendingResumptions(ctx, now, r.config.Batch); err == nil {
		r.startLeased(ctx, leasesOf(woken))
	}
	if due, err := r.config.Journal.ClaimDueWaits(ctx, now, r.config.Batch); err == nil {
		r.startLeased(ctx, leasesOf(due))
	}
	// A run is interrupted only once its lease lapses, so looking a third
	// of a lease apart finds every one within a lease and a third; it scans
	// live runs, which mostly wait, so it is not done every sweep.
	r.mu.Lock()
	due := r.scanned.IsZero() || now.Sub(r.scanned) >= r.config.Lease/3 || now.Before(r.scanned)
	if due {
		r.scanned = now
	}
	r.mu.Unlock()
	if !due {
		return
	}
	if interrupted, err := r.config.Journal.InterruptedRuns(ctx, now, r.config.Batch); err == nil {
		r.startLeased(ctx, interrupted)
	}
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

// Start leases an admitted run and starts executing it. A run another
// holder has is journal.ErrLeaseLost.
func (r *Resumer) Start(ctx context.Context, runID string) error {
	token, err := r.config.Journal.TakeRunLease(ctx, runID, r.config.Clock())
	if err != nil {
		return err
	}
	r.startLeased(ctx, []journal.RunLease{{RunID: runID, Token: token}})
	return nil
}

func (r *Resumer) startLeased(ctx context.Context, leases []journal.RunLease) {
	for _, lease := range leases {
		r.mu.Lock()
		if r.closing {
			r.mu.Unlock()
			// Not executed: hand the run back at once.
			_ = r.config.Journal.ReleaseRunLease(context.WithoutCancel(ctx), lease.RunID, lease.Token)
			continue
		}
		runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		r.running[lease.RunID] = cancel
		r.executing.Add(1)
		r.mu.Unlock()
		go func() {
			defer r.executing.Done()
			defer cancel()
			r.slots <- struct{}{}
			defer func() { <-r.slots }()
			outcome, err := r.execute(runCtx, lease)
			r.mu.Lock()
			delete(r.running, lease.RunID)
			r.mu.Unlock()
			if r.config.Settled != nil {
				r.config.Settled(lease.RunID, outcome, err)
			}
		}()
	}
}

// execute runs one leased run until it suspends or ends, renewing its lease
// meanwhile, and settles the outcome under that lease.
func (r *Resumer) execute(ctx context.Context, lease journal.RunLease) (Outcome, error) {
	j := r.config.Journal
	release := func() { _ = j.ReleaseRunLease(context.WithoutCancel(ctx), lease.RunID, lease.Token) }
	if ctx.Err() != nil {
		release()
		return Interrupted, ctx.Err()
	}
	run, err := j.Run(ctx, lease.RunID)
	if err != nil {
		release()
		return Interrupted, err
	}
	workflow, ok := r.config.Workflows[run.Workflow]
	if !ok || workflow.Program.Digest != run.ArtifactDigest {
		// Another build may know it: leave the run, do not fail it.
		release()
		return Interrupted, fmt.Errorf("resumer: run %s needs workflow %q at %s, not registered here", run.RunID, run.Workflow, run.ArtifactDigest)
	}
	program := workflow.Program
	input, decodeErr := workflow.DecodeInput(run.Input)
	var encoded []byte
	if decodeErr == nil {
		encoded, decodeErr = json.Marshal(input)
	}
	if decodeErr != nil {
		err := j.ForRun(lease.RunID, lease.Token).FailRun(context.WithoutCancel(ctx), "input_decode", "validation")
		release()
		if err != nil {
			return Interrupted, err
		}
		return Failed, decodeErr
	}
	execCtx, cancel := context.WithCancel(ctx)
	renewed := make(chan struct{})
	var lost bool // set before renewed closes
	go func() {
		defer close(renewed)
		ticker := time.NewTicker(r.config.Lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-execCtx.Done():
				return
			case <-ticker.C:
				if err := j.RenewRunLease(execCtx, lease.RunID, lease.Token, r.config.Clock()); errors.Is(err, journal.ErrLeaseLost) {
					lost = true
					cancel()
					return
				}
			}
		}
	}()
	runJournal := j.ForRun(lease.RunID, lease.Token).WithInput(encoded)
	result, runErr := r.config.Engine.RunJournaled(execCtx, program, input, lease.RunID, runJournal)
	cancel()
	<-renewed
	settle := context.WithoutCancel(ctx)
	var engineErr *engine.Error
	switch {
	case runErr == nil:
		output, err := json.Marshal(result.Output)
		if err == nil {
			err = runJournal.CompleteRun(settle, output)
		}
		release()
		if err != nil {
			return Interrupted, err
		}
		return Completed, nil
	case errors.As(runErr, &engineErr) && engineErr.Suspended:
		release()
		return Suspended, nil
	case lost, errors.Is(runErr, journal.ErrLeaseLost), ctx.Err() != nil:
		// Not ours to settle: another holder has the run, or we are
		// stopping. Interrupted runs are taken again once the lease is
		// free (InterruptedRuns).
		release()
		return Interrupted, runErr
	case errors.As(runErr, &engineErr) && engineErr.Uncertain:
		err := runJournal.MarkRunUncertain(settle, engineErr.Code, engineErr.Class)
		release()
		if err != nil {
			return Interrupted, err
		}
		return Uncertain, runErr
	case errors.As(runErr, &engineErr) && engineErr.Class != "persistence":
		err := runJournal.FailRun(settle, engineErr.Code, engineErr.Class)
		release()
		if err != nil {
			return Interrupted, err
		}
		return Failed, runErr
	default:
		// A journal fault is not the workflow's failure: leave the run to
		// be retried.
		release()
		return Interrupted, runErr
	}
}

// Close stops sweeping, waits up to ctx's deadline for running executions
// to finish, then cancels the rest; every execution releases its run lease
// as it ends, so the next holder may take its run at once. It returns
// ctx.Err() when the drain was cut short.
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
		<-drained
		return ctx.Err()
	}
}
