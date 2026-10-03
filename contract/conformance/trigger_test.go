package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/trigger"
)

// reference is a synthetic in-process adapter. With no defect it obeys the
// trigger contract for its declaration; each defect breaks one rule so the
// harness can be shown to catch it.
type reference struct {
	declaration trigger.Declaration
	defect      string
	env         TriggerEnv
	input       schema.Schema
	mu          sync.Mutex
	started     bool
	accepted    map[string]string
	effects     int
	pending     []Call
	stop        chan struct{}
}

func memoryReference(defect string) *reference {
	return &reference{declaration: trigger.Declaration{Kind: trigger.HTTP, Adapter: "reference/memory", Completion: trigger.Memory, Disconnect: trigger.CancelWork, Authentication: trigger.Caller}, defect: defect}
}

func durableReference(defect string) *reference {
	return &reference{declaration: trigger.Declaration{Kind: trigger.Webhook, Adapter: "reference/durable", Completion: trigger.Durable, Disconnect: trigger.StopWaiting, Authentication: trigger.Caller}, defect: defect}
}

// queueReference behaves like a durable queue consumer: producers are
// trusted, a lost consumer leaves the delivery pending, and saturation defers.
func queueReference(defect string) *reference {
	return &reference{declaration: trigger.Declaration{Kind: trigger.Worker, Adapter: "reference/queue", Completion: trigger.Durable, Disconnect: trigger.Redeliver, Authentication: trigger.TrustedProducer}, defect: defect}
}

func (r *reference) Declaration() trigger.Declaration {
	d := r.declaration
	switch r.defect {
	case "bad-declaration":
		d.Disconnect = trigger.StopWaiting
	case "unauthenticated-http":
		d.Authentication = trigger.TrustedProducer
	}
	return d
}

func (r *reference) Open(_ context.Context, env TriggerEnv) error {
	parsed, err := schema.Parse(env.InputSchema)
	if err != nil {
		return err
	}
	r.env, r.input, r.accepted, r.stop = env, parsed, map[string]string{}, make(chan struct{})
	if r.defect == "construction-goroutine" {
		go func() { <-r.stop }()
	}
	return nil
}

func (r *reference) Endpoint() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started || r.defect == "listen-at-construction" {
		return "reference://listening"
	}
	return ""
}

func (r *reference) Start(context.Context) error {
	r.mu.Lock()
	r.started = true
	r.mu.Unlock()
	if r.defect == "leak-goroutine" {
		go func() { select {} }()
	}
	return nil
}

func (r *reference) Stop(context.Context) error {
	r.mu.Lock()
	r.started = false
	r.mu.Unlock()
	close(r.stop)
	return nil
}

func (r *reference) Restart(context.Context) error { return nil }

func (r *reference) CommittedEffects(context.Context) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.effects, nil
}

func (r *reference) commit() {
	r.mu.Lock()
	r.effects++
	r.mu.Unlock()
}

func (r *reference) Deliver(ctx context.Context, d Delivery) (Outcome, error) {
	r.mu.Lock()
	started := r.started
	r.mu.Unlock()
	if !started && r.defect != "dispatch-after-stop" {
		return Outcome{Kind: Rejected, Code: "unavailable"}, nil
	}
	redeliver := r.declaration.Disconnect == trigger.Redeliver
	var principal trigger.Principal
	if !redeliver || r.defect == "fabricate-principal" {
		var err error
		principal, err = r.env.Authenticate(d.Credential)
		if err != nil && r.defect != "skip-auth" && !redeliver {
			return Outcome{Kind: Rejected, Code: "unauthorized"}, nil
		}
	}
	if r.defect == "payload-principal" {
		var claimed struct {
			Principal string `json:"principal"`
		}
		if json.Unmarshal(d.Payload, &claimed) == nil && claimed.Principal != "" {
			principal.ID = claimed.Principal
		}
	}
	if r.defect != "skip-validation" {
		candidate := d.Payload
		if len(candidate) == 0 {
			candidate = json.RawMessage("null")
		}
		if _, err := r.input.Normalize(candidate); err != nil {
			return Outcome{Kind: Rejected, Code: "invalid_input"}, nil
		}
	}
	durable := r.declaration.Completion == trigger.Durable
	if durable && r.defect != "no-dedup" {
		r.mu.Lock()
		previous, seen := r.accepted[d.Key]
		if !seen {
			r.accepted[d.Key] = string(canonical(d.Payload))
		}
		r.mu.Unlock()
		if seen && previous == string(canonical(d.Payload)) {
			return Outcome{Kind: Duplicate}, nil
		}
		if seen {
			return Outcome{Kind: Rejected, Code: "conflict"}, nil
		}
	}
	call := Call{Input: d.Payload, Principal: principal}
	if redeliver {
		return r.consume(ctx, d.Disconnect, call), nil
	}

	// Memory work runs under a context the caller's disconnect cancels;
	// durable detach work runs under a context the caller cannot cancel.
	workCtx, cancel := context.WithCancel(ctx)
	if durable && r.defect != "cancel-on-detach" {
		workCtx, cancel = context.WithCancel(context.WithoutCancel(ctx))
	}
	type reply struct {
		output json.RawMessage
		err    error
	}
	done := make(chan reply, 1)
	go func() {
		defer cancel()
		output, err := r.env.Workflow(workCtx, call)
		if err != nil && r.defect == "retry" {
			output, err = r.env.Workflow(workCtx, call)
		}
		if err == nil && durable {
			if r.defect == "ack-before-commit" {
				go func() { time.Sleep(20 * time.Millisecond); r.commit() }()
			} else {
				r.commit()
			}
		}
		done <- reply{output, err}
	}()
	select {
	case <-d.Disconnect:
		if (!durable && r.defect != "ignore-disconnect") || r.defect == "cancel-on-detach" {
			cancel()
		}
		return Outcome{Kind: Disconnected}, nil
	case result := <-done:
		return r.outcome(result.output, result.err), nil
	}
}

func (r *reference) outcome(output json.RawMessage, err error) Outcome {
	if err == nil {
		if r.defect == "swallow-output" {
			return Outcome{Kind: Completed}
		}
		return Outcome{Kind: Completed, Output: output}
	}
	if errors.Is(err, trigger.ErrSaturated) {
		return Outcome{Kind: Rejected, Code: "saturated"}
	}
	if code, _, ok := trigger.Classify(err); ok && r.defect != "wrong-error-code" {
		return Outcome{Kind: Rejected, Code: code}
	}
	if r.defect == "leak-error" {
		return Outcome{Kind: Rejected, Code: "internal", Message: err.Error()}
	}
	return Outcome{Kind: Rejected, Code: "internal"}
}

// consume runs one claim for the queue reference. A consumer lost before
// acknowledgment commits nothing and leaves the delivery pending.
func (r *reference) consume(ctx context.Context, disconnect <-chan struct{}, call Call) Outcome {
	work, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-disconnect:
			cancel()
		case <-work.Done():
		}
	}()
	output, err := r.env.Workflow(work, call)
	if err != nil && r.defect == "retry" {
		output, err = r.env.Workflow(work, call)
	}
	lost := false
	if disconnect != nil {
		select {
		case <-disconnect:
			lost = true
		default:
		}
	}
	if lost {
		if r.defect == "ack-lost-consumer" {
			r.commit()
		}
		r.mu.Lock()
		r.pending = append(r.pending, call)
		r.mu.Unlock()
		return Outcome{Kind: Disconnected}
	}
	if err == nil {
		r.commit()
		return Outcome{Kind: Completed}
	}
	if errors.Is(err, trigger.ErrSaturated) && r.defect != "dead-letter-saturation" {
		r.mu.Lock()
		r.pending = append(r.pending, call)
		r.mu.Unlock()
		return Outcome{Kind: Deferred}
	}
	return r.outcome(output, err)
}

func (r *reference) Recover(ctx context.Context) (Outcome, error) {
	r.mu.Lock()
	if len(r.pending) == 0 {
		r.mu.Unlock()
		return Outcome{}, errors.New("nothing pending")
	}
	call := r.pending[0]
	r.pending = r.pending[1:]
	r.mu.Unlock()
	return r.consume(ctx, nil, call), nil
}

// withoutRestart hides the reference's Restarter.
type withoutRestart struct{ inner *reference }

func (w withoutRestart) Declaration() trigger.Declaration             { return w.inner.Declaration() }
func (w withoutRestart) Open(ctx context.Context, e TriggerEnv) error { return w.inner.Open(ctx, e) }
func (w withoutRestart) Start(ctx context.Context) error              { return w.inner.Start(ctx) }
func (w withoutRestart) Deliver(ctx context.Context, d Delivery) (Outcome, error) {
	return w.inner.Deliver(ctx, d)
}
func (w withoutRestart) Stop(ctx context.Context) error { return w.inner.Stop(ctx) }
func (w withoutRestart) CommittedEffects(ctx context.Context) (int, error) {
	return w.inner.CommittedEffects(ctx)
}

var fastOptions = TriggerOptions{CancelTimeout: 300 * time.Millisecond, SettleTimeout: 500 * time.Millisecond}

func loadCorpus(t *testing.T) TriggerCorpus {
	t.Helper()
	corpus, err := LoadTriggerCorpus()
	if err != nil {
		t.Fatal(err)
	}
	return corpus
}

func TestReferenceAdaptersPassTriggerConformance(t *testing.T) {
	corpus := loadCorpus(t)
	for _, driver := range []*reference{memoryReference(""), durableReference(""), queueReference("")} {
		t.Run(driver.declaration.Adapter, func(t *testing.T) {
			report, err := RunTrigger(context.Background(), driver, corpus, fastOptions)
			if err != nil {
				t.Fatal(err)
			}
			applicable := 0
			for _, result := range report.Results {
				if result.Applicable {
					applicable++
				}
			}
			if applicable == 0 || len(report.Results) != len(corpus.Cases) {
				t.Fatalf("report=%+v", report)
			}
		})
	}
}

func TestBrokenAdaptersFailTriggerConformance(t *testing.T) {
	corpus := loadCorpus(t)
	for _, tc := range []struct {
		name   string
		driver TriggerDriver
		code   string
	}{
		{"memory work declared as detach", memoryReference("bad-declaration"), "invalid_declaration"},
		{"goroutine started by construction", memoryReference("construction-goroutine"), "construction_side_effect"},
		{"listener opened by construction", memoryReference("listen-at-construction"), "listener_before_start"},
		{"adapter retries a failed invocation", memoryReference("retry"), "dispatch_count_mismatch"},
		{"principal taken from caller data", memoryReference("payload-principal"), "principal_mismatch"},
		{"dispatch before validation", memoryReference("skip-validation"), "outcome_mismatch"},
		{"dispatch without authentication", memoryReference("skip-auth"), "outcome_mismatch"},
		{"memory work survives disconnect", memoryReference("ignore-disconnect"), "cancellation_mismatch"},
		{"internal error text exposed", memoryReference("leak-error"), "error_leak"},
		{"domain code replaced", memoryReference("wrong-error-code"), "code_mismatch"},
		{"memory completion without output", memoryReference("swallow-output"), "output_mismatch"},
		{"goroutine leaked after stop", memoryReference("leak-goroutine"), "goroutine_leak"},
		{"dispatch after stop", memoryReference("dispatch-after-stop"), "dispatch_after_stop"},
		{"durable adapter without deduplication", durableReference("no-dedup"), "outcome_mismatch"},
		{"durable acknowledgment before commit", durableReference("ack-before-commit"), "ack_before_commit"},
		{"detached work canceled by disconnect", durableReference("cancel-on-detach"), "cancellation_mismatch"},
		{"durable adapter without restart", withoutRestart{durableReference("")}, "capability_missing"},
		{"caller protocol declared as trusted producer", memoryReference("unauthenticated-http"), "invalid_declaration"},
		{"queue dead-letters saturation", queueReference("dead-letter-saturation"), "outcome_mismatch"},
		{"queue acknowledges a lost consumer", queueReference("ack-lost-consumer"), "effect_count_mismatch"},
		{"queue retries inside a claim", queueReference("retry"), "dispatch_count_mismatch"},
		{"queue fabricates a principal", queueReference("fabricate-principal"), "principal_mismatch"},
		{"queue without deduplication", queueReference("no-dedup"), "outcome_mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := RunTrigger(context.Background(), tc.driver, corpus, fastOptions)
			var failure *TriggerFailure
			if !errors.As(err, &failure) || failure.Code != tc.code {
				t.Fatalf("err=%v, want failure %s", err, tc.code)
			}
		})
	}
}

func TestRunTriggerRefusesWeakenedCorpus(t *testing.T) {
	trimmed := loadCorpus(t)
	trimmed.Cases = trimmed.Cases[:1]
	altered := loadCorpus(t)
	altered.Cases[0].Expect.Dispatches++
	future := loadCorpus(t)
	future.Version++
	for _, tc := range []struct {
		name   string
		corpus TriggerCorpus
		code   string
	}{
		{"empty", TriggerCorpus{}, "corpus_mismatch"},
		{"future version", future, "corpus_mismatch"},
		{"trimmed", trimmed, "corpus_incomplete"},
		{"altered expectation", altered, "corpus_incomplete"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := RunTrigger(context.Background(), memoryReference(""), tc.corpus, fastOptions)
			var failure *TriggerFailure
			if !errors.As(err, &failure) || failure.Code != tc.code {
				t.Fatalf("err=%v, want %s", err, tc.code)
			}
		})
	}
	extended := loadCorpus(t)
	extra := extended.Cases[0]
	extra.ID = "adapter-specific-extra"
	extended.Cases = append(extended.Cases, extra)
	if _, err := RunTrigger(context.Background(), memoryReference(""), extended, fastOptions); err != nil {
		t.Fatalf("an added case was refused: %v", err)
	}
}

func TestTriggerCorpusIsSelfConsistent(t *testing.T) {
	corpus := loadCorpus(t)
	seen := map[string]bool{}
	for _, c := range corpus.Cases {
		if c.ID == "" || seen[c.ID] {
			t.Fatalf("missing or duplicate case id %q", c.ID)
		}
		seen[c.ID] = true
		want := len(c.Deliveries)
		if c.Recover {
			want++
		}
		if len(c.Expect.Outcomes) != want || len(c.Expect.Codes) != want {
			t.Fatalf("case %s: %d outcomes/%d codes for %d source observations", c.ID, len(c.Expect.Outcomes), len(c.Expect.Codes), want)
		}
		counts := map[OutcomeKind]int{}
		for _, outcome := range c.Expect.Outcomes {
			counts[outcome]++
		}
		if counts[Completed] != c.Expect.Output || counts[Rejected] != c.Expect.Errors || counts[Duplicate] != c.Expect.Duplicates {
			t.Fatalf("case %s: declared counts disagree with declared outcomes", c.ID)
		}
		// Every case must apply to at least one declaration the contract allows.
		reachable := false
		for _, kind := range trigger.Kinds() {
			for _, completion := range []trigger.Completion{trigger.Memory, trigger.Durable} {
				for _, disconnect := range []trigger.Disconnect{trigger.CancelWork, trigger.StopWaiting, trigger.Redeliver} {
					for _, auth := range []trigger.Authentication{trigger.Caller, trigger.TrustedProducer} {
						d := trigger.Declaration{Kind: kind, Adapter: "x", Completion: completion, Disconnect: disconnect, Authentication: auth}
						if d.Validate() == nil && applicable(c.Applies, d) == "" {
							reachable = true
						}
					}
				}
			}
		}
		if !reachable {
			t.Fatalf("case %s applies to no valid declaration", c.ID)
		}
	}
}
