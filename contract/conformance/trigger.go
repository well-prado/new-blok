package conformance

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/well-prado/new-blok/trigger"
)

// TriggerCorpusVersion is the version of the embedded trigger case corpus.
const TriggerCorpusVersion = 1

//go:embed testdata/trigger-cases.json
var triggerCorpus []byte

// OutcomeKind is what the protocol source observed for one delivery.
type OutcomeKind string

const (
	Completed    OutcomeKind = "completed"
	Duplicate    OutcomeKind = "duplicate"
	Rejected     OutcomeKind = "rejected"
	Deferred     OutcomeKind = "deferred"
	Disconnected OutcomeKind = "disconnected"
)

// Workflow behaviors a case script can request for successive dispatches.
const (
	BehaviorSucceed       = "succeed"
	BehaviorDomainError   = "domain-error"
	BehaviorInternalError = "internal-error"
	BehaviorSaturated     = "saturated"
	BehaviorBlock         = "block"
	BehaviorDetach        = "detach"
)

// SecretMarker appears only inside the internal-error case's error text. A
// source-visible outcome containing it leaked an internal detail.
const SecretMarker = "synthetic-secret-detail"

// PrincipalID is the identity ValidCredential authenticates as.
const (
	ValidCredential = "valid"
	PrincipalID     = "user-1"
	DomainErrorCode = "out_of_stock"
)

// TriggerCase is one versioned scenario with predeclared counts.
type TriggerCase struct {
	ID          string            `json:"id"`
	Description string            `json:"description"`
	Applies     Applies           `json:"applies"`
	Script      []string          `json:"script"`
	Deliveries  []TriggerDelivery `json:"deliveries"`
	Recover     bool              `json:"recover"`
	Expect      TriggerExpect     `json:"expect"`
}

// Applies restricts a case to adapters whose declaration matches. Empty
// fields match any declaration; a filtered-out case is reported, not passed.
type Applies struct {
	Completion     trigger.Completion     `json:"completion,omitempty"`
	Disconnect     trigger.Disconnect     `json:"disconnect,omitempty"`
	Authentication trigger.Authentication `json:"authentication,omitempty"`
}

type TriggerDelivery struct {
	Key           string          `json:"key"`
	Payload       json.RawMessage `json:"payload,omitempty"`
	Credential    string          `json:"credential,omitempty"`
	Disconnect    bool            `json:"disconnect,omitempty"`
	RestartBefore bool            `json:"restartBefore,omitempty"`
}

type TriggerExpect struct {
	Outcomes   []OutcomeKind `json:"outcomes"`
	Codes      []string      `json:"codes"`
	Output     int           `json:"output"`
	Errors     int           `json:"errors"`
	Effects    int           `json:"effects"`
	Dispatches int           `json:"dispatches"`
	Duplicates int           `json:"duplicates"`
	// Principal is the identity every dispatch must observe for caller
	// authentication. Trusted-producer adapters must pass no principal.
	Principal string `json:"principal"`
	Canceled  bool   `json:"canceled"`
}

// TriggerCorpus is the embedded case corpus and the domain input schema its
// payloads are written against.
type TriggerCorpus struct {
	Version     int             `json:"version"`
	InputSchema json.RawMessage `json:"inputSchema"`
	Cases       []TriggerCase   `json:"cases"`
}

// LoadTriggerCorpus returns a copy of the embedded trigger corpus.
func LoadTriggerCorpus() (TriggerCorpus, error) {
	var corpus TriggerCorpus
	if err := json.Unmarshal(triggerCorpus, &corpus); err != nil {
		return TriggerCorpus{}, err
	}
	if corpus.Version != TriggerCorpusVersion {
		return TriggerCorpus{}, fmt.Errorf("trigger corpus version %d, want %d", corpus.Version, TriggerCorpusVersion)
	}
	return corpus, nil
}

// Call is what an adapter hands the workflow after authentication, mapping and
// validation.
type Call struct {
	Input     json.RawMessage
	Principal trigger.Principal
}

// Workflow is the harness-owned admission handler an adapter must invoke.
type Workflow func(context.Context, Call) (json.RawMessage, error)

// TriggerEnv is what the harness gives a driver when it opens the adapter.
type TriggerEnv struct {
	Workflow Workflow
	// Authenticate resolves an opaque credential carried by the protocol. It
	// is the only valid source of a principal.
	Authenticate func(credential string) (trigger.Principal, error)
	// InputSchema is the domain schema the binding validates before dispatch.
	InputSchema json.RawMessage
}

// Delivery is one protocol delivery the driver performs against its adapter.
type Delivery struct {
	Key        string
	Payload    json.RawMessage
	Credential string
	// Disconnect, when non-nil, is closed once the workflow is running. The
	// driver must then make its caller or consumer go away the way the
	// protocol does (cancel the request, drop the consumer) and report
	// Disconnected.
	Disconnect <-chan struct{}
}

// Outcome is what the source observed. Code is a stable public error code;
// Message is any source-visible error text.
type Outcome struct {
	Kind    OutcomeKind
	Code    string
	Output  json.RawMessage
	Message string
}

// TriggerDriver is implemented by the adapter author. It drives the real
// adapter through its real protocol; it must not call the workflow itself.
type TriggerDriver interface {
	Declaration() trigger.Declaration
	// Open constructs the adapter. It must not start goroutines or listeners.
	Open(context.Context, TriggerEnv) error
	Start(context.Context) error
	Deliver(context.Context, Delivery) (Outcome, error)
	Stop(context.Context) error
}

// Endpoint is implemented by drivers whose adapter listens. Before Start and
// after Stop it must return "".
type Endpoint interface{ Endpoint() string }

// Recoverer is required for redeliver adapters: it lets the source redeliver
// whatever was not acknowledged (for example after a lease or backoff) and
// reports the outcome of that redelivery.
type Recoverer interface {
	Recover(context.Context) (Outcome, error)
}

// Restarter is required for durable adapters: it restarts the adapter over
// the same durable state.
type Restarter interface{ Restart(context.Context) error }

// EffectLedger is implemented by durable drivers that commit an effect record
// in the acknowledgment transaction. Effects are then counted from committed
// state instead of from successful workflow returns.
type EffectLedger interface {
	CommittedEffects(context.Context) (int, error)
}

// TriggerOptions bounds harness waits.
type TriggerOptions struct {
	// CancelTimeout bounds how long a blocked workflow waits for
	// cancellation before the case fails.
	CancelTimeout time.Duration
	// SettleTimeout bounds how long goroutines may take to exit after Stop.
	SettleTimeout time.Duration
}

// TriggerFailure is a stable conformance failure.
type TriggerFailure struct {
	Case    string `json:"case,omitempty"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (f *TriggerFailure) Error() string {
	if f.Case == "" {
		return f.Code + ": " + f.Message
	}
	return "case " + f.Case + ": " + f.Code + ": " + f.Message
}

type TriggerResult struct {
	CaseID     string        `json:"caseId"`
	Applicable bool          `json:"applicable"`
	Reason     string        `json:"reason,omitempty"`
	Outcomes   []OutcomeKind `json:"outcomes,omitempty"`
	Output     int           `json:"output"`
	Errors     int           `json:"errors"`
	Effects    int           `json:"effects"`
	Dispatches int           `json:"dispatches"`
	Duplicates int           `json:"duplicates"`
}

type TriggerReport struct {
	Declaration trigger.Declaration `json:"declaration"`
	Results     []TriggerResult     `json:"results"`
}

// RunTrigger verifies one adapter against its declaration and the corpus. It
// measures process-wide goroutines, so callers must not run it in parallel
// with other tests.
func RunTrigger(ctx context.Context, driver TriggerDriver, corpus TriggerCorpus, options TriggerOptions) (TriggerReport, error) {
	if driver == nil {
		return TriggerReport{}, &TriggerFailure{Code: "driver_required", Message: "a trigger driver is required"}
	}
	if options.CancelTimeout <= 0 {
		options.CancelTimeout = 5 * time.Second
	}
	if options.SettleTimeout <= 0 {
		options.SettleTimeout = 5 * time.Second
	}
	declaration := driver.Declaration()
	report := TriggerReport{Declaration: declaration}
	if err := declaration.Validate(); err != nil {
		return report, &TriggerFailure{Code: "invalid_declaration", Message: err.Error()}
	}
	if _, ok := driver.(Restarter); declaration.Completion == trigger.Durable && !ok {
		return report, &TriggerFailure{Code: "capability_missing", Message: "durable adapters must support restart over the same state"}
	}
	if _, ok := driver.(EffectLedger); declaration.Completion == trigger.Durable && !ok {
		return report, &TriggerFailure{Code: "capability_missing", Message: "durable adapters must report committed effects so acknowledgment order is checked"}
	}
	if _, ok := driver.(Recoverer); declaration.Disconnect == trigger.Redeliver && !ok {
		return report, &TriggerFailure{Code: "capability_missing", Message: "redeliver adapters must support recovery of unacknowledged deliveries"}
	}
	if err := checkCorpus(corpus); err != nil {
		return report, err
	}
	h := &harness{options: options}
	env := TriggerEnv{Workflow: h.workflow, Authenticate: authenticate, InputSchema: append(json.RawMessage(nil), corpus.InputSchema...)}

	baseline := quiescentGoroutines(2 * time.Second)
	if err := driver.Open(ctx, env); err != nil {
		return report, fmt.Errorf("open: %w", err)
	}
	if extra := settle(baseline, 50*time.Millisecond); extra != "" {
		return report, &TriggerFailure{Code: "construction_side_effect", Message: "constructing the adapter started a goroutine: " + extra}
	}
	if endpoint, ok := driver.(Endpoint); ok && endpoint.Endpoint() != "" {
		return report, &TriggerFailure{Code: "listener_before_start", Message: "constructing the adapter opened a listener"}
	}
	if err := driver.Start(ctx); err != nil {
		return report, fmt.Errorf("start: %w", err)
	}
	stopped := false
	defer func() {
		if !stopped {
			_ = driver.Stop(context.Background())
		}
	}()

	cases := append([]TriggerCase(nil), corpus.Cases...)
	sort.Slice(cases, func(i, j int) bool { return cases[i].ID < cases[j].ID })
	ran := 0
	for _, c := range cases {
		if reason := applicable(c.Applies, declaration); reason != "" {
			report.Results = append(report.Results, TriggerResult{CaseID: c.ID, Reason: reason})
			continue
		}
		result, err := h.run(ctx, driver, declaration, c)
		if err != nil {
			return report, err
		}
		ran++
		report.Results = append(report.Results, result)
	}
	if ran == 0 {
		return report, &TriggerFailure{Code: "no_applicable_cases", Message: "no corpus case applies to the declaration"}
	}

	stopped = true
	if err := driver.Stop(ctx); err != nil {
		return report, fmt.Errorf("stop: %w", err)
	}
	if endpoint, ok := driver.(Endpoint); ok && endpoint.Endpoint() != "" {
		return report, &TriggerFailure{Code: "listener_after_stop", Message: "the adapter still listens after Stop"}
	}
	h.reset([]string{BehaviorSucceed})
	if outcome, err := driver.Deliver(ctx, Delivery{Key: "after-stop", Payload: json.RawMessage(`{"sku":"coffee","quantity":1}`), Credential: ValidCredential}); err == nil && outcome.Kind != Rejected {
		return report, &TriggerFailure{Code: "dispatch_after_stop", Message: fmt.Sprintf("a delivery after Stop was %s", outcome.Kind)}
	}
	if dispatches, _ := h.counts(); dispatches != 0 {
		return report, &TriggerFailure{Code: "dispatch_after_stop", Message: "a delivery after Stop reached the workflow"}
	}
	if extra := settle(baseline, options.SettleTimeout); extra != "" {
		return report, &TriggerFailure{Code: "goroutine_leak", Message: "a goroutine started during the run outlived Stop: " + extra}
	}
	return report, nil
}

func applicable(applies Applies, d trigger.Declaration) string {
	if applies.Completion != "" && applies.Completion != d.Completion {
		return fmt.Sprintf("applies to %s completion; adapter declares %s", applies.Completion, d.Completion)
	}
	if applies.Disconnect != "" && applies.Disconnect != d.Disconnect {
		return fmt.Sprintf("applies to %s disconnect; adapter declares %s", applies.Disconnect, d.Disconnect)
	}
	if applies.Authentication != "" && applies.Authentication != d.Authentication {
		return fmt.Sprintf("applies to %s authentication; adapter declares %s", applies.Authentication, d.Authentication)
	}
	return ""
}

func authenticate(credential string) (trigger.Principal, error) {
	if credential != ValidCredential {
		return trigger.Principal{}, errors.New("unauthenticated")
	}
	return trigger.Principal{ID: PrincipalID}, nil
}

// domainFailure is a classified business failure the adapter must surface by
// code without retrying.
type domainFailure struct{}

func (domainFailure) Error() string      { return DomainErrorCode + ": item unavailable" }
func (domainFailure) ErrorCode() string  { return DomainErrorCode }
func (domainFailure) ErrorClass() string { return "validation" }

type dispatch struct {
	input     json.RawMessage
	principal trigger.Principal
	canceled  bool
	succeeded bool
}

type harness struct {
	options    TriggerOptions
	mu         sync.Mutex
	script     []string
	dispatches []dispatch
	disconnect chan struct{}
	inflight   int
}

func (h *harness) reset(script []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.script = append([]string(nil), script...)
	if len(h.script) == 0 {
		h.script = []string{BehaviorSucceed}
	}
	h.dispatches = nil
	h.disconnect = nil
}

func (h *harness) counts() (dispatches, succeeded int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, d := range h.dispatches {
		if d.succeeded {
			succeeded++
		}
	}
	return len(h.dispatches), succeeded
}

func (h *harness) workflow(ctx context.Context, call Call) (json.RawMessage, error) {
	h.mu.Lock()
	h.inflight++
	defer func() { h.mu.Lock(); h.inflight--; h.mu.Unlock() }()
	index := len(h.dispatches)
	behavior := h.script[len(h.script)-1]
	if index < len(h.script) {
		behavior = h.script[index]
	}
	h.dispatches = append(h.dispatches, dispatch{input: append(json.RawMessage(nil), call.Input...), principal: call.Principal})
	disconnect := h.disconnect
	h.disconnect = nil
	h.mu.Unlock()

	record := func(canceled, succeeded bool) {
		h.mu.Lock()
		h.dispatches[index].canceled = canceled
		h.dispatches[index].succeeded = succeeded
		h.mu.Unlock()
	}
	signal := func() {
		if disconnect != nil {
			close(disconnect)
		}
	}
	switch behavior {
	case BehaviorSucceed:
		record(false, true)
		return echo(call.Input), nil
	case BehaviorDomainError:
		record(false, false)
		return nil, domainFailure{}
	case BehaviorInternalError:
		record(false, false)
		return nil, errors.New(SecretMarker + ": dsn=postgres://synthetic")
	case BehaviorSaturated:
		record(false, false)
		return nil, trigger.ErrSaturated
	case BehaviorBlock:
		signal()
		select {
		case <-ctx.Done():
			record(true, false)
			return nil, ctx.Err()
		case <-time.After(h.options.CancelTimeout):
			record(false, false)
			return nil, errors.New("workflow was not canceled")
		}
	case BehaviorDetach:
		signal()
		select {
		case <-ctx.Done():
			record(true, false)
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
			record(false, true)
			return echo(call.Input), nil
		}
	default:
		record(false, false)
		return nil, fmt.Errorf("unknown script behavior %q", behavior)
	}
}

func echo(input json.RawMessage) json.RawMessage {
	return json.RawMessage(`{"echo":` + string(input) + `}`)
}

func (h *harness) run(ctx context.Context, driver TriggerDriver, d trigger.Declaration, c TriggerCase) (TriggerResult, error) {
	fail := func(code, format string, args ...any) (TriggerResult, error) {
		return TriggerResult{}, &TriggerFailure{Case: c.ID, Code: code, Message: fmt.Sprintf(format, args...)}
	}
	h.reset(c.Script)
	// Committed effects are the durable acknowledgment evidence; memory
	// adapters are measured by successful workflow returns.
	ledger, hasLedger := driver.(EffectLedger)
	hasLedger = hasLedger && d.Completion == trigger.Durable
	ledgerStart := 0
	if hasLedger {
		count, err := ledger.CommittedEffects(ctx)
		if err != nil {
			return TriggerResult{}, fmt.Errorf("case %s: ledger: %w", c.ID, err)
		}
		ledgerStart = count
	}
	var outcomes []Outcome
	completed := 0
	restarter, canRestart := driver.(Restarter)
	recoverer, canRecover := driver.(Recoverer)
	if c.Recover && !canRecover {
		return fail("capability_missing", "the case requires recovery of unacknowledged deliveries")
	}
	for _, delivery := range c.Deliveries {
		if delivery.RestartBefore {
			if !canRestart {
				return fail("capability_missing", "the case requires restart over the same state")
			}
			if err := restarter.Restart(ctx); err != nil {
				return TriggerResult{}, fmt.Errorf("case %s: restart: %w", c.ID, err)
			}
		}
		request := Delivery{Key: delivery.Key, Payload: append(json.RawMessage(nil), delivery.Payload...), Credential: delivery.Credential}
		if delivery.Disconnect {
			channel := make(chan struct{})
			h.mu.Lock()
			h.disconnect = channel
			h.mu.Unlock()
			request.Disconnect = channel
		}
		outcome, err := driver.Deliver(ctx, request)
		if err != nil {
			return TriggerResult{}, fmt.Errorf("case %s: deliver: %w", c.ID, err)
		}
		outcomes = append(outcomes, outcome)
		if outcome.Kind == Completed {
			completed++
			if hasLedger {
				count, err := ledger.CommittedEffects(ctx)
				if err != nil {
					return TriggerResult{}, fmt.Errorf("case %s: ledger: %w", c.ID, err)
				}
				if count-ledgerStart < completed {
					return fail("ack_before_commit", "the source observed completion before the effect was committed")
				}
			}
		}
	}
	if !h.idle(h.options.CancelTimeout + time.Second) {
		return fail("dispatch_outlived_delivery", "a workflow invocation did not finish")
	}
	if c.Recover {
		for index, outcome := range outcomes {
			if index >= len(c.Expect.Outcomes) || outcome.Kind != c.Expect.Outcomes[index] {
				return fail("outcome_mismatch", "outcome %d was %s before recovery; want outcomes %v", index, outcome.Kind, c.Expect.Outcomes)
			}
		}
		outcome, err := recoverer.Recover(ctx)
		if err != nil {
			return fail("recover_failed", "redelivery of the unacknowledged delivery failed: %v", err)
		}
		outcomes = append(outcomes, outcome)
		if !h.idle(h.options.CancelTimeout + time.Second) {
			return fail("dispatch_outlived_delivery", "a recovered workflow invocation did not finish")
		}
	}

	result := TriggerResult{CaseID: c.ID, Applicable: true}
	for _, outcome := range outcomes {
		result.Outcomes = append(result.Outcomes, outcome.Kind)
		switch outcome.Kind {
		case Completed:
			result.Output++
		case Rejected:
			result.Errors++
		case Duplicate:
			result.Duplicates++
		}
	}
	if !reflect.DeepEqual(result.Outcomes, c.Expect.Outcomes) {
		return fail("outcome_mismatch", "outcomes %v, want %v", result.Outcomes, c.Expect.Outcomes)
	}
	for index, outcome := range outcomes {
		want := ""
		if index < len(c.Expect.Codes) {
			want = c.Expect.Codes[index]
		}
		if outcome.Code != want {
			return fail("code_mismatch", "outcome %d code %q, want %q", index, outcome.Code, want)
		}
		if leaks(outcome) {
			return fail("error_leak", "outcome %d exposed internal error text to the source", index)
		}
		if outcome.Kind == Completed && len(outcome.Output) > 0 && !sameJSON(outcome.Output, echo(c.Deliveries[min(index, len(c.Deliveries)-1)].Payload)) {
			return fail("output_mismatch", "outcome %d output %s is not the workflow output", index, outcome.Output)
		}
		if outcome.Kind == Completed && d.Completion == trigger.Memory && len(outcome.Output) == 0 {
			return fail("output_mismatch", "memory completion must return the workflow output in band")
		}
	}

	h.mu.Lock()
	dispatches := append([]dispatch(nil), h.dispatches...)
	h.mu.Unlock()
	wantPrincipal := c.Expect.Principal
	if d.Authentication == trigger.TrustedProducer {
		wantPrincipal = ""
	}
	payloads := map[string]bool{}
	for _, delivery := range c.Deliveries {
		payloads[string(canonical(delivery.Payload))] = true
	}
	canceled := false
	for index, call := range dispatches {
		if call.principal.ID != wantPrincipal {
			return fail("principal_mismatch", "dispatch %d principal %q, want %q", index, call.principal.ID, wantPrincipal)
		}
		if !payloads[string(canonical(call.input))] {
			return fail("input_mismatch", "dispatch %d input %s was not a delivered payload", index, call.input)
		}
		if call.succeeded {
			result.Effects++
		}
		canceled = canceled || call.canceled
	}
	if canceled != c.Expect.Canceled {
		return fail("cancellation_mismatch", "workflow observed cancellation=%v, want %v", canceled, c.Expect.Canceled)
	}
	if hasLedger {
		count, err := ledger.CommittedEffects(ctx)
		if err != nil {
			return TriggerResult{}, fmt.Errorf("case %s: ledger: %w", c.ID, err)
		}
		result.Effects = count - ledgerStart
	}
	result.Dispatches = len(dispatches)
	for _, check := range []struct {
		name      string
		got, want int
	}{
		{"dispatch", result.Dispatches, c.Expect.Dispatches},
		{"output", result.Output, c.Expect.Output},
		{"error", result.Errors, c.Expect.Errors},
		{"effect", result.Effects, c.Expect.Effects},
		{"duplicate", result.Duplicates, c.Expect.Duplicates},
	} {
		if check.got != check.want {
			return fail(check.name+"_count_mismatch", "%s count %d, want %d", check.name, check.got, check.want)
		}
	}
	return result, nil
}

func leaks(outcome Outcome) bool {
	return strings.Contains(outcome.Message, SecretMarker) || strings.Contains(outcome.Code, SecretMarker) || bytes.Contains(outcome.Output, []byte(SecretMarker))
}

func canonical(data json.RawMessage) []byte {
	if len(bytes.TrimSpace(data)) == 0 {
		return []byte("null")
	}
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return data
	}
	out, err := json.Marshal(value)
	if err != nil {
		return data
	}
	return out
}

func sameJSON(a, b json.RawMessage) bool { return bytes.Equal(canonical(a), canonical(b)) }

// goroutines returns the live goroutines by id, each with its stack. Tracking
// identity rather than a count means an unrelated goroutine exiting during the
// run cannot hide one the adapter started.
func goroutines() map[string]string {
	buffer := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buffer, true)
		if n < len(buffer) {
			buffer = buffer[:n]
			break
		}
		buffer = make([]byte, 2*len(buffer))
	}
	live := map[string]string{}
	for _, block := range strings.Split(string(buffer), "\n\n") {
		header, _, _ := strings.Cut(block, "\n")
		fields := strings.Fields(header)
		if len(fields) >= 2 && fields[0] == "goroutine" {
			live[fields[1]] = block
		}
	}
	return live
}

// quiescentGoroutines waits briefly for goroutines left by earlier work to
// exit, then records the baseline set.
func quiescentGoroutines(timeout time.Duration) map[string]string {
	deadline := time.Now().Add(timeout)
	last, stable := goroutines(), 0
	for stable < 5 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		current := goroutines()
		if len(current) == len(last) {
			stable++
		} else {
			stable = 0
		}
		last = current
	}
	return last
}

// settle waits until no goroutine outside baseline is alive. It returns the
// stack of one that remains, or "".
func settle(baseline map[string]string, timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	for {
		extra := ""
		for id, stack := range goroutines() {
			if _, ok := baseline[id]; !ok {
				extra = stack
				break
			}
		}
		if extra == "" {
			return ""
		}
		if time.Now().After(deadline) {
			if len(extra) > 600 {
				extra = extra[:600]
			}
			return extra
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// checkCorpus requires the supplied corpus to contain every embedded case
// unchanged. Adapters may add cases; they cannot drop or weaken one.
func checkCorpus(corpus TriggerCorpus) error {
	embedded, err := LoadTriggerCorpus()
	if err != nil {
		return err
	}
	if corpus.Version != embedded.Version || !sameJSON(corpus.InputSchema, embedded.InputSchema) {
		return &TriggerFailure{Code: "corpus_mismatch", Message: fmt.Sprintf("corpus version %d or input schema differs from the embedded version %d corpus", corpus.Version, embedded.Version)}
	}
	supplied := map[string]TriggerCase{}
	for _, c := range corpus.Cases {
		supplied[c.ID] = c
	}
	for _, want := range embedded.Cases {
		got, ok := supplied[want.ID]
		if !ok {
			return &TriggerFailure{Case: want.ID, Code: "corpus_incomplete", Message: "the corpus omits a required case"}
		}
		gotJSON, _ := json.Marshal(got)
		wantJSON, _ := json.Marshal(want)
		if !bytes.Equal(gotJSON, wantJSON) {
			return &TriggerFailure{Case: want.ID, Code: "corpus_incomplete", Message: "the corpus changes a required case"}
		}
	}
	return nil
}

func (h *harness) idle(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		h.mu.Lock()
		inflight := h.inflight
		h.mu.Unlock()
		if inflight == 0 {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}
