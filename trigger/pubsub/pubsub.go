// Package pubsub transfers broker messages into durable submission.
//
// A message is the broker's until its submission is committed, and Blok's
// afterwards: the consumer acknowledges the broker only after
// trigger.Submitter returns. A crash or lost connection before that ack leaves
// the message with the broker, which redelivers it; the stable message
// identity makes the redelivery a duplicate submission, so it never creates a
// second run. Execution retries belong to the durable queue, not to the
// broker.
//
// Each message is checked in a fixed order: size, identity, input schema, then
// submission. A message that can never be admitted is recorded on the
// dead-letter destination before it is terminated; if that record cannot be
// written the message stays with the broker and is retried, so it is never
// dropped silently. Saturation is backpressure: the message is redelivered
// with backoff and never dead-lettered for it. Any other submission failure
// is retried within the delivery budget and then dead-lettered. Brokers must
// not cap deliveries themselves; the consumer owns the budget. This package
// imports no broker client: a broker driver implements Source.
package pubsub

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/trigger"
)

// Declaration is the pubsub contract: durable transfer, redelivery of
// anything not acknowledged, and a principal that comes from the
// subscription's configuration because publishers are trusted through the
// broker's own access control.
var Declaration = trigger.Declaration{Kind: trigger.PubSub, Adapter: "trigger/pubsub", Completion: trigger.Durable, Disconnect: trigger.Redeliver, Authentication: trigger.TrustedProducer}

const (
	DefaultMaxInFlight     = 16
	MaxInFlightLimit       = 1024
	DefaultMaxMessageBytes = 1 << 20
	MaxMessageBytesLimit   = 16 << 20
	DefaultMaxDeliver      = 5
	MaxDeliverLimit        = 100
	DefaultFetchWait       = time.Second
	DefaultSubmitTimeout   = 10 * time.Second
	maxRedeliveryDelay     = time.Minute
	// MaxMessageIDBytes bounds the stable message identity.
	MaxMessageIDBytes = 256
)

// Dead-letter reasons recorded with a terminated message.
const (
	ReasonTooLarge         = "too_large"
	ReasonInvalidID        = "invalid_message_id"
	ReasonInvalidInput     = "invalid_input"
	ReasonConflict         = "conflict"
	ReasonBudgetExhausted  = "delivery_budget_exhausted"
	OutcomeAccepted        = "accepted"
	OutcomeDuplicate       = "duplicate"
	OutcomeDeferred        = "deferred"
	OutcomeDeadLettered    = "dead_lettered"
	OutcomeAckFailed       = "ack_failed"
	OutcomeDeadLetterRetry = "dead_letter_failed"
)

// Message is one broker delivery.
type Message interface {
	// ID is the stable identity of the stored message: the same value on
	// every redelivery (a publisher-set id, or the broker's sequence).
	ID() string
	Data() []byte
	// Attempt is the 1-based delivery count reported by the broker.
	Attempt() int
	// Cursor is the broker position, for diagnostics and dead letters.
	Cursor() string
	// Ack must return only after the broker has confirmed the ack.
	Ack(context.Context) error
	Nak(ctx context.Context, delay time.Duration) error
	Term(ctx context.Context, reason string) error
}

// Source is a broker subscription. Fetch must never return more than max
// messages; a driver also enforces the bound broker-side (outstanding
// unacknowledged messages), so a slow consumer cannot accumulate work.
type Source interface {
	Fetch(ctx context.Context, max int, wait time.Duration) ([]Message, error)
	// DeadLetter durably records a message and its reason (for example a
	// publish the broker acknowledged) before the consumer terminates it.
	DeadLetter(ctx context.Context, message Message, reason string) error
}

// Subscription binds a broker source to a durable submission kind.
type Subscription struct {
	// Name namespaces message identities: submissions are keyed
	// "pubsub:<name>:<message id>".
	Name      string
	Principal trigger.Principal
	Kind      string
	Submit    trigger.Submitter
	// InputSchema validates the message body before submission.
	InputSchema     []byte
	MaxInFlight     int
	MaxMessageBytes int
	// MaxDeliver is the number of deliveries after which a message whose
	// submission keeps failing (other than for saturation) is
	// dead-lettered. It is checked only after a submission attempt, so a
	// redelivered message that is already committed is acknowledged, not
	// dead-lettered.
	MaxDeliver    int
	FetchWait     time.Duration
	SubmitTimeout time.Duration
}

// Outcome reports what happened to one message.
type Outcome struct {
	MessageID string
	Cursor    string
	Attempt   int
	Result    string
	Reason    string
}

type Consumer struct {
	source Source
	sub    Subscription
	input  schema.Schema
	// observe, when set, receives every outcome.
	observe func(Outcome)
}

var subscriptionName = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,63}$`)
var messageID = regexp.MustCompile(`^[\x21-\x7e]{1,256}$`)

// New validates the subscription before anything is consumed. It starts no
// goroutine and fetches nothing.
func New(source Source, sub Subscription) (*Consumer, error) {
	if source == nil || sub.Submit == nil || sub.Kind == "" || !subscriptionName.MatchString(sub.Name) {
		return nil, fmt.Errorf("pubsub: subscription %q needs a source, name, kind and submitter", sub.Name)
	}
	if strings.TrimSpace(sub.Principal.ID) == "" {
		return nil, fmt.Errorf("pubsub: subscription %s needs a configured principal", sub.Name)
	}
	parsed, err := schema.Parse(sub.InputSchema)
	if err != nil {
		return nil, fmt.Errorf("pubsub: subscription %s schema: %w", sub.Name, err)
	}
	if sub.MaxInFlight <= 0 {
		sub.MaxInFlight = DefaultMaxInFlight
	}
	if sub.MaxMessageBytes <= 0 {
		sub.MaxMessageBytes = DefaultMaxMessageBytes
	}
	if sub.MaxDeliver <= 0 {
		sub.MaxDeliver = DefaultMaxDeliver
	}
	if sub.FetchWait <= 0 {
		sub.FetchWait = DefaultFetchWait
	}
	if sub.SubmitTimeout <= 0 {
		sub.SubmitTimeout = DefaultSubmitTimeout
	}
	if sub.MaxInFlight > MaxInFlightLimit || sub.MaxMessageBytes > MaxMessageBytesLimit || sub.MaxDeliver > MaxDeliverLimit {
		return nil, fmt.Errorf("pubsub: subscription %s exceeds the in-flight, size or delivery bound", sub.Name)
	}
	return &Consumer{source: source, sub: sub, input: parsed}, nil
}

// Subscription returns the validated subscription with defaults applied, so a
// broker driver can configure its consumer to match.
func (c *Consumer) Subscription() Subscription { return c.sub }

// SubmissionKey is the durable identity of a subscription's message.
func SubmissionKey(subscription, id string) string { return "pubsub:" + subscription + ":" + id }

// Observe registers a callback that receives every outcome.
func (c *Consumer) Observe(observe func(Outcome)) { c.observe = observe }

// Run processes batches until ctx ends and then returns ctx.Err(). A fetch or
// broker error ends Run so the caller can reconnect or restart; nothing
// unacknowledged is lost, because the broker redelivers it.
func (c *Consumer) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		if _, err := c.ProcessBatch(ctx); err != nil {
			if ctx.Err() != nil {
				break
			}
			return err
		}
	}
	return ctx.Err()
}

// ProcessBatch fetches at most MaxInFlight messages and processes them in
// order. Messages a fetch returns alongside an error are still processed.
// Every message gets its turn even if a broker call fails for an earlier one;
// the first error is returned after the batch. It returns how many messages
// it processed.
func (c *Consumer) ProcessBatch(ctx context.Context) (int, error) {
	messages, fetchErr := c.source.Fetch(ctx, c.sub.MaxInFlight, c.sub.FetchWait)
	if len(messages) > c.sub.MaxInFlight {
		return 0, fmt.Errorf("pubsub: source returned %d messages, more than the bound %d", len(messages), c.sub.MaxInFlight)
	}
	var first error
	for _, message := range messages {
		if err := c.process(ctx, message); err != nil && first == nil {
			first = err
		}
	}
	if fetchErr != nil {
		return len(messages), fmt.Errorf("pubsub: fetch: %w", fetchErr)
	}
	return len(messages), first
}

func (c *Consumer) process(parent context.Context, message Message) error {
	// Broker calls and submission are not canceled by shutdown: a message
	// in progress finishes its transfer, bounded by SubmitTimeout.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), c.sub.SubmitTimeout)
	defer cancel()
	outcome := Outcome{MessageID: message.ID(), Cursor: message.Cursor(), Attempt: message.Attempt()}
	report := func(result, reason string) {
		outcome.Result, outcome.Reason = result, reason
		if c.observe != nil {
			c.observe(outcome)
		}
	}
	reject := func(reason string) error {
		// Record first; terminate only once the record is durable.
		if err := c.source.DeadLetter(ctx, message, reason); err != nil {
			report(OutcomeDeadLetterRetry, reason)
			return message.Nak(ctx, c.delay(message.Attempt()))
		}
		report(OutcomeDeadLettered, reason)
		return message.Term(ctx, reason)
	}
	switch {
	case len(message.Data()) > c.sub.MaxMessageBytes:
		return reject(ReasonTooLarge)
	case !messageID.MatchString(message.ID()):
		return reject(ReasonInvalidID)
	}
	candidate := message.Data()
	if len(strings.TrimSpace(string(candidate))) == 0 {
		candidate = []byte("null")
	}
	if _, err := c.input.Normalize(candidate); err != nil {
		return reject(ReasonInvalidInput)
	}
	accepted, err := c.sub.Submit.Submit(ctx, trigger.Submission{Key: SubmissionKey(c.sub.Name, message.ID()), Kind: c.sub.Kind, Payload: message.Data(), Principal: c.sub.Principal})
	switch {
	case errors.Is(err, trigger.ErrConflict):
		return reject(ReasonConflict)
	case errors.Is(err, trigger.ErrInvalidInput):
		return reject(ReasonInvalidInput)
	case errors.Is(err, trigger.ErrSaturated):
		// Backpressure: the message is valid and simply waits.
		report(OutcomeDeferred, "")
		return message.Nak(ctx, c.delay(message.Attempt()))
	case err != nil && message.Attempt() >= c.sub.MaxDeliver:
		return reject(ReasonBudgetExhausted)
	case err != nil:
		// An unavailable store: leave the message with the broker and back
		// off, within the delivery budget.
		report(OutcomeDeferred, "")
		return message.Nak(ctx, c.delay(message.Attempt()))
	}
	if err := message.Ack(ctx); err != nil {
		// The submission is committed; the broker will redeliver and the
		// redelivery is a duplicate, acknowledged then.
		report(OutcomeAckFailed, "")
		return nil
	}
	if accepted {
		report(OutcomeAccepted, "")
	} else {
		report(OutcomeDuplicate, "")
	}
	return nil
}

func (c *Consumer) delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 7 {
		return maxRedeliveryDelay
	}
	return min(time.Second<<(attempt-1), maxRedeliveryDelay)
}
