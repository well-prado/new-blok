// Package natsjs is the NATS JetStream broker driver for trigger/pubsub.
//
// It consumes a durable pull consumer with explicit acknowledgment. The
// broker's acknowledgment floor is the durable cursor: a message is
// redelivered until it is acknowledged, an acknowledgment waits for broker
// confirmation (double ack), and a restarted consumer resumes after the
// acknowledged messages. The broker never caps deliveries (MaxDeliver -1):
// the consumer owns the delivery budget, so the broker cannot give up on a
// message before it is admitted or recorded as a dead letter. The broker
// bounds outstanding unacknowledged messages to the subscription's in-flight
// limit across every instance sharing the durable name; those instances split
// the stream and take over each other's unacknowledged messages after the ack
// wait.
package natsjs

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/well-prado/new-blok/trigger"
	"github.com/well-prado/new-blok/trigger/pubsub"
)

// Declaration is the pubsub contract as provided by this driver.
var Declaration = trigger.Declaration{Kind: trigger.PubSub, Adapter: "trigger/pubsub/natsjs", Completion: trigger.Durable, Disconnect: trigger.Redeliver, Authentication: trigger.TrustedProducer}

const (
	DefaultAckWait = 30 * time.Second
	MinAckWait     = time.Second
	MaxAckWait     = 12 * time.Hour
	// DefaultIDHeader is the publisher-set id NATS also uses for its own
	// duplicate window.
	DefaultIDHeader = "Nats-Msg-Id"
)

// Dead-letter headers carry the reason and the original position.
const (
	HeaderReason       = "Blok-Dead-Letter-Reason"
	HeaderOriginalID   = "Blok-Original-Id"
	HeaderCursor       = "Blok-Original-Cursor"
	HeaderSubscription = "Blok-Subscription"
)

// Config names the broker resources a subscription consumes.
type Config struct {
	Stream   string
	Consumer string
	Subject  string
	// DeadLetterSubject must be stored by a stream and must not be matched
	// by Subject, or dead letters would be consumed again.
	DeadLetterSubject string
	AckWait           time.Duration
	// IDHeader names the header holding the publisher's stable message id.
	// Without it, the stream sequence identifies the message.
	IDHeader string
}

type Source struct {
	js       jetstream.JetStream
	consumer jetstream.Consumer
	config   Config
	name     string
	// incarnation identifies this stream instance (its creation time), so a
	// deleted and recreated stream's restarted sequence numbers cannot
	// collide with identities from the old one.
	incarnation int64
}

var resourceName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// NewConsumer validates the subscription and the broker configuration, then
// binds the durable consumer (creating it if absent). Nothing is fetched
// before every check passes; an existing consumer whose delivery settings
// differ is refused rather than silently changed.
func NewConsumer(ctx context.Context, js jetstream.JetStream, config Config, sub pubsub.Subscription) (*pubsub.Consumer, *Source, error) {
	if js == nil {
		return nil, nil, errors.New("natsjs: JetStream is required")
	}
	if config.AckWait <= 0 {
		config.AckWait = DefaultAckWait
	}
	if config.IDHeader == "" {
		config.IDHeader = DefaultIDHeader
	}
	if !resourceName.MatchString(config.Stream) || !resourceName.MatchString(config.Consumer) || !validSubject(config.Subject) || !validSubject(config.DeadLetterSubject) || strings.ContainsAny(config.DeadLetterSubject, "*>") {
		return nil, nil, errors.New("natsjs: stream, consumer, subject and dead-letter subject must be valid names")
	}
	if config.AckWait < MinAckWait || config.AckWait > MaxAckWait {
		return nil, nil, fmt.Errorf("natsjs: ack wait must be between %s and %s", MinAckWait, MaxAckWait)
	}
	if subjectMatches(config.Subject, config.DeadLetterSubject) {
		return nil, nil, errors.New("natsjs: the consumer's subject would consume its own dead letters")
	}
	source := &Source{js: js, config: config, name: sub.Name}
	consumer, err := pubsub.New(source, sub)
	if err != nil {
		return nil, nil, err
	}
	if err := source.bind(ctx, consumer.Subscription()); err != nil {
		return nil, nil, err
	}
	return consumer, source, nil
}

func (s *Source) desired(sub pubsub.Subscription) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:       s.config.Consumer,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       s.config.AckWait,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		FilterSubject: s.config.Subject,
		// Unlimited: the consumer owns the delivery budget and the broker
		// never drops a message that is not yet admitted or dead-lettered.
		MaxDeliver:      -1,
		MaxAckPending:   sub.MaxInFlight,
		MaxRequestBatch: sub.MaxInFlight,
		ReplayPolicy:    jetstream.ReplayInstantPolicy,
	}
}

// differences lists every delivery-relevant setting on which an existing
// consumer departs from the one this subscription needs.
func differences(got, want jetstream.ConsumerConfig) []string {
	var fields []string
	check := func(name string, differs bool) {
		if differs {
			fields = append(fields, name)
		}
	}
	check("AckPolicy", got.AckPolicy != want.AckPolicy)
	check("AckWait", got.AckWait != want.AckWait)
	check("DeliverPolicy", got.DeliverPolicy != want.DeliverPolicy)
	check("OptStartSeq", got.OptStartSeq != 0)
	check("OptStartTime", got.OptStartTime != nil)
	check("FilterSubject", got.FilterSubject != want.FilterSubject)
	check("FilterSubjects", len(got.FilterSubjects) > 0 && !(len(got.FilterSubjects) == 1 && got.FilterSubjects[0] == want.FilterSubject))
	check("MaxDeliver", got.MaxDeliver != want.MaxDeliver)
	check("BackOff", len(got.BackOff) > 0)
	check("ReplayPolicy", got.ReplayPolicy != want.ReplayPolicy)
	check("MaxAckPending", got.MaxAckPending != want.MaxAckPending)
	check("MaxRequestBatch", got.MaxRequestBatch != want.MaxRequestBatch)
	check("MaxRequestExpires", got.MaxRequestExpires != 0)
	check("MaxRequestMaxBytes", got.MaxRequestMaxBytes != 0)
	check("HeadersOnly", got.HeadersOnly)
	check("RateLimit", got.RateLimit != 0)
	check("InactiveThreshold", got.InactiveThreshold != 0)
	check("PauseUntil", got.PauseUntil != nil)
	check("PriorityGroups", len(got.PriorityGroups) > 0)
	check("DeliverSubject", got.DeliverSubject != "" || got.DeliverGroup != "")
	check("FlowControl", got.FlowControl || got.IdleHeartbeat != 0)
	return fields
}

func (s *Source) bind(ctx context.Context, sub pubsub.Subscription) error {
	stream, err := s.js.StreamNameBySubject(ctx, s.config.Subject)
	if err != nil || stream != s.config.Stream {
		return fmt.Errorf("natsjs: subject %s is not stored by stream %s", s.config.Subject, s.config.Stream)
	}
	handle, err := s.js.Stream(ctx, s.config.Stream)
	if err != nil {
		return fmt.Errorf("natsjs: stream: %w", err)
	}
	info, err := handle.Info(ctx)
	if err != nil {
		return fmt.Errorf("natsjs: stream info: %w", err)
	}
	s.incarnation = info.Created.UnixNano()
	if _, err := s.js.StreamNameBySubject(ctx, s.config.DeadLetterSubject); err != nil {
		return fmt.Errorf("natsjs: dead-letter subject %s is not stored by any stream", s.config.DeadLetterSubject)
	}
	want := s.desired(sub)
	existing, err := s.js.Consumer(ctx, s.config.Stream, s.config.Consumer)
	switch {
	case errors.Is(err, jetstream.ErrConsumerNotFound):
		consumer, err := s.js.CreateConsumer(ctx, s.config.Stream, want)
		if err != nil {
			return fmt.Errorf("natsjs: create consumer: %w", err)
		}
		s.consumer = consumer
		return nil
	case err != nil:
		return fmt.Errorf("natsjs: consumer: %w", err)
	}
	consumerInfo, err := existing.Info(ctx)
	if err != nil {
		return fmt.Errorf("natsjs: consumer info: %w", err)
	}
	if fields := differences(consumerInfo.Config, want); len(fields) > 0 {
		return fmt.Errorf("natsjs: consumer %s exists with different delivery settings (%s); refusing to consume", s.config.Consumer, strings.Join(fields, ", "))
	}
	s.consumer = existing
	return nil
}

// Fetch pulls at most limit messages, waiting at most wait.
func (s *Source) Fetch(_ context.Context, limit int, wait time.Duration) ([]pubsub.Message, error) {
	batch, err := s.consumer.Fetch(limit, jetstream.FetchMaxWait(wait))
	if err != nil {
		return nil, err
	}
	var messages []pubsub.Message
	for msg := range batch.Messages() {
		meta, err := msg.Metadata()
		if err != nil {
			return messages, err
		}
		messages = append(messages, &message{msg: msg, meta: meta, idHeader: s.config.IDHeader, incarnation: s.incarnation})
	}
	if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) {
		return messages, err
	}
	return messages, nil
}

// DeadLetter publishes the message and its reason to the dead-letter subject
// and returns only after the broker stored it.
func (s *Source) DeadLetter(ctx context.Context, m pubsub.Message, reason string) error {
	record := nats.NewMsg(s.config.DeadLetterSubject)
	record.Data = m.Data()
	record.Header.Set(HeaderReason, reason)
	record.Header.Set(HeaderOriginalID, m.ID())
	record.Header.Set(HeaderCursor, m.Cursor())
	record.Header.Set(HeaderSubscription, s.name)
	// The record id is unique per stored message (stream incarnation and
	// sequence), so only a redelivery of the same message deduplicates.
	record.Header.Set(nats.MsgIdHdr, "dlq:"+s.name+":"+m.Cursor())
	_, err := s.js.PublishMsg(ctx, record)
	return err
}

type message struct {
	msg         jetstream.Msg
	meta        *jetstream.MsgMetadata
	idHeader    string
	incarnation int64
}

// ID is "h:<publisher id>" when the publisher set one, else the stored
// position "s:<cursor>"; the prefixes keep the two spaces disjoint.
func (m *message) ID() string {
	if id := m.msg.Headers().Get(m.idHeader); id != "" {
		return "h:" + id
	}
	return "s:" + m.Cursor()
}
func (m *message) Data() []byte { return m.msg.Data() }

// TraceHeaders implements pubsub.TraceCarrier. NATS header names are case
// sensitive; every spelling of traceparent and tracestate is collected, so a
// message carrying two spellings is seen as a duplicate and ignored.
func (m *message) TraceHeaders() (traceparent, tracestate []string) {
	for name, values := range m.msg.Headers() {
		switch {
		case strings.EqualFold(name, trigger.TraceparentField):
			traceparent = append(traceparent, values...)
		case strings.EqualFold(name, trigger.TracestateField):
			tracestate = append(tracestate, values...)
		}
	}
	return traceparent, tracestate
}

var _ pubsub.TraceCarrier = (*message)(nil)

func (m *message) Attempt() int { return int(m.meta.NumDelivered) }
func (m *message) Cursor() string {
	return fmt.Sprintf("%s@%d:%d", m.meta.Stream, m.incarnation, m.meta.Sequence.Stream)
}
func (m *message) Ack(ctx context.Context) error {
	return m.msg.DoubleAck(ctx)
}
func (m *message) Nak(_ context.Context, delay time.Duration) error { return m.msg.NakWithDelay(delay) }
func (m *message) Term(_ context.Context, reason string) error      { return m.msg.TermWithReason(reason) }

func validSubject(subject string) bool {
	if subject == "" || len(subject) > 256 || strings.ContainsAny(subject, " \t\r\n") {
		return false
	}
	for _, token := range strings.Split(subject, ".") {
		if token == "" {
			return false
		}
	}
	return true
}

// subjectMatches reports whether a NATS filter (with * and > wildcards)
// matches a concrete subject.
func subjectMatches(filter, subject string) bool {
	f, s := strings.Split(filter, "."), strings.Split(subject, ".")
	for index, token := range f {
		if token == ">" {
			return len(s) > index
		}
		if index >= len(s) || (token != "*" && token != s[index]) {
			return false
		}
	}
	return len(f) == len(s)
}
